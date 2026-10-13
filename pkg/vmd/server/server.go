// Package server implements the privileged aries-vmd daemon: Kata
// Containers on Firecracker under a dedicated containerd, with per-task
// host networks. ARIES reaches it only through pkg/vmd/api.
package server

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config holds daemon policy. Limits bound every request; clients cannot
// select runtimes, mounts, devices, or privileges.
type Config struct {
	StateDir        string
	Runtime         string
	Snapshotter     string
	HelperPath      string
	SubnetPool      netip.Prefix
	Nameservers     []string
	StorageLimitMB  int
	MaxVCPUs        int
	MaxMemoryMB     int
	DefaultVCPUs    int
	DefaultMemoryMB int
	KataShimPath    string
	FirecrackerPath string
	// CleanupTimeout bounds cancellation and deletion work.
	CleanupTimeout time.Duration
	Logger         *logrus.Logger
}

// Server implements api.Server.
type Server struct {
	cfg    Config
	engine engine
	net    hostNetwork
	store  *store
	logger *logrus.Logger

	// global serializes network allocation, leases, and firewall regeneration.
	global  sync.Mutex
	locksMu sync.Mutex
	locks   map[string]*sync.Mutex

	logsMu sync.Mutex
	logs   map[string][]*os.File

	// processesReferencing lists host processes (Kata shim, Firecracker,
	// jailer) whose command line references id.
	processesReferencing func(id string) ([]int, error)
	// version runs a binary's version command.
	version func(ctx context.Context, path string, args ...string) string
	kvm     func() error
	// chown makes an injected path root-owned.
	chown func(root *os.Root, name string) error
}

var _ api.Server = (*Server)(nil)

// New constructs a server over a containerd engine and host network.
func New(cfg Config, engine engine, network hostNetwork) (*Server, error) {
	if cfg.StateDir == "" || cfg.Runtime == "" || cfg.Snapshotter == "" || cfg.HelperPath == "" {
		return nil, errors.New("state dir, runtime, snapshotter, and helper are required")
	}
	if !cfg.SubnetPool.IsValid() || !cfg.SubnetPool.Addr().Is4() || cfg.SubnetPool.Bits() > 24 {
		return nil, errors.New("subnet pool must be an IPv4 prefix of /24 or larger")
	}
	if cfg.DefaultVCPUs < 1 || cfg.MaxVCPUs < cfg.DefaultVCPUs || cfg.DefaultMemoryMB < minMemoryMB || cfg.MaxMemoryMB < cfg.DefaultMemoryMB || cfg.StorageLimitMB < 1 {
		return nil, errors.New("invalid sandbox size limits")
	}
	if cfg.CleanupTimeout <= 0 {
		cfg.CleanupTimeout = 60 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = logrus.StandardLogger()
	}
	st, err := newStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	return &Server{
		cfg: cfg, engine: engine, net: network, store: st, logger: cfg.Logger,
		locks: map[string]*sync.Mutex{}, logs: map[string][]*os.File{},
		processesReferencing: scanProcesses,
		version:              binaryVersion,
		kvm:                  checkKVM,
		chown:                func(root *os.Root, name string) error { return root.Chown(name, 0, 0) },
	}, nil
}

const minMemoryMB = 256

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,62}$`)

func validateName(kind, name string) error {
	if !namePattern.MatchString(name) {
		return status.Errorf(codes.InvalidArgument, "%s name %q must match %s", kind, name, namePattern)
	}
	return nil
}

func validateLabels(labels map[string]string) error {
	if labels[api.LabelRun] == "" || labels[api.LabelTask] == "" {
		return status.Errorf(codes.InvalidArgument, "labels %s and %s are required", api.LabelRun, api.LabelTask)
	}
	for key, value := range labels {
		if key == "" || strings.ContainsAny(key+value, "\x00") || strings.HasPrefix(key, "aries-vmd.") {
			return status.Errorf(codes.InvalidArgument, "label %q is invalid or reserved", key)
		}
	}
	return nil
}

func ownedLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels)+1)
	for key, value := range labels {
		result[key] = value
	}
	result[api.LabelManaged] = "true"
	return result
}

func labelsMatch(have, want map[string]string) bool {
	for key, value := range want {
		if have[key] != value {
			return false
		}
	}
	return true
}

// lock serializes lifecycle operations on one resource name.
func (s *Server) lock(id string) func() {
	s.locksMu.Lock()
	mu, ok := s.locks[id]
	if !ok {
		mu = &sync.Mutex{}
		s.locks[id] = mu
	}
	s.locksMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

func internal(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

// cleanupContext returns a bounded context that survives caller cancellation.
func (s *Server) cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), s.cfg.CleanupTimeout)
}

func (s *Server) Health(ctx context.Context, _ *api.HealthRequest) (*api.HealthReply, error) {
	reply := &api.HealthReply{
		APIVersion: api.Version, Isolation: api.IsolationKataFirecracker, Snapshotter: s.cfg.Snapshotter,
		StorageLimitMB: s.cfg.StorageLimitMB, MaxVCPUs: s.cfg.MaxVCPUs, MaxMemoryMB: s.cfg.MaxMemoryMB,
		DefaultVCPUs: s.cfg.DefaultVCPUs, DefaultMemoryMB: s.cfg.DefaultMemoryMB,
	}
	if err := s.kvm(); err != nil {
		reply.KVMError = err.Error()
	} else {
		reply.KVM = true
	}
	version, err := s.engine.Version(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "containerd: %v", err)
	}
	reply.ContainerdVersion = version
	if s.cfg.KataShimPath != "" {
		reply.KataVersion = s.version(ctx, s.cfg.KataShimPath, "--version")
	}
	if s.cfg.FirecrackerPath != "" {
		reply.FirecrackerVersion = s.version(ctx, s.cfg.FirecrackerPath, "--version")
	}
	reply.HelperVersion = s.version(ctx, s.cfg.HelperPath, "version")
	return reply, nil
}

func checkKVM() error {
	file, err := os.OpenFile("/dev/kvm", os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return file.Close()
}

func binaryVersion(ctx context.Context, path string, args ...string) string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, path, args...).CombinedOutput()
	if err != nil {
		return ""
	}
	line, _, _ := strings.Cut(strings.TrimSpace(string(output)), "\n")
	return strings.TrimSpace(line)
}

// shimProcessNames are the host processes a Kata/Firecracker sandbox leaves
// behind if teardown is incomplete.
var shimProcessNames = []string{"containerd-shim-kata-v2", "containerd-shim-kata-fc-v2", "firecracker", "jailer"}

func scanProcesses(id string) ([]int, error) {
	return scanProcessesIn("/proc", id)
}

func scanProcessesIn(procRoot, id string) ([]int, error) {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, entry := range entries {
		var pid int
		if _, err := fmt.Sscanf(entry.Name(), "%d", &pid); err != nil || fmt.Sprint(pid) != entry.Name() {
			continue
		}
		content, err := os.ReadFile(filepath.Join(procRoot, entry.Name(), "cmdline"))
		if err != nil || len(content) == 0 {
			continue
		}
		args := strings.Split(strings.TrimRight(string(content), "\x00"), "\x00")
		base := filepath.Base(args[0])
		known := false
		for _, name := range shimProcessNames {
			if base == name {
				known = true
			}
		}
		if !known {
			continue
		}
		for _, arg := range args[1:] {
			if arg == id || strings.Contains(arg, id) {
				pids = append(pids, pid)
				break
			}
		}
	}
	return pids, nil
}

// Reconcile regenerates the firewall from persisted networks and reports
// incomplete allocations. It never deletes resources on its own.
func (s *Server) Reconcile(ctx context.Context) error {
	s.global.Lock()
	defer s.global.Unlock()
	networks, err := s.store.networks()
	if err != nil {
		return err
	}
	if err := s.net.EnsureForwarding(ctx); err != nil {
		return err
	}
	for _, network := range networks {
		if network.Phase != phaseReady {
			s.logger.WithFields(logrus.Fields{"network": network.Spec.Name, "phase": network.Phase}).Warn("task network allocation is incomplete; delete it explicitly")
			continue
		}
		if err := s.net.CreateBridge(ctx, network.Bridge, network.Gateway+"/"+prefixBits(network.Subnet)); err != nil {
			return fmt.Errorf("restore bridge for network %s: %w", network.Spec.Name, err)
		}
		if network.Spec.AllowEgress {
			if err := s.net.SetDockerUserAccept(ctx, network.Bridge, true); err != nil {
				return err
			}
		}
	}
	if err := s.net.ApplyFirewall(ctx, renderFirewall(networks)); err != nil {
		return err
	}
	sandboxes, err := s.store.sandboxes()
	if err != nil {
		return err
	}
	for _, sandbox := range sandboxes {
		if sandbox.Phase != phaseReady {
			s.logger.WithFields(logrus.Fields{"sandbox": sandbox.Spec.Name, "phase": sandbox.Phase, "run": sandbox.Spec.Labels[api.LabelRun]}).Warn("sandbox allocation is incomplete; delete it explicitly")
		} else {
			s.logger.WithFields(logrus.Fields{"sandbox": sandbox.Spec.Name, "run": sandbox.Spec.Labels[api.LabelRun]}).Info("sandbox recorded from a previous daemon process; output logging is not reattached")
		}
	}
	return nil
}

func prefixBits(cidr string) string {
	_, bits, _ := strings.Cut(cidr, "/")
	return bits
}

func (s *Server) List(ctx context.Context, request *api.ListRequest) (*api.ListReply, error) {
	reply := &api.ListReply{Sandboxes: []api.SandboxInfo{}, Networks: []api.NetworkInfo{}}
	sandboxes, err := s.store.sandboxes()
	if err != nil {
		return nil, internal(err)
	}
	for _, record := range sandboxes {
		if labelsMatch(ownedLabels(record.Spec.Labels), request.Labels) {
			reply.Sandboxes = append(reply.Sandboxes, s.sandboxInfo(ctx, record))
		}
	}
	networks, err := s.store.networks()
	if err != nil {
		return nil, internal(err)
	}
	for _, record := range networks {
		if labelsMatch(ownedLabels(record.Spec.Labels), request.Labels) {
			info, err := s.networkInfo(record)
			if err != nil {
				return nil, internal(err)
			}
			reply.Networks = append(reply.Networks, info)
		}
	}
	return reply, nil
}

func (s *Server) Cleanup(ctx context.Context, request *api.CleanupRequest) (*api.CleanupReply, error) {
	if strings.TrimSpace(request.Run) == "" {
		return nil, status.Error(codes.InvalidArgument, "run is required")
	}
	listed, err := s.List(ctx, &api.ListRequest{Labels: map[string]string{api.LabelRun: request.Run}})
	if err != nil {
		return nil, err
	}
	reply := &api.CleanupReply{Sandboxes: []string{}, Networks: []string{}}
	var errs []error
	for _, sandbox := range listed.Sandboxes {
		if _, err := s.Delete(ctx, &api.SandboxRef{ID: sandbox.ID}); err != nil {
			errs = append(errs, fmt.Errorf("sandbox %s: %w", sandbox.ID, err))
			continue
		}
		reply.Sandboxes = append(reply.Sandboxes, sandbox.ID)
	}
	for _, network := range listed.Networks {
		if _, err := s.DeleteNetwork(ctx, &api.NetworkRef{ID: network.ID}); err != nil {
			errs = append(errs, fmt.Errorf("network %s: %w", network.ID, err))
			continue
		}
		reply.Networks = append(reply.Networks, network.ID)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, status.Errorf(codes.Internal, "cleanup of run %s incomplete (removed sandboxes %v, networks %v): %v", request.Run, reply.Sandboxes, reply.Networks, err)
	}
	return reply, nil
}
