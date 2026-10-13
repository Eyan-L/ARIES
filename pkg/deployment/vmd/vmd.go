// Package vmd implements tool-sandbox deployment through the privileged
// aries-vmd host daemon, which runs each runtime as a Kata Containers
// Firecracker microVM. ARIES stays unprivileged and speaks only the narrow
// pkg/vmd/api contract.
package vmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/deployment"
	"github.com/hyscale-lab/aries/pkg/runner"
	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	defaultSocket         = "/run/aries-vmd.sock"
	defaultCleanupTimeout = 30 * time.Second
	chunkSize             = 1 << 20
	maxMessageSize        = 8 << 20
)

// Options configures a client transport to aries-vmd.
type Options struct {
	Socket string
	// Dialer replaces the Unix socket dialer; tests use it for in-memory servers.
	Dialer         func(context.Context, string) (net.Conn, error)
	CleanupTimeout time.Duration
	Logger         *logrus.Logger
	// OnClose runs after the daemon connection closes, for resources the
	// composition attached to this transport's lifetime.
	OnClose func() error
}

// Manager implements deployment operations against aries-vmd.
type Manager struct {
	conn      *grpc.ClientConn
	api       *api.Client
	logger    *logrus.Logger
	timeout   time.Duration
	onClose   func() error
	closeOnce sync.Once
	closeErr  error
}

var _ deployment.Deployment = (*Manager)(nil)

// New configures a transport without contacting the daemon.
func New(options Options) (*Manager, error) {
	socket := options.Socket
	if socket == "" {
		socket = defaultSocket
	}
	dialer := options.Dialer
	if dialer == nil {
		dialer = func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		}
	}
	conn, err := grpc.NewClient("passthrough:///aries-vmd",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(append(api.CallOptions(), grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize))...),
	)
	if err != nil {
		return nil, fmt.Errorf("create vmd deployment client: %w", err)
	}
	logger := options.Logger
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	return &Manager{conn: conn, api: api.NewClient(conn), logger: logger, timeout: options.CleanupTimeout, onClose: options.OnClose}, nil
}

// API exposes the typed daemon client to composition code in this package's consumers.
func (manager *Manager) API() *api.Client { return manager.api }

func (manager *Manager) Close() error {
	if manager == nil {
		return nil
	}
	manager.closeOnce.Do(func() {
		manager.closeErr = manager.conn.Close()
		if manager.onClose != nil {
			manager.closeErr = errors.Join(manager.closeErr, manager.onClose())
		}
	})
	return manager.closeErr
}

func (manager *Manager) cleanupTimeout() time.Duration {
	if manager.timeout > 0 {
		return manager.timeout
	}
	return defaultCleanupTimeout
}

// Preflight confirms that the daemon speaks this contract, provides the
// configured isolation, and can reach KVM.
func (manager *Manager) Preflight(ctx context.Context, isolation, kataVersion string) (*api.HealthReply, error) {
	health, err := manager.api.Health(ctx, &api.HealthRequest{})
	if err != nil {
		return nil, fmt.Errorf("contact aries-vmd: %w", err)
	}
	if health.APIVersion != api.Version {
		return nil, fmt.Errorf("aries-vmd API version %d differs from required version %d", health.APIVersion, api.Version)
	}
	if health.Isolation != isolation {
		return nil, fmt.Errorf("aries-vmd isolation %q differs from configured %q", health.Isolation, isolation)
	}
	if !health.KVM {
		return nil, fmt.Errorf("aries-vmd cannot use KVM: %s", health.KVMError)
	}
	if !reportsVersion(health.KataVersion, kataVersion) {
		return nil, fmt.Errorf("aries-vmd Kata version %q differs from pinned %q", health.KataVersion, kataVersion)
	}
	return health, nil
}

// reportsVersion reports whether a `--version` line names exactly the pinned
// dotted release, so 4.2.0 matches neither 4.2.01 nor 14.2.0.
func reportsVersion(line, version string) bool {
	if version == "" {
		return false
	}
	pattern := `(^|[^0-9.])` + regexp.QuoteMeta(version) + `($|[^0-9.]|\.($|[^0-9]))`
	return regexp.MustCompile(pattern).MatchString(line)
}

func isNotFound(err error) bool { return status.Code(err) == codes.NotFound }

func sandboxSpec(request deployment.Request) (api.SandboxSpec, error) {
	if strings.TrimSpace(request.Placement.VMNetwork) == "" {
		return api.SandboxSpec{}, errors.New("vmd deployment requires a nonempty VM task attachment")
	}
	if request.Name == "" {
		return api.SandboxSpec{}, errors.New("vmd deployment requires a runtime name")
	}
	if request.GPUs < 0 || request.StorageMB < 0 {
		return api.SandboxSpec{}, errors.New("deployment GPU and storage bounds must be nonnegative")
	}
	if request.GPUs > 0 {
		return api.SandboxSpec{}, errors.New("vmd deployment does not support GPUs")
	}
	if request.ServicePort != 0 {
		return api.SandboxSpec{}, errors.New("vmd deployment does not publish services")
	}
	spec := api.SandboxSpec{
		Name: request.Name, Image: request.Image, Network: request.Placement.VMNetwork,
		Workdir: request.Workdir, Env: request.Env, Labels: request.Labels,
		Entrypoint: request.Entrypoint, Args: request.Args, StorageMB: request.StorageMB,
		NoNewPrivileges: request.NoNewPrivileges, Aliases: request.NetworkAliases,
	}
	if request.CPU != nil {
		cpu := *request.CPU
		if cpu <= 0 || math.IsNaN(cpu) || math.IsInf(cpu, 0) || cpu > math.MaxInt32 {
			return api.SandboxSpec{}, errors.New("deployment CPU must be finite and positive")
		}
		spec.VCPUs = int(math.Ceil(cpu))
	}
	if request.MemoryMB != nil {
		if *request.MemoryMB <= 0 {
			return api.SandboxSpec{}, errors.New("deployment memory must be positive")
		}
		spec.MemoryMB = *request.MemoryMB
	}
	return spec, nil
}

// Create allocates a stopped microVM sandbox. The runtime name is its identity,
// so it is returned whenever the daemon may have allocated anything.
func (manager *Manager) Create(ctx context.Context, request deployment.Request) (string, error) {
	spec, err := sandboxSpec(request)
	if err != nil {
		return "", err
	}
	entry := manager.logger.WithContext(ctx).WithField("sandbox", spec.Name)
	if request.CPU != nil && float64(spec.VCPUs) != *request.CPU {
		entry.WithFields(logrus.Fields{"requested_cpu": *request.CPU, "vcpus": spec.VCPUs}).Info("vmd rounds fractional CPU up to whole vCPUs")
	}
	if request.MemoryMB == nil {
		entry.Info("vmd sizes unbounded memory requests with the daemon default")
	}
	info, err := manager.api.Create(ctx, &spec)
	if err != nil {
		return spec.Name, fmt.Errorf("create vmd sandbox: %w", err)
	}
	if info.ID != spec.Name {
		return spec.Name, fmt.Errorf("vmd returned sandbox identity %q for requested %q", info.ID, spec.Name)
	}
	return spec.Name, nil
}

func (manager *Manager) Validate(ctx context.Context, id string, request deployment.Request, secrets [][]byte) error {
	if strings.TrimSpace(request.Placement.VMNetwork) == "" {
		return errors.New("vmd deployment requires a nonempty VM task attachment")
	}
	info, err := manager.api.Inspect(ctx, &api.SandboxRef{ID: id})
	if err != nil {
		return fmt.Errorf("inspect vmd sandbox: %w", err)
	}
	spec := info.Spec
	if info.ID != id || (request.Name != "" && spec.Name != request.Name) {
		return errors.New("deployment name differs from request")
	}
	if request.Workdir != "" && spec.Workdir != request.Workdir {
		return errors.New("deployment workdir differs from request")
	}
	if spec.Image != request.Image || !slices.Equal(spec.Args, request.Args) || request.Entrypoint != nil && !slices.Equal(spec.Entrypoint, request.Entrypoint) {
		return errors.New("deployment image or command differs from requested configuration")
	}
	for key, value := range request.Labels {
		if spec.Labels[key] != value {
			return errors.New("deployment labels do not match the task")
		}
	}
	if spec.Network != request.Placement.VMNetwork {
		return errors.New("deployment must use only the task network")
	}
	if request.NoNewPrivileges && !spec.NoNewPrivileges {
		return errors.New("deployment no-new-privileges is not enabled")
	}
	if request.Init && !info.Init {
		return errors.New("deployment init is not enabled")
	}
	if info.Isolation != api.IsolationKataFirecracker {
		return fmt.Errorf("deployment isolation %q is not %s", info.Isolation, api.IsolationKataFirecracker)
	}
	if len(info.Mounts) != 0 {
		return errors.New("deployment must not request host mounts")
	}
	metadata := append(append(append([]string{}, spec.Env...), spec.Args...), spec.Entrypoint...)
	for _, value := range spec.Labels {
		metadata = append(metadata, value)
	}
	for _, value := range metadata {
		for _, secret := range secrets {
			if len(secret) > 0 && bytes.Contains([]byte(value), secret) {
				return errors.New("deployment secret entered vmd metadata")
			}
		}
	}
	return nil
}

func (manager *Manager) Start(ctx context.Context, id string) error {
	if _, err := manager.api.Start(ctx, &api.SandboxRef{ID: id}); err != nil {
		return fmt.Errorf("start vmd sandbox: %w", err)
	}
	return nil
}

func (manager *Manager) Running(ctx context.Context, id string) (bool, error) {
	info, err := manager.api.Inspect(ctx, &api.SandboxRef{ID: id})
	if err != nil {
		return false, fmt.Errorf("inspect vmd sandbox: %w", err)
	}
	return info.Status == api.StatusRunning, nil
}

func (manager *Manager) Address(context.Context, string, int) (string, error) {
	return "", errors.New("vmd deployment does not publish services")
}

// Stop removes the sandbox and succeeds only after the daemon reports it absent.
func (manager *Manager) Stop(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	_, deleteErr := manager.api.Delete(ctx, &api.SandboxRef{ID: id})
	if deleteErr != nil && !isNotFound(deleteErr) {
		deleteErr = fmt.Errorf("remove vmd sandbox: %w", deleteErr)
	} else {
		deleteErr = nil
	}
	_, inspectErr := manager.api.Inspect(ctx, &api.SandboxRef{ID: id})
	if isNotFound(inspectErr) {
		return deleteErr
	}
	if inspectErr == nil {
		return errors.Join(deleteErr, errors.New("deployment remains after removal"))
	}
	return errors.Join(deleteErr, fmt.Errorf("verify deployment removal: %w", inspectErr))
}

func (manager *Manager) Logs(ctx context.Context, id string, limit int) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("deployment log bound must be positive")
	}
	var stdout, stderr limitedBuffer
	stdout.limit, stderr.limit = limit, limit
	copyErr := manager.LogsStream(ctx, id, &stdout, &stderr)
	var boundErr error
	if stdout.exceeded || stderr.exceeded {
		boundErr = errors.New("deployment logs exceeded their bound")
	}
	if err := errors.Join(copyErr, boundErr); err != nil {
		return nil, err
	}
	return append(stdout.Bytes(), stderr.Bytes()...), nil
}

func (manager *Manager) LogsStream(ctx context.Context, id string, stdout, stderr io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := manager.api.Logs(streamCtx, &api.LogsRequest{Sandbox: id})
	if err != nil {
		return logsError(err)
	}
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return logsError(err)
		}
		if _, err := stdout.Write(chunk.Stdout); err != nil {
			return err
		}
		if _, err := stderr.Write(chunk.Stderr); err != nil {
			return err
		}
	}
}

func logsError(err error) error {
	if isNotFound(err) {
		return fmt.Errorf("deployment logs: %w: %w", runner.ErrNotFound, err)
	}
	return fmt.Errorf("deployment logs: %w", err)
}

type limitedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (buffer *limitedBuffer) Write(content []byte) (int, error) {
	consumed := len(content)
	remaining := buffer.limit - buffer.Len()
	if len(content) > remaining {
		content = content[:max(0, remaining)]
		buffer.exceeded = true
	}
	_, err := buffer.Buffer.Write(content)
	return consumed, err
}

// Placement-level helpers shared with the task environment.
func validateSearchPort(request core.SandboxRequest) (int, error) {
	port := request.Environment.Services.SearchPort
	if port < 0 || port > 65535 {
		return 0, errors.New("invalid task search service port")
	}
	return port, nil
}
