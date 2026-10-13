package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	annotationVCPUs  = "io.katacontainers.config.hypervisor.default_vcpus"
	annotationMemory = "io.katacontainers.config.hypervisor.default_memory"
	labelNetwork     = "aries-vmd.network"
	labelNetNS       = "aries-vmd.netns"
)

func (s *Server) ImportImage(stream grpc.ClientStreamingServer[api.ImageChunk, api.ImageInfo]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	reference := first.Reference
	if reference == "" || strings.ContainsAny(reference, " \t\r\n\x00") {
		return status.Error(codes.InvalidArgument, "image reference is required")
	}
	reader, writer := io.Pipe()
	go func() {
		if _, err := writer.Write(first.Data); err != nil {
			return
		}
		for {
			chunk, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				writer.Close()
				return
			}
			if err != nil {
				writer.CloseWithError(err)
				return
			}
			if _, err := writer.Write(chunk.Data); err != nil {
				return
			}
		}
	}()
	importErr := s.engine.ImportImage(stream.Context(), reference, reader)
	reader.CloseWithError(errors.New("import finished"))
	if importErr != nil {
		return status.Errorf(codes.Internal, "import image %s: %v", reference, importErr)
	}
	config, err := s.engine.InspectImage(stream.Context(), reference)
	if err != nil {
		return status.Errorf(codes.Internal, "inspect imported image %s: %v", reference, err)
	}
	s.logger.WithFields(map[string]any{"image": reference, "config_digest": config.ConfigDigest}).Info("image imported")
	return stream.SendAndClose(&api.ImageInfo{Present: config.Unpacked, Reference: reference, ConfigDigest: config.ConfigDigest, TargetDigest: config.TargetDigest})
}

func (s *Server) InspectImage(ctx context.Context, request *api.ImageRequest) (*api.ImageInfo, error) {
	config, err := s.engine.InspectImage(ctx, request.Reference)
	if errors.Is(err, errEngineNotFound) {
		return &api.ImageInfo{Reference: request.Reference}, nil
	}
	if err != nil {
		return nil, internal(err)
	}
	return &api.ImageInfo{Present: config.Unpacked, Reference: request.Reference, ConfigDigest: config.ConfigDigest, TargetDigest: config.TargetDigest}, nil
}

func validPath(value string) bool {
	return value != "" && path.IsAbs(value) && path.Clean(value) == value && !strings.ContainsAny(value, "\x00\n")
}

func (s *Server) validateSandboxSpec(spec *api.SandboxSpec) error {
	if err := validateName("sandbox", spec.Name); err != nil {
		return err
	}
	if err := validateName("network", spec.Network); err != nil {
		return err
	}
	if err := validateLabels(spec.Labels); err != nil {
		return err
	}
	if spec.Image == "" || strings.ContainsAny(spec.Image, " \t\r\n\x00") {
		return status.Error(codes.InvalidArgument, "image is required")
	}
	if spec.Workdir != "" && !validPath(spec.Workdir) {
		return status.Errorf(codes.InvalidArgument, "workdir %q must be a clean absolute path", spec.Workdir)
	}
	for _, entry := range spec.Env {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" || strings.ContainsAny(entry, "\x00") {
			return status.Errorf(codes.InvalidArgument, "environment entry %q is invalid", entry)
		}
	}
	for _, arg := range slices.Concat(spec.Entrypoint, spec.Args) {
		if strings.Contains(arg, "\x00") {
			return status.Error(codes.InvalidArgument, "arguments must not contain NUL")
		}
	}
	if spec.VCPUs < 0 || spec.VCPUs > s.cfg.MaxVCPUs {
		return status.Errorf(codes.InvalidArgument, "vcpus must be between 1 and %d", s.cfg.MaxVCPUs)
	}
	if spec.MemoryMB != 0 && (spec.MemoryMB < minMemoryMB || spec.MemoryMB > s.cfg.MaxMemoryMB) {
		return status.Errorf(codes.InvalidArgument, "memory must be between %d and %d MiB", minMemoryMB, s.cfg.MaxMemoryMB)
	}
	if spec.StorageMB < 0 || spec.StorageMB > s.cfg.StorageLimitMB {
		return status.Errorf(codes.InvalidArgument, "storage %d MiB exceeds the per-sandbox root filesystem size %d MiB", spec.StorageMB, s.cfg.StorageLimitMB)
	}
	for _, alias := range spec.Aliases {
		if !namePattern.MatchString(alias) {
			return status.Errorf(codes.InvalidArgument, "alias %q is invalid", alias)
		}
	}
	return nil
}

func specsEqual(a, b api.SandboxSpec) bool {
	return a.Name == b.Name && a.Image == b.Image && a.Network == b.Network && a.Workdir == b.Workdir &&
		slices.Equal(a.Env, b.Env) && maps.Equal(a.Labels, b.Labels) && slices.Equal(a.Entrypoint, b.Entrypoint) &&
		(a.Entrypoint == nil) == (b.Entrypoint == nil) && slices.Equal(a.Args, b.Args) && a.VCPUs == b.VCPUs &&
		a.MemoryMB == b.MemoryMB && a.StorageMB == b.StorageMB && a.NoNewPrivileges == b.NoNewPrivileges &&
		slices.Equal(a.Aliases, b.Aliases)
}

// commandLine applies Docker's entrypoint/cmd rules: an explicit entrypoint
// replaces the image's and resets its cmd.
func commandLine(spec api.SandboxSpec, image imageConfig) []string {
	if spec.Entrypoint != nil {
		return slices.Concat(spec.Entrypoint, spec.Args)
	}
	if spec.Args != nil {
		return slices.Concat(image.Entrypoint, spec.Args)
	}
	return slices.Concat(image.Entrypoint, image.Cmd)
}

func mergeEnv(base, overrides []string) []string {
	result := slices.Clone(base)
	for _, entry := range overrides {
		key, _, _ := strings.Cut(entry, "=")
		index := slices.IndexFunc(result, func(existing string) bool { return strings.HasPrefix(existing, key+"=") })
		if index >= 0 {
			result[index] = entry
		} else {
			result = append(result, entry)
		}
	}
	return result
}

func (s *Server) Create(ctx context.Context, spec *api.SandboxSpec) (*api.SandboxInfo, error) {
	if err := s.validateSandboxSpec(spec); err != nil {
		return nil, err
	}
	unlock := s.lock(spec.Name)
	defer unlock()
	existing, found, err := s.store.sandbox(spec.Name)
	if err != nil {
		return nil, internal(err)
	}
	if found {
		if existing.Phase == phaseReady && specsEqual(existing.Spec, *spec) {
			info := s.sandboxInfo(ctx, existing)
			return &info, nil
		}
		return nil, status.Errorf(codes.AlreadyExists, "sandbox %s already exists (phase %s)", spec.Name, existing.Phase)
	}
	if _, err := s.engine.InspectContainer(ctx, spec.Name); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "container %s exists without a daemon record", spec.Name)
	} else if !errors.Is(err, errEngineNotFound) {
		return nil, internal(err)
	}
	image, err := s.engine.InspectImage(ctx, spec.Image)
	if errors.Is(err, errEngineNotFound) || err == nil && !image.Unpacked {
		return nil, status.Errorf(codes.FailedPrecondition, "image %s is not imported", spec.Image)
	}
	if err != nil {
		return nil, internal(err)
	}
	argv := commandLine(*spec, image)
	if len(argv) == 0 {
		return nil, status.Error(codes.InvalidArgument, "sandbox has no command")
	}
	record := sandboxRecord{
		Spec: *spec, Phase: phaseAllocating, ConfigDigest: image.ConfigDigest,
		VCPUs: spec.VCPUs, MemoryMB: spec.MemoryMB, ImageUser: image.User,
		NetNS: "aries-" + shortHash("sandbox/"+spec.Name, 12), HostVeth: "avh" + shortHash("sandbox/"+spec.Name, 8),
		PeerVeth: "avp" + shortHash("sandbox/"+spec.Name, 8),
		Argv:     argv, Env: mergeEnv(image.Env, spec.Env), Cwd: spec.Workdir,
	}
	record.Spec.Labels = maps.Clone(spec.Labels)
	if record.VCPUs == 0 {
		record.VCPUs = s.cfg.DefaultVCPUs
	}
	if record.MemoryMB == 0 {
		record.MemoryMB = s.cfg.DefaultMemoryMB
	}
	if record.Cwd == "" {
		record.Cwd = image.WorkingDir
	}
	if record.Cwd == "" {
		record.Cwd = "/"
	}

	// Lease an address and record the allocation intent before any side effect.
	s.global.Lock()
	network, found, err := s.store.network(spec.Network)
	if err == nil && (!found || network.Phase != phaseReady) {
		err = status.Errorf(codes.FailedPrecondition, "network %s is not ready", spec.Network)
	}
	var address string
	if err == nil {
		var leased netip.Addr
		leased, err = allocateAddress(network)
		if err == nil {
			address = leased.String()
			record.IP, record.Gateway = address, network.Gateway
			err = s.store.putSandbox(record)
		}
		if err == nil {
			if network.Leases == nil {
				network.Leases = map[string]string{}
			}
			network.Leases[spec.Name] = address
			err = s.store.putNetwork(network)
		}
	}
	s.global.Unlock()
	if err != nil {
		return nil, internal(err)
	}

	if err := s.allocate(ctx, &record, network); err != nil {
		return nil, internal(fmt.Errorf("create sandbox %s (call Delete to release partial allocation): %w", spec.Name, err))
	}
	record.Phase = phaseReady
	if err := s.store.putSandbox(record); err != nil {
		return nil, internal(err)
	}
	s.logger.WithFields(map[string]any{"sandbox": spec.Name, "network": spec.Network, "ip": record.IP, "vcpus": record.VCPUs, "memory_mb": record.MemoryMB}).Info("sandbox created")
	info := s.sandboxInfo(ctx, record)
	return &info, nil
}

// allocate performs Create's side effects, persisting progress as it goes.
func (s *Server) allocate(ctx context.Context, record *sandboxRecord, network networkRecord) error {
	if err := s.net.CreateNetNS(ctx, netnsAttachment{
		NetNS: record.NetNS, HostVeth: record.HostVeth, PeerVeth: record.PeerVeth, Bridge: network.Bridge,
		AddressCIDR: record.IP + "/24", Gateway: record.Gateway,
	}); err != nil {
		return fmt.Errorf("create network namespace: %w", err)
	}
	hostname := record.Spec.Name
	if len(hostname) > 63 {
		hostname = hostname[:63]
	}
	labels := ownedLabels(record.Spec.Labels)
	labels[labelNetwork] = record.Spec.Network
	labels[labelNetNS] = record.NetNS
	if err := s.engine.CreateContainer(ctx, containerSpec{
		ID: record.Spec.Name, Image: record.Spec.Image, Labels: labels,
		Args: slices.Concat([]string{api.HelperPath, "init", "--"}, record.Argv),
		Env:  record.Env, Cwd: record.Cwd, Hostname: hostname,
		NetNSPath: filepath.Join("/var/run/netns", record.NetNS), NoNewPrivileges: record.Spec.NoNewPrivileges,
		Annotations: map[string]string{annotationVCPUs: strconv.Itoa(record.VCPUs), annotationMemory: strconv.Itoa(record.MemoryMB)},
	}); err != nil {
		return fmt.Errorf("create container: %w", err)
	}
	if err := s.engine.PrepareRootfs(ctx, record.Spec.Name, func(root *os.Root) error {
		return s.inject(root, record, network.Spec.AllowEgress, hostname)
	}); err != nil {
		return fmt.Errorf("prepare root filesystem: %w", err)
	}
	record.Injected = true
	user, err := resolveUser(record.ImageUser, record.Passwd, record.Group)
	if err != nil {
		return fmt.Errorf("resolve image user: %w", err)
	}
	record.User = user
	if err := s.store.putSandbox(*record); err != nil {
		return err
	}
	return s.engine.SetProcessUser(ctx, record.Spec.Name, user)
}

// inject installs the helper and task-local files into the unbooted root
// filesystem. os.Root confines every path, including symlinks in the image.
func (s *Server) inject(root *os.Root, record *sandboxRecord, egress bool, hostname string) error {
	helper, err := os.ReadFile(s.cfg.HelperPath)
	if err != nil {
		return fmt.Errorf("read helper: %w", err)
	}
	if err := root.MkdirAll(".aries", 0o755); err != nil {
		return err
	}
	if err := root.WriteFile(strings.TrimPrefix(api.HelperPath, "/"), helper, 0o755); err != nil {
		return err
	}
	if err := s.chown(root, strings.TrimPrefix(api.HelperPath, "/")); err != nil {
		return err
	}
	if err := root.Chmod(strings.TrimPrefix(api.HelperPath, "/"), 0o755); err != nil {
		return err
	}
	state := strings.TrimPrefix(api.StateDir, "/")
	if err := root.MkdirAll(state, 0o755); err != nil {
		return err
	}
	if err := s.chown(root, state); err != nil {
		return err
	}
	if err := root.Chmod(state, 0o733|fs.ModeSticky); err != nil {
		return err
	}
	if err := root.MkdirAll("etc", 0o755); err != nil {
		return err
	}
	hosts := "127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n" + record.IP + "\t" + strings.Join(append(slices.Clone(record.Spec.Aliases), hostname), " ") + "\n"
	if err := replaceFile(root, "etc/hosts", []byte(hosts)); err != nil {
		return err
	}
	if err := replaceFile(root, "etc/hostname", []byte(hostname+"\n")); err != nil {
		return err
	}
	if egress && len(s.cfg.Nameservers) > 0 {
		if err := replaceFile(root, "etc/resolv.conf", []byte(renderResolvConf(s.cfg.Nameservers))); err != nil {
			return err
		}
	}
	workdir := strings.TrimPrefix(record.Cwd, "/")
	if workdir != "" {
		if err := root.MkdirAll(workdir, 0o755); err != nil {
			return fmt.Errorf("create workdir: %w", err)
		}
	}
	record.Passwd = readOptional(root, "etc/passwd")
	record.Group = readOptional(root, "etc/group")
	return nil
}

// replaceFile writes a regular file, replacing a symlink or existing file.
func replaceFile(root *os.Root, name string, content []byte) error {
	if info, err := root.Lstat(name); err == nil && !info.Mode().IsRegular() {
		if err := root.Remove(name); err != nil {
			return err
		}
	}
	if err := root.WriteFile(name, content, 0o644); err != nil {
		return err
	}
	return root.Chmod(name, 0o644)
}

func readOptional(root *os.Root, name string) string {
	content, err := root.ReadFile(name)
	if err != nil || len(content) > 4<<20 {
		return ""
	}
	return string(content)
}

func (s *Server) record(id string) (sandboxRecord, error) {
	record, found, err := s.store.sandbox(id)
	if err != nil {
		return record, internal(err)
	}
	if !found {
		return record, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	return record, nil
}

func (s *Server) sandboxInfo(ctx context.Context, record sandboxRecord) api.SandboxInfo {
	info := api.SandboxInfo{
		Spec: record.Spec, ID: record.Spec.Name, ConfigDigest: record.ConfigDigest,
		VCPUs: record.VCPUs, MemoryMB: record.MemoryMB, IP: record.IP, Status: api.StatusStopped,
	}
	info.Spec.Labels = ownedLabels(record.Spec.Labels)
	state, err := s.engine.InspectContainer(ctx, record.Spec.Name)
	if err != nil {
		return info
	}
	info.Runtime = state.Runtime
	if state.Runtime == s.cfg.Runtime {
		info.Isolation = api.IsolationKataFirecracker
	}
	info.Init = len(state.Args) >= 3 && state.Args[0] == api.HelperPath && state.Args[1] == "init"
	info.Status = state.Status
	info.Mounts = state.HostMounts
	if !labelsMatch(state.Labels, info.Spec.Labels) {
		// Ownership mismatch: report as not ours rather than echoing the record.
		info.Spec.Labels = state.Labels
	}
	return info
}

func (s *Server) Inspect(ctx context.Context, ref *api.SandboxRef) (*api.SandboxInfo, error) {
	record, err := s.record(ref.ID)
	if err != nil {
		return nil, err
	}
	info := s.sandboxInfo(ctx, record)
	return &info, nil
}

func (s *Server) Start(ctx context.Context, ref *api.SandboxRef) (*api.Empty, error) {
	unlock := s.lock(ref.ID)
	defer unlock()
	record, err := s.record(ref.ID)
	if err != nil {
		return nil, err
	}
	if record.Phase != phaseReady {
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is not ready (phase %s)", ref.ID, record.Phase)
	}
	state, err := s.engine.InspectContainer(ctx, ref.ID)
	if err != nil {
		return nil, internal(err)
	}
	if state.Status == api.StatusRunning {
		return &api.Empty{}, nil
	}
	if state.Status != api.StatusCreated {
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s has stopped", ref.ID)
	}
	dir := s.store.logDir(ref.ID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, internal(err)
	}
	stdout, err := os.OpenFile(filepath.Join(dir, "stdout"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, internal(err)
	}
	stderr, err := os.OpenFile(filepath.Join(dir, "stderr"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		stdout.Close()
		return nil, internal(err)
	}
	if err := s.engine.StartTask(ctx, ref.ID, stdout, stderr); err != nil {
		stdout.Close()
		stderr.Close()
		return nil, status.Errorf(codes.Internal, "start sandbox %s: %v", ref.ID, err)
	}
	s.logsMu.Lock()
	s.logs[ref.ID] = []*os.File{stdout, stderr}
	s.logsMu.Unlock()
	s.logger.WithField("sandbox", ref.ID).Info("sandbox started")
	return &api.Empty{}, nil
}

func (s *Server) Delete(ctx context.Context, ref *api.SandboxRef) (*api.Empty, error) {
	if err := validateName("sandbox", ref.ID); err != nil {
		return nil, err
	}
	ctx, cancel := s.cleanupContext(ctx)
	defer cancel()
	unlock := s.lock(ref.ID)
	defer unlock()
	record, found, err := s.store.sandbox(ref.ID)
	if err != nil {
		return nil, internal(err)
	}
	if !found {
		state, err := s.engine.InspectContainer(ctx, ref.ID)
		if errors.Is(err, errEngineNotFound) {
			return &api.Empty{}, nil
		}
		if err != nil {
			return nil, internal(err)
		}
		if state.Labels[api.LabelManaged] != "true" {
			return nil, status.Errorf(codes.FailedPrecondition, "container %s is not owned by aries-vmd", ref.ID)
		}
		// A managed container without a record: remove the container only.
		record = sandboxRecord{Spec: api.SandboxSpec{Name: ref.ID}}
	} else {
		record.Phase = phaseDeleting
		if err := s.store.putSandbox(record); err != nil {
			return nil, internal(err)
		}
	}
	var errs []error
	if err := s.engine.DeleteContainer(ctx, ref.ID); err != nil {
		errs = append(errs, fmt.Errorf("delete container: %w", err))
	}
	if err := s.net.DeleteNetNS(ctx, record.NetNS, record.HostVeth); err != nil {
		errs = append(errs, fmt.Errorf("delete network namespace: %w", err))
	}
	s.closeLogs(ref.ID)
	if err := s.confirmAbsent(ctx, record); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, status.Errorf(codes.Internal, "delete sandbox %s: %v", ref.ID, err)
	}
	if found {
		s.global.Lock()
		network, networkFound, err := s.store.network(record.Spec.Network)
		if err == nil && networkFound {
			delete(network.Leases, ref.ID)
			err = s.store.putNetwork(network)
		}
		s.global.Unlock()
		if err != nil {
			return nil, internal(err)
		}
	}
	if err := os.RemoveAll(s.store.logDir(ref.ID)); err != nil {
		return nil, internal(err)
	}
	if err := s.store.deleteSandbox(ref.ID); err != nil {
		return nil, internal(err)
	}
	s.logger.WithField("sandbox", ref.ID).Info("sandbox removed")
	return &api.Empty{}, nil
}

// confirmAbsent positively checks every resource a sandbox can own. Shim
// and hypervisor processes exit asynchronously, so they are polled briefly.
func (s *Server) confirmAbsent(ctx context.Context, record sandboxRecord) error {
	var errs []error
	absent, err := s.engine.ContainerAbsent(ctx, record.Spec.Name)
	if err != nil {
		errs = append(errs, err)
	} else if !absent {
		errs = append(errs, errors.New("container or snapshot remains"))
	}
	if record.NetNS != "" {
		if exists, err := s.net.NetNSExists(ctx, record.NetNS); err != nil || exists {
			errs = append(errs, errors.Join(err, fmt.Errorf("network namespace %s remains", record.NetNS)))
		}
	}
	if record.HostVeth != "" {
		if exists, err := s.net.LinkExists(ctx, record.HostVeth); err != nil || exists {
			errs = append(errs, errors.Join(err, fmt.Errorf("veth %s remains", record.HostVeth)))
		}
	}
	for {
		pids, err := s.processesReferencing(record.Spec.Name)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if len(pids) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("sandbox processes %v remain", pids))
			return errors.Join(errs...)
		case <-time.After(200 * time.Millisecond):
		}
	}
	return errors.Join(errs...)
}

func (s *Server) closeLogs(id string) {
	s.logsMu.Lock()
	files := s.logs[id]
	delete(s.logs, id)
	s.logsMu.Unlock()
	for _, file := range files {
		file.Close()
	}
}

func (s *Server) Logs(request *api.LogsRequest, stream grpc.ServerStreamingServer[api.LogChunk]) error {
	if _, err := s.record(request.Sandbox); err != nil {
		return err
	}
	dir := s.store.logDir(request.Sandbox)
	for _, name := range []string{"stdout", "stderr"} {
		file, err := os.Open(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return internal(err)
		}
		buffer := make([]byte, chunkSize)
		for {
			n, readErr := file.Read(buffer)
			if n > 0 {
				chunk := &api.LogChunk{}
				if name == "stdout" {
					chunk.Stdout = bytes.Clone(buffer[:n])
				} else {
					chunk.Stderr = bytes.Clone(buffer[:n])
				}
				if err := stream.Send(chunk); err != nil {
					file.Close()
					return err
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				file.Close()
				return internal(readErr)
			}
		}
		file.Close()
	}
	return nil
}

func (s *Server) Stats(ctx context.Context, request *api.StatsRequest) (*api.StatsReply, error) {
	records, err := s.store.sandboxes()
	if err != nil {
		return nil, internal(err)
	}
	reply := &api.StatsReply{Sandboxes: []api.SandboxStats{}}
	for _, record := range records {
		labels := ownedLabels(record.Spec.Labels)
		if record.Phase != phaseReady || !labelsMatch(labels, request.Labels) {
			continue
		}
		state, err := s.engine.InspectContainer(ctx, record.Spec.Name)
		if err != nil || state.Status != api.StatusRunning {
			continue
		}
		observed, err := s.engine.Metrics(ctx, record.Spec.Name)
		if errors.Is(err, errEngineNotFound) {
			continue
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "metrics for %s: %v", record.Spec.Name, err)
		}
		reply.Sandboxes = append(reply.Sandboxes, api.SandboxStats{
			ID: record.Spec.Name, Labels: labels, ObservedAt: time.Now().UTC(),
			CPUUsageNanoseconds: observed.CPUUsageNanoseconds, MemoryUsageBytes: observed.MemoryUsageBytes, MemoryLimitBytes: observed.MemoryLimitBytes,
		})
	}
	return reply, nil
}
