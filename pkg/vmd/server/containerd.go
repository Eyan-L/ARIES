package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	cgroup1stats "github.com/containerd/cgroups/v3/cgroup1/stats"
	cgroup2stats "github.com/containerd/cgroups/v3/cgroup2/stats"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	"github.com/distribution/reference"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// containerdEngine implements engine with the containerd Go client.
type containerdEngine struct {
	client      *containerd.Client
	namespace   string
	runtime     string
	snapshotter string
	// workDir holds FIFOs and temporary root filesystem mounts.
	workDir string
}

// NewContainerdEngine connects lazily to a dedicated containerd instance.
func NewContainerdEngine(address, namespace, runtime, snapshotter, workDir string) (*containerdEngine, error) {
	client, err := containerd.New(address, containerd.WithDefaultNamespace(namespace), containerd.WithTimeout(10*time.Second))
	if err != nil {
		return nil, fmt.Errorf("connect containerd at %s: %w", address, err)
	}
	for _, dir := range []string{filepath.Join(workDir, "fifo"), filepath.Join(workDir, "mnt")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			client.Close()
			return nil, err
		}
	}
	return &containerdEngine{client: client, namespace: namespace, runtime: runtime, snapshotter: snapshotter, workDir: workDir}, nil
}

func (e *containerdEngine) Close() error { return e.client.Close() }

func (e *containerdEngine) ctx(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, e.namespace)
}

func notFound(err error) error {
	if errdefs.IsNotFound(err) {
		return fmt.Errorf("%w: %v", errEngineNotFound, err)
	}
	return err
}

func (e *containerdEngine) Version(ctx context.Context) (string, error) {
	version, err := e.client.Version(e.ctx(ctx))
	if err != nil {
		return "", err
	}
	return version.Version, nil
}

func normalizedName(value string) string {
	named, err := reference.ParseDockerRef(value)
	if err != nil {
		return value
	}
	return named.String()
}

func (e *containerdEngine) ImportImage(ctx context.Context, ref string, archive io.Reader) error {
	ctx = e.ctx(ctx)
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	indexName := "aries-vmd-import/" + hex.EncodeToString(nonce[:])
	imported, err := e.client.Import(ctx, archive, containerd.WithIndexName(indexName))
	if err != nil {
		return err
	}
	store := e.client.ImageService()
	defer func() { _ = store.Delete(context.WithoutCancel(ctx), indexName) }()
	// Prefer the manifest the archive names after ref; fall back to the only
	// manifest, then the archive index (which unpacks for this platform).
	var target *images.Image
	want := normalizedName(ref)
	for index := range imported {
		if imported[index].Name != indexName && normalizedName(imported[index].Name) == want {
			target = &imported[index]
			break
		}
	}
	if target == nil {
		var manifests []images.Image
		for _, image := range imported {
			if image.Name != indexName {
				manifests = append(manifests, image)
			}
		}
		if len(manifests) == 1 {
			target = &manifests[0]
		}
	}
	if target == nil {
		for index := range imported {
			if imported[index].Name == indexName {
				target = &imported[index]
			}
		}
	}
	if target == nil {
		return errors.New("archive contained no image")
	}
	record := images.Image{Name: ref, Target: target.Target}
	if _, err := store.Update(ctx, record, "target"); errdefs.IsNotFound(err) {
		_, err = store.Create(ctx, record)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	image, err := e.client.GetImage(ctx, ref)
	if err != nil {
		return err
	}
	return image.Unpack(ctx, e.snapshotter)
}

func (e *containerdEngine) InspectImage(ctx context.Context, ref string) (imageConfig, error) {
	ctx = e.ctx(ctx)
	image, err := e.client.GetImage(ctx, ref)
	if err != nil {
		return imageConfig{}, notFound(err)
	}
	descriptor, err := image.Config(ctx)
	if err != nil {
		return imageConfig{}, err
	}
	spec, err := image.Spec(ctx)
	if err != nil {
		return imageConfig{}, err
	}
	unpacked, err := image.IsUnpacked(ctx, e.snapshotter)
	if err != nil {
		return imageConfig{}, err
	}
	return imageConfig{
		ConfigDigest: descriptor.Digest.String(), TargetDigest: image.Target().Digest.String(), Unpacked: unpacked,
		Env: spec.Config.Env, Entrypoint: spec.Config.Entrypoint, Cmd: spec.Config.Cmd,
		WorkingDir: spec.Config.WorkingDir, User: spec.Config.User,
	}, nil
}

func (e *containerdEngine) CreateContainer(ctx context.Context, spec containerSpec) error {
	ctx = e.ctx(ctx)
	image, err := e.client.GetImage(ctx, spec.Image)
	if err != nil {
		return notFound(err)
	}
	privileges := oci.WithNewPrivileges
	if spec.NoNewPrivileges {
		privileges = oci.WithNoNewPrivileges
	}
	_, err = e.client.NewContainer(ctx, spec.ID,
		containerd.WithImage(image),
		containerd.WithSnapshotter(e.snapshotter),
		containerd.WithNewSnapshot(spec.ID, image),
		containerd.WithRuntime(e.runtime, nil),
		containerd.WithContainerLabels(spec.Labels),
		containerd.WithNewSpec(
			oci.WithProcessArgs(spec.Args...),
			oci.WithEnv(spec.Env),
			oci.WithProcessCwd(spec.Cwd),
			oci.WithHostname(spec.Hostname),
			oci.WithLinuxNamespace(specs.LinuxNamespace{Type: specs.NetworkNamespace, Path: spec.NetNSPath}),
			oci.WithAnnotations(spec.Annotations),
			oci.WithUIDGID(0, 0),
			privileges,
		),
	)
	return err
}

func (e *containerdEngine) load(ctx context.Context, id string) (containerd.Container, error) {
	container, err := e.client.LoadContainer(ctx, id)
	if err != nil {
		return nil, notFound(err)
	}
	return container, nil
}

func (e *containerdEngine) PrepareRootfs(ctx context.Context, id string, inject func(*os.Root) error) (returnErr error) {
	ctx = e.ctx(ctx)
	container, err := e.load(ctx, id)
	if err != nil {
		return err
	}
	if _, err := container.Task(ctx, nil); err == nil {
		return errors.New("refusing to mount the root filesystem of a sandbox with a task")
	}
	info, err := container.Info(ctx)
	if err != nil {
		return err
	}
	mounts, err := e.client.SnapshotService(info.Snapshotter).Mounts(ctx, info.SnapshotKey)
	if err != nil {
		return err
	}
	target, err := os.MkdirTemp(filepath.Join(e.workDir, "mnt"), id+"-")
	if err != nil {
		return err
	}
	if err := mount.All(mounts, target); err != nil {
		os.Remove(target)
		return err
	}
	defer func() {
		if err := mount.UnmountAll(target, 0); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("unmount root filesystem: %w", err))
			return
		}
		returnErr = errors.Join(returnErr, os.Remove(target))
	}()
	root, err := os.OpenRoot(target)
	if err != nil {
		return err
	}
	defer root.Close()
	return inject(root)
}

func (e *containerdEngine) SetProcessUser(ctx context.Context, id string, user identity) error {
	ctx = e.ctx(ctx)
	container, err := e.load(ctx, id)
	if err != nil {
		return err
	}
	spec, err := container.Spec(ctx)
	if err != nil {
		return err
	}
	spec.Process.User = specs.User{UID: user.UID, GID: user.GID, AdditionalGids: user.AdditionalGIDs}
	return container.Update(ctx, func(_ context.Context, _ *containerd.Client, c *containers.Container) error {
		value, err := typeurl.MarshalAnyToProto(spec)
		if err != nil {
			return err
		}
		c.Spec = value
		return nil
	})
}

func (e *containerdEngine) InspectContainer(ctx context.Context, id string) (containerState, error) {
	ctx = e.ctx(ctx)
	container, err := e.load(ctx, id)
	if err != nil {
		return containerState{}, err
	}
	info, err := container.Info(ctx)
	if err != nil {
		return containerState{}, notFound(err)
	}
	spec, err := container.Spec(ctx)
	if err != nil {
		return containerState{}, err
	}
	state := containerState{Runtime: info.Runtime.Name, Labels: info.Labels, Status: "created"}
	if spec.Process != nil {
		state.Args = spec.Process.Args
	}
	for _, m := range spec.Mounts {
		if m.Type == "bind" || slices.Contains(m.Options, "bind") || slices.Contains(m.Options, "rbind") {
			state.HostMounts = append(state.HostMounts, m.Source+":"+m.Destination)
		}
	}
	task, err := container.Task(ctx, nil)
	if errdefs.IsNotFound(err) {
		return state, nil
	}
	if err != nil {
		return containerState{}, err
	}
	status, err := task.Status(ctx)
	if errdefs.IsNotFound(err) {
		return state, nil
	}
	if err != nil {
		return containerState{}, err
	}
	switch status.Status {
	case containerd.Running:
		state.Status = "running"
	case containerd.Created:
		state.Status = "created"
	default:
		state.Status = "stopped"
	}
	return state, nil
}

func (e *containerdEngine) StartTask(ctx context.Context, id string, stdout, stderr io.Writer) error {
	ctx = e.ctx(ctx)
	container, err := e.load(ctx, id)
	if err != nil {
		return err
	}
	task, err := container.NewTask(ctx, cio.NewCreator(cio.WithStreams(nil, stdout, stderr), cio.WithFIFODir(filepath.Join(e.workDir, "fifo"))))
	if err != nil {
		return err
	}
	if err := task.Start(ctx); err != nil {
		_, deleteErr := task.Delete(context.WithoutCancel(ctx), containerd.WithProcessKill)
		return errors.Join(err, deleteErr)
	}
	return nil
}

// stdinCloser half-closes the process's stdin when the caller's reader ends.
type stdinCloser struct {
	reader  io.Reader
	ready   chan containerd.Process
	once    sync.Once
	process containerd.Process
}

func (s *stdinCloser) Read(buffer []byte) (int, error) {
	n, err := s.reader.Read(buffer)
	if err == io.EOF {
		s.once.Do(func() {
			if process, ok := <-s.ready; ok {
				_ = process.CloseIO(context.Background(), containerd.WithStdinCloser)
			}
		})
	}
	return n, err
}

func (e *containerdEngine) Exec(ctx context.Context, id, execID string, process processSpec, stdin io.Reader, stdout, stderr io.Writer) (execProcess, error) {
	ctx = e.ctx(ctx)
	container, err := e.load(ctx, id)
	if err != nil {
		return nil, err
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		return nil, notFound(err)
	}
	spec, err := container.Spec(ctx)
	if err != nil {
		return nil, err
	}
	processSpec := *spec.Process
	processSpec.Args = process.Args
	processSpec.Env = process.Env
	processSpec.Cwd = process.Cwd
	processSpec.Terminal = false
	processSpec.User = specs.User{UID: process.User.UID, GID: process.User.GID, AdditionalGids: process.User.AdditionalGIDs}
	var closer *stdinCloser
	if stdin != nil {
		closer = &stdinCloser{reader: stdin, ready: make(chan containerd.Process, 1)}
		stdin = closer
	}
	started, err := task.Exec(ctx, execID, &processSpec, cio.NewCreator(cio.WithStreams(stdin, stdout, stderr), cio.WithFIFODir(filepath.Join(e.workDir, "fifo"))))
	if err != nil {
		if closer != nil {
			close(closer.ready)
		}
		return nil, err
	}
	exited, err := started.Wait(ctx)
	if err == nil {
		err = started.Start(ctx)
	}
	if err != nil {
		if closer != nil {
			close(closer.ready)
		}
		_, deleteErr := started.Delete(context.WithoutCancel(ctx), containerd.WithProcessKill)
		return nil, errors.Join(err, deleteErr)
	}
	if closer != nil {
		closer.ready <- started
		close(closer.ready)
	}
	return &containerdProcess{process: started, exited: exited}, nil
}

type containerdProcess struct {
	process containerd.Process
	exited  <-chan containerd.ExitStatus
	mu      sync.Mutex
	status  *containerd.ExitStatus
}

func (p *containerdProcess) Wait(ctx context.Context) (int, error) {
	p.mu.Lock()
	if p.status != nil {
		defer p.mu.Unlock()
		code, _, err := p.status.Result()
		return int(code), err
	}
	p.mu.Unlock()
	select {
	case status, ok := <-p.exited:
		if !ok {
			return -1, errors.New("exec exit channel closed")
		}
		p.mu.Lock()
		p.status = &status
		p.mu.Unlock()
		code, _, err := status.Result()
		return int(code), err
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

func (p *containerdProcess) Kill(ctx context.Context, signal syscall.Signal) error {
	return p.process.Kill(ctx, signal)
}

func (p *containerdProcess) Drain(timeoutSeconds float64) {
	done := make(chan struct{})
	go func() {
		p.process.IO().Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Duration(timeoutSeconds * float64(time.Second))):
		p.process.IO().Cancel()
		<-done
	}
}

func (p *containerdProcess) Delete(ctx context.Context) error {
	_, err := p.process.Delete(ctx)
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}

func (e *containerdEngine) DeleteContainer(ctx context.Context, id string) error {
	ctx = e.ctx(ctx)
	container, err := e.load(ctx, id)
	if errors.Is(err, errEngineNotFound) {
		return e.removeSnapshot(ctx, id)
	}
	if err != nil {
		return err
	}
	var errs []error
	task, err := container.Task(ctx, nil)
	switch {
	case errdefs.IsNotFound(err):
	case err != nil:
		errs = append(errs, err)
	default:
		exited, waitErr := task.Wait(ctx)
		if killErr := task.Kill(ctx, syscall.SIGKILL, containerd.WithKillAll); killErr != nil && !errdefs.IsNotFound(killErr) {
			errs = append(errs, fmt.Errorf("kill task: %w", killErr))
		}
		if waitErr == nil {
			select {
			case <-exited:
			case <-ctx.Done():
				errs = append(errs, fmt.Errorf("wait for task exit: %w", ctx.Err()))
			}
		}
		if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete task: %w", err))
		}
	}
	if err := container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("delete container: %w", err))
	}
	if err := e.removeSnapshot(ctx, id); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (e *containerdEngine) removeSnapshot(ctx context.Context, id string) error {
	if err := e.client.SnapshotService(e.snapshotter).Remove(ctx, id); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove snapshot: %w", err)
	}
	return nil
}

func (e *containerdEngine) ContainerAbsent(ctx context.Context, id string) (bool, error) {
	ctx = e.ctx(ctx)
	if _, err := e.client.LoadContainer(ctx, id); err == nil {
		return false, nil
	} else if !errdefs.IsNotFound(err) {
		return false, err
	}
	if _, err := e.client.SnapshotService(e.snapshotter).Stat(ctx, id); err == nil {
		return false, nil
	} else if !errdefs.IsNotFound(err) {
		return false, err
	}
	return true, nil
}

func (e *containerdEngine) Metrics(ctx context.Context, id string) (metrics, error) {
	ctx = e.ctx(ctx)
	container, err := e.load(ctx, id)
	if err != nil {
		return metrics{}, err
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		return metrics{}, notFound(err)
	}
	metric, err := task.Metrics(ctx)
	if err != nil {
		return metrics{}, notFound(err)
	}
	decoded, err := typeurl.UnmarshalAny(metric.Data)
	if err != nil {
		return metrics{}, err
	}
	return decodeMetrics(decoded)
}

func decodeMetrics(value any) (metrics, error) {
	switch typed := value.(type) {
	case *cgroup1stats.Metrics:
		var result metrics
		if typed.CPU != nil && typed.CPU.Usage != nil {
			result.CPUUsageNanoseconds = typed.CPU.Usage.Total
		}
		if typed.Memory != nil && typed.Memory.Usage != nil {
			result.MemoryUsageBytes, result.MemoryLimitBytes = typed.Memory.Usage.Usage, typed.Memory.Usage.Limit
		}
		return result, nil
	case *cgroup2stats.Metrics:
		var result metrics
		if typed.CPU != nil {
			result.CPUUsageNanoseconds = typed.CPU.UsageUsec * 1000
		}
		if typed.Memory != nil {
			result.MemoryUsageBytes, result.MemoryLimitBytes = typed.Memory.Usage, typed.Memory.UsageLimit
		}
		return result, nil
	default:
		return metrics{}, fmt.Errorf("unsupported metrics type %T", value)
	}
}
