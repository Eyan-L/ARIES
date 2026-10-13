package vmd

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/deployment"
	"github.com/hyscale-lab/aries/pkg/runner"
	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeDaemon is an in-memory aries-vmd.
type fakeDaemon struct {
	api.Server
	mu        sync.Mutex
	calls     []string
	sandboxes map[string]*api.SandboxInfo
	networks  map[string]*api.NetworkInfo
	createErr error
	files     map[string][]byte
	uploads   map[string][]byte
	exec      func(grpc.BidiStreamingServer[api.ExecFrame, api.ExecFrame]) error
	stats     []api.SandboxStats
	images    map[string]string
	imported  []byte
	importCfg string
}

func newFakeDaemon() *fakeDaemon {
	return &fakeDaemon{sandboxes: map[string]*api.SandboxInfo{}, networks: map[string]*api.NetworkInfo{}, files: map[string][]byte{}, uploads: map[string][]byte{}, images: map[string]string{}}
}

func (d *fakeDaemon) record(call string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, call)
}

func (d *fakeDaemon) Health(context.Context, *api.HealthRequest) (*api.HealthReply, error) {
	return &api.HealthReply{APIVersion: api.Version, Isolation: api.IsolationKataFirecracker, KVM: true, KataVersion: `Kata Containers containerd shim (Golang): id: "io.containerd.kata.v2", version: 4.2.0, commit: abc`}, nil
}

func (d *fakeDaemon) Create(_ context.Context, spec *api.SandboxSpec) (*api.SandboxInfo, error) {
	d.record("create")
	d.mu.Lock()
	defer d.mu.Unlock()
	info := &api.SandboxInfo{Spec: *spec, ID: spec.Name, Runtime: "io.containerd.kata-fc.v2", Isolation: api.IsolationKataFirecracker, Init: true, Status: api.StatusCreated}
	d.sandboxes[spec.Name] = info
	return info, d.createErr
}

func (d *fakeDaemon) Inspect(_ context.Context, ref *api.SandboxRef) (*api.SandboxInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	info, ok := d.sandboxes[ref.ID]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", ref.ID)
	}
	copied := *info
	return &copied, nil
}

func (d *fakeDaemon) Start(_ context.Context, ref *api.SandboxRef) (*api.Empty, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	info, ok := d.sandboxes[ref.ID]
	if !ok {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	info.Status = api.StatusRunning
	return &api.Empty{}, nil
}

func (d *fakeDaemon) Delete(_ context.Context, ref *api.SandboxRef) (*api.Empty, error) {
	d.record("delete " + ref.ID)
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.sandboxes, ref.ID)
	return &api.Empty{}, nil
}

func (d *fakeDaemon) CreateNetwork(_ context.Context, spec *api.NetworkSpec) (*api.NetworkInfo, error) {
	d.record("create-network")
	d.mu.Lock()
	defer d.mu.Unlock()
	info := &api.NetworkInfo{ID: spec.Name, Name: spec.Name, Labels: spec.Labels, AllowEgress: spec.AllowEgress, Members: map[string]string{}}
	d.networks[spec.Name] = info
	return info, nil
}

func (d *fakeDaemon) InspectNetwork(_ context.Context, ref *api.NetworkRef) (*api.NetworkInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	info, ok := d.networks[ref.ID]
	if !ok {
		return nil, status.Error(codes.NotFound, "network not found")
	}
	copied := *info
	return &copied, nil
}

func (d *fakeDaemon) DeleteNetwork(_ context.Context, ref *api.NetworkRef) (*api.Empty, error) {
	d.record("delete-network")
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.networks, ref.ID)
	return &api.Empty{}, nil
}

func (d *fakeDaemon) GetArchive(request *api.ArchiveRequest, stream grpc.ServerStreamingServer[api.ArchiveChunk]) error {
	d.mu.Lock()
	_, live := d.sandboxes[request.Sandbox]
	content, exists := d.files[request.Path]
	d.mu.Unlock()
	if !live {
		return status.Error(codes.NotFound, "sandbox not found")
	}
	if !exists {
		return stream.Send(&api.ArchiveChunk{Missing: true})
	}
	if err := stream.Send(&api.ArchiveChunk{Stat: &api.FileStat{Name: "f", Size: int64(len(content)), Mode: 0o640}}); err != nil {
		return err
	}
	for len(content) > 0 {
		n := min(3, len(content))
		if err := stream.Send(&api.ArchiveChunk{Data: content[:n]}); err != nil {
			return err
		}
		content = content[n:]
	}
	return nil
}

func (d *fakeDaemon) PutArchive(stream grpc.ClientStreamingServer[api.ArchiveChunk, api.Empty]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	var data []byte
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		data = append(data, chunk.Data...)
	}
	d.mu.Lock()
	d.uploads[first.Sandbox+":"+first.Path] = data
	d.mu.Unlock()
	return stream.SendAndClose(&api.Empty{})
}

func (d *fakeDaemon) Exec(stream grpc.BidiStreamingServer[api.ExecFrame, api.ExecFrame]) error {
	return d.exec(stream)
}

func (d *fakeDaemon) Logs(request *api.LogsRequest, stream grpc.ServerStreamingServer[api.LogChunk]) error {
	if _, err := d.Inspect(stream.Context(), &api.SandboxRef{ID: request.Sandbox}); err != nil {
		return err
	}
	return stream.Send(&api.LogChunk{Stdout: []byte("out"), Stderr: []byte("err")})
}

func (d *fakeDaemon) Stats(_ context.Context, request *api.StatsRequest) (*api.StatsReply, error) {
	return &api.StatsReply{Sandboxes: d.stats}, nil
}

func (d *fakeDaemon) InspectImage(_ context.Context, request *api.ImageRequest) (*api.ImageInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	digest, ok := d.images[request.Reference]
	return &api.ImageInfo{Present: ok, Reference: request.Reference, ConfigDigest: digest}, nil
}

func (d *fakeDaemon) ImportImage(stream grpc.ClientStreamingServer[api.ImageChunk, api.ImageInfo]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	var data []byte
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		data = append(data, chunk.Data...)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.imported = data
	d.images[first.Reference] = d.importCfg
	return stream.SendAndClose(&api.ImageInfo{Present: true, Reference: first.Reference, ConfigDigest: d.importCfg})
}

func startFake(t *testing.T, daemon *fakeDaemon) Options {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(api.ServerOptions()...)
	api.Register(server, daemon)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return Options{Dialer: func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }, Logger: logger, CleanupTimeout: 2 * time.Second}
}

func newTestManager(t *testing.T, daemon *fakeDaemon) *Manager {
	t.Helper()
	manager, err := New(startFake(t, daemon))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.Close() })
	return manager
}

func testRequest() deployment.Request {
	cpu, memory := 1.5, 512
	return deployment.Request{
		Name: "aries-task-1", Image: "example/image:1", Workdir: "/app",
		Entrypoint: []string{"/bin/sleep"}, Args: []string{"infinity"},
		Labels: map[string]string{"aries.run": "run", "aries.task": "task"}, Env: []string{"A=1"},
		Placement: core.RuntimePlacement{DockerNetwork: "aries-net-x", VMNetwork: "aries-vnet-x"},
		CPU:       &cpu, MemoryMB: &memory, Init: true, NoNewPrivileges: true, NetworkAliases: []string{"task-sandbox"},
	}
}

func TestCreateValidateAndSecretScan(t *testing.T) {
	daemon := newFakeDaemon()
	manager := newTestManager(t, daemon)
	ctx := context.Background()
	request := testRequest()
	id, err := manager.Create(ctx, request)
	if err != nil || id != request.Name {
		t.Fatalf("create = %q, %v", id, err)
	}
	spec := daemon.sandboxes[id].Spec
	if spec.VCPUs != 2 || spec.MemoryMB != 512 || spec.Network != "aries-vnet-x" || spec.Aliases[0] != "task-sandbox" || !spec.NoNewPrivileges {
		t.Fatalf("spec = %+v", spec)
	}
	if err := manager.Validate(ctx, id, request, [][]byte{[]byte("hunter2")}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Validate(ctx, id, request, [][]byte{[]byte("A=1")}); err == nil || !strings.Contains(err.Error(), "secret entered vmd metadata") {
		t.Fatalf("secret scan error = %v", err)
	}
	for name, mutate := range map[string]func(*deployment.Request){
		"image":   func(r *deployment.Request) { r.Image = "other" },
		"args":    func(r *deployment.Request) { r.Args = []string{"1"} },
		"labels":  func(r *deployment.Request) { r.Labels = map[string]string{"aries.run": "other"} },
		"network": func(r *deployment.Request) { r.Placement.VMNetwork = "other" },
		"workdir": func(r *deployment.Request) { r.Workdir = "/other" },
	} {
		changed := testRequest()
		mutate(&changed)
		if err := manager.Validate(ctx, id, changed, nil); err == nil {
			t.Errorf("%s mismatch accepted", name)
		}
	}
	daemon.sandboxes[id].Mounts = []string{"/host"}
	if err := manager.Validate(ctx, id, request, nil); err == nil {
		t.Fatal("host mount accepted")
	}
	daemon.sandboxes[id].Mounts = nil
	daemon.sandboxes[id].Isolation = "runc"
	if err := manager.Validate(ctx, id, request, nil); err == nil {
		t.Fatal("non-microVM isolation accepted")
	}
}

func TestCreateRejectsUnsupportedRequestsBeforeContact(t *testing.T) {
	daemon := newFakeDaemon()
	manager := newTestManager(t, daemon)
	for name, mutate := range map[string]func(*deployment.Request){
		"gpu":       func(r *deployment.Request) { r.GPUs = 1 },
		"service":   func(r *deployment.Request) { r.ServicePort = 80 },
		"placement": func(r *deployment.Request) { r.Placement.VMNetwork = "" },
		"name":      func(r *deployment.Request) { r.Name = "" },
	} {
		request := testRequest()
		mutate(&request)
		if id, err := manager.Create(context.Background(), request); err == nil || id != "" {
			t.Errorf("%s: id=%q err=%v", name, id, err)
		}
	}
	if len(daemon.calls) != 0 {
		t.Fatalf("daemon contacted: %v", daemon.calls)
	}
}

func TestCreateReturnsIdentityOnDaemonError(t *testing.T) {
	daemon := newFakeDaemon()
	daemon.createErr = status.Error(codes.Internal, "boot failed")
	manager := newTestManager(t, daemon)
	request := testRequest()
	id, err := manager.Create(context.Background(), request)
	if err == nil || id != request.Name {
		t.Fatalf("create = %q, %v", id, err)
	}
	if err := manager.Stop(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := manager.Stop(context.Background(), id); err != nil {
		t.Fatalf("repeated stop: %v", err)
	}
}

func TestStartRunningLogsAndAddress(t *testing.T) {
	daemon := newFakeDaemon()
	manager := newTestManager(t, daemon)
	ctx := context.Background()
	id, err := manager.Create(ctx, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	if running, err := manager.Running(ctx, id); err != nil || running {
		t.Fatalf("running before start = %v, %v", running, err)
	}
	if err := manager.Start(ctx, id); err != nil {
		t.Fatal(err)
	}
	if running, err := manager.Running(ctx, id); err != nil || !running {
		t.Fatalf("running = %v, %v", running, err)
	}
	logs, err := manager.Logs(ctx, id, 16)
	if err != nil || string(logs) != "outerr" {
		t.Fatalf("logs = %q, %v", logs, err)
	}
	if _, err := manager.Logs(ctx, id, 2); err == nil {
		t.Fatal("log bound ignored")
	}
	if err := manager.LogsStream(ctx, "absent", nil, nil); !errors.Is(err, runner.ErrNotFound) {
		t.Fatalf("missing logs = %v", err)
	}
	if _, err := manager.Address(ctx, id, 80); err == nil {
		t.Fatal("address accepted")
	}
}

func TestArchiveTransfers(t *testing.T) {
	daemon := newFakeDaemon()
	manager := newTestManager(t, daemon)
	ctx := context.Background()
	id, err := manager.Create(ctx, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	daemon.files["/app/result"] = []byte("payload")
	reader, info, err := manager.DownloadArchive(ctx, id, "/app/result")
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(content) != "payload" || info.Size != 7 || info.Mode != 0o640 {
		t.Fatalf("download = %q %+v %v", content, info, err)
	}
	if _, _, err := manager.DownloadArchive(ctx, id, "/app/missing"); !errors.Is(err, runner.ErrNotFound) {
		t.Fatalf("missing source = %v", err)
	}
	if _, _, err := manager.DownloadArchive(ctx, "gone", "/app/result"); err == nil || errors.Is(err, runner.ErrNotFound) {
		t.Fatalf("missing sandbox = %v", err)
	}
	payload := bytes.Repeat([]byte("x"), chunkSize+5)
	if err := manager.UploadArchive(ctx, id, "/app", bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if got := daemon.uploads[id+":/app"]; !bytes.Equal(got, payload) {
		t.Fatalf("upload length %d", len(got))
	}
}

func execResult(t *testing.T, manager *Manager, ctx context.Context, command core.Command, stdin io.Reader) (core.CommandResult, string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	result, err := manager.ExecStream(ctx, "aries-task-1", command, stdin, &stdout, &stderr)
	return result, stdout.String(), stderr.String(), err
}

func TestExecStreamsAndReturnsExitCode(t *testing.T) {
	daemon := newFakeDaemon()
	var start *api.ExecStart
	daemon.exec = func(stream grpc.BidiStreamingServer[api.ExecFrame, api.ExecFrame]) error {
		first, err := stream.Recv()
		if err != nil {
			return err
		}
		start = first.Start
		var input []byte
		for {
			frame, err := stream.Recv()
			if err != nil {
				return err
			}
			input = append(input, frame.Stdin...)
			if frame.StdinEOF {
				break
			}
		}
		if err := stream.Send(&api.ExecFrame{Stdout: input, Stderr: []byte("warn")}); err != nil {
			return err
		}
		return stream.Send(&api.ExecFrame{Exit: &api.ExecExit{Code: 3}})
	}
	manager := newTestManager(t, daemon)
	command := core.Command{Path: "/bin/cat", Args: []string{"-"}, Dir: "/app", User: "1000:1000", Env: map[string]string{"B": "2", "A": "1"}}
	result, stdout, stderr, err := execResult(t, manager, context.Background(), command, strings.NewReader("hello"))
	if err != nil || result.ExitCode != 3 || stdout != "hello" || stderr != "warn" {
		t.Fatalf("result=%+v stdout=%q stderr=%q err=%v", result, stdout, stderr, err)
	}
	if strings.Join(start.Args, " ") != "/bin/cat -" || strings.Join(start.Env, ",") != "A=1,B=2" || start.User != "1000:1000" || start.Dir != "/app" || !start.Stdin {
		t.Fatalf("start = %+v", start)
	}
	if _, _, _, err := execResult(t, manager, context.Background(), core.Command{Path: "/bin/true", User: "root"}, nil); err == nil {
		t.Fatal("named user accepted")
	}
}

func cancellingDaemon(confirm bool, cancelled chan<- struct{}) func(grpc.BidiStreamingServer[api.ExecFrame, api.ExecFrame]) error {
	return func(stream grpc.BidiStreamingServer[api.ExecFrame, api.ExecFrame]) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(&api.ExecFrame{Stdout: []byte("started")}); err != nil {
			return err
		}
		for {
			frame, err := stream.Recv()
			if err != nil {
				return err
			}
			if frame.Cancel {
				close(cancelled)
				exit := &api.ExecExit{Code: -1, Canceled: true, CancelConfirmed: confirm}
				if !confirm {
					exit.Error = "process group survived"
				}
				return stream.Send(&api.ExecFrame{Exit: exit})
			}
		}
	}
}

func TestExecCancellationRequiresConfirmation(t *testing.T) {
	for _, confirm := range []bool{true, false} {
		cancelled := make(chan struct{})
		daemon := newFakeDaemon()
		daemon.exec = cancellingDaemon(confirm, cancelled)
		manager := newTestManager(t, daemon)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, stdout, _, err := execResult(t, manager, ctx, core.Command{Path: "/bin/sleep", Args: []string{"60"}}, nil)
		cancel()
		select {
		case <-cancelled:
		default:
			t.Fatalf("confirm=%v: cancel frame not sent", confirm)
		}
		if !errors.Is(err, context.DeadlineExceeded) || stdout != "started" {
			t.Fatalf("confirm=%v: err=%v stdout=%q", confirm, err, stdout)
		}
		if unconfirmed := errors.Is(err, errTerminationUnconfirmed); unconfirmed == confirm {
			t.Fatalf("confirm=%v: err=%v", confirm, err)
		}
	}
}

func TestExecTimeoutAndOutputLimitTerminate(t *testing.T) {
	cancelled := make(chan struct{})
	daemon := newFakeDaemon()
	daemon.exec = cancellingDaemon(true, cancelled)
	manager := newTestManager(t, daemon)
	_, _, _, err := execResult(t, manager, context.Background(), core.Command{Path: "/bin/sleep", Timeout: 50 * time.Millisecond}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout err = %v", err)
	}
	cancelled = make(chan struct{})
	daemon.exec = cancellingDaemon(true, cancelled)
	_, _, _, err = execResult(t, manager, context.Background(), core.Command{Path: "/bin/yes", OutputLimitBytes: 3}, nil)
	if err == nil || !strings.Contains(err.Error(), "output exceeds 3 bytes") {
		t.Fatalf("output limit err = %v", err)
	}
	<-cancelled
}

func TestResourceSourceFiltersOccurrence(t *testing.T) {
	daemon := newFakeDaemon()
	now := time.Now()
	labels := func(component string) map[string]string {
		return map[string]string{"aries.run": "run", "aries.task": "task", "aries.component": component}
	}
	daemon.stats = []api.SandboxStats{
		{ID: "b", Labels: labels("sandbox"), ObservedAt: now, CPUUsageNanoseconds: 5, MemoryUsageBytes: 6, MemoryLimitBytes: 7},
		{ID: "a", Labels: labels("evaluation-sandbox"), ObservedAt: now},
		{ID: "c", Labels: labels("other"), ObservedAt: now},
	}
	source, err := NewResourceSource(startFake(t, daemon), "run", "task")
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	readings, err := source.Sample(context.Background())
	if err != nil || len(readings) != 2 || readings[0].Component != "evaluation-sandbox" || readings[1].MemoryLimitBytes != 7 {
		t.Fatalf("readings = %+v, %v", readings, err)
	}
	daemon.stats = []api.SandboxStats{{ID: "x", Labels: map[string]string{"aries.run": "other", "aries.task": "task"}, ObservedAt: now}}
	if _, err := source.Sample(context.Background()); err == nil {
		t.Fatal("foreign reading accepted")
	}
}

func dockerSave(t *testing.T, config string) []byte {
	t.Helper()
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for name, content := range map[string]string{"blobs/sha256/layer": "layer", "manifest.json": `[{"Config":"` + config + `"}]`} {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		writer.Write([]byte(content))
	}
	writer.Close()
	return archive.Bytes()
}

func TestEnsureImagesImportsAndVerifies(t *testing.T) {
	daemon := newFakeDaemon()
	manager := newTestManager(t, daemon)
	saved := dockerSave(t, "blobs/sha256/abc")
	exports := 0
	source := ImageSource{
		ID: func(context.Context, string) (string, error) { return "sha256:abc", nil },
		Save: func(context.Context, string) (io.ReadCloser, error) {
			exports++
			return io.NopCloser(bytes.NewReader(saved)), nil
		},
	}
	daemon.importCfg = "sha256:abc"
	if err := manager.EnsureImages(context.Background(), []string{"img", "img"}, source); err != nil {
		t.Fatal(err)
	}
	if exports != 1 || !bytes.Equal(daemon.imported, saved) {
		t.Fatalf("exports=%d imported=%d bytes", exports, len(daemon.imported))
	}
	if err := manager.EnsureImages(context.Background(), []string{"img"}, source); err != nil || exports != 1 {
		t.Fatalf("present image re-exported: %d %v", exports, err)
	}
	daemon.importCfg = "sha256:wrong"
	if err := manager.EnsureImages(context.Background(), []string{"other"}, source); err == nil || !strings.Contains(err.Error(), "want exported") {
		t.Fatalf("digest mismatch = %v", err)
	}
}

func TestPreflight(t *testing.T) {
	manager := newTestManager(t, newFakeDaemon())
	if _, err := manager.Preflight(context.Background(), api.IsolationKataFirecracker, "4.2.0"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Preflight(context.Background(), "other", "4.2.0"); err == nil {
		t.Fatal("isolation mismatch accepted")
	}
	if _, err := manager.Preflight(context.Background(), api.IsolationKataFirecracker, "4.1.0"); err == nil {
		t.Fatal("Kata version mismatch accepted")
	}
}

func TestReportsVersion(t *testing.T) {
	for _, test := range []struct {
		line string
		want bool
	}{
		{"version: 4.2.0, commit: abc", true},
		{"4.2.0", true},
		{"kata 4.2.0.", true},
		{"version: 4.2.01", false},
		{"version: 14.2.0", false},
		{"version: 4.2.0.1", false},
		{"", false},
	} {
		if got := reportsVersion(test.line, "4.2.0"); got != test.want {
			t.Errorf("reportsVersion(%q) = %v, want %v", test.line, got, test.want)
		}
	}
	if reportsVersion("anything", "") {
		t.Error("empty pin matched")
	}
}
