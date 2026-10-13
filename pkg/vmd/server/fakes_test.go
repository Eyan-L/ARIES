package server

import (
	"context"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type fakeContainer struct {
	spec   containerSpec
	status string
	user   identity
	root   string
}

type fakeEngine struct {
	mu         sync.Mutex
	t          *testing.T
	images     map[string]imageConfig
	containers map[string]*fakeContainer
	snapshots  map[string]bool
	// absent overrides ContainerAbsent when set.
	absent *bool
	// exec handles one exec; nil uses a process that exits 0.
	exec  func(id string, process processSpec, stdin io.Reader, stdout, stderr io.Writer) *fakeProcess
	execs []processSpec
}

func newFakeEngine(t *testing.T) *fakeEngine {
	return &fakeEngine{t: t, images: map[string]imageConfig{
		"example/task:1": {ConfigDigest: "sha256:cfg", TargetDigest: "sha256:target", Unpacked: true, Env: []string{"PATH=/usr/bin", "A=image"}, Cmd: []string{"/bin/sh"}, WorkingDir: "/app", User: "app"},
	}, containers: map[string]*fakeContainer{}, snapshots: map[string]bool{}}
}

func (e *fakeEngine) Version(context.Context) (string, error) { return "v2.3.6", nil }
func (e *fakeEngine) ImportImage(_ context.Context, ref string, archive io.Reader) error {
	if _, err := io.Copy(io.Discard, archive); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.images[ref] = imageConfig{ConfigDigest: "sha256:imported", Unpacked: true}
	return nil
}
func (e *fakeEngine) InspectImage(_ context.Context, ref string) (imageConfig, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	image, ok := e.images[ref]
	if !ok {
		return imageConfig{}, errEngineNotFound
	}
	return image, nil
}
func (e *fakeEngine) CreateContainer(_ context.Context, spec containerSpec) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	root := filepath.Join(e.t.TempDir(), "rootfs")
	if err := os.MkdirAll(filepath.Join(root, "etc"), 0o755); err != nil {
		return err
	}
	os.WriteFile(filepath.Join(root, "etc", "passwd"), []byte("root:x:0:0::/root:/bin/sh\napp:x:1000:1000::/home/app:/bin/sh\n"), 0o644)
	os.WriteFile(filepath.Join(root, "etc", "group"), []byte("root:x:0:\napp:x:1000:\ndocker:x:999:app\n"), 0o644)
	e.containers[spec.ID] = &fakeContainer{spec: spec, status: api.StatusCreated, root: root}
	e.snapshots[spec.ID] = true
	return nil
}
func (e *fakeEngine) PrepareRootfs(_ context.Context, id string, inject func(*os.Root) error) error {
	e.mu.Lock()
	container := e.containers[id]
	e.mu.Unlock()
	root, err := os.OpenRoot(container.root)
	if err != nil {
		return err
	}
	defer root.Close()
	return inject(root)
}
func (e *fakeEngine) SetProcessUser(_ context.Context, id string, user identity) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.containers[id].user = user
	return nil
}
func (e *fakeEngine) InspectContainer(_ context.Context, id string) (containerState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	container, ok := e.containers[id]
	if !ok {
		return containerState{}, errEngineNotFound
	}
	return containerState{Runtime: "io.containerd.kata-fc.v2", Labels: container.spec.Labels, Args: container.spec.Args, Status: container.status}, nil
}
func (e *fakeEngine) StartTask(_ context.Context, id string, _, _ io.Writer) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.containers[id].status = api.StatusRunning
	return nil
}
func (e *fakeEngine) Exec(_ context.Context, id, _ string, process processSpec, stdin io.Reader, stdout, stderr io.Writer) (execProcess, error) {
	e.mu.Lock()
	e.execs = append(e.execs, process)
	handler := e.exec
	e.mu.Unlock()
	if handler == nil {
		return exitedProcess(0), nil
	}
	return handler(id, process, stdin, stdout, stderr), nil
}
func (e *fakeEngine) DeleteContainer(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.containers, id)
	delete(e.snapshots, id)
	return nil
}
func (e *fakeEngine) ContainerAbsent(_ context.Context, id string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.absent != nil {
		return *e.absent, nil
	}
	_, container := e.containers[id]
	return !container && !e.snapshots[id], nil
}
func (e *fakeEngine) Metrics(context.Context, string) (metrics, error) {
	return metrics{CPUUsageNanoseconds: 5, MemoryUsageBytes: 6, MemoryLimitBytes: 7}, nil
}

type fakeProcess struct {
	done     chan struct{}
	once     sync.Once
	code     int
	killable bool
	killed   chan syscall.Signal
	deleted  bool
}

func newFakeProcess() *fakeProcess {
	return &fakeProcess{done: make(chan struct{}), killed: make(chan syscall.Signal, 4), killable: true}
}

func exitedProcess(code int) *fakeProcess {
	process := newFakeProcess()
	process.exit(code)
	return process
}

func (p *fakeProcess) exit(code int) {
	p.once.Do(func() {
		p.code = code
		close(p.done)
	})
}

func (p *fakeProcess) Wait(ctx context.Context) (int, error) {
	select {
	case <-p.done:
		return p.code, nil
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}
func (p *fakeProcess) Kill(_ context.Context, signal syscall.Signal) error {
	p.killed <- signal
	if p.killable {
		p.exit(137)
	}
	return nil
}
func (p *fakeProcess) Drain(float64)                {}
func (p *fakeProcess) Delete(context.Context) error { p.deleted = true; return nil }

type fakeHostNetwork struct {
	mu       sync.Mutex
	links    map[string]bool
	netns    map[string]bool
	firewall string
	docker   map[string]bool
	// keepVeth leaves host veths behind on deletion.
	keepVeth bool
}

func newFakeHostNetwork() *fakeHostNetwork {
	return &fakeHostNetwork{links: map[string]bool{}, netns: map[string]bool{}, docker: map[string]bool{}}
}

func (h *fakeHostNetwork) EnsureForwarding(context.Context) error { return nil }
func (h *fakeHostNetwork) CreateBridge(_ context.Context, bridge, _ string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.links[bridge] = true
	return nil
}
func (h *fakeHostNetwork) DeleteBridge(_ context.Context, bridge string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.links, bridge)
	return nil
}
func (h *fakeHostNetwork) LinkExists(_ context.Context, name string) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.links[name], nil
}
func (h *fakeHostNetwork) CreateNetNS(_ context.Context, attachment netnsAttachment) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.netns[attachment.NetNS] = true
	h.links[attachment.HostVeth] = true
	return nil
}
func (h *fakeHostNetwork) DeleteNetNS(_ context.Context, netns, veth string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.netns, netns)
	if !h.keepVeth {
		delete(h.links, veth)
	}
	return nil
}
func (h *fakeHostNetwork) NetNSExists(_ context.Context, netns string) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.netns[netns], nil
}
func (h *fakeHostNetwork) ApplyFirewall(_ context.Context, ruleset string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.firewall = ruleset
	return nil
}
func (h *fakeHostNetwork) SetDockerUserAccept(_ context.Context, bridge string, present bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.docker[bridge] = present
	return nil
}

type harness struct {
	server *Server
	engine *fakeEngine
	net    *fakeHostNetwork
	client *api.Client
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	helper := filepath.Join(dir, "helper")
	if err := os.WriteFile(helper, []byte("#!helper"), 0o755); err != nil {
		t.Fatal(err)
	}
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	engine := newFakeEngine(t)
	network := newFakeHostNetwork()
	server, err := New(Config{
		StateDir: filepath.Join(dir, "state"), Runtime: "io.containerd.kata-fc.v2", Snapshotter: "devmapper", HelperPath: helper,
		SubnetPool: netip.MustParsePrefix("10.213.0.0/16"), Nameservers: []string{"9.9.9.9"}, StorageLimitMB: 20480,
		MaxVCPUs: 8, MaxMemoryMB: 32768, DefaultVCPUs: 2, DefaultMemoryMB: 4096, CleanupTimeout: 5 * time.Second, Logger: logger,
	}, engine, network)
	if err != nil {
		t.Fatal(err)
	}
	server.processesReferencing = func(string) ([]int, error) { return nil, nil }
	server.version = func(context.Context, string, ...string) string { return "test 1.0" }
	server.kvm = func() error { return nil }
	// Unprivileged tests cannot chown to root.
	server.chown = func(*os.Root, string) error { return nil }
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer(api.ServerOptions()...)
	api.Register(grpcServer, server)
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough:///vmd",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(api.CallOptions()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &harness{server: server, engine: engine, net: network, client: api.NewClient(conn)}
}

func taskLabels() map[string]string {
	return map[string]string{api.LabelRun: "run-1", api.LabelTask: "task-1"}
}

func (h *harness) network(t *testing.T, name string, egress bool) *api.NetworkInfo {
	t.Helper()
	info, err := h.client.CreateNetwork(context.Background(), &api.NetworkSpec{Name: name, Labels: taskLabels(), AllowEgress: egress})
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func sandboxSpec(name, network string) *api.SandboxSpec {
	return &api.SandboxSpec{
		Name: name, Image: "example/task:1", Network: network, Workdir: "/work", Env: []string{"A=task", "B=1"},
		Labels: taskLabels(), Entrypoint: []string{"/bin/sleep"}, Args: []string{"infinity"},
		NoNewPrivileges: true, Aliases: []string{"task-sandbox"},
	}
}

// running creates and starts a sandbox on a fresh network.
func (h *harness) running(t *testing.T, name string) {
	t.Helper()
	h.network(t, name+"-net", false)
	if _, err := h.client.Create(context.Background(), sandboxSpec(name, name+"-net")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.Start(context.Background(), &api.SandboxRef{ID: name}); err != nil {
		t.Fatal(err)
	}
}
