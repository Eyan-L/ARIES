package server

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("error = %v, want code %s", err, code)
	}
}

func TestHealthReportsLimitsAndVersions(t *testing.T) {
	h := newHarness(t)
	reply, err := h.client.Health(context.Background(), &api.HealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.KVM || reply.Isolation != api.IsolationKataFirecracker || reply.ContainerdVersion != "v2.3.6" || reply.StorageLimitMB != 20480 || reply.HelperVersion != "test 1.0" {
		t.Fatalf("health = %+v", reply)
	}
	h.server.kvm = func() error { return errors.New("no kvm") }
	reply, err = h.client.Health(context.Background(), &api.HealthRequest{})
	if err != nil || reply.KVM || reply.KVMError != "no kvm" {
		t.Fatalf("health without KVM = %+v, %v", reply, err)
	}
}

func TestNetworkLifecycle(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, err := h.client.CreateNetwork(ctx, &api.NetworkSpec{Name: "Bad Name", Labels: taskLabels()})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.client.CreateNetwork(ctx, &api.NetworkSpec{Name: "net", Labels: map[string]string{api.LabelRun: "r"}})
	wantCode(t, err, codes.InvalidArgument)

	first := h.network(t, "net-a", true)
	second := h.network(t, "net-b", false)
	if first.Subnet != "10.213.0.0/24" || first.Gateway != "10.213.0.1" || second.Subnet != "10.213.1.0/24" {
		t.Fatalf("subnets = %s %s %s", first.Subnet, first.Gateway, second.Subnet)
	}
	if first.Labels[api.LabelManaged] != "true" {
		t.Fatalf("labels = %v", first.Labels)
	}
	again := h.network(t, "net-a", true)
	if again.Subnet != first.Subnet {
		t.Fatalf("idempotent create changed subnet: %s", again.Subnet)
	}
	_, err = h.client.CreateNetwork(ctx, &api.NetworkSpec{Name: "net-a", Labels: taskLabels(), AllowEgress: false})
	wantCode(t, err, codes.AlreadyExists)
	if !h.net.docker[bridgeName("net-a")] || h.net.docker[bridgeName("net-b")] {
		t.Fatalf("DOCKER-USER rules = %v", h.net.docker)
	}
	if !strings.Contains(h.net.firewall, `iifname "`+bridgeName("net-b")+`" drop`) {
		t.Fatalf("firewall lacks egress drop:\n%s", h.net.firewall)
	}

	if _, err := h.client.Create(ctx, sandboxSpec("sb", "net-a")); err != nil {
		t.Fatal(err)
	}
	inspected, err := h.client.InspectNetwork(ctx, &api.NetworkRef{ID: "net-a"})
	if err != nil || inspected.Members["task-sandbox"] != "10.213.0.2" {
		t.Fatalf("members = %+v, %v", inspected, err)
	}
	_, err = h.client.DeleteNetwork(ctx, &api.NetworkRef{ID: "net-a"})
	wantCode(t, err, codes.FailedPrecondition)
	if _, err := h.client.Delete(ctx, &api.SandboxRef{ID: "sb"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.DeleteNetwork(ctx, &api.NetworkRef{ID: "net-a"}); err != nil {
		t.Fatal(err)
	}
	if h.net.links[bridgeName("net-a")] || h.net.docker[bridgeName("net-a")] || strings.Contains(h.net.firewall, bridgeName("net-a")) {
		t.Fatalf("network leftovers: links %v docker %v\n%s", h.net.links, h.net.docker, h.net.firewall)
	}
	if _, err := h.client.DeleteNetwork(ctx, &api.NetworkRef{ID: "net-a"}); err != nil {
		t.Fatalf("repeated delete: %v", err)
	}
	_, err = h.client.InspectNetwork(ctx, &api.NetworkRef{ID: "net-a"})
	wantCode(t, err, codes.NotFound)
	// The released subnet is reused.
	if reused := h.network(t, "net-c", false); reused.Subnet != "10.213.0.0/24" {
		t.Fatalf("reused subnet = %s", reused.Subnet)
	}
}

func TestSandboxValidation(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.network(t, "net", false)
	cases := map[string]func(*api.SandboxSpec){
		"name":       func(s *api.SandboxSpec) { s.Name = "-bad" },
		"labels":     func(s *api.SandboxSpec) { s.Labels = map[string]string{api.LabelRun: "r"} },
		"reserved":   func(s *api.SandboxSpec) { s.Labels["aries-vmd.network"] = "x" },
		"vcpus":      func(s *api.SandboxSpec) { s.VCPUs = 9 },
		"memory":     func(s *api.SandboxSpec) { s.MemoryMB = 100 },
		"memory-max": func(s *api.SandboxSpec) { s.MemoryMB = 40000 },
		"storage":    func(s *api.SandboxSpec) { s.StorageMB = 20481 },
		"workdir":    func(s *api.SandboxSpec) { s.Workdir = "relative" },
		"env":        func(s *api.SandboxSpec) { s.Env = []string{"NOEQUALS"} },
		"alias":      func(s *api.SandboxSpec) { s.Aliases = []string{"Bad Alias"} },
	}
	for name, mutate := range cases {
		spec := sandboxSpec("sb", "net")
		mutate(spec)
		_, err := h.client.Create(ctx, spec)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: error = %v, want InvalidArgument", name, err)
		}
	}
	spec := sandboxSpec("sb", "net")
	spec.Image = "missing:1"
	_, err := h.client.Create(ctx, spec)
	wantCode(t, err, codes.FailedPrecondition)
	_, err = h.client.Create(ctx, sandboxSpec("sb", "no-such-net"))
	wantCode(t, err, codes.FailedPrecondition)
	if _, found, _ := h.server.store.sandbox("sb"); found {
		t.Fatal("rejected create left a record")
	}
}

func TestSandboxCreateInjectsAndIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.network(t, "net", true)
	spec := sandboxSpec("sb", "net")
	info, err := h.client.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != "sb" || info.IP != "10.213.0.2" || info.VCPUs != 2 || info.MemoryMB != 4096 || !info.Init || info.Isolation != api.IsolationKataFirecracker || info.Status != api.StatusCreated || info.ConfigDigest != "sha256:cfg" {
		t.Fatalf("info = %+v", info)
	}
	container := h.engine.containers["sb"]
	wantArgs := []string{api.HelperPath, "init", "--", "/bin/sleep", "infinity"}
	if !slices.Equal(container.spec.Args, wantArgs) {
		t.Fatalf("args = %v", container.spec.Args)
	}
	if !slices.Equal(container.spec.Env, []string{"PATH=/usr/bin", "A=task", "B=1"}) || container.spec.Cwd != "/work" || !container.spec.NoNewPrivileges {
		t.Fatalf("process = %+v", container.spec)
	}
	if container.spec.Annotations[annotationVCPUs] != "2" || container.spec.Annotations[annotationMemory] != "4096" {
		t.Fatalf("annotations = %v", container.spec.Annotations)
	}
	if container.spec.Labels[api.LabelManaged] != "true" || container.spec.Labels[api.LabelRun] != "run-1" {
		t.Fatalf("labels = %v", container.spec.Labels)
	}
	if container.user.UID != 1000 || container.user.GID != 1000 || !slices.Equal(container.user.AdditionalGIDs, []uint32{999}) {
		t.Fatalf("image user = %+v", container.user)
	}
	read := func(name string) string {
		content, err := os.ReadFile(filepath.Join(container.root, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(content)
	}
	if !strings.Contains(read("etc/hosts"), "10.213.0.2\ttask-sandbox sb") || read("etc/resolv.conf") != "nameserver 9.9.9.9\n" || read(".aries/helper") != "#!helper" {
		t.Fatalf("injected files: hosts %q resolv %q", read("etc/hosts"), read("etc/resolv.conf"))
	}
	stateInfo, err := os.Stat(filepath.Join(container.root, ".aries/state"))
	if err != nil || stateInfo.Mode()&os.ModeSticky == 0 || stateInfo.Mode().Perm() != 0o733 {
		t.Fatalf("state dir mode = %v, %v", stateInfo, err)
	}
	if _, err := os.Stat(filepath.Join(container.root, "work")); err != nil {
		t.Fatalf("workdir not created: %v", err)
	}

	again, err := h.client.Create(ctx, spec)
	if err != nil || again.IP != info.IP {
		t.Fatalf("idempotent create = %+v, %v", again, err)
	}
	changed := sandboxSpec("sb", "net")
	changed.Args = []string{"1"}
	_, err = h.client.Create(ctx, changed)
	wantCode(t, err, codes.AlreadyExists)

	second, err := h.client.Create(ctx, sandboxSpec("sb2", "net"))
	if err != nil || second.IP != "10.213.0.3" {
		t.Fatalf("second sandbox = %+v, %v", second, err)
	}
}

func TestCommandLineFollowsDockerRules(t *testing.T) {
	image := imageConfig{Entrypoint: []string{"/entry"}, Cmd: []string{"cmd"}}
	cases := []struct {
		spec api.SandboxSpec
		want []string
	}{
		{api.SandboxSpec{}, []string{"/entry", "cmd"}},
		{api.SandboxSpec{Args: []string{"x"}}, []string{"/entry", "x"}},
		{api.SandboxSpec{Entrypoint: []string{"/bin/sleep"}}, []string{"/bin/sleep"}},
		{api.SandboxSpec{Entrypoint: []string{"/bin/sleep"}, Args: []string{"1"}}, []string{"/bin/sleep", "1"}},
	}
	for _, c := range cases {
		if got := commandLine(c.spec, image); !slices.Equal(got, c.want) {
			t.Errorf("commandLine(%+v) = %v, want %v", c.spec, got, c.want)
		}
	}
}

func TestSandboxStartInspectAndDelete(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.running(t, "sb")
	info, err := h.client.Inspect(ctx, &api.SandboxRef{ID: "sb"})
	if err != nil || info.Status != api.StatusRunning {
		t.Fatalf("inspect = %+v, %v", info, err)
	}
	stats, err := h.client.Stats(ctx, &api.StatsRequest{Labels: map[string]string{api.LabelRun: "run-1"}})
	if err != nil || len(stats.Sandboxes) != 1 || stats.Sandboxes[0].MemoryLimitBytes != 7 {
		t.Fatalf("stats = %+v, %v", stats, err)
	}
	record, _, _ := h.server.store.sandbox("sb")
	if _, err := h.client.Delete(ctx, &api.SandboxRef{ID: "sb"}); err != nil {
		t.Fatal(err)
	}
	if h.net.netns[record.NetNS] || h.net.links[record.HostVeth] {
		t.Fatal("network namespace or veth remains")
	}
	network, _, _ := h.server.store.network("sb-net")
	if len(network.Leases) != 0 {
		t.Fatalf("lease not released: %v", network.Leases)
	}
	_, err = h.client.Inspect(ctx, &api.SandboxRef{ID: "sb"})
	wantCode(t, err, codes.NotFound)
	if _, err := h.client.Delete(ctx, &api.SandboxRef{ID: "sb"}); err != nil {
		t.Fatalf("repeated delete: %v", err)
	}
	_, err = h.client.Start(ctx, &api.SandboxRef{ID: "sb"})
	wantCode(t, err, codes.NotFound)
}

func TestDeleteKeepsRecordWhenAbsenceIsUnconfirmed(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.running(t, "sb")
	h.net.keepVeth = true
	_, err := h.client.Delete(ctx, &api.SandboxRef{ID: "sb"})
	wantCode(t, err, codes.Internal)
	if record, found, _ := h.server.store.sandbox("sb"); !found || record.Phase != phaseDeleting {
		t.Fatalf("record after failed delete = %+v found=%v", record, found)
	}
	h.net.keepVeth = false
	record, _, _ := h.server.store.sandbox("sb")
	delete(h.net.links, record.HostVeth)

	var pending atomic.Bool
	pending.Store(true)
	h.server.processesReferencing = func(string) ([]int, error) {
		if pending.Load() {
			return []int{42}, nil
		}
		return nil, nil
	}
	time.AfterFunc(300*time.Millisecond, func() { pending.Store(false) })
	if _, err := h.client.Delete(ctx, &api.SandboxRef{ID: "sb"}); err != nil {
		t.Fatalf("retried delete: %v", err)
	}
	if _, found, _ := h.server.store.sandbox("sb"); found {
		t.Fatal("record remains after confirmed delete")
	}

	h.running(t, "sb2")
	absent := false
	h.engine.absent = &absent
	_, err = h.client.Delete(ctx, &api.SandboxRef{ID: "sb2"})
	wantCode(t, err, codes.Internal)
	if _, found, _ := h.server.store.sandbox("sb2"); !found {
		t.Fatal("record removed although the container remains")
	}
}

func TestDeleteRefusesUnownedContainer(t *testing.T) {
	h := newHarness(t)
	h.engine.containers["foreign"] = &fakeContainer{spec: containerSpec{ID: "foreign", Labels: map[string]string{}}}
	_, err := h.client.Delete(context.Background(), &api.SandboxRef{ID: "foreign"})
	wantCode(t, err, codes.FailedPrecondition)
	if _, ok := h.engine.containers["foreign"]; !ok {
		t.Fatal("unowned container was deleted")
	}
}

type execOutcome struct {
	stdout string
	exit   *api.ExecExit
}

func runExec(t *testing.T, h *harness, start *api.ExecStart, stdin []byte, cancelAfter time.Duration) execOutcome {
	t.Helper()
	stream, err := h.client.Exec(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&api.ExecFrame{Start: start}); err != nil {
		t.Fatal(err)
	}
	if stdin != nil {
		stream.Send(&api.ExecFrame{Stdin: stdin})
		stream.Send(&api.ExecFrame{StdinEOF: true})
	}
	if cancelAfter > 0 {
		go func() {
			time.Sleep(cancelAfter)
			stream.Send(&api.ExecFrame{Cancel: true})
		}()
	}
	var outcome execOutcome
	for outcome.exit == nil {
		frame, err := stream.Recv()
		if err != nil {
			t.Fatalf("exec recv: %v", err)
		}
		outcome.stdout += string(frame.Stdout)
		outcome.exit = frame.Exit
	}
	return outcome
}

func TestExecRunsWrappedCommand(t *testing.T) {
	h := newHarness(t)
	h.running(t, "sb")
	h.engine.exec = func(_ string, process processSpec, stdin io.Reader, stdout, _ io.Writer) *fakeProcess {
		proc := newFakeProcess()
		go func() {
			io.Copy(stdout, stdin)
			proc.exit(3)
		}()
		return proc
	}
	outcome := runExec(t, h, &api.ExecStart{Sandbox: "sb", Args: []string{"/bin/cat"}, Env: []string{"A=exec"}, User: "1000:5", Stdin: true}, []byte("hello"), 0)
	if outcome.stdout != "hello" || outcome.exit.Code != 3 || outcome.exit.Canceled {
		t.Fatalf("outcome = %+v exit %+v", outcome, outcome.exit)
	}
	process := h.engine.execs[len(h.engine.execs)-1]
	if !slices.Equal(process.Args[:3], []string{api.HelperPath, "run", "--state"}) || !strings.HasPrefix(process.Args[3], api.StateDir+"/") || !slices.Equal(process.Args[4:], []string{"--", "/bin/cat"}) {
		t.Fatalf("args = %v", process.Args)
	}
	if process.User.UID != 1000 || process.User.GID != 5 || process.Cwd != "/work" || !slices.Contains(process.Env, "A=exec") {
		t.Fatalf("process = %+v", process)
	}

	stream, _ := h.client.Exec(context.Background())
	stream.Send(&api.ExecFrame{Start: &api.ExecStart{Sandbox: "sb", Args: []string{"/bin/true"}, User: "app"}})
	_, err := stream.Recv()
	wantCode(t, err, codes.InvalidArgument)
	stream, _ = h.client.Exec(context.Background())
	stream.Send(&api.ExecFrame{Start: &api.ExecStart{Sandbox: "missing", Args: []string{"/bin/true"}}})
	_, err = stream.Recv()
	wantCode(t, err, codes.NotFound)
}

// cancelHarness runs a command that blocks until the helper kill (or a
// direct SIGKILL) ends it, with killCode as the helper's exit status.
func cancelHarness(t *testing.T, killCode int, wrapperKillable bool) (*harness, *fakeProcess) {
	h := newHarness(t)
	h.running(t, "sb")
	command := newFakeProcess()
	command.killable = wrapperKillable
	h.engine.exec = func(_ string, process processSpec, _ io.Reader, _, _ io.Writer) *fakeProcess {
		switch process.Args[1] {
		case "run":
			return command
		case "kill":
			if process.User.UID != 0 {
				t.Errorf("helper kill ran as %+v", process.User)
			}
			if killCode == 0 {
				command.exit(137)
			}
			return exitedProcess(killCode)
		}
		t.Errorf("unexpected exec %v", process.Args)
		return exitedProcess(1)
	}
	return h, command
}

func TestExecCancelConfirmedByHelper(t *testing.T) {
	h, command := cancelHarness(t, 0, true)
	outcome := runExec(t, h, &api.ExecStart{Sandbox: "sb", Args: []string{"/bin/sleep", "100"}}, nil, 50*time.Millisecond)
	if !outcome.exit.Canceled || !outcome.exit.CancelConfirmed || outcome.exit.Code != -1 || !command.deleted {
		t.Fatalf("exit = %+v deleted=%v", outcome.exit, command.deleted)
	}
	if len(command.killed) != 0 {
		t.Fatal("wrapper was signalled although the helper confirmed termination")
	}
	if h.engine.containers["sb"].status != api.StatusRunning {
		t.Fatal("cancellation stopped the sandbox")
	}
}

func TestExecCancelBeforeStateRecordedKillsExec(t *testing.T) {
	h, command := cancelHarness(t, api.HelperExitStateMissing, true)
	outcome := runExec(t, h, &api.ExecStart{Sandbox: "sb", Args: []string{"/bin/sleep", "100"}}, nil, 50*time.Millisecond)
	if !outcome.exit.Canceled || !outcome.exit.CancelConfirmed {
		t.Fatalf("exit = %+v", outcome.exit)
	}
	if signal := <-command.killed; signal != syscall.SIGKILL {
		t.Fatalf("signal = %v", signal)
	}
}

func TestExecCancelUnconfirmed(t *testing.T) {
	h, _ := cancelHarness(t, api.HelperExitUnconfirmed, false)
	outcome := runExec(t, h, &api.ExecStart{Sandbox: "sb", Args: []string{"/bin/sleep", "100"}}, nil, 50*time.Millisecond)
	if !outcome.exit.Canceled || outcome.exit.CancelConfirmed || !strings.Contains(outcome.exit.Error, "exit 71") {
		t.Fatalf("exit = %+v", outcome.exit)
	}
}

func TestGetArchiveDistinguishesMissingPathFromMissingSandbox(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.running(t, "sb")
	var payload bytes.Buffer
	writer := tar.NewWriter(&payload)
	writer.WriteHeader(&tar.Header{Name: "f", Mode: 0o644, Size: 2})
	writer.Write([]byte("hi"))
	writer.Close()
	h.engine.exec = func(_ string, process processSpec, _ io.Reader, stdout, _ io.Writer) *fakeProcess {
		if process.Args[1] != "tar-create" || process.User.UID != 0 {
			t.Errorf("unexpected exec %+v", process)
		}
		if process.Args[2] == "/missing" {
			return exitedProcess(api.HelperExitNotFound)
		}
		stdout.Write([]byte(`{"name":"f","size":2,"mode":420}` + "\n"))
		stdout.Write(payload.Bytes())
		return exitedProcess(0)
	}
	stream, err := h.client.GetArchive(ctx, &api.ArchiveRequest{Sandbox: "sb", Path: "/tmp/f"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil || first.Stat == nil || first.Stat.Size != 2 || first.Stat.Mode != 0o644 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	var data []byte
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, chunk.Data...)
	}
	if !bytes.Equal(data, payload.Bytes()) {
		t.Fatal("archive payload differs")
	}

	stream, _ = h.client.GetArchive(ctx, &api.ArchiveRequest{Sandbox: "sb", Path: "/missing"})
	first, err = stream.Recv()
	if err != nil || !first.Missing {
		t.Fatalf("missing source = %+v, %v", first, err)
	}
	stream, _ = h.client.GetArchive(ctx, &api.ArchiveRequest{Sandbox: "gone", Path: "/tmp/f"})
	_, err = stream.Recv()
	wantCode(t, err, codes.NotFound)
}

func TestPutArchive(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.running(t, "sb")
	var received bytes.Buffer
	h.engine.exec = func(_ string, process processSpec, stdin io.Reader, _, _ io.Writer) *fakeProcess {
		if !slices.Equal(process.Args[1:4], []string{"tar-extract", "--owner-user", "app"}) {
			t.Errorf("args = %v", process.Args)
		}
		proc := newFakeProcess()
		go func() {
			io.Copy(&received, stdin)
			if process.Args[4] == "/missing" {
				proc.exit(api.HelperExitNotFound)
			} else {
				proc.exit(0)
			}
		}()
		return proc
	}
	put := func(path string) error {
		stream, err := h.client.PutArchive(ctx)
		if err != nil {
			return err
		}
		stream.Send(&api.ArchiveChunk{Sandbox: "sb", Path: path, Data: []byte("ab")})
		stream.Send(&api.ArchiveChunk{Data: []byte("cd")})
		_, err = stream.CloseAndRecv()
		return err
	}
	if err := put("/dest"); err != nil || received.String() != "abcd" {
		t.Fatalf("put = %v, received %q", err, received.String())
	}
	wantCode(t, put("/missing"), codes.FailedPrecondition)
}

func TestImportAndInspectImage(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	info, err := h.client.InspectImage(ctx, &api.ImageRequest{Reference: "new:1"})
	if err != nil || info.Present {
		t.Fatalf("absent image = %+v, %v", info, err)
	}
	stream, err := h.client.ImportImage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream.Send(&api.ImageChunk{Reference: "new:1", Data: []byte("tar")})
	stream.Send(&api.ImageChunk{Data: []byte("more")})
	imported, err := stream.CloseAndRecv()
	if err != nil || !imported.Present || imported.ConfigDigest != "sha256:imported" {
		t.Fatalf("import = %+v, %v", imported, err)
	}
	info, err = h.client.InspectImage(ctx, &api.ImageRequest{Reference: "example/task:1"})
	if err != nil || info.TargetDigest != "sha256:target" {
		t.Fatalf("inspect = %+v, %v", info, err)
	}
}

func TestCleanupRemovesOnlyTheRun(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.running(t, "sb")
	other, err := h.client.CreateNetwork(ctx, &api.NetworkSpec{Name: "other", Labels: map[string]string{api.LabelRun: "run-2", api.LabelTask: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := h.client.Cleanup(ctx, &api.CleanupRequest{Run: "run-1"})
	if err != nil || !slices.Equal(reply.Sandboxes, []string{"sb"}) || !slices.Equal(reply.Networks, []string{"sb-net"}) {
		t.Fatalf("cleanup = %+v, %v", reply, err)
	}
	if _, err := h.client.InspectNetwork(ctx, &api.NetworkRef{ID: other.ID}); err != nil {
		t.Fatalf("other run's network removed: %v", err)
	}
}

func TestResolveUser(t *testing.T) {
	passwd := "root:x:0:0::/root:/bin/sh\n# comment\napp:x:1000:1001::/home/app:/bin/sh\nbroken:x:abc:1\n"
	group := "root:x:0:\napp:x:1001:\nwheel:x:10:app,other\nstaff:x:50:\n"
	cases := []struct {
		user string
		want identity
		err  bool
	}{
		{"", identity{}, false},
		{"0", identity{}, false},
		{"1000", identity{UID: 1000, GID: 1001, AdditionalGIDs: []uint32{10}}, false},
		{"app", identity{UID: 1000, GID: 1001, AdditionalGIDs: []uint32{10}}, false},
		{"1000:50", identity{UID: 1000, GID: 50, AdditionalGIDs: []uint32{10}}, false},
		{"app:staff", identity{UID: 1000, GID: 50, AdditionalGIDs: []uint32{10}}, false},
		{"4242", identity{UID: 4242}, false},
		{"4242:7", identity{UID: 4242, GID: 7}, false},
		{"nobody", identity{}, true},
		{"app:nogroup", identity{}, true},
		{"1000:", identity{}, true},
	}
	for _, c := range cases {
		got, err := resolveUser(c.user, passwd, group)
		if (err != nil) != c.err || err == nil && (got.UID != c.want.UID || got.GID != c.want.GID || !slices.Equal(got.AdditionalGIDs, c.want.AdditionalGIDs)) {
			t.Errorf("resolveUser(%q) = %+v, %v; want %+v err=%v", c.user, got, err, c.want, c.err)
		}
	}
}

func TestRenderFirewallGolden(t *testing.T) {
	got := renderFirewall([]networkRecord{
		{Spec: api.NetworkSpec{Name: "b", AllowEgress: false}, Bridge: "avbbbbb", Subnet: "10.213.1.0/24"},
		{Spec: api.NetworkSpec{Name: "a", AllowEgress: true}, Bridge: "avbaaaa", Subnet: "10.213.0.0/24"},
	})
	want := `table inet aries_vmd {}
delete table inet aries_vmd
table inet aries_vmd {
	set bridges {
		type ifname
		elements = { "avbaaaa", "avbbbbb" }
	}
	chain forward {
		type filter hook forward priority filter; policy accept;
		iifname @bridges oifname @bridges drop
		iifname @bridges oifname "docker0" drop
		iifname @bridges oifname "br-*" drop
		iifname "avbaaaa" accept
		oifname "avbaaaa" ct state established,related accept
		iifname "avbbbbb" drop
		oifname @bridges drop
	}
	chain input {
		type filter hook input priority filter; policy accept;
		iifname @bridges ct state established,related accept
		iifname @bridges drop
	}
	chain postrouting {
		type nat hook postrouting priority srcnat; policy accept;
		ip saddr 10.213.0.0/24 oifname != @bridges masquerade
	}
}
`
	if got != want {
		t.Fatalf("ruleset:\n%s\nwant:\n%s", got, want)
	}
	if empty := renderFirewall(nil); strings.Contains(empty, "elements") || strings.Contains(empty, "masquerade") {
		t.Fatalf("empty ruleset:\n%s", empty)
	}
}

func TestAddressAllocation(t *testing.T) {
	record := networkRecord{Spec: api.NetworkSpec{Name: "n"}, Subnet: "10.213.4.0/24", Gateway: "10.213.4.1", Leases: map[string]string{"a": "10.213.4.2", "c": "10.213.4.4"}}
	address, err := allocateAddress(record)
	if err != nil || address.String() != "10.213.4.3" {
		t.Fatalf("address = %v, %v", address, err)
	}
	record.Leases = map[string]string{}
	for host := 2; host < 255; host++ {
		record.Leases[string(rune(host))] = netip.AddrFrom4([4]byte{10, 213, 4, byte(host)}).String()
	}
	_, err = allocateAddress(record)
	wantCode(t, err, codes.ResourceExhausted)

	pool := netip.MustParsePrefix("10.213.0.0/23")
	first, _ := allocateSubnet(pool, nil)
	second, _ := allocateSubnet(pool, []networkRecord{{Subnet: first.String()}})
	if first.String() != "10.213.0.0/24" || second.String() != "10.213.1.0/24" {
		t.Fatalf("subnets = %s %s", first, second)
	}
	_, err = allocateSubnet(pool, []networkRecord{{Subnet: first.String()}, {Subnet: second.String()}})
	wantCode(t, err, codes.ResourceExhausted)
}

func TestParseNameservers(t *testing.T) {
	content := "# generated\nnameserver 127.0.0.53\nnameserver 10.0.0.2\nsearch example.com\nnameserver ::1\nnameserver 2001:db8::1\nnameserver bogus\n"
	if got := parseNameservers(content); !slices.Equal(got, []string{"10.0.0.2", "2001:db8::1"}) {
		t.Fatalf("nameservers = %v", got)
	}
	if renderResolvConf([]string{"1.1.1.1"}) != "nameserver 1.1.1.1\n" {
		t.Fatal("resolv.conf rendering")
	}
}

func TestScanProcesses(t *testing.T) {
	root := t.TempDir()
	write := func(pid, cmdline string) {
		os.MkdirAll(filepath.Join(root, pid), 0o755)
		os.WriteFile(filepath.Join(root, pid, "cmdline"), []byte(cmdline), 0o644)
	}
	write("10", "/opt/kata/bin/containerd-shim-kata-v2\x00-namespace\x00aries-vmd\x00-id\x00sb\x00")
	write("11", "/opt/kata/bin/firecracker\x00--id\x00sb-other\x00")
	write("12", "/usr/bin/sleep\x00sb\x00")
	write("self", "x")
	pids, err := scanProcessesIn(root, "sb")
	if err != nil || !slices.Equal(pids, []int{10, 11}) {
		t.Fatalf("pids = %v, %v", pids, err)
	}
}

func TestHostNetworkArgv(t *testing.T) {
	var calls []string
	sys := t.TempDir()
	h := &ipHostNetwork{sysRoot: sys, run: func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		if len(args) > 3 && args[2] == "-C" && args[3] == "DOCKER-USER" {
			return nil, errors.New("absent")
		}
		return nil, nil
	}}
	ctx := context.Background()
	if err := h.CreateNetNS(ctx, netnsAttachment{NetNS: "aries-1", HostVeth: "avh1", PeerVeth: "avp1", Bridge: "avb1", AddressCIDR: "10.213.0.2/24", Gateway: "10.213.0.1"}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ip netns add aries-1",
		"ip link add name avh1 type veth peer name avp1 netns aries-1",
		"ip link set dev avh1 master avb1",
		"ip link set dev avh1 up",
		"ip -n aries-1 link set dev avp1 name eth0",
		"ip -n aries-1 addr add 10.213.0.2/24 dev eth0",
		"ip -n aries-1 link set dev eth0 up",
		"ip -n aries-1 link set dev lo up",
		"ip -n aries-1 route add default via 10.213.0.1",
	}
	if !slices.Equal(calls, want) {
		t.Fatalf("calls = %q", calls)
	}
	calls = nil
	// Absent links and namespaces are not deleted again.
	if err := h.DeleteNetNS(ctx, "aries-1", "avh1"); err != nil || len(calls) != 0 {
		t.Fatalf("delete absent = %v calls %q", err, calls)
	}
	os.MkdirAll(filepath.Join(sys, "sys/class/net/avh1"), 0o755)
	os.MkdirAll(filepath.Join(sys, "var/run/netns"), 0o755)
	os.WriteFile(filepath.Join(sys, "var/run/netns/aries-1"), nil, 0o644)
	if err := h.DeleteNetNS(ctx, "aries-1", "avh1"); err != nil || !slices.Equal(calls, []string{"ip link del dev avh1", "ip netns del aries-1"}) {
		t.Fatalf("delete = %v calls %q", err, calls)
	}
	calls = nil
	if err := h.SetDockerUserAccept(ctx, "avb1", true); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(calls, "iptables -t filter -I DOCKER-USER 1 -i avb1 -j ACCEPT") {
		t.Fatalf("calls = %q", calls)
	}
}
