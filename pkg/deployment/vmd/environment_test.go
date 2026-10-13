package vmd

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/deployment"
)

type fakeHarnessEnvironment struct {
	calls    *[]string
	startErr error
	stopErr  error
	request  core.SandboxRequest
}

func (e *fakeHarnessEnvironment) Start(_ context.Context, request core.SandboxRequest) (core.HarnessConnectivity, error) {
	*e.calls = append(*e.calls, "harness-start")
	e.request = request
	return core.HarnessConnectivity{Placement: core.RuntimePlacement{DockerNetwork: "aries-net-1"}}, e.startErr
}
func (e *fakeHarnessEnvironment) Validate(context.Context) error { return nil }
func (e *fakeHarnessEnvironment) Stop(context.Context) error {
	*e.calls = append(*e.calls, "harness-stop")
	return e.stopErr
}
func (e *fakeHarnessEnvironment) BridgeListen(context.Context) (core.BridgeListen, error) {
	return core.BridgeListen{BindHost: "127.0.0.1", AdvertiseHost: "127.0.0.1"}, nil
}

func newEnvironment(t *testing.T) (*fakeDaemon, *fakeHarnessEnvironment, deployment.TaskEnvironment, *[]string) {
	t.Helper()
	daemon := newFakeDaemon()
	manager := newTestManager(t, daemon)
	calls := &[]string{}
	harness := &fakeHarnessEnvironment{calls: calls}
	return daemon, harness, NewTaskEnvironmentFactory(manager, func() deployment.TaskEnvironment { return harness })(), calls
}

func TestTaskEnvironmentLifecycle(t *testing.T) {
	daemon, harness, environment, calls := newEnvironment(t)
	ctx := context.Background()
	request := core.SandboxRequest{RunID: "run", TaskID: "task", Environment: core.Environment{AllowNetwork: true}}
	connectivity, err := environment.Start(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	network := connectivity.Placement.VMNetwork
	if connectivity.Placement.DockerNetwork != "aries-net-1" || !strings.HasPrefix(network, "aries-vnet-") || connectivity.SearchURL != "" {
		t.Fatalf("connectivity = %+v", connectivity)
	}
	if info := daemon.networks[network]; !info.AllowEgress || info.Labels["aries.run"] != "run" || info.Labels["aries.task"] != "task" {
		t.Fatalf("network = %+v", info)
	}
	if err := environment.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	if listen, err := environment.BridgeListen(ctx); err != nil || listen.BindHost != "127.0.0.1" {
		t.Fatalf("bridge listen = %+v, %v", listen, err)
	}
	if _, err := environment.Start(ctx, request); err == nil {
		t.Fatal("environment reused")
	}
	if err := environment.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := daemon.networks[network]; ok {
		t.Fatal("network remains")
	}
	if got := strings.Join(*calls, ","); got != "harness-start,harness-stop" {
		t.Fatalf("harness calls = %s", got)
	}
	if err := environment.Stop(ctx); err != nil {
		t.Fatalf("repeated stop: %v", err)
	}
	_ = harness
}

func TestTaskEnvironmentRejectsInvalidSearchPortBeforeAllocation(t *testing.T) {
	daemon, _, environment, calls := newEnvironment(t)
	_, err := environment.Start(context.Background(), core.SandboxRequest{RunID: "run", TaskID: "task", Environment: core.Environment{Services: core.TaskServices{SearchPort: 70000}}})
	if err == nil || len(*calls) != 0 || len(daemon.calls) != 0 {
		t.Fatalf("err=%v harness=%v daemon=%v", err, *calls, daemon.calls)
	}
}

func TestTaskEnvironmentStopsAfterFailedStart(t *testing.T) {
	daemon, harness, environment, calls := newEnvironment(t)
	harness.startErr = errors.New("network create lost")
	if _, err := environment.Start(context.Background(), core.SandboxRequest{RunID: "run", TaskID: "task"}); err == nil {
		t.Fatal("start succeeded")
	}
	if err := environment.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(*calls, ",") != "harness-start,harness-stop" || len(daemon.calls) != 0 {
		t.Fatalf("harness=%v daemon=%v", *calls, daemon.calls)
	}
	harness.stopErr = errors.New("still attached")
	_, harness2, environment2, _ := newEnvironment(t)
	harness2.stopErr = errors.New("still attached")
	if _, err := environment2.Start(context.Background(), core.SandboxRequest{RunID: "run", TaskID: "task"}); err != nil {
		t.Fatal(err)
	}
	if err := environment2.Stop(context.Background()); err == nil {
		t.Fatal("unconfirmed harness attachment removal reported success")
	}
	harness2.stopErr = nil
	if err := environment2.Stop(context.Background()); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestSearchForwarderReachesSandboxAlias(t *testing.T) {
	daemon, harness, environment, _ := newEnvironment(t)
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("TCP listen unavailable: %v", err)
	}
	defer upstream.Close()
	go func() {
		for {
			conn, err := upstream.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buffer := make([]byte, 4)
				if _, err := io.ReadFull(conn, buffer); err == nil {
					conn.Write(append([]byte("echo:"), buffer...))
				}
			}()
		}
	}()
	port := upstream.Addr().(*net.TCPAddr).Port
	ctx := context.Background()
	connectivity, err := environment.Start(ctx, core.SandboxRequest{RunID: "run", TaskID: "task", Environment: core.Environment{Services: core.TaskServices{SearchPort: port}}})
	if err != nil {
		t.Fatal(err)
	}
	if harness.request.Environment.Services.SearchPort != 0 {
		t.Fatal("harness attachment saw the sandbox service")
	}
	daemon.mu.Lock()
	daemon.networks[connectivity.Placement.VMNetwork].Members = map[string]string{deployment.TaskSandboxAlias: "127.0.0.1"}
	daemon.mu.Unlock()
	address := strings.TrimPrefix(connectivity.SearchURL, "http://")
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	conn.Write([]byte("ping"))
	reply, err := io.ReadAll(conn)
	conn.Close()
	if err != nil || string(reply) != "echo:ping" {
		t.Fatalf("reply = %q, %v", reply, err)
	}
	if err := environment.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if conn, err := net.Dial("tcp", address); err == nil {
		conn.Close()
		t.Fatal("forwarder still listening")
	}
}
