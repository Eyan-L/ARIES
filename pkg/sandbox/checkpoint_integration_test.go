//go:build integration

package sandbox

import (
	"context"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/moby/moby/client"
	"github.com/sirupsen/logrus"
)

// counterLoop runs detached from its exec session, records its PID, and
// writes an in-memory counter. A process that was restarted rather than
// restored would have a new PID and restart the counter at 1.
const counterLoop = `setsid /bin/sh -c 'echo $$ > /work/loop.pid; i=0; while true; do i=$((i+1)); echo $i > /work/counter; sleep 0.1; done' </dev/null >/dev/null 2>&1 &`

// TestDockerSandboxCheckpointRestorePreservesProcessState checks that the
// local daemon supports the checkpoint sandbox lifecycle: CRIU must stop the
// task container and resume its filesystem and running processes in place.
// It is skipped unless the daemon runs with experimental features; on an
// experimental daemon, a missing or broken CRIU fails the first checkpoint.
func TestDockerSandboxCheckpointRestorePreservesProcessState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	api, err := client.New(client.FromEnv, client.WithUserAgent("aries-checkpoint-integration-test/1"))
	if err != nil {
		t.Fatal(err)
	}
	ping, err := api.Ping(ctx, client.PingOptions{})
	if err != nil {
		t.Fatalf("Docker daemon is required for integration tests: %v", err)
	}
	if !ping.Experimental {
		t.Skip("Docker daemon does not enable experimental features, which checkpoint/restore requires")
	}
	ensureFixtureImage(t, ctx, api)

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	manager, err := newIntegrationManager(t, Options{OutputDir: t.TempDir(), CleanupTimeout: 30 * time.Second, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	live, err := manager.Start(ctx, core.SandboxRequest{
		RunID: "checkpoint-integration", TaskID: "checkpoint-task",
		Environment: core.Environment{Image: fixtureImage, Workdir: "/work", MemoryMB: 64},
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	sandbox := live.(*Sandbox)
	t.Cleanup(func() { _ = manager.Stop(context.Background(), live) })

	assertExec(t, ctx, sandbox, core.Command{Path: "/bin/sh", Args: []string{"-c", "echo kept > /work/marker"}}, 0, "", "")
	assertExec(t, ctx, sandbox, core.Command{Path: "/bin/sh", Args: []string{"-c", counterLoop}}, 0, "", "")
	pid := readTrimmed(t, ctx, sandbox, "/work/loop.pid")
	waitForCounter(t, ctx, sandbox, 3)

	for round := 1; round <= 2; round++ {
		before := counterValue(t, ctx, sandbox)
		started := time.Now()
		if err := sandbox.Checkpoint(ctx); err != nil {
			t.Fatalf("round %d Checkpoint() error = %v; the daemon needs experimental features and a working CRIU", round, err)
		}
		checkpointDuration := time.Since(started)
		if running, err := manager.deployment.Running(ctx, sandbox.ContainerID()); err != nil || running {
			t.Fatalf("round %d container running after checkpoint = %v, %v", round, running, err)
		}
		if _, err := sandbox.Exec(ctx, core.Command{Path: "/bin/true"}); err == nil {
			t.Fatalf("round %d Exec() succeeded in a checkpointed container", round)
		}
		checkpoints, err := api.CheckpointList(ctx, sandbox.ContainerID(), client.CheckpointListOptions{})
		if err != nil {
			t.Fatalf("round %d CheckpointList() error = %v", round, err)
		}
		var names []string
		for _, checkpoint := range checkpoints.Items {
			names = append(names, checkpoint.Name)
		}
		if want := []string{"aries-" + strconv.Itoa(round)}; !slices.Equal(names, want) {
			t.Fatalf("round %d checkpoints = %v, want %v with the superseded one deleted", round, names, want)
		}

		started = time.Now()
		if err := sandbox.Restore(ctx); err != nil {
			t.Fatalf("round %d Restore() error = %v", round, err)
		}
		restoreDuration := time.Since(started)
		t.Logf("round %d: checkpoint %s, restore %s", round, checkpointDuration, restoreDuration)

		if got := readTrimmed(t, ctx, sandbox, "/work/marker"); got != "kept" {
			t.Fatalf("round %d marker = %q, want filesystem state preserved", round, got)
		}
		if got := readTrimmed(t, ctx, sandbox, "/work/loop.pid"); got != pid {
			t.Fatalf("round %d loop PID = %q, want %q", round, got, pid)
		}
		assertExec(t, ctx, sandbox, core.Command{Path: "/bin/kill", Args: []string{"-0", pid}}, 0, "", "")
		// The restored loop continues from its in-memory count.
		if after := counterValue(t, ctx, sandbox); after < before {
			t.Fatalf("round %d counter = %d after restore, want at least %d", round, after, before)
		}
		waitForCounter(t, ctx, sandbox, before+3)
		if err := sandbox.Restore(ctx); err != nil {
			t.Fatalf("round %d Restore() of a running sandbox error = %v", round, err)
		}
	}

	if err := manager.Stop(ctx, live); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if _, err := api.ContainerInspect(ctx, sandbox.ContainerID(), client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatalf("container remains after Stop: %v", err)
	}
}

func readTrimmed(t *testing.T, ctx context.Context, sandbox *Sandbox, path string) string {
	t.Helper()
	result := execForTest(t, ctx, sandbox, core.Command{Path: "/bin/cat", Args: []string{path}})
	if result.ExitCode != 0 {
		t.Fatalf("cat %s = %#v", path, result)
	}
	return strings.TrimSpace(result.Stdout)
}

func counterValue(t *testing.T, ctx context.Context, sandbox *Sandbox) int {
	t.Helper()
	value, err := strconv.Atoi(readTrimmed(t, ctx, sandbox, "/work/counter"))
	if err != nil {
		t.Fatalf("parse counter: %v", err)
	}
	return value
}

// waitForCounter fails if the count drops, which would mean the loop restarted.
func waitForCounter(t *testing.T, ctx context.Context, sandbox *Sandbox, minimum int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	previous := 0
	for {
		value := counterValue(t, ctx, sandbox)
		if value < previous {
			t.Fatalf("counter went from %d to %d; the loop lost its memory state", previous, value)
		}
		if value >= minimum {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("counter = %d, want at least %d within 10s", value, minimum)
		}
		previous = value
		time.Sleep(100 * time.Millisecond)
	}
}
