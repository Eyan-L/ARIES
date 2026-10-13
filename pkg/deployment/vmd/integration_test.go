//go:build vmdintegration

package vmd_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hyscale-lab/aries/pkg/config"
	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/deployment"
	dockerdeployment "github.com/hyscale-lab/aries/pkg/deployment/docker"
	vmddeployment "github.com/hyscale-lab/aries/pkg/deployment/vmd"
	"github.com/hyscale-lab/aries/pkg/runner"
	"github.com/hyscale-lab/aries/pkg/sandbox"
	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"github.com/sirupsen/logrus"
)

const fixtureImage = "docker.io/library/busybox:1.37.0-musl@sha256:222ad6d973c0d198014546a65cd02c5fdedcc172123c5b4c2bf0af636550bd94"

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// TestVMDSandboxLifecycle drives the real tool sandbox policy against a
// running aries-vmd daemon and the local Docker daemon: startup, exec,
// targeted cancellation, transfers, egress denial, the search forwarder,
// and confirmed cleanup.
func TestVMDSandboxLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	vmdSocket := envOr("ARIES_VMD_SOCKET", "/run/aries-vmd.sock")
	dockerSocket := envOr("ARIES_DOCKER_SOCKET", "/var/run/docker.sock")
	logger := logrus.StandardLogger()

	versionsFile, err := os.Open(filepath.Join("..", "..", "..", "configs", "versions.json"))
	if err != nil {
		t.Fatal(err)
	}
	versions, err := config.DecodeVersions(versionsFile)
	versionsFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	transport, err := vmddeployment.New(vmddeployment.Options{Socket: vmdSocket, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.Preflight(ctx, api.IsolationKataFirecracker, versions.VMD.Kata.Version); err != nil {
		t.Fatal(err)
	}
	if err := dockerdeployment.PullImages(ctx, dockerSocket, []string{fixtureImage}); err != nil {
		t.Fatal(err)
	}
	if err := transport.EnsureImages(ctx, []string{fixtureImage}, vmddeployment.ImageSource{
		ID: func(ctx context.Context, image string) (string, error) {
			return dockerdeployment.ImageID(ctx, dockerSocket, image)
		},
		Save: func(ctx context.Context, image string) (io.ReadCloser, error) {
			return dockerdeployment.SaveImage(ctx, dockerSocket, image)
		},
	}); err != nil {
		t.Fatal(err)
	}
	docker, err := dockerdeployment.New(dockerdeployment.Options{Socket: dockerSocket, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer docker.Close()
	manager, err := sandbox.New(sandbox.Options{
		Deployment:     transport,
		NewEnvironment: vmddeployment.NewTaskEnvironmentFactory(transport, docker.NewTaskEnvironment),
		OutputDir:      t.TempDir(),
		Logger:         logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	runID := "vmd-integration-" + time.Now().UTC().Format("20060102t150405")
	live, err := manager.Start(ctx, core.SandboxRequest{RunID: runID, TaskID: "lifecycle", Environment: core.Environment{
		Image: fixtureImage, Workdir: "/tmp", CPU: 1, MemoryMB: 512,
		Services: core.TaskServices{SearchPort: 8888},
	}})
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = manager.Stop(context.Background(), live)
		}
	}()

	exec := func(ctx context.Context, args ...string) (core.CommandResult, error) {
		return live.Exec(ctx, core.Command{Path: "/bin/sh", Args: append([]string{"-c"}, args...)})
	}
	result, err := exec(ctx, "echo out; echo err >&2; exit 3")
	if err != nil || result.ExitCode != 3 || result.Stdout != "out\n" || result.Stderr != "err\n" {
		t.Fatalf("exec = %+v, %v", result, err)
	}

	// Cancellation terminates the command's whole process group, not the VM.
	cancelCtx, cancelCommand := context.WithTimeout(ctx, 2*time.Second)
	_, err = exec(cancelCtx, "sleep 300 & sleep 300")
	cancelCommand()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled exec error = %v, want confirmed deadline", err)
	}
	result, err = exec(ctx, `for p in /proc/[0-9]*; do cat "$p/cmdline" 2>/dev/null | tr '\0' ' '; echo; done | grep -c '^sleep 300' || true`)
	if err != nil || strings.TrimSpace(result.Stdout) != "0" {
		t.Fatalf("cancelled group survived: %+v, %v", result, err)
	}

	// Transfers keep content and classify missing sources.
	source := filepath.Join(t.TempDir(), "payload")
	if err := os.WriteFile(source, []byte("payload\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := live.Upload(ctx, source, "/tmp/payload"); err != nil {
		t.Fatal(err)
	}
	if err := live.Download(ctx, "/tmp/payload", "lifecycle/payload"); err != nil {
		t.Fatal(err)
	}
	if err := live.Download(ctx, "/tmp/absent", "lifecycle/absent"); !errors.Is(err, runner.ErrNotFound) {
		t.Fatalf("missing download error = %v, want ErrNotFound", err)
	}

	// AllowNetwork is false: no egress from the VM.
	result, err = exec(ctx, "wget -T 3 -q -O- http://1.1.1.1/ >/dev/null 2>&1; echo $?")
	if err != nil || strings.TrimSpace(result.Stdout) == "0" {
		t.Fatalf("egress was not denied: %+v, %v", result, err)
	}

	// The harness-side search URL reaches the sandbox service through the forwarder.
	if _, err := exec(ctx, "mkdir -p /tmp/www && echo search-ok >/tmp/www/index.html && httpd -p 8888 -h /tmp/www"); err != nil {
		t.Fatal(err)
	}
	searchURL := live.Connectivity().SearchURL
	if searchURL == "" || strings.Contains(searchURL, deployment.TaskSandboxAlias) {
		t.Fatalf("search URL = %q, want a forwarder address", searchURL)
	}
	response, err := http.Get(searchURL + "/index.html")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if strings.TrimSpace(string(body)) != "search-ok" {
		t.Fatalf("search response = %q", body)
	}

	if err := manager.Stop(ctx, live); err != nil {
		t.Fatal(err)
	}
	stopped = true
	listed, err := transport.API().List(ctx, &api.ListRequest{Labels: map[string]string{api.LabelRun: runID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Sandboxes) != 0 || len(listed.Networks) != 0 {
		t.Fatalf("resources remain after stop: %+v", listed)
	}
}
