package kubernetes

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	portForwardStartTimeout = 30 * time.Second
	portForwardRetryDelay   = time.Second
)

// portForward keeps one kubectl port-forward bound to a fixed loopback port.
// kubectl exits when its connection to the pod breaks, so the forward restarts
// it on the same port and clients reconnect to an unchanged address.
type portForward struct {
	address string
	cancel  context.CancelFunc
	done    chan struct{}

	mu      sync.Mutex
	lastErr string
}

func (forward *portForward) stop() {
	forward.cancel()
	<-forward.done
}

// startPortForward reserves a free local port and waits until it accepts
// connections. The forward outlives ctx: it serves the runtime until Stop or Close.
func (manager *Manager) startPortForward(ctx context.Context, pod string, port int) (*portForward, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("reserve Kubernetes port-forward port: %w", err)
	}
	address := listener.Addr().String()
	_, local, _ := net.SplitHostPort(address)
	if err := listener.Close(); err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	forward := &portForward{address: address, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(forward.done)
		for runCtx.Err() == nil {
			var stderr bytes.Buffer
			command := exec.CommandContext(runCtx, manager.options.KubectlPath, "port-forward", "-n", manager.options.Namespace, "--address", "127.0.0.1", "pod/"+pod, local+":"+strconv.Itoa(port))
			command.Stderr = &stderr
			runErr := command.Run()
			forward.mu.Lock()
			forward.lastErr = strings.TrimSpace(fmt.Sprintf("%v: %s", runErr, stderr.String()))
			forward.mu.Unlock()
			select {
			case <-runCtx.Done():
			case <-time.After(portForwardRetryDelay):
			}
		}
	}()
	deadline := time.NewTimer(portForwardStartTimeout)
	defer deadline.Stop()
	for {
		connection, dialErr := net.DialTimeout("tcp", address, time.Second)
		if dialErr == nil {
			_ = connection.Close()
			return forward, nil
		}
		select {
		case <-ctx.Done():
			forward.stop()
			return nil, ctx.Err()
		case <-deadline.C:
			forward.stop()
			forward.mu.Lock()
			defer forward.mu.Unlock()
			return nil, errors.New("timed out waiting for Kubernetes port-forward: " + forward.lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
