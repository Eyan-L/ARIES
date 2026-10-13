package vmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/deployment"
	"github.com/hyscale-lab/aries/pkg/vmd/api"
)

const forwardDialTimeout = 10 * time.Second

// NewTaskEnvironmentFactory composes the harness-side attachment (a Docker task
// network, which also hosts bridge listeners) with an aries-vmd task network
// for the microVM sandbox. Each call returns a fresh owner for one occurrence.
func NewTaskEnvironmentFactory(manager *Manager, harness func() deployment.TaskEnvironment) func() deployment.TaskEnvironment {
	return func() deployment.TaskEnvironment {
		return &taskEnvironment{manager: manager, harness: harness()}
	}
}

type taskEnvironment struct {
	mu        sync.Mutex
	manager   *Manager
	harness   deployment.TaskEnvironment
	spec      api.NetworkSpec
	network   string
	forwarder *forwarder
	started   bool
	stopped   bool
}

func (e *taskEnvironment) Start(ctx context.Context, request core.SandboxRequest) (core.HarnessConnectivity, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started || e.stopped {
		return core.HarnessConnectivity{}, errors.New("task environment cannot be reused")
	}
	e.started = true
	if e.harness == nil {
		return core.HarnessConnectivity{}, errors.New("vmd task environment requires a harness attachment")
	}
	searchPort, err := validateSearchPort(request)
	if err != nil {
		return core.HarnessConnectivity{}, err
	}
	// The harness attachment never sees the search service declaration: the
	// forwarder below replaces its sandbox alias, which cannot cross backends.
	harnessRequest := request
	harnessRequest.Environment.Services = core.TaskServices{}
	connectivity, err := e.harness.Start(ctx, harnessRequest)
	if err != nil {
		return core.HarnessConnectivity{}, fmt.Errorf("start harness task attachment: %w", err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return core.HarnessConnectivity{}, err
	}
	labels := map[string]string{"aries.managed": "true", "aries.kind": "task-network", api.LabelRun: request.RunID, api.LabelTask: request.TaskID}
	if request.Purpose != "" {
		labels["aries.purpose"] = request.Purpose
	}
	e.spec = api.NetworkSpec{Name: "aries-vnet-" + hex.EncodeToString(nonce[:]), Labels: labels, AllowEgress: request.Environment.AllowNetwork}
	// The name is the identity, so a lost response still leaves Stop a target.
	e.network = e.spec.Name
	info, err := e.manager.api.CreateNetwork(ctx, &e.spec)
	if err != nil {
		return core.HarnessConnectivity{}, fmt.Errorf("create vmd task network: %w", err)
	}
	if err := validateNetwork(info, e.spec); err != nil {
		return core.HarnessConnectivity{}, err
	}
	connectivity.Placement.VMNetwork = e.network
	if searchPort != 0 {
		listen, err := e.harness.BridgeListen(ctx)
		if err != nil {
			return core.HarnessConnectivity{}, fmt.Errorf("resolve search forwarder address: %w", err)
		}
		e.forwarder, err = startForwarder(listen.BindHost, func(ctx context.Context) (string, error) {
			return e.memberAddress(ctx, searchPort)
		})
		if err != nil {
			return core.HarnessConnectivity{}, fmt.Errorf("start search forwarder: %w", err)
		}
		connectivity.SearchURL = "http://" + net.JoinHostPort(listen.AdvertiseHost, strconv.Itoa(e.forwarder.port()))
	}
	return connectivity, nil
}

func (e *taskEnvironment) memberAddress(ctx context.Context, port int) (string, error) {
	info, err := e.manager.api.InspectNetwork(ctx, &api.NetworkRef{ID: e.network})
	if err != nil {
		return "", err
	}
	if err := validateNetwork(info, e.spec); err != nil {
		return "", err
	}
	address := info.Members[deployment.TaskSandboxAlias]
	if net.ParseIP(address).To4() == nil {
		return "", errors.New("task sandbox has no IPv4 address on its vmd network")
	}
	return net.JoinHostPort(address, strconv.Itoa(port)), nil
}

func validateNetwork(info *api.NetworkInfo, spec api.NetworkSpec) error {
	if info.ID != spec.Name || info.Name != spec.Name || info.AllowEgress != spec.AllowEgress {
		return errors.New("vmd task network identity or isolation differs from request")
	}
	for key, value := range spec.Labels {
		if info.Labels[key] != value {
			return errors.New("vmd task network ownership labels differ from request")
		}
	}
	return nil
}

func (e *taskEnvironment) Validate(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.network == "" || e.stopped {
		return errors.New("task environment is not active")
	}
	if err := e.harness.Validate(ctx); err != nil {
		return err
	}
	info, err := e.manager.api.InspectNetwork(ctx, &api.NetworkRef{ID: e.network})
	if err != nil {
		return fmt.Errorf("inspect vmd task network: %w", err)
	}
	return validateNetwork(info, e.spec)
}

func (e *taskEnvironment) BridgeListen(ctx context.Context) (core.BridgeListen, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.network == "" || e.stopped {
		return core.BridgeListen{}, errors.New("task environment is not active")
	}
	return e.harness.BridgeListen(ctx)
}

// Stop removes the forwarder, the vmd network, and the harness attachment in
// that order, confirming absence of each. Failed removals can be retried.
func (e *taskEnvironment) Stop(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return nil
	}
	var errs []error
	if e.forwarder != nil {
		e.forwarder.close()
		e.forwarder = nil
	}
	if e.network != "" {
		if err := e.deleteNetwork(ctx); err != nil {
			errs = append(errs, err)
		} else {
			e.network = ""
		}
	}
	if e.harness != nil && e.started {
		if err := e.harness.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	e.stopped = true
	return nil
}

func (e *taskEnvironment) deleteNetwork(ctx context.Context) error {
	ref := &api.NetworkRef{ID: e.network}
	_, deleteErr := e.manager.api.DeleteNetwork(ctx, ref)
	if deleteErr != nil && !isNotFound(deleteErr) {
		deleteErr = fmt.Errorf("remove vmd task network: %w", deleteErr)
	} else {
		deleteErr = nil
	}
	info, inspectErr := e.manager.api.InspectNetwork(ctx, ref)
	if isNotFound(inspectErr) {
		return deleteErr
	}
	if inspectErr != nil {
		return errors.Join(deleteErr, fmt.Errorf("verify vmd task network removal: %w", inspectErr))
	}
	if err := validateNetwork(info, e.spec); err != nil {
		return errors.Join(deleteErr, fmt.Errorf("vmd task network %s is not owned by this occurrence: %w", e.network, err))
	}
	return errors.Join(deleteErr, errors.New("vmd task network remains after removal"))
}

// forwarder relays host TCP connections from the harness attachment to the
// sandbox's service port, resolving the sandbox address per connection.
type forwarder struct {
	listener net.Listener
	target   func(context.Context) (string, error)
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
}

func startForwarder(host string, target func(context.Context) (string, error)) (*forwarder, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &forwarder{listener: listener, target: target, ctx: ctx, cancel: cancel, conns: map[net.Conn]struct{}{}}
	f.wg.Add(1)
	go f.serve()
	return f, nil
}

func (f *forwarder) port() int { return f.listener.Addr().(*net.TCPAddr).Port }

func (f *forwarder) serve() {
	defer f.wg.Done()
	for {
		client, err := f.listener.Accept()
		if err != nil {
			return
		}
		if !f.track(client) {
			client.Close()
			return
		}
		f.wg.Add(1)
		go f.relay(client)
	}
}

func (f *forwarder) track(conn net.Conn) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conns == nil {
		return false
	}
	f.conns[conn] = struct{}{}
	return true
}

func (f *forwarder) untrack(conn net.Conn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.conns, conn)
	conn.Close()
}

func (f *forwarder) relay(client net.Conn) {
	defer f.wg.Done()
	defer f.untrack(client)
	ctx, cancel := context.WithTimeout(f.ctx, forwardDialTimeout)
	address, err := f.target(ctx)
	var upstream net.Conn
	if err == nil {
		var dialer net.Dialer
		upstream, err = dialer.DialContext(ctx, "tcp", address)
	}
	cancel()
	if err != nil {
		return
	}
	if !f.track(upstream) {
		upstream.Close()
		return
	}
	defer f.untrack(upstream)
	copied := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if tcp, ok := dst.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		copied <- struct{}{}
	}
	go pipe(upstream, client)
	go pipe(client, upstream)
	<-copied
	<-copied
}

// close stops accepting, closes every relayed connection, and waits for all
// relay goroutines to exit.
func (f *forwarder) close() {
	f.cancel()
	_ = f.listener.Close()
	f.mu.Lock()
	for conn := range f.conns {
		conn.Close()
	}
	f.conns = nil
	f.mu.Unlock()
	f.wg.Wait()
}
