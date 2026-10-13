// Command aries-vmd is the privileged host daemon that runs ARIES tool
// sandboxes as Kata Containers on Firecracker. It must run as root; ARIES
// talks to it over a group-restricted Unix socket.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"github.com/hyscale-lab/aries/pkg/vmd/server"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "aries-vmd:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		socket          = flag.String("socket", "/run/aries-vmd.sock", "Unix socket to serve the API on")
		group           = flag.String("group", "aries", "group allowed to use the socket")
		containerdAddr  = flag.String("containerd-address", "/run/aries-containerd/containerd.sock", "dedicated containerd socket")
		namespace       = flag.String("namespace", "aries-vmd", "containerd namespace")
		runtime         = flag.String("runtime", "io.containerd.kata-fc.v2", "Kata Firecracker runtime name")
		snapshotter     = flag.String("snapshotter", "devmapper", "containerd snapshotter")
		stateDir        = flag.String("state-dir", "/var/lib/aries-vmd", "daemon state directory")
		helper          = flag.String("helper", "/usr/local/libexec/aries/aries-vmd-helper", "static in-guest helper binary")
		subnetPool      = flag.String("subnet-pool", "10.213.0.0/16", "IPv4 pool carved into /24 task networks")
		dns             = flag.String("dns", "", "comma-separated nameservers for egress-enabled sandboxes (default: host upstream resolvers)")
		storageLimitMB  = flag.Int("storage-limit-mb", 24576, "per-sandbox root filesystem size; must equal devmapper base_image_size")
		maxVCPUs        = flag.Int("max-vcpus", 16, "largest vCPU count a sandbox may request")
		maxMemoryMB     = flag.Int("max-memory-mb", 65536, "largest memory size a sandbox may request")
		defaultVCPUs    = flag.Int("default-vcpus", 2, "vCPUs for sandboxes that request none")
		defaultMemoryMB = flag.Int("default-memory-mb", 4096, "memory for sandboxes that request none")
		kataShim        = flag.String("kata-shim", "/opt/kata/bin/containerd-shim-kata-v2", "Kata shim binary, for version reporting")
		firecracker     = flag.String("firecracker", "/opt/kata/bin/firecracker", "Firecracker binary, for version reporting")
		logLevel        = flag.String("log-level", "info", "log level")
		allowNonroot    = flag.Bool("allow-nonroot", false, "skip the root check (testing only)")
	)
	flag.Parse()
	logger := logrus.New()
	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		return err
	}
	logger.SetLevel(level)
	if os.Geteuid() != 0 && !*allowNonroot {
		return errors.New("must run as root")
	}
	pool, err := netip.ParsePrefix(*subnetPool)
	if err != nil {
		return fmt.Errorf("--subnet-pool: %w", err)
	}
	var nameservers []string
	if *dns != "" {
		for _, value := range strings.Split(*dns, ",") {
			address, err := netip.ParseAddr(strings.TrimSpace(value))
			if err != nil {
				return fmt.Errorf("--dns: %w", err)
			}
			nameservers = append(nameservers, address.String())
		}
	} else if nameservers, err = server.DefaultNameservers(); err != nil {
		logger.WithError(err).Warn("egress-enabled sandboxes keep their image resolver configuration")
	}
	engine, err := server.NewContainerdEngine(*containerdAddr, *namespace, *runtime, *snapshotter, filepath.Join(*stateDir, "runtime"))
	if err != nil {
		return err
	}
	defer engine.Close()
	daemon, err := server.New(server.Config{
		StateDir: *stateDir, Runtime: *runtime, Snapshotter: *snapshotter, HelperPath: *helper,
		SubnetPool: pool, Nameservers: nameservers, StorageLimitMB: *storageLimitMB,
		MaxVCPUs: *maxVCPUs, MaxMemoryMB: *maxMemoryMB, DefaultVCPUs: *defaultVCPUs, DefaultMemoryMB: *defaultMemoryMB,
		KataShimPath: *kataShim, FirecrackerPath: *firecracker, Logger: logger,
	}, engine, server.NewHostNetwork())
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := daemon.Reconcile(ctx); err != nil {
		return fmt.Errorf("reconcile state: %w", err)
	}
	listener, err := listen(*socket, *group)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(api.ServerOptions()...)
	api.Register(grpcServer, daemon)
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		grpcServer.GracefulStop()
	}()
	logger.WithFields(logrus.Fields{"socket": *socket, "runtime": *runtime, "snapshotter": *snapshotter}).Info("aries-vmd serving")
	if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// listen replaces a stale socket and restricts it to root and group.
func listen(path, groupName string) (net.Listener, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("%s exists and is not a socket", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	gid := 0
	if groupName != "" {
		group, err := user.LookupGroup(groupName)
		if err != nil {
			listener.Close()
			return nil, fmt.Errorf("--group: %w", err)
		}
		if gid, err = strconv.Atoi(group.Gid); err != nil {
			listener.Close()
			return nil, err
		}
	}
	if err := os.Chown(path, 0, gid); err != nil {
		listener.Close()
		return nil, err
	}
	if err := os.Chmod(path, 0o660); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}
