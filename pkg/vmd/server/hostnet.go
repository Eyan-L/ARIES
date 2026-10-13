package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// hostNetwork owns host-side network plumbing for task networks and sandboxes.
type hostNetwork interface {
	EnsureForwarding(context.Context) error
	CreateBridge(ctx context.Context, bridge, gatewayCIDR string) error
	DeleteBridge(ctx context.Context, bridge string) error
	LinkExists(ctx context.Context, name string) (bool, error)
	CreateNetNS(ctx context.Context, attachment netnsAttachment) error
	DeleteNetNS(ctx context.Context, netns, hostVeth string) error
	NetNSExists(ctx context.Context, netns string) (bool, error)
	ApplyFirewall(ctx context.Context, ruleset string) error
	// SetDockerUserAccept adds or removes accept rules for an egress bridge in
	// Docker's DOCKER-USER chain when that chain exists.
	SetDockerUserAccept(ctx context.Context, bridge string, present bool) error
}

type netnsAttachment struct {
	NetNS    string
	HostVeth string
	PeerVeth string
	Bridge   string
	// AddressCIDR is the sandbox address with prefix, such as 10.213.0.2/24.
	AddressCIDR string
	Gateway     string
}

// commandRunner executes argv without a shell and returns combined output.
type commandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

// ipHostNetwork implements hostNetwork with iproute2, nft, and iptables.
type ipHostNetwork struct {
	run commandRunner
	// sysRoot prefixes /sys, /proc, and /var/run paths for tests.
	sysRoot  string
	nftInput func(ctx context.Context, ruleset string) error
}

func newIPHostNetwork() *ipHostNetwork {
	return &ipHostNetwork{run: runCommand, nftInput: func(ctx context.Context, ruleset string) error {
		command := exec.CommandContext(ctx, "nft", "-f", "-")
		command.Stdin = strings.NewReader(ruleset)
		output, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf("nft -f -: %w: %s", err, strings.TrimSpace(string(output)))
		}
		return nil
	}}
}

func (h *ipHostNetwork) path(parts ...string) string {
	return filepath.Join(append([]string{"/", h.sysRoot}, parts...)...)
}

func (h *ipHostNetwork) EnsureForwarding(context.Context) error {
	path := h.path("proc", "sys", "net", "ipv4", "ip_forward")
	content, err := os.ReadFile(path)
	if err == nil && strings.TrimSpace(string(content)) == "1" {
		return nil
	}
	if err := os.WriteFile(path, []byte("1\n"), 0o644); err != nil {
		return fmt.Errorf("enable IPv4 forwarding: %w", err)
	}
	return nil
}

func (h *ipHostNetwork) LinkExists(_ context.Context, name string) (bool, error) {
	return pathExists(h.path("sys", "class", "net", name))
}

func (h *ipHostNetwork) NetNSExists(_ context.Context, netns string) (bool, error) {
	return pathExists(h.path("var", "run", "netns", netns))
}

func pathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (h *ipHostNetwork) CreateBridge(ctx context.Context, bridge, gatewayCIDR string) error {
	exists, err := h.LinkExists(ctx, bridge)
	if err != nil {
		return err
	}
	if !exists {
		if _, err := h.run(ctx, "ip", "link", "add", "name", bridge, "type", "bridge"); err != nil {
			return err
		}
	}
	output, err := h.run(ctx, "ip", "-o", "addr", "show", "dev", bridge)
	if err != nil {
		return err
	}
	if !slices.Contains(strings.Fields(string(output)), gatewayCIDR) {
		if _, err := h.run(ctx, "ip", "addr", "add", gatewayCIDR, "dev", bridge); err != nil {
			return err
		}
	}
	_, err = h.run(ctx, "ip", "link", "set", "dev", bridge, "up")
	return err
}

func (h *ipHostNetwork) DeleteBridge(ctx context.Context, bridge string) error {
	exists, err := h.LinkExists(ctx, bridge)
	if err != nil || !exists {
		return err
	}
	_, err = h.run(ctx, "ip", "link", "del", "dev", bridge)
	return err
}

func (h *ipHostNetwork) CreateNetNS(ctx context.Context, a netnsAttachment) error {
	steps := [][]string{
		{"ip", "netns", "add", a.NetNS},
		{"ip", "link", "add", "name", a.HostVeth, "type", "veth", "peer", "name", a.PeerVeth, "netns", a.NetNS},
		{"ip", "link", "set", "dev", a.HostVeth, "master", a.Bridge},
		{"ip", "link", "set", "dev", a.HostVeth, "up"},
		{"ip", "-n", a.NetNS, "link", "set", "dev", a.PeerVeth, "name", "eth0"},
		{"ip", "-n", a.NetNS, "addr", "add", a.AddressCIDR, "dev", "eth0"},
		{"ip", "-n", a.NetNS, "link", "set", "dev", "eth0", "up"},
		{"ip", "-n", a.NetNS, "link", "set", "dev", "lo", "up"},
		{"ip", "-n", a.NetNS, "route", "add", "default", "via", a.Gateway},
	}
	for _, step := range steps {
		if _, err := h.run(ctx, step[0], step[1:]...); err != nil {
			return err
		}
	}
	return nil
}

func (h *ipHostNetwork) DeleteNetNS(ctx context.Context, netns, hostVeth string) error {
	var errs []error
	if hostVeth != "" {
		if exists, err := h.LinkExists(ctx, hostVeth); err != nil {
			errs = append(errs, err)
		} else if exists {
			if _, err := h.run(ctx, "ip", "link", "del", "dev", hostVeth); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if netns != "" {
		if exists, err := h.NetNSExists(ctx, netns); err != nil {
			errs = append(errs, err)
		} else if exists {
			if _, err := h.run(ctx, "ip", "netns", "del", netns); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (h *ipHostNetwork) ApplyFirewall(ctx context.Context, ruleset string) error {
	return h.nftInput(ctx, ruleset)
}

func (h *ipHostNetwork) SetDockerUserAccept(ctx context.Context, bridge string, present bool) error {
	if _, err := h.run(ctx, "iptables", "-t", "filter", "-S", "DOCKER-USER"); err != nil {
		// No Docker forwarding policy is installed; nothing to bypass.
		return nil
	}
	rules := [][]string{
		{"-o", bridge, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"},
		{"-i", bridge, "-j", "ACCEPT"},
	}
	for _, rule := range rules {
		check := append([]string{"-t", "filter", "-C", "DOCKER-USER"}, rule...)
		_, checkErr := h.run(ctx, "iptables", check...)
		switch {
		case present && checkErr != nil:
			if _, err := h.run(ctx, "iptables", append([]string{"-t", "filter", "-I", "DOCKER-USER", "1"}, rule...)...); err != nil {
				return err
			}
		case !present:
			for attempts := 0; checkErr == nil && attempts < 16; attempts++ {
				if _, err := h.run(ctx, "iptables", append([]string{"-t", "filter", "-D", "DOCKER-USER"}, rule...)...); err != nil {
					return err
				}
				_, checkErr = h.run(ctx, "iptables", check...)
			}
			if checkErr == nil {
				return fmt.Errorf("DOCKER-USER rule for %s remains after removal", bridge)
			}
		}
	}
	return nil
}

const firewallTable = "aries_vmd"

// renderFirewall builds the complete nft ruleset for all task networks. It is
// applied atomically: the table is created (if absent), deleted, and rebuilt.
func renderFirewall(networks []networkRecord) string {
	sorted := slices.Clone(networks)
	slices.SortFunc(sorted, func(a, b networkRecord) int { return strings.Compare(a.Bridge, b.Bridge) })
	var bridges []string
	for _, network := range sorted {
		bridges = append(bridges, `"`+network.Bridge+`"`)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "table inet %s {}\ndelete table inet %s\ntable inet %s {\n", firewallTable, firewallTable, firewallTable)
	b.WriteString("\tset bridges {\n\t\ttype ifname\n")
	if len(bridges) > 0 {
		fmt.Fprintf(&b, "\t\telements = { %s }\n", strings.Join(bridges, ", "))
	}
	b.WriteString("\t}\n")
	b.WriteString("\tchain forward {\n\t\ttype filter hook forward priority filter; policy accept;\n")
	b.WriteString("\t\tiifname @bridges oifname @bridges drop\n")
	// Egress means the outside world, not Docker networks on this host: the
	// DOCKER-USER accept would otherwise bypass Docker's network isolation
	// and let a VM reach harness containers.
	b.WriteString("\t\tiifname @bridges oifname \"docker0\" drop\n")
	b.WriteString("\t\tiifname @bridges oifname \"br-*\" drop\n")
	for _, network := range sorted {
		if network.Spec.AllowEgress {
			fmt.Fprintf(&b, "\t\tiifname %q accept\n", network.Bridge)
			fmt.Fprintf(&b, "\t\toifname %q ct state established,related accept\n", network.Bridge)
		} else {
			fmt.Fprintf(&b, "\t\tiifname %q drop\n", network.Bridge)
		}
	}
	b.WriteString("\t\toifname @bridges drop\n\t}\n")
	b.WriteString("\tchain input {\n\t\ttype filter hook input priority filter; policy accept;\n")
	b.WriteString("\t\tiifname @bridges ct state established,related accept\n")
	b.WriteString("\t\tiifname @bridges drop\n\t}\n")
	b.WriteString("\tchain postrouting {\n\t\ttype nat hook postrouting priority srcnat; policy accept;\n")
	for _, network := range sorted {
		if network.Spec.AllowEgress {
			fmt.Fprintf(&b, "\t\tip saddr %s oifname != @bridges masquerade\n", network.Subnet)
		}
	}
	b.WriteString("\t}\n}\n")
	return b.String()
}

// NewHostNetwork returns the iproute2/nft/iptables host network implementation.
func NewHostNetwork() hostNetwork { return newIPHostNetwork() }
