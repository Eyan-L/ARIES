package server

import (
	"errors"
	"net/netip"
	"os"
	"strings"
)

// parseNameservers returns the non-loopback nameservers in resolv.conf
// content. Loopback resolvers (such as systemd-resolved's stub) are
// unreachable from a VM.
func parseNameservers(content string) []string {
	var servers []string
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "nameserver" {
			continue
		}
		address, err := netip.ParseAddr(fields[1])
		if err != nil || address.IsLoopback() || address.IsUnspecified() {
			continue
		}
		servers = append(servers, address.String())
	}
	return servers
}

// DefaultNameservers reads the host resolver configuration, preferring
// systemd-resolved's upstream list over the stub configuration.
func DefaultNameservers() ([]string, error) {
	for _, path := range []string{"/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"} {
		content, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if servers := parseNameservers(string(content)); len(servers) > 0 {
			return servers, nil
		}
	}
	return nil, errors.New("no non-loopback nameserver found in host resolver configuration")
}

func renderResolvConf(servers []string) string {
	var builder strings.Builder
	for _, server := range servers {
		builder.WriteString("nameserver " + server + "\n")
	}
	return builder.String()
}
