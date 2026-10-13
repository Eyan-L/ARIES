package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func shortHash(value string, length int) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:length]
}

func bridgeName(network string) string { return "avb" + shortHash("network/"+network, 8) }

// allocateSubnet returns the first /24 in the pool unused by records.
func allocateSubnet(pool netip.Prefix, records []networkRecord) (netip.Prefix, error) {
	used := map[netip.Prefix]bool{}
	for _, record := range records {
		if prefix, err := netip.ParsePrefix(record.Subnet); err == nil {
			used[prefix.Masked()] = true
		}
	}
	pool = pool.Masked()
	for address := pool.Addr(); pool.Contains(address); {
		candidate := netip.PrefixFrom(address, 24)
		if !used[candidate] {
			return candidate, nil
		}
		next := address.As4()
		if next[2] == 255 {
			if next[1] == 255 {
				break
			}
			next[1]++
			next[2] = 0
		} else {
			next[2]++
		}
		address = netip.AddrFrom4(next)
	}
	return netip.Prefix{}, status.Error(codes.ResourceExhausted, "subnet pool is exhausted")
}

// allocateAddress returns the lowest free host address from .2 to .254.
func allocateAddress(record networkRecord) (netip.Addr, error) {
	prefix, err := netip.ParsePrefix(record.Subnet)
	if err != nil {
		return netip.Addr{}, err
	}
	used := map[string]bool{record.Gateway: true}
	for _, address := range record.Leases {
		used[address] = true
	}
	base := prefix.Masked().Addr().As4()
	for host := 2; host < 255; host++ {
		candidate := base
		candidate[3] = byte(host)
		address := netip.AddrFrom4(candidate)
		if !used[address.String()] {
			return address, nil
		}
	}
	return netip.Addr{}, status.Errorf(codes.ResourceExhausted, "network %s has no free addresses", record.Spec.Name)
}

func (s *Server) networkInfo(record networkRecord) (api.NetworkInfo, error) {
	info := api.NetworkInfo{
		ID: record.Spec.Name, Name: record.Spec.Name, Labels: ownedLabels(record.Spec.Labels),
		AllowEgress: record.Spec.AllowEgress, Subnet: record.Subnet, Gateway: record.Gateway,
	}
	sandboxes, err := s.store.sandboxes()
	if err != nil {
		return info, err
	}
	for _, sandbox := range sandboxes {
		if sandbox.Spec.Network != record.Spec.Name || sandbox.IP == "" || sandbox.Phase != phaseReady {
			continue
		}
		if info.Members == nil {
			info.Members = map[string]string{}
		}
		for _, alias := range sandbox.Spec.Aliases {
			info.Members[alias] = sandbox.IP
		}
	}
	return info, nil
}

func (s *Server) CreateNetwork(ctx context.Context, spec *api.NetworkSpec) (*api.NetworkInfo, error) {
	if err := validateName("network", spec.Name); err != nil {
		return nil, err
	}
	if err := validateLabels(spec.Labels); err != nil {
		return nil, err
	}
	s.global.Lock()
	defer s.global.Unlock()
	existing, found, err := s.store.network(spec.Name)
	if err != nil {
		return nil, internal(err)
	}
	if found {
		if existing.Phase == phaseReady && existing.Spec.AllowEgress == spec.AllowEgress && maps.Equal(existing.Spec.Labels, spec.Labels) {
			info, err := s.networkInfo(existing)
			return &info, internal(err)
		}
		return nil, status.Errorf(codes.AlreadyExists, "network %s already exists", spec.Name)
	}
	records, err := s.store.networks()
	if err != nil {
		return nil, internal(err)
	}
	subnet, err := allocateSubnet(s.cfg.SubnetPool, records)
	if err != nil {
		return nil, err
	}
	gateway := subnet.Addr().As4()
	gateway[3] = 1
	record := networkRecord{
		Spec:  api.NetworkSpec{Name: spec.Name, Labels: maps.Clone(spec.Labels), AllowEgress: spec.AllowEgress},
		Phase: phaseAllocating, Subnet: subnet.String(), Gateway: netip.AddrFrom4(gateway).String(), Bridge: bridgeName(spec.Name),
	}
	if err := s.store.putNetwork(record); err != nil {
		return nil, internal(err)
	}
	// The firewall covers the bridge before the bridge exists.
	if err := s.applyFirewall(ctx, append(records, record)); err != nil {
		return nil, internal(err)
	}
	if err := s.net.EnsureForwarding(ctx); err != nil {
		return nil, internal(err)
	}
	if err := s.net.CreateBridge(ctx, record.Bridge, record.Gateway+"/24"); err != nil {
		return nil, internal(err)
	}
	if record.Spec.AllowEgress {
		if err := s.net.SetDockerUserAccept(ctx, record.Bridge, true); err != nil {
			return nil, internal(err)
		}
	}
	record.Phase = phaseReady
	if err := s.store.putNetwork(record); err != nil {
		return nil, internal(err)
	}
	s.logger.WithFields(map[string]any{"network": record.Spec.Name, "subnet": record.Subnet, "egress": record.Spec.AllowEgress}).Info("task network created")
	info, err := s.networkInfo(record)
	return &info, internal(err)
}

func (s *Server) applyFirewall(ctx context.Context, records []networkRecord) error {
	return s.net.ApplyFirewall(ctx, renderFirewall(records))
}

func (s *Server) InspectNetwork(_ context.Context, ref *api.NetworkRef) (*api.NetworkInfo, error) {
	record, found, err := s.store.network(ref.ID)
	if err != nil {
		return nil, internal(err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "network %s not found", ref.ID)
	}
	info, err := s.networkInfo(record)
	return &info, internal(err)
}

func (s *Server) DeleteNetwork(ctx context.Context, ref *api.NetworkRef) (*api.Empty, error) {
	if err := validateName("network", ref.ID); err != nil {
		return nil, err
	}
	ctx, cancel := s.cleanupContext(ctx)
	defer cancel()
	s.global.Lock()
	defer s.global.Unlock()
	record, found, err := s.store.network(ref.ID)
	if err != nil {
		return nil, internal(err)
	}
	if !found {
		return &api.Empty{}, nil
	}
	sandboxes, err := s.store.sandboxes()
	if err != nil {
		return nil, internal(err)
	}
	var attached []string
	for _, sandbox := range sandboxes {
		if sandbox.Spec.Network == ref.ID {
			attached = append(attached, sandbox.Spec.Name)
		}
	}
	if len(attached) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "network %s still has sandboxes %v", ref.ID, attached)
	}
	record.Phase = phaseDeleting
	if err := s.store.putNetwork(record); err != nil {
		return nil, internal(err)
	}
	var errs []error
	if err := s.net.SetDockerUserAccept(ctx, record.Bridge, false); err != nil {
		errs = append(errs, err)
	}
	if err := s.net.DeleteBridge(ctx, record.Bridge); err != nil {
		errs = append(errs, err)
	}
	records, err := s.store.networks()
	if err != nil {
		return nil, internal(err)
	}
	remaining := slices.DeleteFunc(records, func(other networkRecord) bool { return other.Spec.Name == ref.ID })
	if err := s.applyFirewall(ctx, remaining); err != nil {
		errs = append(errs, err)
	}
	exists, err := s.net.LinkExists(ctx, record.Bridge)
	if err != nil {
		errs = append(errs, err)
	} else if exists {
		errs = append(errs, fmt.Errorf("bridge %s remains after removal", record.Bridge))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, status.Errorf(codes.Internal, "delete network %s: %v", ref.ID, err)
	}
	if err := s.store.deleteNetwork(ref.ID); err != nil {
		return nil, internal(err)
	}
	s.logger.WithField("network", ref.ID).Info("task network removed")
	return &api.Empty{}, nil
}
