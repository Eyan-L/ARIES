package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
)

// Allocation phases recorded before side effects so that Delete knows what to reverse.
const (
	phaseAllocating = "allocating"
	phaseReady      = "ready"
	phaseDeleting   = "deleting"
)

type networkRecord struct {
	Spec    api.NetworkSpec `json:"spec"`
	Phase   string          `json:"phase"`
	Subnet  string          `json:"subnet"`
	Gateway string          `json:"gateway"`
	Bridge  string          `json:"bridge"`
	// Leases maps sandbox IDs to their IPv4 addresses.
	Leases map[string]string `json:"leases,omitempty"`
}

type sandboxRecord struct {
	Spec         api.SandboxSpec `json:"spec"`
	Phase        string          `json:"phase"`
	ConfigDigest string          `json:"config_digest,omitempty"`
	VCPUs        int             `json:"vcpus"`
	MemoryMB     int             `json:"memory_mb"`
	IP           string          `json:"ip"`
	Gateway      string          `json:"gateway"`
	NetNS        string          `json:"netns"`
	HostVeth     string          `json:"host_veth"`
	PeerVeth     string          `json:"peer_veth"`
	// ImageUser is the image's USER string; User is its resolved identity.
	ImageUser string   `json:"image_user,omitempty"`
	User      identity `json:"user"`
	Passwd    string   `json:"passwd,omitempty"`
	Group     string   `json:"group,omitempty"`
	Argv      []string `json:"argv"`
	Env       []string `json:"env,omitempty"`
	Cwd       string   `json:"cwd"`
	Injected  bool     `json:"injected,omitempty"`
}

// store persists daemon-owned resource records as one JSON file per resource.
type store struct {
	root string
}

func newStore(root string) (*store, error) {
	for _, dir := range []string{"networks", "sandboxes", "logs"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
	}
	return &store{root: root}, nil
}

func (s *store) path(kind, name string) string {
	return filepath.Join(s.root, kind, name+".json")
}

func (s *store) logDir(name string) string { return filepath.Join(s.root, "logs", name) }

func writeAtomic(path string, value any) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if _, err := temporary.Write(content); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

func readRecord(path string, value any) (bool, error) {
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(content, value); err != nil {
		return false, fmt.Errorf("decode %s: %w", path, err)
	}
	return true, nil
}

func removeRecord(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func listNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		name, ok := strings.CutSuffix(entry.Name(), ".json")
		if ok && !entry.IsDir() && !strings.HasPrefix(name, ".") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (s *store) putNetwork(record networkRecord) error {
	return writeAtomic(s.path("networks", record.Spec.Name), record)
}

func (s *store) network(name string) (networkRecord, bool, error) {
	var record networkRecord
	found, err := readRecord(s.path("networks", name), &record)
	return record, found, err
}

func (s *store) deleteNetwork(name string) error { return removeRecord(s.path("networks", name)) }

func (s *store) networks() ([]networkRecord, error) {
	names, err := listNames(filepath.Join(s.root, "networks"))
	if err != nil {
		return nil, err
	}
	records := make([]networkRecord, 0, len(names))
	for _, name := range names {
		record, found, err := s.network(name)
		if err != nil {
			return nil, err
		}
		if found {
			records = append(records, record)
		}
	}
	return records, nil
}

func (s *store) putSandbox(record sandboxRecord) error {
	return writeAtomic(s.path("sandboxes", record.Spec.Name), record)
}

func (s *store) sandbox(name string) (sandboxRecord, bool, error) {
	var record sandboxRecord
	found, err := readRecord(s.path("sandboxes", name), &record)
	return record, found, err
}

func (s *store) deleteSandbox(name string) error { return removeRecord(s.path("sandboxes", name)) }

func (s *store) sandboxes() ([]sandboxRecord, error) {
	names, err := listNames(filepath.Join(s.root, "sandboxes"))
	if err != nil {
		return nil, err
	}
	records := make([]sandboxRecord, 0, len(names))
	for _, name := range names {
		record, found, err := s.sandbox(name)
		if err != nil {
			return nil, err
		}
		if found {
			records = append(records, record)
		}
	}
	return records, nil
}
