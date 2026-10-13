// Package api defines the wire contract between ARIES and the privileged
// aries-vmd host daemon. The service is gRPC over a Unix socket with a JSON
// codec, so the contract is plain Go types and needs no generated code.
//
// Identities are caller-chosen names: a sandbox or network ID equals the
// name in its spec. A caller that loses a Create response therefore still
// holds the identity it must clean up, and Delete of an absent resource
// succeeds. Delete succeeds only after the daemon has confirmed absence.
package api

import (
	"encoding/json"
	"io/fs"
	"time"
)

// Version is the wire contract version reported by Health.
const Version = 1

// IsolationKataFirecracker is the only isolation class the daemon provides:
// Kata Containers on the Firecracker hypervisor.
const IsolationKataFirecracker = "kata-fc"

// Ownership labels. Callers must supply LabelRun and LabelTask on every
// sandbox and network; the daemon adds LabelManaged.
const (
	LabelManaged = "aries-vmd.managed"
	LabelRun     = "aries.run"
	LabelTask    = "aries.task"
)

// Helper locations inside every sandbox root filesystem.
const (
	HelperPath = "/.aries/helper"
	StateDir   = "/.aries/state"
)

// Helper exit codes shared by the daemon and the in-guest helper.
const (
	// HelperExitNotFound reports a missing archive source or destination.
	HelperExitNotFound = 44
	// HelperExitUsage reports invalid helper arguments.
	HelperExitUsage = 64
	// HelperExitStateMissing reports that a cancelled command never recorded
	// its process group, so the helper could not target it.
	HelperExitStateMissing = 70
	// HelperExitUnconfirmed reports that a process group survived termination.
	HelperExitUnconfirmed = 71
)

// HealthRequest asks for daemon readiness.
type HealthRequest struct{}

// HealthReply reports host readiness and the pinned component versions.
type HealthReply struct {
	APIVersion         int    `json:"api_version"`
	Isolation          string `json:"isolation"`
	KVM                bool   `json:"kvm"`
	KVMError           string `json:"kvm_error,omitempty"`
	ContainerdVersion  string `json:"containerd_version"`
	KataVersion        string `json:"kata_version"`
	FirecrackerVersion string `json:"firecracker_version"`
	HelperVersion      string `json:"helper_version"`
	Snapshotter        string `json:"snapshotter"`
	// StorageLimitMB is the per-sandbox root filesystem size; requests above it are rejected.
	StorageLimitMB  int `json:"storage_limit_mb"`
	MaxVCPUs        int `json:"max_vcpus"`
	MaxMemoryMB     int `json:"max_memory_mb"`
	DefaultVCPUs    int `json:"default_vcpus"`
	DefaultMemoryMB int `json:"default_memory_mb"`
}

// ImageChunk streams a `docker save` archive. The first chunk names the image.
type ImageChunk struct {
	Reference string `json:"reference,omitempty"`
	Data      []byte `json:"data,omitempty"`
}

// ImageRequest names one image.
type ImageRequest struct {
	Reference string `json:"reference"`
}

// ImageInfo describes an imported, unpacked image. The Docker image ID of the
// exported image equals ConfigDigest with the classic image store, or
// TargetDigest with Docker's containerd image store.
type ImageInfo struct {
	Present      bool   `json:"present"`
	Reference    string `json:"reference,omitempty"`
	ConfigDigest string `json:"config_digest,omitempty"`
	TargetDigest string `json:"target_digest,omitempty"`
}

// NetworkSpec requests one private task network.
type NetworkSpec struct {
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels"`
	AllowEgress bool              `json:"allow_egress"`
}

// NetworkInfo describes an owned task network. Members maps sandbox
// network aliases to their IPv4 addresses.
type NetworkInfo struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels"`
	AllowEgress bool              `json:"allow_egress"`
	Subnet      string            `json:"subnet"`
	Gateway     string            `json:"gateway"`
	Members     map[string]string `json:"members,omitempty"`
}

// NetworkRef names one network.
type NetworkRef struct {
	ID string `json:"id"`
}

// SandboxSpec requests one Kata/Firecracker sandbox. There are deliberately no
// fields for mounts, devices, privileges, or runtime selection.
type SandboxSpec struct {
	Name    string            `json:"name"`
	Image   string            `json:"image"`
	Network string            `json:"network"`
	Workdir string            `json:"workdir,omitempty"`
	Env     []string          `json:"env,omitempty"`
	Labels  map[string]string `json:"labels"`
	// Entrypoint nil preserves the image entrypoint.
	Entrypoint []string `json:"entrypoint,omitempty"`
	Args       []string `json:"args,omitempty"`
	// Zero VCPUs or MemoryMB selects the daemon default.
	VCPUs           int      `json:"vcpus,omitempty"`
	MemoryMB        int      `json:"memory_mb,omitempty"`
	StorageMB       int      `json:"storage_mb,omitempty"`
	NoNewPrivileges bool     `json:"no_new_privileges,omitempty"`
	Aliases         []string `json:"aliases,omitempty"`
}

// SandboxInfo reports what the daemon allocated. Requested fields echo the
// stored spec; observed fields come from containerd and the host.
type SandboxInfo struct {
	Spec         SandboxSpec `json:"spec"`
	ID           string      `json:"id"`
	ConfigDigest string      `json:"config_digest,omitempty"`
	Runtime      string      `json:"runtime"`
	Isolation    string      `json:"isolation"`
	// VCPUs and MemoryMB are the effective VM sizes after defaults.
	VCPUs    int    `json:"vcpus"`
	MemoryMB int    `json:"memory_mb"`
	Init     bool   `json:"init"`
	IP       string `json:"ip,omitempty"`
	// Status is one of "created", "running", or "stopped".
	Status string `json:"status"`
	// Mounts lists host mounts in the runtime spec beyond the root filesystem; it must be empty.
	Mounts []string `json:"mounts,omitempty"`
}

// Sandbox statuses.
const (
	StatusCreated = "created"
	StatusRunning = "running"
	StatusStopped = "stopped"
)

// SandboxRef names one sandbox.
type SandboxRef struct {
	ID string `json:"id"`
}

// Empty is an empty message.
type Empty struct{}

// ExecFrame is one message of the bidirectional Exec stream. The client sends
// Start first, then optional Stdin chunks, StdinEOF, and at most one Cancel.
// The server sends Stdout/Stderr chunks and exactly one final Exit.
type ExecFrame struct {
	Start    *ExecStart `json:"start,omitempty"`
	Stdin    []byte     `json:"stdin,omitempty"`
	StdinEOF bool       `json:"stdin_eof,omitempty"`
	Cancel   bool       `json:"cancel,omitempty"`
	Stdout   []byte     `json:"stdout,omitempty"`
	Stderr   []byte     `json:"stderr,omitempty"`
	Exit     *ExecExit  `json:"exit,omitempty"`
}

// ExecStart describes one command. Args holds the complete argv; Path is not
// separate. Env entries override the sandbox environment. User is "", "uid",
// or "uid:gid"; empty selects the sandbox user.
type ExecStart struct {
	Sandbox string   `json:"sandbox"`
	Args    []string `json:"args"`
	Env     []string `json:"env,omitempty"`
	Dir     string   `json:"dir,omitempty"`
	User    string   `json:"user,omitempty"`
	Stdin   bool     `json:"stdin,omitempty"`
}

// ExecExit ends an Exec stream. Canceled reports that the client's Cancel
// took effect; CancelConfirmed reports that the command's process group was
// confirmed absent while the sandbox kept running.
type ExecExit struct {
	Code            int    `json:"code"`
	Canceled        bool   `json:"canceled,omitempty"`
	CancelConfirmed bool   `json:"cancel_confirmed,omitempty"`
	Error           string `json:"error,omitempty"`
}

// ArchiveChunk carries a tar stream. For PutArchive the first chunk names the
// sandbox and destination directory. For GetArchive the first chunk carries
// Stat, or Missing when the source does not exist in a live sandbox.
type ArchiveChunk struct {
	Sandbox string    `json:"sandbox,omitempty"`
	Path    string    `json:"path,omitempty"`
	Stat    *FileStat `json:"stat,omitempty"`
	Missing bool      `json:"missing,omitempty"`
	Data    []byte    `json:"data,omitempty"`
}

// ArchiveRequest names a source path in a sandbox.
type ArchiveRequest struct {
	Sandbox string `json:"sandbox"`
	Path    string `json:"path"`
}

// FileStat describes an archive source. Mode uses io/fs encoding.
type FileStat struct {
	Name string      `json:"name"`
	Size int64       `json:"size"`
	Mode fs.FileMode `json:"mode"`
}

// LogsRequest asks for a sandbox's main-process output.
type LogsRequest struct {
	Sandbox string `json:"sandbox"`
}

// LogChunk carries main-process output.
type LogChunk struct {
	Stdout []byte `json:"stdout,omitempty"`
	Stderr []byte `json:"stderr,omitempty"`
}

// StatsRequest selects sandboxes whose labels contain all of Labels.
type StatsRequest struct {
	Labels map[string]string `json:"labels"`
}

// StatsReply carries one reading per running sandbox.
type StatsReply struct {
	Sandboxes []SandboxStats `json:"sandboxes"`
}

// SandboxStats is a raw resource observation for one sandbox VM.
type SandboxStats struct {
	ID                  string            `json:"id"`
	Labels              map[string]string `json:"labels"`
	ObservedAt          time.Time         `json:"observed_at"`
	CPUUsageNanoseconds uint64            `json:"cpu_usage_nanoseconds"`
	MemoryUsageBytes    uint64            `json:"memory_usage_bytes"`
	MemoryLimitBytes    uint64            `json:"memory_limit_bytes"`
}

// ListRequest selects resources whose labels contain all of Labels.
type ListRequest struct {
	Labels map[string]string `json:"labels"`
}

// ListReply lists owned resources.
type ListReply struct {
	Sandboxes []SandboxInfo `json:"sandboxes"`
	Networks  []NetworkInfo `json:"networks"`
}

// CleanupRequest removes every resource owned by one run.
type CleanupRequest struct {
	Run string `json:"run"`
}

// CleanupReply lists what was removed.
type CleanupReply struct {
	Sandboxes []string `json:"sandboxes"`
	Networks  []string `json:"networks"`
}

// Codec is the JSON gRPC codec shared by client and server.
type Codec struct{}

// CodecName is the gRPC content subtype for Codec.
const CodecName = "json"

func (Codec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (Codec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (Codec) Name() string                       { return CodecName }
