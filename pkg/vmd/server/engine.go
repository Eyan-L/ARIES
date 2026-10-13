package server

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
)

// errEngineNotFound marks an absent container, task, snapshot, or image.
var errEngineNotFound = errors.New("not found")

// imageConfig is the subset of an OCI image configuration the daemon uses.
type imageConfig struct {
	ConfigDigest string
	// TargetDigest is the image's manifest or index digest.
	TargetDigest string
	Unpacked     bool
	Env          []string
	Entrypoint   []string
	Cmd          []string
	WorkingDir   string
	User         string
}

// containerSpec describes the Kata container to create. Process arguments
// are complete, including the in-guest init helper.
type containerSpec struct {
	ID              string
	Image           string
	Labels          map[string]string
	Args            []string
	Env             []string
	Cwd             string
	Hostname        string
	NetNSPath       string
	NoNewPrivileges bool
	Annotations     map[string]string
}

// containerState is what containerd reports about one container.
type containerState struct {
	Runtime string
	Labels  map[string]string
	Args    []string
	// Status is api.StatusCreated without a task, api.StatusRunning, or api.StatusStopped.
	Status string
	// HostMounts lists bind mounts beyond the root filesystem.
	HostMounts []string
}

// processSpec describes one exec inside a running sandbox.
type processSpec struct {
	Args []string
	Env  []string
	Cwd  string
	User identity
}

// execProcess is a started exec. Wait returns its exit status; Drain waits
// for output copying, abandoning it after the timeout.
type execProcess interface {
	Wait(context.Context) (int, error)
	Kill(context.Context, syscall.Signal) error
	Drain(timeoutSeconds float64)
	Delete(context.Context) error
}

type metrics struct {
	CPUUsageNanoseconds uint64
	MemoryUsageBytes    uint64
	MemoryLimitBytes    uint64
}

// engine is the containerd surface the daemon uses; containerdEngine
// implements it and tests substitute fakes.
type engine interface {
	Version(context.Context) (string, error)
	ImportImage(ctx context.Context, reference string, archive io.Reader) error
	// InspectImage returns errEngineNotFound for an absent image.
	InspectImage(ctx context.Context, reference string) (imageConfig, error)
	CreateContainer(context.Context, containerSpec) error
	// PrepareRootfs mounts the container's root filesystem on the host and
	// runs inject against it. It must only be called before the task starts.
	PrepareRootfs(ctx context.Context, id string, inject func(*os.Root) error) error
	SetProcessUser(ctx context.Context, id string, user identity) error
	// InspectContainer returns errEngineNotFound for an absent container.
	InspectContainer(ctx context.Context, id string) (containerState, error)
	StartTask(ctx context.Context, id string, stdout, stderr io.Writer) error
	Exec(ctx context.Context, id, execID string, process processSpec, stdin io.Reader, stdout, stderr io.Writer) (execProcess, error)
	// DeleteContainer kills and removes the task, container, and snapshot; absent parts are skipped.
	DeleteContainer(ctx context.Context, id string) error
	// ContainerAbsent confirms neither the container nor its snapshot exists.
	ContainerAbsent(ctx context.Context, id string) (bool, error)
	Metrics(ctx context.Context, id string) (metrics, error)
}
