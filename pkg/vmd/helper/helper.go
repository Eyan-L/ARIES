// Package helper implements the static binary that aries-vmd places at
// /.aries/helper inside every sandbox root filesystem. It runs only inside
// the guest: as the container's init, as the wrapper for each exec, and as
// the archive tool for uploads and downloads. Task images therefore need no
// shell, tar, or ps.
//
// The binary deliberately avoids importing pkg/vmd/api (and through it gRPC)
// to stay small; tests assert that the shared constants and wire types match.
package helper

import (
	"fmt"
	"io"
	"io/fs"
	"os"
)

// Version identifies the helper's command-line contract.
const Version = "aries-vmd-helper/1"

// Exit codes; they equal the api.HelperExit* constants.
const (
	ExitNotFound     = 44
	ExitUsage        = 64
	ExitStateMissing = 70
	ExitUnconfirmed  = 71
	exitFailure      = 1
	exitStateWrite   = 125
	exitNotRunnable  = 126
	exitNoCommand    = 127
)

// FileStat is the first line of tar-create output; it matches api.FileStat.
type FileStat struct {
	Name string      `json:"name"`
	Size int64       `json:"size"`
	Mode fs.FileMode `json:"mode"`
}

// Helper carries the process environment. Tests replace the roots.
type Helper struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	// ProcRoot is the procfs mount used to confirm process-group absence.
	ProcRoot string
	// Root is the guest root that holds /etc/passwd and /etc/group.
	Root string
}

// Default returns a helper bound to the real process environment.
func Default() *Helper {
	return &Helper{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr, ProcRoot: "/proc", Root: "/"}
}

// Main runs one subcommand and returns the process exit code.
func (h *Helper) Main(args []string) int {
	if len(args) == 0 {
		return h.usage("missing subcommand")
	}
	switch args[0] {
	case "version":
		if len(args) != 1 {
			return h.usage("version takes no arguments")
		}
		fmt.Fprintln(h.Stdout, Version)
		return 0
	case "init":
		return h.init(args[1:])
	case "run":
		return h.run(args[1:])
	case "kill":
		return h.kill(args[1:])
	case "tar-extract":
		return h.tarExtract(args[1:])
	case "tar-create":
		return h.tarCreate(args[1:])
	default:
		return h.usage(fmt.Sprintf("unknown subcommand %q", args[0]))
	}
}

func (h *Helper) usage(message string) int {
	fmt.Fprintf(h.Stderr, "aries-vmd-helper: %s\n", message)
	return ExitUsage
}

func (h *Helper) fail(err error) int {
	fmt.Fprintf(h.Stderr, "aries-vmd-helper: %v\n", err)
	return exitFailure
}
