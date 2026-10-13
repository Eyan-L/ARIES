package helper

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const pollInterval = 10 * time.Millisecond

// init runs ARGV as the container's main process, forwards termination
// signals to it, and reaps every child that reparents to this process.
func (h *Helper) init(args []string) int {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	if err := flags.Parse(args); err != nil {
		return h.usage("init: " + err.Error())
	}
	argv := flags.Args()
	if len(argv) == 0 {
		return h.usage("init requires -- ARGV")
	}
	// A subreaper adopts orphans even when this process is not PID 1.
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return h.fail(fmt.Errorf("set child subreaper: %w", err))
	}
	path, code, err := resolve(argv[0])
	if err != nil {
		fmt.Fprintf(h.Stderr, "aries-vmd-helper: %v\n", err)
		return code
	}
	signals := make(chan os.Signal, 16)
	signal.Notify(signals, syscall.SIGCHLD, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	defer signal.Stop(signals)
	process, err := os.StartProcess(path, argv, &os.ProcAttr{Files: []*os.File{fileOr(h.Stdin, os.Stdin), fileOr(h.Stdout, os.Stdout), fileOr(h.Stderr, os.Stderr)}})
	if err != nil {
		fmt.Fprintf(h.Stderr, "aries-vmd-helper: start %s: %v\n", argv[0], err)
		return startFailureCode(err)
	}
	for {
		// Reap before waiting so a child that exited before Notify is not missed.
		if status, done := reapAll(process.Pid); done {
			return status
		}
		received := <-signals
		if received != syscall.SIGCHLD {
			_ = process.Signal(received)
		}
	}
}

// reapAll collects every exited child without blocking. It reports the main
// child's exit code once that child has been reaped.
func reapAll(main int) (int, bool) {
	code, done := 0, false
	for {
		var status unix.WaitStatus
		pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil || pid <= 0 {
			return code, done
		}
		if pid == main {
			code, done = waitCode(status), true
		}
	}
}

func waitCode(status unix.WaitStatus) int {
	if status.Signaled() {
		return 128 + int(status.Signal())
	}
	return status.ExitStatus()
}

// run starts ARGV in a new session and records its process group in the
// state file so a concurrent kill can target exactly this command.
func (h *Helper) run(args []string) int {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	state := flags.String("state", "", "process-group state file")
	if err := flags.Parse(args); err != nil {
		return h.usage("run: " + err.Error())
	}
	argv := flags.Args()
	if len(argv) == 0 {
		return h.usage("run requires -- ARGV")
	}
	if !filepath.IsAbs(*state) {
		return h.usage("run requires an absolute --state path")
	}
	path, code, err := resolve(argv[0])
	if err != nil {
		fmt.Fprintf(h.Stderr, "aries-vmd-helper: %v\n", err)
		return code
	}
	command := &exec.Cmd{Path: path, Args: argv, Stdin: h.Stdin, Stdout: h.Stdout, Stderr: h.Stderr, SysProcAttr: &syscall.SysProcAttr{Setsid: true}}
	if err := command.Start(); err != nil {
		fmt.Fprintf(h.Stderr, "aries-vmd-helper: start %s: %v\n", argv[0], err)
		return startFailureCode(err)
	}
	pgid := command.Process.Pid
	if err := writeState(*state, pgid); err != nil {
		_ = unix.Kill(-pgid, unix.SIGKILL)
		_ = command.Wait()
		fmt.Fprintf(h.Stderr, "aries-vmd-helper: record process group: %v\n", err)
		return exitStateWrite
	}
	waitErr := command.Wait()
	_ = os.Remove(*state)
	if status, ok := command.ProcessState.Sys().(syscall.WaitStatus); ok {
		return waitCode(unix.WaitStatus(status))
	}
	return h.fail(fmt.Errorf("wait for %s: %v", argv[0], waitErr))
}

func writeState(path string, pgid int) error {
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, []byte(strconv.Itoa(pgid)+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

// resolve finds an executable with shell exit-code conventions: 127 when it
// cannot be found and 126 when it exists but cannot be executed.
func resolve(name string) (string, int, error) {
	if !strings.Contains(name, "/") {
		path, err := exec.LookPath(name)
		if errors.Is(err, exec.ErrDot) {
			err = nil
		}
		if err != nil {
			if errors.Is(err, fs.ErrPermission) {
				return "", exitNotRunnable, fmt.Errorf("%s: permission denied", name)
			}
			return "", exitNoCommand, fmt.Errorf("%s: command not found", name)
		}
		return path, 0, nil
	}
	info, err := os.Stat(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", exitNoCommand, fmt.Errorf("%s: no such file or directory", name)
		}
		return "", exitNotRunnable, fmt.Errorf("%s: %v", name, err)
	}
	if info.IsDir() || info.Mode().Perm()&0o111 == 0 {
		return "", exitNotRunnable, fmt.Errorf("%s: permission denied", name)
	}
	return name, 0, nil
}

func startFailureCode(err error) int {
	if errors.Is(err, fs.ErrNotExist) {
		return exitNoCommand
	}
	return exitNotRunnable
}

func fileOr(stream any, fallback *os.File) *os.File {
	if file, ok := stream.(*os.File); ok {
		return file
	}
	return fallback
}

// kill terminates the process group recorded by run and confirms that no
// live process remains in it. It never touches the sandbox's main process.
func (h *Helper) kill(args []string) int {
	flags := flag.NewFlagSet("kill", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	state := flags.String("state", "", "process-group state file")
	wait := flags.Duration("wait", 2*time.Second, "time to wait for the state file")
	grace := flags.Duration("grace", 200*time.Millisecond, "delay between SIGTERM and SIGKILL")
	confirm := flags.Duration("confirm", 5*time.Second, "time to confirm process-group absence")
	if err := flags.Parse(args); err != nil {
		return h.usage("kill: " + err.Error())
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*state) {
		return h.usage("kill requires an absolute --state path and no arguments")
	}
	content, err := waitForFile(*state, *wait)
	if err != nil {
		fmt.Fprintf(h.Stderr, "aries-vmd-helper: process group state unavailable: %v\n", err)
		return ExitStateMissing
	}
	pgid, err := strconv.Atoi(strings.TrimSpace(string(content)))
	if err != nil || pgid <= 1 {
		fmt.Fprintf(h.Stderr, "aries-vmd-helper: invalid process group %q\n", strings.TrimSpace(string(content)))
		return ExitUnconfirmed
	}
	_ = unix.Kill(-pgid, unix.SIGTERM)
	time.Sleep(*grace)
	_ = unix.Kill(-pgid, unix.SIGKILL)
	deadline := time.Now().Add(*confirm)
	for {
		present, err := groupPresent(h.ProcRoot, pgid)
		if err == nil && !present {
			_ = os.Remove(*state)
			return 0
		}
		if time.Now().After(deadline) {
			if err != nil {
				fmt.Fprintf(h.Stderr, "aries-vmd-helper: scan processes: %v\n", err)
			} else {
				fmt.Fprintf(h.Stderr, "aries-vmd-helper: process group %d survived termination\n", pgid)
			}
			return ExitUnconfirmed
		}
		// Kill again in case a member forked between the scan and now.
		_ = unix.Kill(-pgid, unix.SIGKILL)
		time.Sleep(pollInterval)
	}
}

func waitForFile(path string, wait time.Duration) ([]byte, error) {
	deadline := time.Now().Add(wait)
	for {
		content, err := os.ReadFile(path)
		if err == nil {
			return content, nil
		}
		if !errors.Is(err, fs.ErrNotExist) || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(pollInterval)
	}
}

// procStat holds the /proc/<pid>/stat fields the helper needs.
type procStat struct {
	State byte
	PPID  int
	PGRP  int
}

// parseStat parses /proc/<pid>/stat. The command name may contain spaces and
// parentheses, so fields are read after the last closing parenthesis.
func parseStat(content string) (procStat, error) {
	end := strings.LastIndexByte(content, ')')
	if end < 0 {
		return procStat{}, errors.New("malformed process stat")
	}
	fields := strings.Fields(content[end+1:])
	if len(fields) < 3 || len(fields[0]) != 1 {
		return procStat{}, errors.New("malformed process stat")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return procStat{}, fmt.Errorf("malformed process stat parent: %w", err)
	}
	pgrp, err := strconv.Atoi(fields[2])
	if err != nil {
		return procStat{}, fmt.Errorf("malformed process stat group: %w", err)
	}
	return procStat{State: fields[0][0], PPID: ppid, PGRP: pgrp}, nil
}

// processes lists the stat of every process visible in procRoot. Processes
// that exit during the scan are skipped.
func processes(procRoot string) (map[int]procStat, error) {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil, err
	}
	result := make(map[int]procStat, len(entries))
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		content, err := os.ReadFile(filepath.Join(procRoot, entry.Name(), "stat"))
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return nil, err
		}
		stat, err := parseStat(string(content))
		if err != nil {
			return nil, fmt.Errorf("process %d: %w", pid, err)
		}
		result[pid] = stat
	}
	return result, nil
}

// groupPresent reports whether a non-zombie process belongs to pgid. A zombie
// can no longer run; its parent reaps it.
func groupPresent(procRoot string, pgid int) (bool, error) {
	table, err := processes(procRoot)
	if err != nil {
		return false, err
	}
	for _, stat := range table {
		if stat.PGRP == pgid && stat.State != 'Z' && stat.State != 'X' {
			return true, nil
		}
	}
	return false, nil
}
