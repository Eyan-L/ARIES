package kubernetes

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/deployment"
	"github.com/hyscale-lab/aries/pkg/runner"
)

const (
	maxExecInput        = 16 << 20
	maxExecOutput       = 16 << 20
	maxConfiguredOutput = 1 << 30
	execTrailerKeep     = 512
	// Exec state lives in the gate volume, outside the task's own filesystem.
	execStatePrefix = gateDirectory + "/exec-"
	// execShell mirrors the Docker provider: the command runs in its own
	// session so cancellation can signal the whole group, and its exit status
	// travels in a trailer because kubectl's own failures also exit nonzero.
	//
	// kubectl exec cannot select a user, so a command for another user starts
	// as the container's root and drops to that UID and GID here, with no
	// supplementary groups, through whichever tool the image provides.
	execShell = `state=$1; token=$2; dir=$3; user=$4; shift 4; umask 077; trap 'rm -f "$state" "$state.tmp"' EXIT; if [ -n "$dir" ]; then cd "$dir" || exit 126; fi; ` + dropUserShell + `exec 3<&0; setsid "$@" <&3 & pid=$!; printf '%s\n' "$pid" >"$state.tmp" || exit 125; mv "$state.tmp" "$state" || exit 125; wait "$pid"; status=$?; rm -f "$state" "$state.tmp"; trap - EXIT; printf '\036ARIES_EXEC_EXIT_%s=%d\037' "$token" "$status" >&2; exit "$status"`
	// dropUserShell rewrites "$@" to run as $user. Each tool is probed by
	// running it, because BusyBox ships a setpriv and a chroot without these
	// options. HOME follows Docker, which gives an unknown UID the root directory.
	dropUserShell = `if [ -n "$user" ] && [ "$user" != "0:0" ]; then uid=${user%%:*}; gid=${user##*:}; ` +
		`if setpriv --reuid="$uid" --regid="$gid" --clear-groups true 2>/dev/null; then set -- setpriv --reuid="$uid" --regid="$gid" --clear-groups "$@"; ` +
		`elif chroot --skip-chdir --userspec="$user" --groups="" / true 2>/dev/null; then set -- chroot --skip-chdir --userspec="$user" --groups="" / "$@"; ` +
		`elif python3 -c 'import os' 2>/dev/null; then set -- python3 -c 'import os,sys;os.setgroups([]);os.setgid(int(sys.argv[2]));os.setuid(int(sys.argv[1]));os.execvp(sys.argv[3],sys.argv[3:])' "$uid" "$gid" "$@"; ` +
		`else echo "aries: this image has no setpriv, GNU chroot or python3 to run a command as $user" >&2; exit 125; fi; ` +
		`HOME=/; export HOME; fi; `
	cancelExecShell = `state=$1; attempts=0; while [ ! -r "$state" ]; do attempts=$((attempts+1)); [ "$attempts" -ge 200 ] && exit 70; sleep 0.01; done; IFS= read -r pgid <"$state" || exit 71; case "$pgid" in ''|*[!0-9]*|0|1) exit 71;; esac; kill -TERM "-$pgid" 2>/dev/null || :; sleep 0.2; kill -KILL "-$pgid" 2>/dev/null || :; attempts=0; while kill -0 "-$pgid" 2>/dev/null; do attempts=$((attempts+1)); [ "$attempts" -ge 100 ] && exit 72; sleep 0.05; done; rm -f "$state"; exit 0`
	// cancelNoState is cancelExecShell's status when the wrapper left no state:
	// the command had already finished, or never started.
	cancelNoState = 70
)

// Exec runs one argv to completion with buffered output. Nonzero exits are
// returned as results, not transport errors.
func (manager *Manager) Exec(ctx context.Context, id string, command core.Command) (core.CommandResult, error) {
	started := time.Now()
	if len(command.Stdin) > maxExecInput {
		return core.CommandResult{ExitCode: -1, Duration: time.Since(started)}, fmt.Errorf("Kubernetes exec stdin exceeds %d bytes", maxExecInput)
	}
	var stdout, stderr bytes.Buffer
	var stdin io.Reader
	if len(command.Stdin) > 0 {
		stdin = bytes.NewReader(command.Stdin)
	}
	result, err := manager.ExecStream(ctx, id, command, stdin, &stdout, &stderr)
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	return result, err
}

// ExecStream preserves the exact argv and streams I/O. Cancellation and
// timeouts terminate the command's process group and confirm it is gone.
func (manager *Manager) ExecStream(ctx context.Context, id string, command core.Command, stdin io.Reader, stdout, stderr io.Writer) (core.CommandResult, error) {
	started := time.Now()
	failure := func() core.CommandResult { return core.CommandResult{ExitCode: -1, Duration: time.Since(started)} }
	if err := validateCommand(command); err != nil {
		return failure(), err
	}
	outputLimit := command.OutputLimitBytes
	if outputLimit == 0 {
		outputLimit = maxExecOutput
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	execCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if command.Timeout > 0 {
		var cancelTimeout context.CancelFunc
		execCtx, cancelTimeout = context.WithTimeout(execCtx, command.Timeout)
		defer cancelTimeout()
	}
	token, err := randomID()
	if err != nil {
		return failure(), fmt.Errorf("generate Kubernetes exec token: %w", err)
	}
	statePath := execStatePrefix + token

	args := []string{"exec", "-n", manager.options.Namespace, id, "-c", containerName}
	if stdin != nil {
		args = append(args, "-i")
	}
	args = append(args, "--", "/bin/sh", "-c", execShell, "aries-exec", statePath, token, command.Dir, command.User)
	if len(command.Env) != 0 {
		args = append(args, "env")
		for _, key := range slices.Sorted(maps.Keys(command.Env)) {
			args = append(args, key+"="+command.Env[key])
		}
	}
	args = append(append(args, command.Path), command.Args...)

	// An exceeded limit cancels kubectl; otherwise it would block on a pipe
	// that nobody reads.
	exceeded := func() { cancel(fmt.Errorf("Kubernetes exec output exceeds %d bytes", outputLimit)) }
	trailer := newExitTrailer(&limitedWriter{writer: stderr, limit: outputLimit, onExceed: exceeded}, token)
	runErr := manager.kubectl.Run(execCtx, stdin, &limitedWriter{writer: stdout, limit: outputLimit, onExceed: exceeded}, trailer, args...)

	if execCtx.Err() != nil {
		cause := context.Cause(execCtx)
		terminateCtx, terminateCancel := context.WithTimeout(context.WithoutCancel(ctx), manager.options.CleanupTimeout)
		defer terminateCancel()
		if terminateErr := manager.terminateExec(terminateCtx, id, statePath); terminateErr != nil {
			return failure(), errors.Join(cause, terminateErr)
		}
		return failure(), cause
	}
	exitCode, err := trailer.Finish()
	if err != nil {
		return failure(), errors.Join(err, runErr)
	}
	return core.CommandResult{ExitCode: exitCode, Duration: time.Since(started)}, nil
}

func (manager *Manager) terminateExec(ctx context.Context, id, statePath string) error {
	var stderr bytes.Buffer
	err := manager.kubectl.Run(ctx, nil, io.Discard, &stderr, "exec", "-n", manager.options.Namespace, id, "-c", containerName, "--", "/bin/sh", "-c", cancelExecShell, "aries-cancel", statePath)
	if err == nil {
		return nil
	}
	var exit interface{ ExitCode() int }
	if errors.As(err, &exit) && exit.ExitCode() == cancelNoState {
		return nil
	}
	// A pod that is already gone has no process left to confirm.
	if running, runErr := manager.Running(ctx, id); runErr == nil && !running {
		return nil
	}
	return fmt.Errorf("confirm terminated Kubernetes exec process-group exit: %w: %s", err, strings.TrimSpace(stderr.String()))
}

// UploadArchive extracts a tar stream beneath destination. Root containers keep
// the archive's ownership; a non-root container owns what it extracts.
func (manager *Manager) UploadArchive(ctx context.Context, id, destination string, archive io.Reader) error {
	if destination == "" || !strings.HasPrefix(destination, "/") || strings.ContainsRune(destination, 0) {
		return errors.New("deployment upload destination must be absolute")
	}
	var stderr bytes.Buffer
	if err := manager.kubectl.Run(ctx, archive, io.Discard, &stderr, "exec", "-i", "-n", manager.options.Namespace, id, "-c", containerName, "--", "tar", "-x", "-f", "-", "-C", destination); err != nil {
		return fmt.Errorf("extract archive in Kubernetes pod: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// DownloadArchive returns a tar stream of source, named by its base, with the
// source's own metadata.
func (manager *Manager) DownloadArchive(ctx context.Context, id, source string) (io.ReadCloser, deployment.FileInfo, error) {
	source, err := cleanContainerPath(source)
	if err != nil {
		return nil, deployment.FileInfo{}, fmt.Errorf("invalid deployment download source: %w", err)
	}
	stat, err := manager.Exec(ctx, id, core.Command{Path: "stat", Args: []string{"-c", "%s %f", source}})
	if err != nil {
		return nil, deployment.FileInfo{}, fmt.Errorf("inspect deployment archive source: %w", err)
	}
	if stat.ExitCode != 0 {
		// Exec succeeded, so the pod exists and only the source is missing.
		if strings.Contains(stat.Stderr, "No such file") {
			return nil, deployment.FileInfo{}, fmt.Errorf("deployment archive: %w: %s", runner.ErrNotFound, strings.TrimSpace(stat.Stderr))
		}
		return nil, deployment.FileInfo{}, fmt.Errorf("inspect deployment archive source: %s", strings.TrimSpace(stat.Stderr))
	}
	info, err := parseStat(stat.Stdout)
	if err != nil {
		return nil, deployment.FileInfo{}, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	go func() {
		var stderr bytes.Buffer
		err := manager.kubectl.Run(streamCtx, nil, writer, &stderr, "exec", "-n", manager.options.Namespace, id, "-c", containerName, "--", "tar", "-c", "-f", "-", "-C", path.Dir(source), path.Base(source))
		if err != nil {
			err = fmt.Errorf("archive Kubernetes pod path: %w: %s", err, strings.TrimSpace(stderr.String()))
		}
		_ = writer.CloseWithError(err)
	}()
	return &archiveStream{PipeReader: reader, cancel: cancel}, info, nil
}

type archiveStream struct {
	*io.PipeReader
	cancel context.CancelFunc
}

func (stream *archiveStream) Close() error {
	stream.cancel()
	return stream.PipeReader.Close()
}

// parseStat reads `stat -c '%s %f'`: a decimal size and the raw mode in hex.
func parseStat(output string) (deployment.FileInfo, error) {
	fields := strings.Fields(output)
	if len(fields) != 2 {
		return deployment.FileInfo{}, fmt.Errorf("unexpected stat output %q", output)
	}
	size, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return deployment.FileInfo{}, fmt.Errorf("parse stat size: %w", err)
	}
	raw, err := strconv.ParseUint(fields[1], 16, 32)
	if err != nil {
		return deployment.FileInfo{}, fmt.Errorf("parse stat mode: %w", err)
	}
	mode := fs.FileMode(raw & 0o777)
	switch raw & 0o170000 {
	case 0o100000:
	case 0o040000:
		mode |= fs.ModeDir
	case 0o120000:
		mode |= fs.ModeSymlink
	default:
		mode |= fs.ModeIrregular
	}
	return deployment.FileInfo{Size: size, Mode: mode}, nil
}

// exitTrailerWriter forwards stderr while withholding the wrapper's trailer.
// kubectl may append its own message after the trailer; that text is dropped.
type exitTrailerWriter struct {
	destination io.Writer
	prefix      []byte
	buffer      bytes.Buffer
}

func newExitTrailer(destination io.Writer, token string) *exitTrailerWriter {
	return &exitTrailerWriter{destination: destination, prefix: []byte("\x1eARIES_EXEC_EXIT_" + token + "=")}
}

func (w *exitTrailerWriter) Write(content []byte) (int, error) {
	written, _ := w.buffer.Write(content)
	if excess := w.buffer.Len() - execTrailerKeep; excess > 0 {
		chunk := w.buffer.Next(excess)
		if n, err := w.destination.Write(chunk); err != nil {
			return 0, err
		} else if n != len(chunk) {
			return 0, io.ErrShortWrite
		}
	}
	return written, nil
}

func (w *exitTrailerWriter) Finish() (int, error) {
	content := w.buffer.Bytes()
	start := bytes.LastIndex(content, w.prefix)
	if start < 0 {
		return -1, fmt.Errorf("Kubernetes exec output is missing its exit trailer: %s", strings.TrimSpace(string(content)))
	}
	rest := content[start+len(w.prefix):]
	end := bytes.IndexByte(rest, '\x1f')
	if end < 0 {
		return -1, errors.New("Kubernetes exec output has an invalid exit trailer")
	}
	exitCode, err := strconv.Atoi(string(rest[:end]))
	if err != nil || exitCode < 0 || exitCode > 255 {
		return -1, errors.New("Kubernetes exec output has an invalid exit code")
	}
	if _, err := w.destination.Write(content[:start]); err != nil {
		return -1, fmt.Errorf("write Kubernetes exec stderr: %w", err)
	}
	return exitCode, nil
}

type limitedWriter struct {
	writer   io.Writer
	wrote    int
	limit    int
	once     sync.Once
	onExceed func()
}

func (w *limitedWriter) Write(content []byte) (int, error) {
	if len(content) > w.limit-w.wrote {
		w.once.Do(w.onExceed)
		return 0, fmt.Errorf("Kubernetes exec output exceeds %d bytes", w.limit)
	}
	written, err := w.writer.Write(content)
	w.wrote += written
	return written, err
}

func validateCommand(command core.Command) error {
	if command.Path == "" || strings.ContainsRune(command.Path, 0) {
		return errors.New("command path must be nonempty and NUL-free")
	}
	if command.Dir != "" {
		if !strings.HasPrefix(command.Dir, "/") || strings.ContainsRune(command.Dir, 0) || path.Clean(command.Dir) != command.Dir {
			return errors.New("invalid command workdir: path must be absolute, clean, and NUL-free")
		}
	}
	if command.User != "" {
		if err := validateNumericUser(command.User); err != nil {
			return fmt.Errorf("invalid command user: %w", err)
		}
	}
	if command.Timeout < 0 {
		return errors.New("command timeout must be nonnegative")
	}
	if command.OutputLimitBytes < 0 || command.OutputLimitBytes > maxConfiguredOutput {
		return fmt.Errorf("command output limit must be between 0 and %d bytes", maxConfiguredOutput)
	}
	for _, argument := range command.Args {
		if strings.ContainsRune(argument, 0) {
			return errors.New("command argument contains NUL")
		}
	}
	for key, value := range command.Env {
		if !validEnvName(key) || strings.ContainsRune(value, 0) {
			return fmt.Errorf("invalid command environment %q", key)
		}
	}
	return nil
}

func validEnvName(value string) bool {
	for index, r := range value {
		if r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || index > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return value != ""
}

func randomID() (string, error) {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
