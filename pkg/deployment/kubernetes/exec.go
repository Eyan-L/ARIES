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

// ExecStream preserves argv and streams I/O through kubectl. Cancellation
// closes the client connection; the remote command may continue running.
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
	args := append([]string{command.Path}, command.Args...)
	if len(command.Env) != 0 {
		env := []string{"env"}
		for _, key := range slices.Sorted(maps.Keys(command.Env)) {
			env = append(env, key+"="+command.Env[key])
		}
		args = append(env, args...)
	}
	// kubectl has no per-exec user option. Images used with a non-root
	// command user must provide util-linux setpriv.
	if command.User != "" && command.User != "0:0" {
		uid, gid, _ := strings.Cut(command.User, ":")
		args = append([]string{"env", "HOME=/", "setpriv", "--reuid=" + uid, "--regid=" + gid, "--clear-groups", "--"}, args...)
	}
	if command.Dir != "" {
		args = append([]string{"/bin/sh", "-c", `cd "$1" || exit 126; shift; exec "$@"`, "aries-exec", command.Dir}, args...)
	}

	exceeded := func() { cancel(fmt.Errorf("Kubernetes exec output exceeds %d bytes", outputLimit)) }
	runErr := manager.execInPod(execCtx, id, stdin,
		&limitedWriter{writer: stdout, limit: outputLimit, onExceed: exceeded},
		&limitedWriter{writer: stderr, limit: outputLimit, onExceed: exceeded}, args...)
	if execCtx.Err() != nil {
		return failure(), context.Cause(execCtx)
	}
	exitCode := 0
	if runErr != nil {
		var exit interface{ ExitCode() int }
		if !errors.As(runErr, &exit) || exit.ExitCode() < 0 {
			return failure(), fmt.Errorf("Kubernetes exec: %w", runErr)
		}
		exitCode = exit.ExitCode()
	}
	return core.CommandResult{ExitCode: exitCode, Duration: time.Since(started)}, nil
}

// execInPod is the common kubectl transport for commands and archive streams.
func (manager *Manager) execInPod(ctx context.Context, id string, stdin io.Reader, stdout, stderr io.Writer, command ...string) error {
	args := []string{"exec", "-n", manager.options.Namespace, id, "-c", containerName}
	if stdin != nil {
		args = append(args, "-i")
	}
	args = append(args, "--")
	return manager.kubectl.Run(ctx, stdin, stdout, stderr, append(args, command...)...)
}

// UploadArchive extracts a tar stream beneath destination. Root containers keep
// the archive's ownership; a non-root container owns what it extracts.
func (manager *Manager) UploadArchive(ctx context.Context, id, destination string, archive io.Reader) error {
	if destination == "" || !strings.HasPrefix(destination, "/") || strings.ContainsRune(destination, 0) {
		return errors.New("deployment upload destination must be absolute")
	}
	var stderr bytes.Buffer
	if err := manager.execInPod(ctx, id, archive, io.Discard, &stderr, "tar", "-x", "-f", "-", "-C", destination); err != nil {
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
		// stat reports a missing source on stderr.
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
		err := manager.execInPod(streamCtx, id, nil, writer, &stderr, "tar", "-c", "-f", "-", "-C", path.Dir(source), path.Base(source))
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
