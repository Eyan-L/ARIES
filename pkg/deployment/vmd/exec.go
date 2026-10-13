package vmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/vmd/api"
)

const (
	maxExecInput        = 16 << 20
	maxExecOutput       = 16 << 20
	maxConfiguredOutput = 1 << 30
	stdinChunk          = 32 << 10
)

var errTerminationUnconfirmed = errors.New("vmd exec process-group termination unconfirmed")

func (manager *Manager) Exec(ctx context.Context, id string, command core.Command) (core.CommandResult, error) {
	started := time.Now()
	if len(command.Stdin) > maxExecInput {
		return core.CommandResult{ExitCode: -1, Duration: time.Since(started)}, fmt.Errorf("vmd exec stdin exceeds %d bytes", maxExecInput)
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

type execOutcome struct {
	exit *api.ExecExit
	err  error
}

// ExecStream runs exact argv in the sandbox. Cancellation asks the daemon to
// terminate the command's process group while the sandbox keeps running, and
// succeeds only when the daemon confirms that termination.
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
	execCtx := ctx
	cancel := func() {}
	if command.Timeout > 0 {
		execCtx, cancel = context.WithTimeout(ctx, command.Timeout)
	}
	defer cancel()

	// The stream outlives caller cancellation so that termination can still
	// be requested and confirmed over it.
	streamCtx, closeStream := context.WithCancel(context.WithoutCancel(ctx))
	defer closeStream()
	stream, err := manager.api.Exec(streamCtx)
	if err != nil {
		return failure(), fmt.Errorf("open vmd exec: %w", err)
	}
	var sendMu sync.Mutex
	send := func(frame *api.ExecFrame) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(frame)
	}
	start := &api.ExecStart{
		Sandbox: id, Args: append([]string{command.Path}, command.Args...),
		Env: environment(command.Env), Dir: command.Dir, User: command.User, Stdin: stdin != nil,
	}
	if err := send(&api.ExecFrame{Start: start}); err != nil {
		return failure(), fmt.Errorf("start vmd exec: %w", err)
	}

	inputFailed := make(chan error, 1)
	if stdin != nil {
		go func() {
			if err := forwardStdin(stdin, send); err != nil {
				inputFailed <- err
			}
		}()
	}
	outputFailed := make(chan error, 1)
	done := make(chan execOutcome, 1)
	go func() {
		out := &limitedWriter{writer: stdout, limit: outputLimit}
		errOut := &limitedWriter{writer: stderr, limit: outputLimit}
		var writeErr error
		for {
			frame, err := stream.Recv()
			if err != nil {
				done <- execOutcome{err: err}
				return
			}
			if writeErr == nil {
				if _, err := out.Write(frame.Stdout); err != nil {
					writeErr = err
				} else if _, err := errOut.Write(frame.Stderr); err != nil {
					writeErr = err
				}
				if writeErr != nil {
					outputFailed <- writeErr
				}
			}
			if frame.Exit != nil {
				done <- execOutcome{exit: frame.Exit}
				return
			}
		}
	}()

	terminate := func(cause error) (core.CommandResult, error) {
		cancelErr := send(&api.ExecFrame{Cancel: true})
		timer := time.NewTimer(manager.cleanupTimeout())
		defer timer.Stop()
		select {
		case outcome := <-done:
			if outcome.exit != nil && (outcome.exit.CancelConfirmed || !outcome.exit.Canceled && outcome.exit.Error == "") {
				return failure(), cause
			}
			detail := errTerminationUnconfirmed
			if outcome.exit != nil && outcome.exit.Error != "" {
				detail = fmt.Errorf("%w: %s", errTerminationUnconfirmed, outcome.exit.Error)
			} else if outcome.err != nil {
				detail = fmt.Errorf("%w: %w", errTerminationUnconfirmed, outcome.err)
			}
			return failure(), errors.Join(cause, cancelErr, detail)
		case <-timer.C:
			return failure(), errors.Join(cause, cancelErr, fmt.Errorf("%w: no reply within %s", errTerminationUnconfirmed, manager.cleanupTimeout()))
		}
	}

	select {
	case <-execCtx.Done():
		return terminate(execCtx.Err())
	case err := <-inputFailed:
		return terminate(err)
	case err := <-outputFailed:
		return terminate(err)
	case outcome := <-done:
		if outcome.err != nil {
			return failure(), errors.Join(fmt.Errorf("vmd exec stream: %w", outcome.err), errTerminationUnconfirmed)
		}
		if outcome.exit.Error != "" {
			return failure(), fmt.Errorf("vmd exec: %s", outcome.exit.Error)
		}
		select {
		case err := <-outputFailed:
			return failure(), err
		default:
		}
		return core.CommandResult{ExitCode: outcome.exit.Code, Duration: time.Since(started)}, nil
	}
}

func forwardStdin(stdin io.Reader, send func(*api.ExecFrame) error) error {
	buffer := make([]byte, stdinChunk)
	total := 0
	for {
		read, readErr := stdin.Read(buffer)
		if read > 0 {
			total += read
			if total > maxExecInput {
				return fmt.Errorf("vmd exec stdin exceeds %d bytes", maxExecInput)
			}
			if err := send(&api.ExecFrame{Stdin: append([]byte(nil), buffer[:read]...)}); err != nil {
				return nil // the stream ended; its outcome reports why
			}
		}
		if errors.Is(readErr, io.EOF) {
			_ = send(&api.ExecFrame{StdinEOF: true})
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read vmd exec stdin: %w", readErr)
		}
	}
}

type limitedWriter struct {
	writer io.Writer
	wrote  int
	limit  int
}

func (w *limitedWriter) Write(content []byte) (int, error) {
	if len(content) == 0 {
		return 0, nil
	}
	if len(content) > w.limit-w.wrote {
		return 0, fmt.Errorf("vmd exec output exceeds %d bytes", w.limit)
	}
	written, err := w.writer.Write(content)
	w.wrote += written
	return written, err
}

func environment(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for _, key := range slices.Sorted(maps.Keys(values)) {
		result = append(result, key+"="+values[key])
	}
	return result
}

func validateCommand(command core.Command) error {
	if command.Path == "" || strings.ContainsRune(command.Path, 0) {
		return errors.New("command path must be nonempty and NUL-free")
	}
	if command.Dir != "" {
		if command.Dir != filepath.Clean(command.Dir) || !strings.HasPrefix(command.Dir, "/") || strings.ContainsRune(command.Dir, 0) {
			return errors.New("invalid command workdir: path must be absolute, clean, and NUL-free")
		}
	}
	if err := validateExecUser(command.User); err != nil {
		return fmt.Errorf("invalid command user: %w", err)
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

func validateExecUser(value string) error {
	if value == "" {
		return nil
	}
	uid, gid, found := strings.Cut(value, ":")
	if !found || strings.ContainsRune(gid, ':') || !decimalDigits(uid) || !decimalDigits(gid) {
		return errors.New("exec user must be a numeric UID:GID pair")
	}
	return nil
}

func decimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
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
