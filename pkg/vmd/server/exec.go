package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	chunkSize = 32 << 10
	// drainSeconds bounds output copying after the command exits, when
	// background children may still hold its pipes open.
	drainSeconds = 2.0
	// exitWaitTimeout bounds waiting for the wrapper to exit after its
	// process group was confirmed terminated.
	exitWaitTimeout = 10 * time.Second
)

var rootIdentity = identity{}

func randomToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

// runningRecord returns a sandbox that can execute commands.
func (s *Server) runningRecord(ctx context.Context, id string) (sandboxRecord, error) {
	record, err := s.record(id)
	if err != nil {
		return record, err
	}
	state, err := s.engine.InspectContainer(ctx, id)
	if errors.Is(err, errEngineNotFound) {
		return record, status.Errorf(codes.NotFound, "sandbox %s container not found", id)
	}
	if err != nil {
		return record, internal(err)
	}
	if record.Phase != phaseReady || state.Status != api.StatusRunning {
		return record, status.Errorf(codes.FailedPrecondition, "sandbox %s is not running", id)
	}
	return record, nil
}

func (s *Server) execUser(record sandboxRecord, user string) (identity, error) {
	if user == "" {
		return record.User, nil
	}
	userPart, groupPart, hasGroup := strings.Cut(user, ":")
	for _, part := range []string{userPart, groupPart} {
		if strings.Trim(part, "0123456789") != "" {
			return identity{}, status.Errorf(codes.InvalidArgument, "user %q must be numeric uid or uid:gid", user)
		}
	}
	if userPart == "" || hasGroup && groupPart == "" {
		return identity{}, status.Errorf(codes.InvalidArgument, "user %q must be numeric uid or uid:gid", user)
	}
	resolved, err := resolveUser(user, record.Passwd, record.Group)
	if err != nil {
		return identity{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return resolved, nil
}

// frameSender serializes sends on one stream and keeps draining after the
// client has gone, so the guest process never blocks on a full pipe.
type frameSender struct {
	mu     sync.Mutex
	stream grpc.BidiStreamingServer[api.ExecFrame, api.ExecFrame]
	failed bool
}

func (f *frameSender) send(frame *api.ExecFrame) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed {
		return
	}
	if err := f.stream.Send(frame); err != nil {
		f.failed = true
	}
}

type frameWriter struct {
	sender *frameSender
	stderr bool
}

func (w frameWriter) Write(content []byte) (int, error) {
	for offset := 0; offset < len(content); offset += chunkSize {
		chunk := bytes.Clone(content[offset:min(len(content), offset+chunkSize)])
		if w.stderr {
			w.sender.send(&api.ExecFrame{Stderr: chunk})
		} else {
			w.sender.send(&api.ExecFrame{Stdout: chunk})
		}
	}
	return len(content), nil
}

func (s *Server) Exec(stream grpc.BidiStreamingServer[api.ExecFrame, api.ExecFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.Start
	if start == nil || len(start.Args) == 0 || !path.IsAbs(start.Args[0]) {
		return status.Error(codes.InvalidArgument, "exec requires a start frame with an absolute argv[0]")
	}
	if start.Dir != "" && !validPath(start.Dir) {
		return status.Errorf(codes.InvalidArgument, "exec dir %q must be a clean absolute path", start.Dir)
	}
	for _, entry := range start.Env {
		if key, _, ok := strings.Cut(entry, "="); !ok || key == "" {
			return status.Errorf(codes.InvalidArgument, "environment entry %q is invalid", entry)
		}
	}
	record, err := s.runningRecord(stream.Context(), start.Sandbox)
	if err != nil {
		return err
	}
	user, err := s.execUser(record, start.User)
	if err != nil {
		return err
	}
	token, err := randomToken()
	if err != nil {
		return internal(err)
	}
	statePath := api.StateDir + "/" + token
	cwd := start.Dir
	if cwd == "" {
		cwd = record.Cwd
	}
	process := processSpec{
		Args: slices.Concat([]string{api.HelperPath, "run", "--state", statePath, "--"}, start.Args),
		Env:  mergeEnv(record.Env, start.Env), Cwd: cwd, User: user,
	}
	sender := &frameSender{stream: stream}
	var stdin io.Reader
	var stdinWriter *io.PipeWriter
	if start.Stdin {
		var reader *io.PipeReader
		reader, stdinWriter = io.Pipe()
		stdin = reader
	}
	// The exec outlives the stream context so cancellation can be confirmed.
	execCtx := context.WithoutCancel(stream.Context())
	proc, err := s.engine.Exec(execCtx, record.Spec.Name, "exec-"+token, process, stdin, frameWriter{sender: sender}, frameWriter{sender: sender, stderr: true})
	if err != nil {
		if stdinWriter != nil {
			stdinWriter.Close()
		}
		return status.Errorf(codes.Internal, "start exec: %v", err)
	}

	cancelRequested := make(chan struct{})
	var cancelOnce sync.Once
	requestCancel := func() { cancelOnce.Do(func() { close(cancelRequested) }) }
	go func() {
		closeStdin := func(err error) {
			if stdinWriter != nil {
				stdinWriter.CloseWithError(err)
			}
		}
		for {
			frame, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				closeStdin(nil)
				return
			}
			if err != nil {
				closeStdin(err)
				requestCancel()
				return
			}
			if frame.Stdin != nil && stdinWriter != nil {
				if _, err := stdinWriter.Write(frame.Stdin); err != nil {
					stdinWriter = nil
				}
			}
			if frame.StdinEOF {
				closeStdin(nil)
			}
			if frame.Cancel {
				requestCancel()
			}
		}
	}()
	go func() {
		<-stream.Context().Done()
		requestCancel()
	}()

	type exitResult struct {
		code int
		err  error
	}
	exited := make(chan exitResult, 1)
	go func() {
		code, err := proc.Wait(execCtx)
		exited <- exitResult{code, err}
	}()

	select {
	case result := <-exited:
		return s.finishExec(stream.Context(), proc, sender, result.code, result.err)
	case <-cancelRequested:
		select {
		case result := <-exited:
			return s.finishExec(stream.Context(), proc, sender, result.code, result.err)
		default:
		}
	}
	ctx, cancel := s.cleanupContext(stream.Context())
	defer cancel()
	confirmed, cancelErr := s.terminateExec(ctx, record.Spec.Name, statePath, proc, func() bool {
		select {
		case <-exited:
			return true
		case <-time.After(exitWaitTimeout):
			return false
		case <-ctx.Done():
			return false
		}
	})
	proc.Drain(drainSeconds)
	if err := proc.Delete(ctx); err != nil {
		cancelErr = errors.Join(cancelErr, fmt.Errorf("delete exec: %w", err))
		confirmed = false
	}
	exit := &api.ExecExit{Code: -1, Canceled: true, CancelConfirmed: confirmed}
	if cancelErr != nil {
		exit.Error = cancelErr.Error()
	}
	s.logger.WithFields(map[string]any{"sandbox": record.Spec.Name, "confirmed": confirmed}).Info("exec cancelled")
	sender.send(&api.ExecFrame{Exit: exit})
	return nil
}

func (s *Server) finishExec(ctx context.Context, proc execProcess, sender *frameSender, code int, waitErr error) error {
	proc.Drain(drainSeconds)
	ctx, cancel := s.cleanupContext(ctx)
	defer cancel()
	deleteErr := proc.Delete(ctx)
	if err := errors.Join(waitErr, deleteErr); err != nil {
		sender.send(&api.ExecFrame{Exit: &api.ExecExit{Code: -1, Error: err.Error()}})
		return nil
	}
	sender.send(&api.ExecFrame{Exit: &api.ExecExit{Code: code}})
	return nil
}

// terminateExec kills the command's process group through the in-guest
// helper and confirms the wrapper exited. The sandbox keeps running.
func (s *Server) terminateExec(ctx context.Context, id, statePath string, proc execProcess, waitExit func() bool) (bool, error) {
	var output bytes.Buffer
	code, err := s.runHelper(ctx, id, []string{api.HelperPath, "kill", "--state", statePath}, nil, &output)
	if err != nil {
		return false, fmt.Errorf("run helper kill: %w", err)
	}
	switch code {
	case 0:
		if waitExit() {
			return true, nil
		}
		// The group is gone but the wrapper lingers; kill the wrapper itself.
		if err := proc.Kill(ctx, syscall.SIGKILL); err != nil {
			return false, fmt.Errorf("kill exec wrapper: %w", err)
		}
		if waitExit() {
			return true, nil
		}
		return false, errors.New("exec wrapper did not exit after its process group was terminated")
	case api.HelperExitStateMissing:
		// The command never recorded a process group; kill the exec directly.
		if err := proc.Kill(ctx, syscall.SIGKILL); err != nil {
			return false, fmt.Errorf("kill unstarted exec: %w", err)
		}
		if waitExit() {
			return true, nil
		}
		return false, errors.New("exec did not exit after SIGKILL")
	default:
		return false, fmt.Errorf("helper could not confirm process group termination (exit %d): %s", code, strings.TrimSpace(limitString(output.String(), 4096)))
	}
}

func limitString(value string, limit int) string {
	if len(value) > limit {
		return value[:limit]
	}
	return value
}

// runHelper executes a root helper command and waits for it.
func (s *Server) runHelper(ctx context.Context, id string, args []string, stdin io.Reader, output io.Writer) (int, error) {
	token, err := randomToken()
	if err != nil {
		return -1, err
	}
	proc, err := s.engine.Exec(ctx, id, "helper-"+token, processSpec{Args: args, Cwd: "/", User: rootIdentity, Env: []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin"}}, stdin, output, output)
	if err != nil {
		return -1, err
	}
	code, waitErr := proc.Wait(ctx)
	proc.Drain(drainSeconds)
	deleteCtx, cancel := s.cleanupContext(ctx)
	defer cancel()
	return code, errors.Join(waitErr, proc.Delete(deleteCtx))
}

func (s *Server) PutArchive(stream grpc.ClientStreamingServer[api.ArchiveChunk, api.Empty]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if !validPath(first.Path) {
		return status.Errorf(codes.InvalidArgument, "archive destination %q must be a clean absolute path", first.Path)
	}
	record, err := s.runningRecord(stream.Context(), first.Sandbox)
	if err != nil {
		return err
	}
	reader, writer := io.Pipe()
	go func() {
		if _, err := writer.Write(first.Data); err != nil {
			return
		}
		for {
			chunk, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				writer.Close()
				return
			}
			if err != nil {
				writer.CloseWithError(err)
				return
			}
			if _, err := writer.Write(chunk.Data); err != nil {
				return
			}
		}
	}()
	var output limitedBuffer
	output.limit = 8192
	code, err := s.runHelper(stream.Context(), record.Spec.Name, []string{api.HelperPath, "tar-extract", "--owner-user", record.ImageUser, first.Path}, reader, &output)
	reader.CloseWithError(errors.New("archive extraction finished"))
	if err != nil {
		return status.Errorf(codes.Internal, "extract archive: %v", err)
	}
	switch code {
	case 0:
		return stream.SendAndClose(&api.Empty{})
	case api.HelperExitNotFound:
		return status.Errorf(codes.FailedPrecondition, "destination not found: %s", first.Path)
	default:
		return status.Errorf(codes.Internal, "extract archive failed (exit %d): %s", code, strings.TrimSpace(output.String()))
	}
}

type limitedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(content []byte) (int, error) {
	if remaining := b.limit - b.Len(); remaining > 0 {
		b.Buffer.Write(content[:min(len(content), remaining)])
	}
	return len(content), nil
}

// archiveWriter parses the helper's leading JSON stat line, sends it as the
// first chunk, and forwards the following tar stream.
type archiveWriter struct {
	stream  grpc.ServerStreamingServer[api.ArchiveChunk]
	header  bytes.Buffer
	sentHdr bool
	err     error
}

func (w *archiveWriter) Write(content []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	consumed := len(content)
	if !w.sentHdr {
		index := bytes.IndexByte(content, '\n')
		if index < 0 {
			if w.header.Len()+len(content) > 64<<10 {
				w.err = errors.New("archive stat header is too long")
				return 0, w.err
			}
			w.header.Write(content)
			return consumed, nil
		}
		w.header.Write(content[:index])
		content = content[index+1:]
		var stat api.FileStat
		if err := json.Unmarshal(w.header.Bytes(), &stat); err != nil {
			w.err = fmt.Errorf("decode archive stat: %w", err)
			return 0, w.err
		}
		if err := w.stream.Send(&api.ArchiveChunk{Stat: &stat}); err != nil {
			w.err = err
			return 0, err
		}
		w.sentHdr = true
	}
	for offset := 0; offset < len(content); offset += chunkSize {
		chunk := bytes.Clone(content[offset:min(len(content), offset+chunkSize)])
		if err := w.stream.Send(&api.ArchiveChunk{Data: chunk}); err != nil {
			w.err = err
			return 0, err
		}
	}
	return consumed, nil
}

func (s *Server) GetArchive(request *api.ArchiveRequest, stream grpc.ServerStreamingServer[api.ArchiveChunk]) error {
	if !validPath(request.Path) {
		return status.Errorf(codes.InvalidArgument, "archive source %q must be a clean absolute path", request.Path)
	}
	record, err := s.runningRecord(stream.Context(), request.Sandbox)
	if err != nil {
		return err
	}
	token, err := randomToken()
	if err != nil {
		return internal(err)
	}
	archive := &archiveWriter{stream: stream}
	diagnostics := &limitedBuffer{limit: 8192}
	proc, err := s.engine.Exec(stream.Context(), record.Spec.Name, "helper-"+token,
		processSpec{Args: []string{api.HelperPath, "tar-create", request.Path}, Cwd: "/", User: rootIdentity, Env: []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin"}},
		nil, archive, diagnostics)
	if err != nil {
		return status.Errorf(codes.Internal, "start archive helper: %v", err)
	}
	code, waitErr := proc.Wait(stream.Context())
	proc.Drain(drainSeconds)
	ctx, cancel := s.cleanupContext(stream.Context())
	defer cancel()
	if err := errors.Join(waitErr, proc.Delete(ctx)); err != nil {
		return status.Errorf(codes.Internal, "archive helper: %v", err)
	}
	switch {
	case code == api.HelperExitNotFound && !archive.sentHdr:
		return stream.Send(&api.ArchiveChunk{Missing: true})
	case code != 0:
		return status.Errorf(codes.Internal, "create archive failed (exit %d): %s", code, strings.TrimSpace(diagnostics.String()))
	case archive.err != nil:
		return status.Errorf(codes.Internal, "stream archive: %v", archive.err)
	case !archive.sentHdr:
		return status.Error(codes.Internal, "archive helper produced no stat header")
	}
	return nil
}
