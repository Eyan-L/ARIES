package vmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/hyscale-lab/aries/pkg/deployment"
	"github.com/hyscale-lab/aries/pkg/runner"
	"github.com/hyscale-lab/aries/pkg/vmd/api"
)

// UploadArchive extracts a tar stream into an existing sandbox directory.
func (manager *Manager) UploadArchive(ctx context.Context, id, destination string, archive io.Reader) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := manager.api.PutArchive(streamCtx)
	if err != nil {
		return fmt.Errorf("open vmd archive upload: %w", err)
	}
	if err := stream.Send(&api.ArchiveChunk{Sandbox: id, Path: destination}); err != nil {
		return fmt.Errorf("send vmd archive upload: %w", err)
	}
	buffer := make([]byte, chunkSize)
	for {
		read, readErr := archive.Read(buffer)
		if read > 0 {
			if err := stream.Send(&api.ArchiveChunk{Data: append([]byte(nil), buffer[:read]...)}); err != nil {
				// The server's status explains a rejected stream better than the send error.
				_, closeErr := stream.CloseAndRecv()
				return fmt.Errorf("send vmd archive upload: %w", errors.Join(err, closeErr))
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read vmd archive upload: %w", readErr)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("complete vmd archive upload: %w", err)
	}
	return nil
}

// DownloadArchive returns a tar stream of one sandbox path. A missing source
// in a live sandbox wraps runner.ErrNotFound; a missing sandbox does not.
func (manager *Manager) DownloadArchive(ctx context.Context, id, path string) (io.ReadCloser, deployment.FileInfo, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := manager.api.GetArchive(streamCtx, &api.ArchiveRequest{Sandbox: id, Path: path})
	if err != nil {
		cancel()
		return nil, deployment.FileInfo{}, fmt.Errorf("open vmd archive download: %w", err)
	}
	first, err := stream.Recv()
	if err != nil {
		cancel()
		if errors.Is(err, io.EOF) {
			return nil, deployment.FileInfo{}, errors.New("vmd archive download ended before metadata")
		}
		return nil, deployment.FileInfo{}, fmt.Errorf("vmd archive download: %w", err)
	}
	if first.Missing {
		cancel()
		return nil, deployment.FileInfo{}, fmt.Errorf("deployment archive: %w: %s", runner.ErrNotFound, path)
	}
	if first.Stat == nil {
		cancel()
		return nil, deployment.FileInfo{}, errors.New("vmd archive download omitted source metadata")
	}
	reader := &archiveReader{recv: stream.Recv, cancel: cancel, pending: first.Data}
	return reader, deployment.FileInfo{Size: first.Stat.Size, Mode: first.Stat.Mode}, nil
}

type archiveReader struct {
	recv    func() (*api.ArchiveChunk, error)
	cancel  context.CancelFunc
	pending []byte
	err     error
}

func (r *archiveReader) Read(buffer []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		chunk, err := r.recv()
		if errors.Is(err, io.EOF) {
			r.err = io.EOF
			continue
		}
		if err != nil {
			r.err = fmt.Errorf("vmd archive download: %w", err)
			continue
		}
		r.pending = chunk.Data
	}
	read := copy(buffer, r.pending)
	r.pending = r.pending[read:]
	return read, nil
}

func (r *archiveReader) Close() error {
	r.cancel()
	return nil
}
