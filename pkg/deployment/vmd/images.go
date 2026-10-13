package vmd

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
	"github.com/sirupsen/logrus"
)

// ImageSource is the local Docker Engine that holds the pinned images.
type ImageSource struct {
	// ID returns the Docker image ID for a reference.
	ID func(context.Context, string) (string, error)
	// Save exports a reference as a `docker save` archive.
	Save func(context.Context, string) (io.ReadCloser, error)
}

// EnsureImages makes each image available to aries-vmd as the same image
// configuration Docker holds. An image is skipped when the daemon already
// reports the Docker image ID as its configuration or target digest (classic
// or containerd Docker image store); otherwise it is
// exported, imported, and verified against the exported configuration digest.
func (manager *Manager) EnsureImages(ctx context.Context, images []string, source ImageSource) error {
	seen := make(map[string]struct{}, len(images))
	for _, image := range images {
		if _, ok := seen[image]; ok {
			continue
		}
		seen[image] = struct{}{}
		dockerID, err := source.ID(ctx, image)
		if err != nil {
			return fmt.Errorf("inspect Docker image %q: %w", image, err)
		}
		present, err := manager.api.InspectImage(ctx, &api.ImageRequest{Reference: image})
		if err != nil && !isNotFound(err) {
			return fmt.Errorf("inspect vmd image %q: %w", image, err)
		}
		if err == nil && present.Present && (present.ConfigDigest == dockerID || present.TargetDigest == dockerID) {
			continue
		}
		manager.logger.WithContext(ctx).WithField("image", image).Info("importing image into aries-vmd")
		if err := manager.importImage(ctx, image, source); err != nil {
			return err
		}
	}
	return nil
}

func (manager *Manager) importImage(ctx context.Context, image string, source ImageSource) error {
	archive, err := source.Save(ctx, image)
	if err != nil {
		return fmt.Errorf("export Docker image %q: %w", image, err)
	}
	defer archive.Close()
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := manager.api.ImportImage(streamCtx)
	if err != nil {
		return fmt.Errorf("open vmd image import %q: %w", image, err)
	}
	if err := stream.Send(&api.ImageChunk{Reference: image}); err != nil {
		return fmt.Errorf("send vmd image import %q: %w", image, err)
	}
	// The exported manifest names the configuration blob, whose digest is the
	// identity the daemon must report after import.
	parsedReader, parsedWriter := io.Pipe()
	parsed := make(chan configResult, 1)
	go func() {
		digest, err := exportedConfigDigest(parsedReader)
		_, _ = io.Copy(io.Discard, parsedReader)
		parsed <- configResult{digest: digest, err: err}
	}()
	buffer := make([]byte, chunkSize)
	var transferErr error
	for {
		read, readErr := archive.Read(buffer)
		if read > 0 {
			_, _ = parsedWriter.Write(buffer[:read])
			if err := stream.Send(&api.ImageChunk{Data: append([]byte(nil), buffer[:read]...)}); err != nil {
				_, closeErr := stream.CloseAndRecv()
				transferErr = errors.Join(err, closeErr)
				break
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			transferErr = readErr
			break
		}
	}
	_ = parsedWriter.CloseWithError(transferErr)
	expected := <-parsed
	if transferErr != nil {
		return fmt.Errorf("stream image %q to aries-vmd: %w", image, transferErr)
	}
	info, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("import image %q into aries-vmd: %w", image, err)
	}
	if expected.err != nil {
		return fmt.Errorf("read exported image %q configuration: %w", image, expected.err)
	}
	if !info.Present || info.ConfigDigest != expected.digest {
		return fmt.Errorf("aries-vmd image %q has configuration %q, want exported %q", image, info.ConfigDigest, expected.digest)
	}
	manager.logger.WithContext(ctx).WithFields(logrus.Fields{"image": image, "config_digest": info.ConfigDigest}).Info("aries-vmd image imported")
	return nil
}

type configResult struct {
	digest string
	err    error
}

// exportedConfigDigest reads a `docker save` archive and returns the digest of
// its single image configuration.
func exportedConfigDigest(archive io.Reader) (string, error) {
	reader := tar.NewReader(archive)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return "", errors.New("export has no manifest.json")
		}
		if err != nil {
			return "", err
		}
		if path.Clean(header.Name) != "manifest.json" {
			continue
		}
		var manifest []struct {
			Config string `json:"Config"`
		}
		if err := json.NewDecoder(io.LimitReader(reader, 1<<20)).Decode(&manifest); err != nil {
			return "", fmt.Errorf("decode manifest.json: %w", err)
		}
		if len(manifest) != 1 {
			return "", fmt.Errorf("export contains %d images, want 1", len(manifest))
		}
		config := path.Clean(manifest[0].Config)
		// Legacy exports name "<hex>.json"; OCI-layout exports name "blobs/sha256/<hex>".
		if hex, ok := strings.CutPrefix(config, "blobs/sha256/"); ok {
			return "sha256:" + hex, nil
		}
		if hex, ok := strings.CutSuffix(config, ".json"); ok && !strings.Contains(hex, "/") {
			return "sha256:" + hex, nil
		}
		return "", fmt.Errorf("unrecognized configuration path %q", manifest[0].Config)
	}
}
