package deployment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/hyscale-lab/aries/pkg/config"
	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/deployment"
	dockerdeployment "github.com/hyscale-lab/aries/pkg/deployment/docker"
	vmddeployment "github.com/hyscale-lab/aries/pkg/deployment/vmd"
	"github.com/hyscale-lab/aries/pkg/monitor"
	"github.com/sirupsen/logrus"
)

// NewVMDSandbox constructs the microVM sandbox transport, its task-environment
// constructor, and the occurrence's resource source. The harness Docker daemon
// hosts the task network that the harness and embedded bridge share; its
// client closes with the returned transport. On success the caller owns the
// transport and resources; partial construction closes everything it opened.
func NewVMDSandbox(sandbox, harness config.DeploymentConfig, runID, occurrenceID string, logger *logrus.Logger) (deployment.Deployment, func() deployment.TaskEnvironment, monitor.ResourceSource, error) {
	if sandbox.VMD == nil || harness.Docker == nil {
		return nil, nil, nil, errors.New("vmd sandbox requires normalized vmd and harness Docker deployment settings")
	}
	docker, err := NewDocker(harness, logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("construct harness task network deployment: %w", err)
	}
	options := vmddeployment.Options{Socket: sandbox.VMD.Socket, Logger: logger}
	transportOptions := options
	transportOptions.OnClose = docker.Close
	transport, err := vmddeployment.New(transportOptions)
	if err != nil {
		return nil, nil, nil, errors.Join(fmt.Errorf("construct sandbox deployment: %w", err), docker.Close())
	}
	dockerResources, err := dockerdeployment.NewResourceSource(dockerdeployment.ResourceOptions{DockerSocket: harness.Docker.Socket, RunID: runID, TaskIDs: []string{occurrenceID}})
	if err != nil {
		return nil, nil, nil, errors.Join(fmt.Errorf("construct Docker resource source: %w", err), transport.Close())
	}
	vmdResources, err := vmddeployment.NewResourceSource(options, runID, occurrenceID)
	if err != nil {
		return nil, nil, nil, errors.Join(fmt.Errorf("construct vmd resource source: %w", err), dockerResources.Close(), transport.Close())
	}
	resources := &combinedResources{sources: []monitor.ResourceSource{dockerResources, vmdResources}}
	return transport, vmddeployment.NewTaskEnvironmentFactory(transport, docker.NewTaskEnvironment), resources, nil
}

// PrepareVMDImages checks aries-vmd readiness and imports each sandbox image
// from the harness Docker daemon, which pullImages already prepared.
func PrepareVMDImages(ctx context.Context, sandbox, harness config.DeploymentConfig, kataVersion string, images []string, logger *logrus.Logger) error {
	if sandbox.VMD == nil || harness.Docker == nil {
		return errors.New("vmd sandbox requires normalized vmd and harness Docker deployment settings")
	}
	transport, err := vmddeployment.New(vmddeployment.Options{Socket: sandbox.VMD.Socket, Logger: logger})
	if err != nil {
		return err
	}
	defer transport.Close()
	health, err := transport.Preflight(ctx, sandbox.VMD.Isolation, kataVersion)
	if err != nil {
		return fmt.Errorf("sandbox.deployment: %w", err)
	}
	logger.WithFields(logrus.Fields{"containerd": health.ContainerdVersion, "kata": health.KataVersion, "firecracker": health.FirecrackerVersion, "helper": health.HelperVersion}).Info("aries-vmd ready")
	socket := harness.Docker.Socket
	return transport.EnsureImages(ctx, images, vmddeployment.ImageSource{
		ID: func(ctx context.Context, image string) (string, error) {
			return dockerdeployment.ImageID(ctx, socket, image)
		},
		Save: func(ctx context.Context, image string) (io.ReadCloser, error) {
			return dockerdeployment.SaveImage(ctx, socket, image)
		},
	})
}

// SandboxImages removes the harness image, which never runs under aries-vmd.
func SandboxImages(images []string, harnessImage string) []string {
	return slices.DeleteFunc(slices.Clone(images), func(image string) bool { return image == harnessImage })
}

type combinedResources struct {
	sources []monitor.ResourceSource
}

func (combined *combinedResources) Sample(ctx context.Context) ([]core.ResourceReading, error) {
	var readings []core.ResourceReading
	for _, source := range combined.sources {
		sampled, err := source.Sample(ctx)
		if err != nil {
			return nil, err
		}
		readings = append(readings, sampled...)
	}
	return readings, nil
}

func (combined *combinedResources) Close() error {
	var errs []error
	for _, source := range combined.sources {
		errs = append(errs, source.Close())
	}
	return errors.Join(errs...)
}
