package deployment

import (
	"fmt"

	"github.com/hyscale-lab/aries/pkg/config"
	kubernetesdeployment "github.com/hyscale-lab/aries/pkg/deployment/kubernetes"
	"github.com/hyscale-lab/aries/pkg/monitor"
	"github.com/sirupsen/logrus"
)

func kubernetesOptions(cfg config.DeploymentConfig, logger *logrus.Logger) kubernetesdeployment.Options {
	k := cfg.Kubernetes
	return kubernetesdeployment.Options{
		Namespace: k.Namespace, NodeRole: k.NodeRole, PodCIDR: k.PodCIDR, ServiceCIDR: k.ServiceCIDR,
		ServiceAccount: k.ServiceAccount, ImagePullSecret: k.ImagePullSecret, Images: k.Images, Logger: logger,
	}
}

func NewKubernetes(cfg config.DeploymentConfig, logger *logrus.Logger) (*kubernetesdeployment.Manager, error) {
	return kubernetesdeployment.New(kubernetesOptions(cfg, logger))
}

// NewKubernetesHarness adds the staging roots a harness image needs. A pod's
// files are extracted by the image's own user, and OpenClaw runs as UID 1000
// while staging under root-owned /run and /opt. Hermes starts as root.
func NewKubernetesHarness(cfg config.DeploymentConfig, harnessType string, logger *logrus.Logger) (*kubernetesdeployment.Manager, error) {
	options := kubernetesOptions(cfg, logger)
	if harnessType == "openclaw" {
		options.StagingDirs = []string{"/run/aries", "/opt/aries"}
		options.StagingOwner = "1000:1000"
	}
	return kubernetesdeployment.New(options)
}

// NewKubernetesSandbox constructs the transport and occurrence-scoped monitoring
// source. The caller owns both on success.
func NewKubernetesSandbox(cfg config.DeploymentConfig, runID, occurrenceID string, logger *logrus.Logger) (*kubernetesdeployment.Manager, monitor.ResourceSource, error) {
	transport, err := NewKubernetes(cfg, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("construct sandbox deployment: %w", err)
	}
	source, err := kubernetesdeployment.NewResourceSource(kubernetesdeployment.ResourceOptions{Namespace: cfg.Kubernetes.Namespace, RunID: runID, TaskIDs: []string{occurrenceID}})
	if err != nil {
		return nil, nil, fmt.Errorf("construct Kubernetes resource source: %w", err)
	}
	return transport, source, nil
}

// NewKubernetesRunResources observes the run's shared bridge pod.
func NewKubernetesRunResources(cfg config.DeploymentConfig, runID string) (monitor.ResourceSource, error) {
	return kubernetesdeployment.NewResourceSource(kubernetesdeployment.ResourceOptions{Scope: "run", Namespace: cfg.Kubernetes.Namespace, RunID: runID})
}
