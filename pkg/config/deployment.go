package config

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

// DeploymentConfig selects placement independently of the component identity.
// Kubernetes settings are recognized for preflight's not-implemented error.
// The vmd backend runs tool sandboxes through the privileged aries-vmd daemon.
type DeploymentConfig struct {
	Backend    string                      `json:"backend"`
	Docker     *DockerDeploymentConfig     `json:"docker,omitempty"`
	Kubernetes *KubernetesDeploymentConfig `json:"kubernetes,omitempty"`
	VMD        *VMDDeploymentConfig        `json:"vmd,omitempty"`
}

type DockerDeploymentConfig struct {
	Socket string `json:"socket,omitempty"`
}

// VMDDeploymentConfig selects the local aries-vmd daemon and its isolation class.
type VMDDeploymentConfig struct {
	Socket    string `json:"socket,omitempty"`
	Isolation string `json:"isolation,omitempty"`
}

// VMDIsolationKataFirecracker is the only isolation class aries-vmd provides.
const VMDIsolationKataFirecracker = "kata-fc"

type KubernetesDeploymentConfig struct {
	Context          string `json:"context,omitempty"`
	Namespace        string `json:"namespace,omitempty"`
	RuntimeClassName string `json:"runtime_class_name,omitempty"`
}

// NormalizeDeployment applies compatibility defaults and validates placement.
// It does no I/O and also supports configurations constructed by command callers.
// Calling it repeatedly preserves the normalized configuration.
func (c *Config) NormalizeDeployment() error {
	if c.Sandbox.Type != "" && c.Sandbox.Type != "docker" {
		return fmt.Errorf("sandbox.type: unsupported legacy value %q", c.Sandbox.Type)
	}
	if c.Sandbox.Type == "docker" && c.Sandbox.Deployment.Backend != "" && c.Sandbox.Deployment.Backend != "docker" {
		return fmt.Errorf("sandbox.type conflicts with sandbox.deployment.backend %q", c.Sandbox.Deployment.Backend)
	}
	if err := c.Harness.Deployment.normalize("harness.deployment"); err != nil {
		return err
	}
	if c.Harness.Deployment.Backend == "vmd" {
		return fmt.Errorf("harness.deployment: backend vmd supports only the tool sandbox")
	}
	if err := c.Sandbox.Deployment.normalize("sandbox.deployment"); err != nil {
		return err
	}
	if c.Harness.Deployment.Backend == "docker" && c.Sandbox.Deployment.Backend == "docker" && c.Harness.Deployment.Docker.Socket != c.Sandbox.Deployment.Docker.Socket {
		return fmt.Errorf("harness.deployment.docker.socket and sandbox.deployment.docker.socket must select the same local Docker daemon")
	}
	if c.Bridge.Mode == "" {
		c.Bridge.Mode = "embedded"
	}
	if c.Bridge.Mode != "embedded" {
		return fmt.Errorf("bridge.mode %q is unsupported; only embedded is implemented", c.Bridge.Mode)
	}
	c.Sandbox.Type = ""
	return nil
}

func (d *DeploymentConfig) normalize(path string) error {
	if d.Backend == "" {
		if d.Docker != nil || d.Kubernetes != nil || d.VMD != nil {
			return fmt.Errorf("%s.backend is required with backend options", path)
		}
		d.Backend = "docker"
	}
	switch d.Backend {
	case "docker":
		if d.Kubernetes != nil {
			return fmt.Errorf("%s.kubernetes requires backend kubernetes", path)
		}
		if d.VMD != nil {
			return fmt.Errorf("%s.vmd requires backend vmd", path)
		}
		socket := ""
		if d.Docker != nil {
			socket = d.Docker.Socket
		}
		socket, err := localSocket(socket, "/var/run/docker.sock")
		if err != nil {
			return fmt.Errorf("%s.docker.socket %w", path, err)
		}
		d.Docker = &DockerDeploymentConfig{Socket: socket}
	case "vmd":
		if d.Docker != nil {
			return fmt.Errorf("%s.docker requires backend docker", path)
		}
		if d.Kubernetes != nil {
			return fmt.Errorf("%s.kubernetes requires backend kubernetes", path)
		}
		var vmd VMDDeploymentConfig
		if d.VMD != nil {
			vmd = *d.VMD
		}
		socket, err := localSocket(vmd.Socket, "/run/aries-vmd.sock")
		if err != nil {
			return fmt.Errorf("%s.vmd.socket %w", path, err)
		}
		if vmd.Isolation == "" {
			vmd.Isolation = VMDIsolationKataFirecracker
		}
		if vmd.Isolation != VMDIsolationKataFirecracker {
			return fmt.Errorf("%s.vmd.isolation %q is unsupported; only %s is implemented", path, vmd.Isolation, VMDIsolationKataFirecracker)
		}
		d.VMD = &VMDDeploymentConfig{Socket: socket, Isolation: vmd.Isolation}
	case "kubernetes":
		if d.Docker != nil {
			return fmt.Errorf("%s.docker requires backend docker", path)
		}
		if d.VMD != nil {
			return fmt.Errorf("%s.vmd requires backend vmd", path)
		}
		if d.Kubernetes == nil {
			d.Kubernetes = &KubernetesDeploymentConfig{}
		}
		k := d.Kubernetes
		if strings.TrimSpace(k.Context) != k.Context || strings.ContainsFunc(k.Context, unicode.IsControl) {
			return fmt.Errorf("%s.kubernetes.context must not contain surrounding whitespace or control characters", path)
		}
		if k.Namespace != "" && (len(k.Namespace) > 63 || !kubernetesLabel.MatchString(k.Namespace)) {
			return fmt.Errorf("%s.kubernetes.namespace must be a DNS label", path)
		}
		if k.RuntimeClassName != "" {
			if len(k.RuntimeClassName) > 253 {
				return fmt.Errorf("%s.kubernetes.runtime_class_name must be a DNS subdomain", path)
			}
			for _, label := range strings.Split(k.RuntimeClassName, ".") {
				if !kubernetesLabel.MatchString(label) {
					return fmt.Errorf("%s.kubernetes.runtime_class_name must be a DNS subdomain", path)
				}
			}
		}
	default:
		return fmt.Errorf("%s.backend %q is unsupported", path, d.Backend)
	}
	return nil
}

// localSocket normalizes a local absolute Unix socket path, accepting the
// unix:// form; an empty value selects fallback.
func localSocket(socket, fallback string) (string, error) {
	if socket == "" {
		socket = fallback
	}
	if strings.HasPrefix(socket, "unix://") {
		u, err := url.Parse(socket)
		if err != nil || u.Host != "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
			return "", errors.New("must be a local absolute Unix socket path")
		}
		socket = u.Path
	}
	if !filepath.IsAbs(socket) || strings.ContainsAny(socket, "\x00\r\n") || strings.TrimSpace(socket) != socket || filepath.Clean(socket) == "/" {
		return "", errors.New("must be a local absolute Unix socket path")
	}
	return filepath.Clean(socket), nil
}

var kubernetesLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
