package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestDeploymentNormalization(t *testing.T) {
	for _, legacy := range []string{"", "docker"} {
		cfg := Config{Sandbox: SandboxConfig{Type: legacy}}
		if err := cfg.NormalizeDeployment(); err != nil {
			t.Fatal(err)
		}
		if cfg.Harness.Deployment.Backend != "docker" || cfg.Sandbox.Deployment.Docker.Socket != "/var/run/docker.sock" || cfg.Bridge.Mode != "embedded" || cfg.Sandbox.Type != "" {
			t.Fatalf("normalization: %#v", cfg)
		}
		before := cfg
		if err := cfg.NormalizeDeployment(); err != nil || !reflect.DeepEqual(before, cfg) {
			t.Fatalf("not idempotent: %v", err)
		}
	}
	cfg := Config{Harness: HarnessConfig{Deployment: DeploymentConfig{Backend: "docker", Docker: &DockerDeploymentConfig{Socket: "unix:///var/run/../run/docker.sock"}}}}
	if err := cfg.NormalizeDeployment(); err != nil {
		t.Fatal(err)
	}
	if cfg.Harness.Deployment.Docker.Socket != cfg.Sandbox.Deployment.Docker.Socket {
		t.Fatal("equivalent sockets differ")
	}
}

func TestDeploymentDecodeAndValidation(t *testing.T) {
	cases := []struct{ name, harness, sandbox, mode, want string }{
		{"independent Kubernetes", `{"backend":"kubernetes","kubernetes":{"context":"cluster","namespace":"aries","runtime_class_name":"kata"}}`, `{"deployment":{"backend":"docker"}}`, "embedded", ""},
		{"legacy compatible", `{"backend":"docker"}`, `{"type":"docker","deployment":{"backend":"docker"}}`, "embedded", ""},
		{"legacy conflict", `{}`, `{"type":"docker","deployment":{"backend":"kubernetes"}}`, "embedded", "sandbox.type"},
		{"unknown legacy", `{}`, `{"type":"other"}`, "embedded", "sandbox.type"},
		{"unknown backend", `{"backend":"remote"}`, `{}`, "embedded", "harness.deployment.backend"},
		{"missing backend", `{"docker":{}}`, `{}`, "embedded", "harness.deployment.backend"},
		{"wrong Docker block", `{"backend":"kubernetes","docker":{}}`, `{}`, "embedded", "harness.deployment.docker"},
		{"wrong Kubernetes block", `{"backend":"docker","kubernetes":{}}`, `{}`, "embedded", "harness.deployment.kubernetes"},
		{"remote", `{"backend":"docker","docker":{"socket":"tcp://localhost:2375"}}`, `{}`, "embedded", "harness.deployment.docker.socket"},
		{"relative", `{"backend":"docker","docker":{"socket":"docker.sock"}}`, `{}`, "embedded", "harness.deployment.docker.socket"},
		{"unix host", `{"backend":"docker","docker":{"socket":"unix://remote/run/docker.sock"}}`, `{}`, "embedded", "harness.deployment.docker.socket"},
		{"different daemons", `{"backend":"docker","docker":{"socket":"/other/docker.sock"}}`, `{}`, "embedded", "same local Docker daemon"},
		{"namespace", `{"backend":"kubernetes","kubernetes":{"namespace":"Bad_Name"}}`, `{}`, "embedded", "namespace"},
		{"runtime class", `{"backend":"kubernetes","kubernetes":{"runtime_class_name":"bad/name"}}`, `{}`, "embedded", "runtime_class_name"},
		{"context", `{"backend":"kubernetes","kubernetes":{"context":" cluster "}}`, `{}`, "embedded", "context"},
		{"unknown field", `{"backend":"docker","docker":{"host":"localhost"}}`, `{}`, "embedded", "unknown field"},
		{"vmd sandbox", `{"backend":"docker"}`, `{"deployment":{"backend":"vmd","vmd":{"socket":"unix:///run/../run/aries-vmd.sock"}}}`, "embedded", ""},
		{"vmd defaults", `{}`, `{"deployment":{"backend":"vmd"}}`, "embedded", ""},
		{"vmd harness", `{"backend":"vmd"}`, `{}`, "embedded", "harness.deployment: backend vmd supports only the tool sandbox"},
		{"vmd isolation", `{}`, `{"deployment":{"backend":"vmd","vmd":{"isolation":"runc"}}}`, "embedded", "sandbox.deployment.vmd.isolation"},
		{"vmd socket", `{}`, `{"deployment":{"backend":"vmd","vmd":{"socket":"tcp://host:1"}}}`, "embedded", "sandbox.deployment.vmd.socket"},
		{"vmd with Docker block", `{}`, `{"deployment":{"backend":"vmd","docker":{}}}`, "embedded", "sandbox.deployment.docker"},
		{"Docker with vmd block", `{}`, `{"deployment":{"backend":"docker","vmd":{}}}`, "embedded", "sandbox.deployment.vmd"},
		{"vmd block without backend", `{}`, `{"deployment":{"vmd":{}}}`, "embedded", "sandbox.deployment.backend"},
		{"vmd with different harness socket", `{"backend":"docker","docker":{"socket":"/other/docker.sock"}}`, `{"deployment":{"backend":"vmd"}}`, "embedded", ""},
		{"managed bridge", `{}`, `{}`, "managed", "bridge.mode"},
		{"external bridge", `{}`, `{}`, "external", "bridge.mode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Replace(validConfig, `"harness":{"type":"openclaw"}`, `"harness":{"type":"openclaw","deployment":`+tc.harness+`}`, 1)
			input = strings.Replace(input, `"sandbox":{"type":"docker"}`, `"sandbox":`+tc.sandbox, 1)
			input = strings.Replace(input, `"bridge":{"type":"openclaw-ssh"}`, `"bridge":{"type":"openclaw-ssh","mode":"`+tc.mode+`"}`, 1)
			cfg, err := Decode(strings.NewReader(input))
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error=%v; want %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(tc.name, "vmd") && tc.want == "" {
				if vmd := cfg.Sandbox.Deployment.VMD; vmd == nil || vmd.Socket != "/run/aries-vmd.sock" || vmd.Isolation != "kata-fc" || cfg.Sandbox.Deployment.Docker != nil {
					t.Fatalf("vmd settings: %#v", cfg.Sandbox.Deployment)
				}
			}
			if tc.name == "independent Kubernetes" && (cfg.Harness.Deployment.Kubernetes.RuntimeClassName != "kata" || cfg.Sandbox.Deployment.Backend != "docker") {
				t.Fatalf("independent settings lost: %#v", cfg)
			}
		})
	}
}

func TestKubernetesRuntimeClassNameLength(t *testing.T) {
	// Kubernetes DNS subdomain names limit total length, unlike namespace labels.
	for _, length := range []int{253, 254} {
		cfg := Config{Harness: HarnessConfig{Deployment: DeploymentConfig{Backend: "kubernetes", Kubernetes: &KubernetesDeploymentConfig{RuntimeClassName: strings.Repeat("a", length)}}}}
		err := cfg.NormalizeDeployment()
		if (err != nil) != (length > 253) {
			t.Fatalf("length %d: %v", length, err)
		}
	}
}
