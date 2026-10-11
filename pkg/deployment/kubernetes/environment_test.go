package kubernetes

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/deployment"
)

func taskRequest(allowNetwork bool) deployment.TaskEnvironmentRequest {
	return deployment.TaskEnvironmentRequest{
		SandboxRequest: core.SandboxRequest{RunID: "run-1", TaskID: "task-1", Environment: core.Environment{AllowNetwork: allowNetwork}},
		RuntimeName:    "aries-task-abc123",
	}
}

// Naming a direction in policyTypes with no rule block denies it, so the
// air-gapped policy must omit the egress key entirely.
func TestTaskPolicyDeniesIngressAndScopesEgress(t *testing.T) {
	manager, _ := newTestManager(t, Options{PodCIDR: "192.168.0.0/16", ServiceCIDR: "10.96.0.0/12"})
	for _, allow := range []bool{false, true} {
		raw, err := manager.taskPolicyManifest(taskRequest(allow))
		if err != nil {
			t.Fatal(err)
		}
		spec := decodeManifest(t, raw)["spec"].(map[string]any)
		if selector := spec["podSelector"].(map[string]any)["matchLabels"].(map[string]any); len(selector) != 1 || selector[runtimeLabel] != "aries-task-abc123" {
			t.Fatalf("selector = %v", selector)
		}
		if types := spec["policyTypes"].([]any); !slices.Equal(types, []any{"Ingress", "Egress"}) {
			t.Fatalf("policyTypes = %v", types)
		}
		if _, present := spec["ingress"]; present {
			t.Fatal("ingress rule would admit traffic")
		}
		egress, present := spec["egress"].([]any)
		if present != allow {
			t.Fatalf("allow=%v egress present=%v", allow, present)
		}
		if allow {
			block := egress[1].(map[string]any)["to"].([]any)[0].(map[string]any)["ipBlock"].(map[string]any)
			if block["cidr"] != "0.0.0.0/0" || !slices.Equal(block["except"].([]any), []any{"192.168.0.0/16", "10.96.0.0/12"}) {
				t.Fatalf("ipBlock = %v", block)
			}
		}
	}
}

func TestEnvironmentsOwnTheirPolicies(t *testing.T) {
	manager, fake := newTestManager(t, Options{})
	policies := map[string]bool{}
	fake.respond = func(_ context.Context, args []string, stdin []byte, stdout, _ io.Writer) error {
		switch {
		case args[0] == "create":
			name := decodeManifest(t, stdin)["metadata"].(map[string]any)["name"].(string)
			policies[name] = true
		case args[0] == "delete" && args[1] == "networkpolicy":
			delete(policies, args[4])
		case args[0] == "get" && args[1] == "networkpolicy" && policies[args[4]]:
			_, _ = io.WriteString(stdout, "networkpolicy.networking.k8s.io/"+args[4])
		}
		return nil
	}
	run := manager.NewRunEnvironment("run-1")
	placement, err := run.Start(context.Background())
	if err != nil || placement.AttachmentID != "aries" || run.NetworkName() != "aries" {
		t.Fatalf("Start = %+v, %v", placement, err)
	}
	if _, err := run.Start(context.Background()); err == nil {
		t.Fatal("run environment reused")
	}
	// The bridge accepts unauthenticated clients, so its policy keeps sandboxes out.
	bridge := decodeManifest(t, fake.inputs[0])["spec"].(map[string]any)
	if string(fake.inputs[0]) == "" || bridge["podSelector"].(map[string]any)["matchLabels"].(map[string]any)[componentLabel] != "bridge" || !strings.Contains(string(fake.inputs[0]), `"NotIn","values":["sandbox","evaluation-sandbox"]`) {
		t.Fatalf("bridge policy = %s", fake.inputs[0])
	}

	task := run.NewTaskEnvironment()
	connectivity, err := task.Start(context.Background(), taskRequest(false))
	if err != nil || connectivity.Placement.AttachmentID != "aries" || connectivity.SearchURL != "" {
		t.Fatalf("task Start = %+v, %v", connectivity, err)
	}
	if !policies["aries-task-abc123"] || task.Validate(context.Background()) != nil {
		t.Fatalf("task policy missing: %v", policies)
	}
	if err := task.Stop(context.Background()); err != nil || policies["aries-task-abc123"] {
		t.Fatalf("task Stop = %v, policies = %v", err, policies)
	}
	if err := task.Validate(context.Background()); err == nil {
		t.Fatal("stopped task environment validated")
	}
	if len(policies) != 1 {
		t.Fatalf("task Stop removed run connectivity: %v", policies)
	}
	if err := run.Stop(context.Background()); err != nil || len(policies) != 0 {
		t.Fatalf("run Stop = %v, policies = %v", err, policies)
	}
	if err := run.Stop(context.Background()); err != nil {
		t.Fatalf("repeated Stop = %v", err)
	}
}

func TestTaskEnvironmentRejectsForeignRunsAndTaskServices(t *testing.T) {
	manager, _ := newTestManager(t, Options{})
	run := manager.NewRunEnvironment("run-1")
	if _, err := run.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	foreign := taskRequest(false)
	foreign.RunID = "run-2"
	if _, err := run.NewTaskEnvironment().Start(context.Background(), foreign); err == nil {
		t.Fatal("foreign run accepted")
	}
	search := taskRequest(true)
	search.Environment.Services.SearchPort = 8080
	if _, err := run.NewTaskEnvironment().Start(context.Background(), search); err == nil || !strings.Contains(err.Error(), "task services") {
		t.Fatalf("task service = %v", err)
	}
}

func TestValidateBridgeTarget(t *testing.T) {
	manager, fake := newTestManager(t, Options{})
	request := testRequest()
	pod := podJSON(t, manager, request, nil)
	fake.respond = func(_ context.Context, _ []string, _ []byte, stdout, _ io.Writer) error {
		_, _ = io.WriteString(stdout, pod)
		return nil
	}
	target := core.BridgeTarget{SandboxID: request.Name, RuntimeID: request.Name, RunID: "run-1", TaskID: "task-1", Backend: "kubernetes", Workdir: "/app"}
	if err := manager.ValidateBridgeTarget(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*core.BridgeTarget){
		"backend": func(d *core.BridgeTarget) { d.Backend = "docker" },
		"run":     func(d *core.BridgeTarget) { d.RunID = "run-2" },
		"task":    func(d *core.BridgeTarget) { d.TaskID = "task-2" },
		"runtime": func(d *core.BridgeTarget) { d.RuntimeID = "aries-task-other" },
	} {
		other := target
		mutate(&other)
		if err := manager.ValidateBridgeTarget(context.Background(), other); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A harness pod is never a valid execution target.
	harness := testRequest()
	harness.Labels["aries.component"] = "harness"
	pod = podJSON(t, manager, harness, nil)
	if err := manager.ValidateBridgeTarget(context.Background(), target); err == nil {
		t.Fatal("harness pod accepted as a sandbox")
	}
}

func TestResourceSourceJoinsPodsWithKubeletSummary(t *testing.T) {
	manager, _ := newTestManager(t, Options{})
	sandbox := testRequest()
	bridge := testRequest()
	bridge.Name = "aries-bridge-1"
	bridge.MemoryMB = nil
	bridge.Labels = map[string]string{"aries.managed": "true", "aries.kind": "tool-bridge", "aries.component": "bridge", "aries.run": "run-1"}
	other := testRequest()
	other.Name = "aries-task-other"
	other.Labels["aries.run"] = "run-2"
	onNode := func(m map[string]any) { m["spec"].(map[string]any)["nodeName"] = "node-a" }
	list := `{"items":[` + podJSON(t, manager, sandbox, onNode) + `,` + podJSON(t, manager, bridge, onNode) + `,` + podJSON(t, manager, other, onNode) + `]}`
	observed := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	summary := func() string {
		stamp := observed.Format(time.RFC3339)
		return `{"pods":[` +
			`{"podRef":{"name":"aries-task-abc123","namespace":"aries"},"cpu":{"time":"` + stamp + `","usageCoreNanoSeconds":500},"memory":{"workingSetBytes":4096}},` +
			`{"podRef":{"name":"aries-bridge-1","namespace":"aries"},"cpu":{"time":"` + stamp + `","usageCoreNanoSeconds":70},"memory":{"workingSetBytes":10,"availableBytes":90}}]}`
	}
	fake := &fakeKubectl{respond: func(_ context.Context, args []string, _ []byte, stdout, _ io.Writer) error {
		if args[1] == "--raw" {
			_, _ = io.WriteString(stdout, summary())
		} else {
			_, _ = io.WriteString(stdout, list)
		}
		return nil
	}}
	task, err := NewResourceSource(ResourceOptions{RunID: "run-1", TaskIDs: []string{"task-1"}})
	if err != nil {
		t.Fatal(err)
	}
	task.kubectl = fake
	readings, err := task.Sample(context.Background())
	if err != nil || len(readings) != 1 {
		t.Fatalf("task readings = %+v, %v", readings, err)
	}
	if r := readings[0]; r.Scope != "task" || r.TaskID != "task-1" || r.Component != "sandbox" || r.RuntimeID != "aries-task-abc123" || r.CPUUsageNanoseconds != 500 || r.MemoryLimitBytes != 512<<20 {
		t.Fatalf("task reading = %+v", r)
	}
	// The kubelet serves a cache: an unchanged observation is not a new sample.
	if repeated, err := task.Sample(context.Background()); err != nil || len(repeated) != 0 {
		t.Fatalf("repeated readings = %+v, %v", repeated, err)
	}
	observed = observed.Add(10 * time.Second)
	if advanced, err := task.Sample(context.Background()); err != nil || len(advanced) != 1 {
		t.Fatalf("advanced readings = %+v, %v", advanced, err)
	}

	run, err := NewResourceSource(ResourceOptions{Scope: "run", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	run.kubectl = fake
	readings, err = run.Sample(context.Background())
	if err != nil || len(readings) != 1 {
		t.Fatalf("run readings = %+v, %v", readings, err)
	}
	// Without a limit in the spec the ceiling is usage plus kubelet headroom.
	if r := readings[0]; r.Scope != "run" || r.RunID != "run-1" || r.TaskID != "" || r.Component != "bridge" || r.MemoryLimitBytes != 100 {
		t.Fatalf("run reading = %+v", r)
	}
	if _, err := NewResourceSource(ResourceOptions{Scope: "run", RunID: "run-1", TaskIDs: []string{"task-1"}}); err == nil {
		t.Fatal("run scope accepted task IDs")
	}
}

func TestResourceSourceAbsorbsTransientDiscoveryFailures(t *testing.T) {
	source, err := NewResourceSource(ResourceOptions{RunID: "run-1", TaskIDs: []string{"task-1"}})
	if err != nil {
		t.Fatal(err)
	}
	source.kubectl = &fakeKubectl{respond: func(context.Context, []string, []byte, io.Writer, io.Writer) error {
		return exitError(1)
	}}
	for attempt := 1; attempt <= discoveryFailureBudget; attempt++ {
		if readings, err := source.Sample(context.Background()); err != nil || readings != nil {
			t.Fatalf("attempt %d = %+v, %v", attempt, readings, err)
		}
	}
	if _, err := source.Sample(context.Background()); err == nil {
		t.Fatal("persistent discovery failure never surfaced")
	}
}
