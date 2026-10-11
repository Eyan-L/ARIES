package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hyscale-lab/aries/pkg/core"
)

// Pod telemetry is read from the kubelet Summary API
// (/api/v1/nodes/<node>/proxy/stats/summary) rather than from metrics-server.
//
//   - Semantics. The monitor wants a monotonically increasing CPU counter and
//     does the rate arithmetic itself. The Summary API reports exactly that as
//     usageCoreNanoSeconds; metrics-server reports a pre-averaged rate that
//     cannot be converted back into a counter.
//
//   - Cost. One request per node returns every pod on it, so sampling cost
//     scales with the node count, not the task count.
//
// The tradeoff is RBAC: the Summary API needs `get` on the cluster-scoped
// nodes/proxy subresource. See the ClusterRole in k8s/aries/templates/rbac.yaml.

const (
	// backstopTimeout bounds one kubectl call when the caller supplied no
	// deadline. The monitor wraps every Sample in a tighter request context.
	backstopTimeout = 20 * time.Second
	// minimumSampleInterval is the fastest cadence that can produce new data.
	minimumSampleInterval = 5 * time.Second
	// discoveryFailureBudget is how many consecutive discovery failures are
	// absorbed before Sample reports one. Discovery spawns a kubectl process,
	// and a single Sample error ends the monitor's loop for the rest of the
	// run, so one slow call must not cost all remaining telemetry. The budget
	// stays small so revoked RBAC still surfaces as an error.
	discoveryFailureBudget = 3
)

// ResourceOptions scope one resource source to a single ARIES run.
type ResourceOptions struct {
	// Scope is "task" (default) for sandbox and harness pods of the selected
	// tasks, or "run" for the run's shared bridge pod.
	Scope       string
	RunID       string
	TaskIDs     []string
	Namespace   string
	KubectlPath string
}

// ResourceSource collects raw cumulative counters for one run's pods. Rate
// calculation and artifact writing stay deployment-neutral in pkg/monitor.
type ResourceSource struct {
	kubectl   commandRunner
	namespace string
	scope     string
	runID     string
	tasks     map[string]struct{}

	mu sync.Mutex
	// lastObserved is the newest kubelet observation already emitted per pod.
	lastObserved      map[string]time.Time
	discoveryFailures int
}

// NewResourceSource constructs a source without contacting the cluster.
func NewResourceSource(options ResourceOptions) (*ResourceSource, error) {
	if options.RunID == "" {
		return nil, errors.New("Kubernetes resource source requires a run ID")
	}
	if options.Scope == "" {
		options.Scope = "task"
	}
	if options.Scope != "task" && options.Scope != "run" {
		return nil, errors.New("invalid Kubernetes resource scope")
	}
	if options.Scope == "run" && len(options.TaskIDs) != 0 {
		return nil, errors.New("run resources cannot select task IDs")
	}
	if options.Scope == "task" && len(options.TaskIDs) == 0 {
		return nil, errors.New("Kubernetes resource source requires at least one task ID")
	}
	tasks := make(map[string]struct{}, len(options.TaskIDs))
	for _, taskID := range options.TaskIDs {
		if taskID == "" {
			return nil, errors.New("Kubernetes resource task ID is empty")
		}
		if _, exists := tasks[taskID]; exists {
			return nil, fmt.Errorf("Kubernetes resource task ID %q is repeated", taskID)
		}
		tasks[taskID] = struct{}{}
	}
	if options.Namespace == "" {
		options.Namespace = defaultNamespace
	}
	if options.KubectlPath == "" {
		options.KubectlPath = defaultKubectl
	}
	return &ResourceSource{
		kubectl: execKubectl{path: options.KubectlPath}, namespace: options.Namespace,
		scope: options.Scope, runID: options.RunID, tasks: tasks,
		lastObserved: make(map[string]time.Time),
	}, nil
}

// Close releases source-level resources. The kubectl backend holds none.
func (source *ResourceSource) Close() error { return nil }

// BaselineGracePeriod implements monitor.BaselineTolerantSource. This source
// emits a reading only when the kubelet's observation time advances, so a live
// pod is absent from most samples. Thirty samples covers the kubelet's 10-20s
// housekeeping cadence with room for jitter.
func (source *ResourceSource) BaselineGracePeriod() int { return 30 }

// MinimumSampleInterval implements monitor.PacedSource. The kubelet refreshes
// its cache every 10-20s, so sampling faster only spawns kubectl processes
// whose results are discarded.
func (source *ResourceSource) MinimumSampleInterval() time.Duration { return minimumSampleInterval }

// resourceTarget is one ARIES-owned pod resolved from the API server, carrying
// the identity the Summary API does not report.
type resourceTarget struct {
	podName   string
	node      string
	taskID    string
	component string
	limit     uint64
}

// Sample discovers this run's pods, then reads each involved node's kubelet
// summary once and joins the two.
func (source *ResourceSource) Sample(ctx context.Context) ([]core.ResourceReading, error) {
	targets, err := source.discover(ctx)
	if err != nil {
		if source.absorbDiscoveryFailure() {
			// Reported as no readings so the monitor keeps sampling. The gap
			// is visible in the artifact's sample count.
			return nil, nil
		}
		return nil, err
	}
	source.mu.Lock()
	source.discoveryFailures = 0
	source.mu.Unlock()
	byNode := map[string][]resourceTarget{}
	for _, target := range targets {
		byNode[target.node] = append(byNode[target.node], target)
	}
	nodes := make([]string, 0, len(byNode))
	for node := range byNode {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	readings := make([]core.ResourceReading, 0, len(targets))
	for _, node := range nodes {
		stats, err := source.nodeSummary(ctx, node)
		if err != nil {
			// A node that stops answering mid-run must not fail the run; the
			// gap shows up as missing samples.
			continue
		}
		for _, target := range byNode[node] {
			reading, ok := stats.reading(source, target)
			if ok && source.observationIsNew(reading.RuntimeID, reading.ObservedAt) {
				readings = append(readings, reading)
			}
		}
	}
	sort.Slice(readings, func(i, j int) bool {
		if readings[i].TaskID != readings[j].TaskID {
			return readings[i].TaskID < readings[j].TaskID
		}
		if readings[i].Component != readings[j].Component {
			return readings[i].Component < readings[j].Component
		}
		return readings[i].RuntimeID < readings[j].RuntimeID
	})
	return readings, nil
}

func (source *ResourceSource) absorbDiscoveryFailure() bool {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.discoveryFailures++
	return source.discoveryFailures <= discoveryFailureBudget
}

// observationIsNew reports whether the kubelet's observation time advanced
// since the last reading emitted for this pod. The kubelet serves a cache, so
// polling it faster returns the same observation, timestamp included; the
// monitor would divide a CPU delta by a zero interval and reject the reading.
func (source *ResourceSource) observationIsNew(runtimeID string, observed time.Time) bool {
	source.mu.Lock()
	defer source.mu.Unlock()
	if previous, seen := source.lastObserved[runtimeID]; seen && !observed.After(previous) {
		return false
	}
	source.lastObserved[runtimeID] = observed
	return true
}

// discover lists this run's running pods. Run and task IDs are annotations, so
// the label selector narrows to ARIES pods and the rest is matched here.
func (source *ResourceSource) discover(ctx context.Context) ([]resourceTarget, error) {
	out, err := source.run(ctx, "get", "pods", "-n", source.namespace, "-l", managedByLabel+"=aries", "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("list Kubernetes resource pods: %w", err)
	}
	var list struct {
		Items []podObject `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, fmt.Errorf("decode Kubernetes resource pod list: %w", err)
	}
	targets := make([]resourceTarget, 0, len(list.Items))
	for _, pod := range list.Items {
		annotations := pod.Metadata.Annotations
		if annotations["aries.managed"] != "true" || annotations["aries.run"] != source.runID {
			continue
		}
		taskID := annotations["aries.task"]
		component := annotations["aries.component"]
		selected := false
		switch component {
		case "sandbox", "evaluation-sandbox", "harness":
			_, selected = source.tasks[taskID]
			selected = selected && source.scope == "task"
		case "bridge":
			selected = source.scope == "run" && taskID == "" && annotations["aries.kind"] == "tool-bridge"
		}
		// Only Running pods report counters.
		if !selected || pod.Status.Phase != "Running" || pod.Spec.NodeName == "" {
			continue
		}
		var limit uint64
		if len(pod.Spec.Containers) > 0 {
			limit = parseMemoryQuantity(pod.Spec.Containers[0].Resources.Limits["memory"])
		}
		targets = append(targets, resourceTarget{podName: pod.Metadata.Name, node: pod.Spec.NodeName, taskID: taskID, component: component, limit: limit})
	}
	return targets, nil
}

// nodeSummary reads one kubelet's stats summary through the API server proxy.
func (source *ResourceSource) nodeSummary(ctx context.Context, node string) (*nodeStats, error) {
	out, err := source.run(ctx, "get", "--raw", "/api/v1/nodes/"+node+"/proxy/stats/summary")
	if err != nil {
		return nil, fmt.Errorf("read kubelet summary for node %s: %w", node, err)
	}
	var stats nodeStats
	if err := json.Unmarshal(out, &stats); err != nil {
		return nil, fmt.Errorf("decode kubelet summary for node %s: %w", node, err)
	}
	return &stats, nil
}

// nodeStats is the subset of the kubelet Summary API this source consumes.
type nodeStats struct {
	Pods []struct {
		PodRef struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"podRef"`
		CPU struct {
			Time                 time.Time `json:"time"`
			UsageCoreNanoSeconds *uint64   `json:"usageCoreNanoSeconds"`
		} `json:"cpu"`
		Memory struct {
			Time            time.Time `json:"time"`
			WorkingSetBytes *uint64   `json:"workingSetBytes"`
			AvailableBytes  *uint64   `json:"availableBytes"`
		} `json:"memory"`
	} `json:"pods"`
}

// reading joins one discovered pod against this node's summary.
func (stats *nodeStats) reading(source *ResourceSource, target resourceTarget) (core.ResourceReading, bool) {
	for _, pod := range stats.Pods {
		if pod.PodRef.Name != target.podName || pod.PodRef.Namespace != source.namespace {
			continue
		}
		if pod.CPU.UsageCoreNanoSeconds == nil || pod.Memory.WorkingSetBytes == nil {
			return core.ResourceReading{}, false
		}
		observed := pod.CPU.Time
		if observed.IsZero() {
			observed = pod.Memory.Time
		}
		if observed.IsZero() {
			return core.ResourceReading{}, false
		}
		// workingSetBytes is what the kubelet's eviction logic uses, the
		// closest analogue to Docker's memory usage.
		usage := *pod.Memory.WorkingSetBytes
		limit := target.limit
		if limit == 0 && pod.Memory.AvailableBytes != nil {
			// No limit in the spec: recover the effective ceiling.
			limit = usage + *pod.Memory.AvailableBytes
		}
		return core.ResourceReading{
			Scope: source.scope, RunID: source.runID, TaskID: target.taskID, Component: target.component,
			// The pod name alone: the monitor uses the runtime ID as a path
			// component and rejects "/".
			RuntimeID: target.podName, RuntimeName: target.podName, ObservedAt: observed,
			CPUUsageNanoseconds: *pod.CPU.UsageCoreNanoSeconds, MemoryUsageBytes: usage, MemoryLimitBytes: limit,
		}, true
	}
	return core.ResourceReading{}, false
}

func (source *ResourceSource) run(ctx context.Context, args ...string) ([]byte, error) {
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, backstopTimeout)
		defer cancel()
	}
	var stdout, stderr bytes.Buffer
	if err := source.kubectl.Run(ctx, nil, &stdout, &stderr, args...); err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// parseMemoryQuantity reads the memory quantities this provider writes. An
// unrecognised form yields 0, so the caller falls back to the kubelet's
// reported headroom rather than recording a wrong limit.
func parseMemoryQuantity(value string) uint64 {
	value = strings.TrimSpace(value)
	for _, unit := range []struct {
		suffix string
		scale  uint64
	}{{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40}, {"k", 1000}, {"M", 1000 * 1000}, {"G", 1000 * 1000 * 1000}, {"", 1}} {
		if digits, found := strings.CutSuffix(value, unit.suffix); found {
			parsed, err := strconv.ParseUint(digits, 10, 64)
			if err != nil {
				return 0
			}
			return parsed * unit.scale
		}
	}
	return 0
}
