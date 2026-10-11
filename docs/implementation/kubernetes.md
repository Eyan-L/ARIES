# Kubernetes implementation

[`pkg/deployment/kubernetes`](../../pkg/deployment/kubernetes/kubernetes.go)
implements the [deployment contract](../design/deployment.md) on pods, so the
existing harness, sandbox and bridge components run on a cluster unchanged. It is
selected per component with `deployment.backend: "kubernetes"`; see
[deployment configuration](../configuration.md#kubernetes-deployment). Cluster
bootstrap, the Helm chart and the runner host are covered in
[`k8s/`](../../k8s/README.md).

## Placement

The Runner stays a host process outside the cluster and reaches it only through
the `kubectl` binary and the context that resolves. Every runtime is one pod in
a single namespace:

| Runtime | Lifetime | Reached by |
| --- | --- | --- |
| Sandbox pod | One per task occurrence, plus fresh evaluation sandboxes | Runner and bridge, through `kubectl exec` |
| Harness pod | One per task occurrence | Runner, through a port-forward to its gateway |
| Bridge pod | One per run | Runner (gRPC control, port-forward); harness pods (SSH, pod address) |

Harness, sandbox and bridge must all select the Kubernetes backend and the same
namespace. The bridge pod execs into sandbox pods and harness pods dial the
bridge pod, so a component left on Docker could not be reached. Profile
normalization rejects mixed compositions before any effect.

The provider shells out to `kubectl` rather than linking client-go. That keeps
the dependency set unchanged and makes the kubeconfig the whole cluster
contract, at the cost of one process per operation.

## Create, stage, start

The contract stages private files between `Create` and `Start`, but a pod cannot
exist without running. Each container therefore starts behind a gate:

```sh
while [ ! -e /run/aries-gate/start ]; do sleep 0.2; done; exec "$@"
```

`Create` applies the pod and waits until it runs, parked at the gate.
`UploadArchive` extracts into the live container. `Start` writes the marker and
the gate execs the requested command, so it becomes PID 1 with its exact argv.
The gate directory is an `emptyDir` owned by ARIES, outside the image's
filesystem; exec bookkeeping lives there too.

Three consequences are explicit provider constraints:

- **Images need `/bin/sh`.** Sandbox images also need `tar`, `stat` and
  `setsid` for file transfer and for exec that can be cancelled.
- **An unset entrypoint is not preserved.** The cluster API does not report an
  image's entrypoint, so a request without one runs its arguments directly.
  OpenClaw relies on this and its launcher does not need the image entrypoint.
- **`Init` is not provided.** The requested command is PID 1 and no init process
  reaps orphans.

### Staging as a non-root image

Docker's copy API writes as root whatever user the image declares. `kubectl
exec` runs as the image's user, which cannot create directories under root-owned
paths. Wiring therefore names staging roots for images that need them: OpenClaw
runs as UID 1000 and stages under `/run/aries` and `/opt/aries`. The provider
mounts an `emptyDir` at each root and a root init container hands them to that
user before the main container starts. Hermes and the bridge start as root and
need none.

## Execution and transfer

`Exec` and `ExecStream` use the same wrapper as the Docker provider: the command
runs under `setsid` in its own session, its PID is recorded, and its exit status
travels in a trailer on stderr. The trailer matters here because `kubectl`
itself exits nonzero on transport failures, which would otherwise be
indistinguishable from a failing command. Cancellation and timeouts signal the
recorded process group and confirm it is gone before returning.

`kubectl exec` cannot select a user, so a command that requests a non-root
`User` is rejected. SWE-bench Pro, which executes as a dedicated agent user, is
therefore unsupported on this backend.

Archives move as tar streams over exec. Pod logs are a single interleaved
stream, so they arrive on stdout only.

## Addresses

`Address` resolves what the Runner dials. Outside the cluster it is a
`kubectl port-forward` on a reserved loopback port, restarted on that same port
if kubectl loses its connection. When the Runner itself runs in a pod it is the
target's pod address. `TaskAddress` is always the pod address, which is how
harness pods reach the bridge's per-sandbox SSH listeners.

## Network isolation

A cluster has one flat pod network, so isolation is expressed with
NetworkPolicy. The task environment creates each sandbox's policy before its pod
and removes it after the pod is gone:

| `allow_internet` | Ingress | Egress |
| --- | --- | --- |
| `false` | Denied | Denied |
| `true` | Denied | DNS, and the internet outside `pod_cidr` and `service_cidr` |

Denied ingress is the load-bearing half: a sandbox that accepts no connections
cannot be reached by another task. The bridge needs none, because exec goes
through the API server. Leaving the CIDRs unset only widens what a
network-allowed task may dial out to.

The run environment owns one more policy, on the bridge pod. Its SSH listeners
accept clients without authentication, so the policy admits only pods in the
namespace that are not sandboxes. The reported run policy is
`per-sandbox-policy`.

NetworkPolicy is enforced by the CNI plugin. Under one that does not implement
it, such as flannel, the objects are accepted and silently ignored.

Task services are not exposed: a task whose environment declares a search port
is rejected, so Deep Research Bench is unsupported on this backend.

## Ownership and cleanup

Pods carry three labels: `app.kubernetes.io/managed-by=aries`, the component,
and `aries.dev/runtime` with the pod's own random name. Run and task IDs can
exceed the 63-byte label limit, so the request's ownership labels are stored as
annotations and matched there.

`Validate` checks the pod against its request: identity, annotations, image,
gated command, working directory, privilege escalation, host namespaces, host
paths and the absence of a service account token. It also searches the whole pod
object for supplied secret values. `Stop` deletes the pod and returns only after
a lookup confirms absence.

Only the bridge pod mounts a service account. Its Role allows `get pods` and
`pods/exec` in the namespace, and before executing it confirms the target is a
live sandbox pod of its own run and task.

There is no controller. A Runner that is killed leaves its pods and policies
behind; remove them by label as shown in [`k8s/`](../../k8s/README.md).

## Images

The cluster pulls images; ARIES cannot build into it. Image preparation is
therefore a no-op on this backend, and two images that Docker runs build locally
must be published first: the bridge image, which additionally needs `kubectl`,
and the derived Hermes image. Each deployment block's `images` map translates a
local name to the reference the cluster pulls.

## Resource measurements

Readings come from the kubelet Summary API through the API server's node proxy.
It reports a cumulative CPU counter, which is what the monitor's rate arithmetic
needs, and one request per node covers every pod on it.

The kubelet serves a cache refreshed every 10 to 20 seconds. The source
therefore declares a five-second minimum sample interval, emits a reading only
when the observation time advances, and asks the recorder to keep a CPU baseline
across those gaps. `second` in `resources.jsonl` is elapsed time, so samples
stay comparable across intervals. A few consecutive discovery failures are
absorbed as missing samples rather than ending the run's telemetry.

This needs `get` on the cluster-scoped `nodes/proxy`, the one permission the
Runner holds outside its namespace.

## Supported composition

Terminal-Bench 2 with OpenClaw or Hermes in text mode, with all three components
on one cluster and namespace. Realtime and voice modes, managed model runtimes,
GPU tasks and the other benchmarks have not been exercised on this backend.
