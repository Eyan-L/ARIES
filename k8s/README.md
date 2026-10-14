# ARIES on Kubernetes

ARIES does not bootstrap clusters. Build one with
[Invitro's `scripts/setup`](https://github.com/vhive-serverless/invitro/tree/main/scripts/setup),
then apply ARIES on top with the script in this directory. For how ARIES places
its components on a cluster, and what that provider does not support, see
[the Kubernetes implementation](../docs/implementation/kubernetes.md).

```
k8s/
  create-cluster.sh     # runs Invitro's setup, then apply-aries.sh, in one go
  apply-aries.sh        # applies everything below to an existing cluster
  aries/                # the ARIES chart: runner and bridge RBAC, dashboard
  monitoring/           # values overlay for an existing kube-prometheus-stack
```

## What ARIES needs from the cluster

| Requirement | Invitro's setup |
| --- | --- |
| A CNI that enforces NetworkPolicy | Calico |
| containerd | Yes |
| `kubectl` and `helm` on the machine that applies ARIES | On the master node; `helm` is installed with Prometheus |
| kube-prometheus-stack, only for the Grafana dashboard | Optional (`DEPLOY_PROMETHEUS`) |

Invitro also installs Knative, Istio and MetalLB. ARIES does not use them; its
pods live in their own namespace.

## 1 and 2 in one command

From your own machine, with passwordless SSH to every node:

```sh
k8s/create-cluster.sh --prometheus user@master user@runner user@worker1 user@worker2
```

As in Invitro, the first node is the control plane and the second is the loader
node; the rest are workers.

Or keep the nodes and settings in a file. Copy `cluster.json.example` to the
gitignored `cluster.json`, fill it in, and run the script with no arguments:

```sh
cp k8s/cluster.json.example k8s/cluster.json
k8s/create-cluster.sh
```

| Key | Meaning |
| --- | --- |
| `master`, `runner`, `workers` | SSH targets. `runner` is Invitro's loader node |
| `runner_dir` | Directory under the runner's home that receives ARIES; default `aries` |
| `ssh_key`, `ssh_options` | Identity file and extra `ssh` options, applied to every connection including Invitro's |
| `deploy_prometheus` | Same as `--prometheus` |
| `kubelet_housekeeping_interval` | Same as `--housekeeping`; `1s` gives one-second pod measurements |
| `node_setup` | Same as `--node-setup` |

`--config FILE` names another file, and command-line options win over the file.

The script:

1. checks out Invitro at the revision pinned at the top of the script into
   `.cache/invitro` and runs its `create_multinode_cluster` against the nodes;
2. copies the committed `k8s/` tree to the master and runs `apply-aries.sh`
   there;
3. builds `bin/aries` and `bin/aries-ssh-client` for the loader node and stages
   them with `profiles/`, `configs/` and a namespaced kubeconfig under `~/aries`.

| Option | Effect |
| --- | --- |
| `--prometheus` | Sets Invitro's `DEPLOY_PROMETHEUS` and applies the ARIES monitoring overlay |
| `--node-setup FILE` | Uses your own Invitro `node_setup.json`; needed when the nodes' internal addresses are not Invitro's default `10.0.1.x` layout |
| `--housekeeping DUR` | Sets the kubelet's `--housekeeping-interval` on every node and restarts it. Invitro leaves the 10-second default, which caps how fine pod measurements can be |
| `--skip-cluster` | The cluster exists already; only apply ARIES and restage the runner |
| `--no-runner` | Do not build or stage the runner |

ARIES holds no copy of Invitro's scripts: only the pinned revision, which
`INVITRO_REVISION` overrides. The two steps below are what the script runs, for
doing them by hand.

## 1. Build the cluster

Follow Invitro's instructions for `create_multinode_cluster`. Set
`"DEPLOY_PROMETHEUS": true` in its `setup.json` if you want the dashboard.

## 2. Apply ARIES

On the master node, or anywhere `kubectl` and `helm` reach the cluster as an
admin, from an ARIES checkout:

```sh
k8s/apply-aries.sh --monitoring --runner-kubeconfig ~/aries-kubeconfig
```

| Option | Effect |
| --- | --- |
| *(none)* | Installs the chart into namespace `aries`: the runner's and the bridge's service accounts and Roles, and the dashboard ConfigMap |
| `--monitoring` | Upgrades the existing kube-prometheus-stack release in place with [`monitoring/values-aries.yaml`](monitoring/values-aries.yaml): cAdvisor scraped every second instead of Invitro's 15, and the Grafana sidecar reading dashboards from every namespace |
| `--runner-kubeconfig PATH` | Writes a kubeconfig for the chart's `aries` ServiceAccount, so the runner holds the namespaced Role rather than cluster-admin |
| `--values FILE` | Extra chart values, such as a `secret.yaml` for a private registry |
| `--namespace NAME` | A namespace other than `aries`; set the same one in the profile |

The script ends by printing the cluster's Kubernetes version, its pod and
service CIDRs, and the Prometheus address, which the next two steps need. It is
safe to run again.

The chart ships no workload. ARIES creates the harness, sandbox and bridge pods
for each run and removes them afterwards.

## 3. Publish the images the cluster pulls

Docker runs build two images locally. A cluster cannot see them, so push them
to a registry your nodes can pull from and map them in the profile.

**The bridge image** must contain `aries-bridge`, `kubectl` and `/bin/sh`. Use a
base whose `kubectl` matches the cluster's version, as printed by the script:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/aries-bridge ./cmd/aries-bridge
docker buildx build --platform linux/amd64 -f Dockerfile.bridge \
  --build-arg BASE_IMAGE=alpine/k8s:<cluster version> \
  -t <registry>/aries-bridge:v1 --push .
```

**The Hermes image** is the pinned upstream image plus `hermes-otel`. Build it
on a Docker host with `./bin/aries setup profiles/hermes-tb2-fix-git-deepseek.json`,
then tag and push the resulting `aries-local/hermes:<tag>` image under the same
tag. OpenClaw and the task images are pulled from their public registries
unchanged, so an OpenClaw run needs only the bridge image.

## 4. Set the profile

The `-k8s-pods` profiles put the agent, the bridge and the sandbox in pods. All
three blocks must select the same backend and namespace:

```json
"harness": { "type": "hermes", "deployment": { "backend": "kubernetes", "kubernetes": {
  "namespace": "aries", "images": { "aries-local/hermes": "<registry>/aries-hermes" } } } },
"sandbox": { "deployment": { "backend": "kubernetes", "kubernetes": {
  "namespace": "aries", "pod_cidr": "<pod CIDR>", "service_cidr": "<service CIDR>" } } },
"bridge":  { "type": "hermes-ssh", "deployment": { "backend": "kubernetes", "kubernetes": {
  "namespace": "aries", "service_account": "aries-bridge", "images": { "aries-bridge": "<registry>/aries-bridge" } } } }
```

- **`pod_cidr`, `service_cidr`:** use the values the script printed. They keep a
  network-allowed sandbox off the cluster's own networks.
- **`prometheus_url`:** add the printed address under
  `sandbox.deployment.kubernetes` so the Recorder can read pod measurements.
  `create-cluster.sh` writes it into the profiles it stages on the runner. It is
  the Service's cluster address, so it only works from a cluster node, which
  Invitro's loader node is.
- **`images`:** map each locally built image to your registry. A key without a
  tag maps the repository and keeps the requested tag.
- **`node_selector`:** add `"node_selector": {"loader-nodetype": "worker"}` to
  each block to keep ARIES pods on Invitro's worker nodes.

See [deployment configuration](../docs/configuration.md#kubernetes-deployment)
for every field.

## 5. Run

The runner is a host process, not a pod. Build ARIES where `kubectl` reaches the
cluster, add the model key, and run a profile:

```sh
make build
export KUBECONFIG=~/aries-kubeconfig
./bin/aries profiles/kubernetes/hermes-tb2-fix-git-deepseek-k8s-pods.json
```

`execution.concurrency` bounds how many task occurrences run at once. Each one
gets its own harness pod and sandbox pod, and its own session on the run's
bridge pod. Size it against the workers' capacity: pods that do not fit sit
`Pending` on `Insufficient cpu`.

Run artifacts land in `runs/`, including each task's `bridge/tool-calls.jsonl`.
A run is tied to your SSH session; use `tmux` for long experiments.

While a run is in progress:

```sh
kubectl -n aries get pods -l app.kubernetes.io/managed-by=aries -o wide
```

## Credentials

- **Model key.** It is not in the chart or in any Kubernetes object. The runner
  reads `DEEPSEEK_API.key` beside its binaries and stages it into each task's
  harness pod itself.
- **Runner access.** `--runner-kubeconfig` uses the `aries` ServiceAccount's
  long-lived token (Secret `aries-token`). Delete that Secret to revoke it.
- **Private registry.** Copy `aries/secret.yaml.example` to the gitignored
  `aries/secret.yaml`, fill it in, pass it with `--values`, and set
  `"image_pull_secret": "aries-registry"` in the profile.

## Status and caveats

- **Supported set.** Terminal-Bench 2, SWE-bench Pro and Deep Research Bench,
  with OpenClaw or Hermes in text mode. See
  [the Kubernetes implementation](../docs/implementation/kubernetes.md) for what
  SWE-bench Pro needs from a task image and how a task service is exposed.
- **Task network isolation** depends on the CNI enforcing NetworkPolicy. Calico
  does; under flannel the policies are created and silently ignored.
- **Pod resource metrics** are read from Prometheus, so they need the
  monitoring stack and a `prometheus_url` in the profile; without one they are
  recorded as unsupported and the run continues. The kubelet refreshes these
  figures every 10 seconds unless it runs with `--housekeeping-interval`, so
  that is their resolution on a stock Invitro cluster. Set
  `kubelet_housekeeping_interval` to `1s` for one-second data, in the dashboard
  and in `resources.jsonl` alike.
- **One bridge pod per run.** If it dies mid-run, the tasks in flight lose
  their tool access and their evidence, and evaluation is blocked for them.
- **An interrupted runner can leave pods behind.** Remove them with
  `kubectl -n aries delete pods,services,networkpolicies -l app.kubernetes.io/managed-by=aries`.
