# MicroVM tool sandbox (`vmd`)

The `vmd` sandbox backend runs each task sandbox and each fresh evaluation
sandbox in its own Firecracker microVM. It uses Kata Containers (Go runtime,
`io.containerd.kata-fc.v2`) under a dedicated containerd. Harnesses stay on
Docker. Profile syntax is in the
[deployment configuration](../configuration.md#microvm-tool-sandbox-vmd); the
contracts it implements are in [deployment](../design/deployment.md).

## Components and privilege split

| Component | Runs as | Role |
| --- | --- | --- |
| [`aries-vmd`](../../cmd/aries-vmd/main.go) ([server](../../pkg/vmd/server)) | root | Owns containerd, Kata, network namespaces, bridges, and nftables. Serves a narrow gRPC API on `/run/aries-vmd.sock` (mode 0660, group `aries`). |
| [API](../../pkg/vmd/api/api.go) | — | gRPC with a JSON codec. Sandbox and network identities are the caller-chosen names, so a lost `Create` response still leaves a cleanup identity; `Delete` of an absent resource succeeds. |
| [`aries-vmd-helper`](../../pkg/vmd/helper) | inside the VM | A static binary copied to `/.aries/helper` before boot. It is PID 1 (reaps zombies), wraps every exec in a new session and records its process group, kills and confirms process groups, and moves tar archives with modes and ownership. Task images need no `tar` and no shell for these operations. |
| [vmd provider](../../pkg/deployment/vmd) | ARIES user | Implements `deployment.Deployment`, the composed `TaskEnvironment`, the search forwarder, image import, and resource sampling. |

ARIES itself stays unprivileged and never touches `/dev/kvm`; KVM readiness
comes from the daemon's `Health` RPC. The daemon accepts no mounts, devices,
privileged flags, or runtime selection from clients. Its runtime is always
Kata/Firecracker, sizes are bounded by its flags, and every resource carries
`aries.run`/`aries.task` labels plus a record in its state directory. That
record is written before any side effect, so incomplete allocations can be
found and removed.

## Host setup

Requirements: x86_64 Linux with KVM, root, containerd 2.x, `nft`, `ip`,
`losetup`, `dmsetup`, `zstd`, and the local Docker Engine used by the harness.

```sh
make build
sudo scripts/vmd/install.sh          # systemd units (default)
sudo scripts/vmd/install.sh --manual # files only; prints the commands to start by hand
sudo usermod -aG aries "$USER"       # then log in again
```

The script installs the Kata release pinned in
[`configs/versions.json`](../../configs/versions.json) (`vmd.kata`, verified by
SHA-256). It enables only the `default_vcpus` and `default_memory` Kata
annotations and installs a `containerd-shim-kata-fc-v2` wrapper. It then
creates a sparse loop-backed devmapper thin pool and a dedicated containerd
(`/run/aries-containerd`) that leaves Docker's containerd untouched. Finally it
installs `aries-vmd` and its helper. Sizes come from `STORAGE_LIMIT_MB`
(default 24576), `POOL_DATA_SIZE` (200G) and `POOL_META_SIZE` (2G).
`STORAGE_LIMIT_MB` becomes both the devmapper `base_image_size` and the
daemon's `--storage-limit-mb`; SWE-bench Pro requests 20 GiB.

Daemon flags (see `aries-vmd -h`) cover the socket and group, containerd
address and namespace, runtime and snapshotter, state directory, helper path,
subnet pool (`10.213.0.0/16`, one /24 per task network), DNS for
egress-enabled sandboxes, the storage limit, and vCPU/memory maxima and
defaults (2 vCPUs, 4096 MiB).

## Preparation

`aries setup` and run preparation pull every image on the harness Docker
daemon. They then call `Health` and fail unless the daemon reports KVM, the
expected API version, `kata-fc` isolation, and the pinned Kata version. Each
task image (not the harness image) is exported with `docker save` and
imported into containerd. This is skipped when the daemon already holds the
same image, matched by config or manifest digest. After an import, the
daemon's config digest must equal the one named in the exported manifest.
Preparation starts no VM and contacts no model.

## Sandbox lifecycle

`Create` validates the request and allocates an address in the task network.
It creates a network namespace with a veth on the task bridge, and a
containerd container on the devmapper snapshot of the image. Before the VM
boots, the daemon mounts the snapshot once. It writes the helper, a
non-listable sticky `/.aries/state` directory, `/etc/hosts` (with
`task-sandbox`), `/etc/hostname`, and `/etc/resolv.conf` (egress networks
only). It also reads `/etc/passwd` and `/etc/group` to resolve the image user.
These writes go through `os.Root`, so image symlinks cannot redirect them. The
snapshot is never mounted while the VM runs. `Start` boots the VM. `Stop`
kills the task, deletes the container and snapshot, and removes the namespace
and veth. It succeeds only after confirming that containerd, the snapshot, the
namespace, the veth, and any Kata shim, Firecracker, or jailer process for the
sandbox are gone. On failure the record is kept so a retry can finish.

Exec runs `helper run` with exact argv, environment, workdir, and numeric
UID:GID. On cancellation the client sends `Cancel` on the same stream, which
was opened detached from the caller's context. The daemon runs `helper kill`
as root: SIGTERM, then SIGKILL to the group, then a scan of `/proc` until the
group is gone. If the command never recorded a group, the daemon kills the
exec process directly. Only a confirmed termination makes the client return
the bare context error; otherwise the error says termination is unconfirmed.
The VM keeps running for evaluation.

Uploads use `helper tar-extract` and give extracted files to the image user,
as Docker's copy does. Downloads use `helper tar-create`, which reports size
and mode first. A missing source wraps `runner.ErrNotFound`; a missing sandbox
does not.

## Networking

Each occurrence gets a composed task environment:

1. The harness's Docker task network, as for Docker sandboxes. The harness
   and the embedded bridge use it unchanged.
2. A daemon-owned VM network: a Linux bridge (`avb…`) with a /24, and a netns
   plus veth (`avh…`) per sandbox, which Kata connects to the VM.
3. When the benchmark declares a search port, an ARIES TCP forwarder on the
   Docker gateway. It resolves the sandbox's `task-sandbox` member on each
   connection, and `SearchURL` points at it.

The nft table `inet aries_vmd` is regenerated atomically from the daemon's
records. Its rules:

- Traffic between task bridges is dropped.
- VM traffic to host services (input) is dropped except established replies.
- VM traffic into Docker bridges (`docker0`, `br-*`) is dropped.
- Networks without `AllowNetwork` drop all forwarding.
- Egress networks are masqueraded. Matching `DOCKER-USER` accept rules are
  added because Docker's FORWARD policy would otherwise drop their traffic.

The host can reach VMs, which the forwarder needs.

## Measurement differences

- A VM needs whole vCPUs, so fractional CPU requests are rounded up and
  logged.
- An omitted memory limit uses the daemon default VM size rather than "no
  limit".
- Root filesystem size is the fixed `base_image_size`. Larger storage
  requests are rejected.
- GPU requests are rejected.
- Resource samples come from the Kata guest cgroup metrics that containerd
  reports.

Compare runs against Docker baselines with these differences in mind.

## Validation

Unit tests use fakes, need neither root nor KVM, and run in `make test`:

- [helper](../../pkg/vmd/helper/helper_test.go)
- [server](../../pkg/vmd/server/server_test.go)
- [client](../../pkg/deployment/vmd/vmd_test.go)

[`make integration-vmd`](../../pkg/deployment/vmd/integration_test.go) needs
KVM, a running daemon, socket access, and Docker. It drives the real sandbox
policy through startup, exec, confirmed cancellation of a process group,
transfers, egress denial, the search forwarder, and confirmed cleanup.
After runs, check for leaks: the daemon's `List`, containerd namespace
`aries-vmd`, Kata shim/Firecracker processes, `aries-*` netns, `avb*`/`avh*`
links, and the `aries_vmd` nft table.

**Known gaps.** The containerd, Kata, and host-network code paths are verified
only by `make integration-vmd` on a KVM host, not by unit tests. Main-process
output is not reattached after a daemon restart. Kata guest images may differ
from Docker in details task images rely on (for example `/.dockerenv`), so
compare benchmark baselines before treating results as equivalent.
