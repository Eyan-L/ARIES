#!/usr/bin/env bash
# Builds a Kubernetes cluster with Invitro's setup, then applies ARIES to it.
#
# ARIES keeps no cluster bootstrap of its own. This script checks out Invitro at
# a pinned revision, runs its create_multinode_cluster against your nodes, and
# then runs k8s/apply-aries.sh on the master. Run it from your own machine; it
# reaches every node with plain `ssh`, as Invitro does, so your SSH config or
# agent must already allow passwordless logins.
set -euo pipefail

INVITRO_REPO=${INVITRO_REPO:-https://github.com/vhive-serverless/invitro.git}
INVITRO_REVISION=${INVITRO_REVISION:-49c90dfd977417a32ff4dbd767a8a82e794480b4}

prometheus=false
node_setup=""
skip_cluster=false
stage_runner=true

usage() {
  cat <<'USAGE'
Usage: k8s/create-cluster.sh [options] <master> <runner> [<worker> ...]

Nodes are SSH targets such as user@host. As in Invitro, the first is the
control plane and the second is the loader node, which joins the cluster and is
where the ARIES runner is staged; the rest are workers.

  --prometheus        also deploy kube-prometheus-stack and the ARIES overlay
  --node-setup FILE   an Invitro node_setup.json, for nodes whose internal
                      addresses are not Invitro's default 10.0.1.x layout
  --skip-cluster      the cluster already exists; only apply ARIES
  --no-runner         do not build and stage the runner on the loader node
  -h, --help          show this help

INVITRO_REPO and INVITRO_REVISION override the pinned Invitro checkout.
USAGE
}

nodes=()
while [ $# -gt 0 ]; do
  case "$1" in
    --prometheus) prometheus=true; shift ;;
    --node-setup) node_setup=$2; shift 2 ;;
    --skip-cluster) skip_cluster=true; shift ;;
    --no-runner) stage_runner=false; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *) nodes+=("$1"); shift ;;
  esac
done
[ ${#nodes[@]} -ge 2 ] || { usage >&2; exit 2; }
master=${nodes[0]}
runner=${nodes[1]}

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
remote() { ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$@"; }

for tool in git go ssh; do
  command -v "$tool" >/dev/null || { echo "$tool is required on PATH" >&2; exit 1; }
done
for node in "${nodes[@]}"; do
  remote "$node" true || { echo "cannot ssh to $node non-interactively" >&2; exit 1; }
done

if [ "$skip_cluster" = false ]; then
  invitro="$root/.cache/invitro"
  if [ ! -d "$invitro/.git" ]; then
    echo "==> Cloning Invitro into .cache/invitro"
    mkdir -p "$root/.cache"
    git clone -q "$INVITRO_REPO" "$invitro"
  fi
  echo "==> Checking out Invitro $INVITRO_REVISION"
  git -C "$invitro" fetch -q origin
  git -C "$invitro" checkout -q --detach "$INVITRO_REVISION"

  # Invitro reads its settings from a config directory. Work on a private copy
  # of its own defaults so the checkout stays pristine.
  configs=$(mktemp -d)
  trap 'rm -rf "$configs"' EXIT
  cp -R "$invitro/scripts/setup/configs/." "$configs/"
  if [ "$prometheus" = true ]; then
    sed 's/"DEPLOY_PROMETHEUS": *false/"DEPLOY_PROMETHEUS": true/' \
      "$invitro/scripts/setup/configs/setup.json" >"$configs/setup.json"
    grep -q '"DEPLOY_PROMETHEUS": true' "$configs/setup.json" ||
      { echo "could not enable DEPLOY_PROMETHEUS in Invitro's setup.json" >&2; exit 1; }
  fi

  echo "==> Creating the cluster with Invitro (this takes a while)"
  if [ -n "$node_setup" ]; then
    cp "$node_setup" "$configs/node_setup.json"
    (cd "$invitro/scripts/setup" && go run . -setup-type create_multinode_cluster \
      -config-dir "$configs" -config node_setup.json)
  else
    (cd "$invitro/scripts/setup" && go run . -setup-type create_multinode_cluster \
      -config-dir "$configs" "${nodes[@]}")
  fi
fi

echo "==> Applying ARIES on $master"
apply_args=(--runner-kubeconfig '$HOME/aries-kubeconfig')
[ "$prometheus" = false ] || apply_args+=(--monitoring)
# Ship the committed k8s/ tree only, so local credentials never leave this
# machine. Invitro installs helm with Prometheus; install it otherwise.
git -C "$root" archive HEAD k8s | remote "$master" "
  set -e
  rm -rf ~/aries-k8s && mkdir -p ~/aries-k8s && tar -xf - -C ~/aries-k8s
  command -v helm >/dev/null ||
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  ~/aries-k8s/k8s/apply-aries.sh ${apply_args[*]}
"

if [ "$stage_runner" = true ]; then
  case "$(remote "$runner" uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) echo "unsupported runner architecture" >&2; exit 1 ;;
  esac
  echo "==> Building the runner for linux/$arch and staging it to $runner:~/aries"
  stage=$(mktemp -d)
  trap 'rm -rf "${configs:-}" "$stage"' EXIT
  mkdir -p "$stage/bin"
  for command in aries aries-ssh-client; do
    (cd "$root" && CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -o "$stage/bin/$command" "./cmd/$command")
  done
  git -C "$root" archive HEAD profiles configs | tar -xf - -C "$stage"
  # Only bin/, profiles/ and configs/ are replaced; runs/, .cache/ and the
  # model key on the runner are kept.
  COPYFILE_DISABLE=1 tar -C "$stage" -cf - bin profiles configs | remote "$runner" '
    mkdir -p ~/aries && cd ~/aries && rm -rf bin profiles configs && tar -xf -'
  # The token passes through a pipe, never a command line or a local file.
  remote "$master" 'cat ~/aries-kubeconfig' | remote "$runner" 'umask 077; cat > ~/aries/kubeconfig'
fi

echo
echo "Cluster ready. Next:"
echo "  1. Publish the bridge image for the Kubernetes version printed above (k8s/README.md)."
echo "  2. Set pod_cidr and service_cidr in the profile to the values printed above."
if [ "$stage_runner" = true ]; then
  echo "  3. scp DEEPSEEK_API.key $runner:aries/ && ssh $runner chmod 600 aries/DEEPSEEK_API.key"
  echo "  4. ssh $runner 'cd ~/aries && KUBECONFIG=\$PWD/kubeconfig ./bin/aries profiles/openclaw-tb2-fix-git-deepseek-k8s-pods.json'"
fi
