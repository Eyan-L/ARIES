#!/usr/bin/env bash
# Removes what Invitro's create_multinode_cluster installs for its serverless
# experiments and ARIES does not use: Knative Serving, Istio, MetalLB, vHive's
# in-cluster image registry, the Prometheus pushgateway and the Knative
# monitors and dashboards. Calico, containerd, metrics-server and
# kube-prometheus-stack stay.
#
# Run it where kubectl and helm reach the cluster as an admin, such as the
# master node. It is safe to run again. The cluster can no longer run Invitro's
# own loader experiments afterwards.
set -euo pipefail

monitoring_namespace=monitoring
pushgateway_release=prometheus-pushgateway
dry_run=false
wait_timeout=5m

usage() {
  cat <<'USAGE'
Usage: k8s/prune-invitro.sh [options]

  --monitoring-namespace NS  kube-prometheus-stack's namespace (default: monitoring)
  --dry-run                  list what would be removed and change nothing
  -h, --help                 show this help
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --monitoring-namespace) monitoring_namespace=$2; shift 2 ;;
    --dry-run) dry_run=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

command -v kubectl >/dev/null || { echo "kubectl is required on PATH" >&2; exit 1; }
kubectl auth can-i delete namespaces >/dev/null 2>&1 ||
  { echo "kubectl must reach the cluster as an admin" >&2; exit 1; }

namespaces=(knative-serving knative-eventing istio-system metallb-system registry)
# Cluster-scoped objects these components leave behind once their namespaces
# are gone. No Calico, kubeadm, Prometheus or ARIES object carries these words.
names='knative|istio|metallb'
groups='\.(knative\.dev|istio\.io|metallb\.io)$'

# remove NAMESPACE OBJECT... deletes the named objects; an empty NAMESPACE means
# cluster-scoped objects or namespaces.
remove() {
  local namespace=$1
  shift
  [ $# -gt 0 ] || return 0
  if [ "$dry_run" = true ]; then
    for object in "$@"; do echo "  would delete $object${namespace:+ in $namespace}"; done
  else
    kubectl delete ${namespace:+-n "$namespace"} --ignore-not-found --wait=false "$@"
  fi
}

# list KIND [kubectl get options...] prints object names; a kind this cluster
# does not serve, such as ServiceMonitor without Prometheus, lists nothing.
list() { kubectl get "$@" -o name 2>/dev/null || true; }

matching() { list "$@" | grep -E "/.*($names)" || true; }

echo "==> Removing admission webhooks"
# First, so nothing calls a webhook whose service is about to disappear.
# Istio's sidecar injector would otherwise block pod creation in any labelled
# namespace.
# shellcheck disable=SC2046
remove "" $(matching mutatingwebhookconfigurations) $(matching validatingwebhookconfigurations)

echo "==> Removing Istio sidecar injection labels"
for label in istio-injection istio.io/rev; do
  for ns in $(list namespaces -l "$label"); do
    if [ "$dry_run" = true ]; then
      echo "  would unlabel $ns $label"
    else
      kubectl label "$ns" "$label-"
    fi
  done
done

echo "==> Removing the pushgateway and Knative monitoring"
if command -v helm >/dev/null && helm status -n "$monitoring_namespace" "$pushgateway_release" >/dev/null 2>&1; then
  if [ "$dry_run" = true ]; then
    echo "  would uninstall helm release $monitoring_namespace/$pushgateway_release"
  else
    helm uninstall -n "$monitoring_namespace" "$pushgateway_release"
  fi
fi
# Invitro moves the Knative dashboards into the monitoring namespace; its
# ServiceMonitors and PodMonitors live in the Knative namespaces.
# shellcheck disable=SC2046
remove "$monitoring_namespace" $(list configmaps -n "$monitoring_namespace" | grep -i knative || true)

echo "==> Removing namespaces"
present=()
for ns in "${namespaces[@]}"; do
  kubectl get namespace "$ns" >/dev/null 2>&1 && present+=("namespace/$ns")
done
remove "" ${present[@]+"${present[@]}"}
if [ "$dry_run" = false ] && [ ${#present[@]} -gt 0 ]; then
  for ns in "${present[@]}"; do
    kubectl wait --for=delete "$ns" --timeout="$wait_timeout" >/dev/null 2>&1 ||
      echo "  $ns is still terminating; check its remaining objects for finalizers" >&2
  done
fi

echo "==> Removing cluster-scoped leftovers"
# The registry's volume is cluster-scoped; its data stays on the node's disk.
registry_volumes=$(kubectl get persistentvolumes \
  -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="registry")]}persistentvolume/{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
# shellcheck disable=SC2046
remove "" $(matching clusterrolebindings) $(matching clusterroles) $registry_volumes
# shellcheck disable=SC2046
remove "" $(list customresourcedefinitions | grep -E "$groups" || true)

echo
if [ "$dry_run" = true ]; then
  echo "Dry run: nothing was changed."
else
  echo "Invitro's serverless components are removed. Remaining namespaces:"
  kubectl get namespaces --no-headers -o custom-columns=NAME:.metadata.name | sed 's/^/  /'
fi
