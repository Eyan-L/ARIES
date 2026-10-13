#!/usr/bin/env bash
# Install the aries-vmd host stack: pinned Kata Containers (Go runtime with
# Firecracker), a loop-backed devmapper thin pool, a dedicated containerd,
# and the aries-vmd daemon. Run as root from the repository after
# `make build`. Re-running is safe; existing pool files and loops are reused.
#
# Usage:
#   sudo scripts/vmd/install.sh            # install and start systemd units
#   sudo scripts/vmd/install.sh --manual   # install files only; start by hand
#
# Manual start (no systemd), in separate root shells:
#   /usr/local/libexec/aries/devmapper-pool.sh /var/lib/aries-vmd/devmapper aries-vmd-pool
#   PATH=/usr/local/bin:$PATH containerd --config /etc/aries-vmd/containerd.toml
#   sudo ./bin/aries-vmd --group aries --storage-limit-mb 24576 \
#     --helper /usr/local/libexec/aries/aries-vmd-helper
#
# Environment overrides:
#   STORAGE_LIMIT_MB  per-sandbox root filesystem size (default 24576)
#   POOL_DATA_SIZE    sparse thin-pool data file size (default 200G)
#   POOL_META_SIZE    thin-pool metadata file size (default 2G)
#   ARIES_GROUP       group allowed to use the daemon socket (default aries)
#   CONTAINERD        containerd binary (default: containerd on PATH)
set -euo pipefail

# Must equal the vmd.kata pin in configs/versions.json (checked by pkg/config tests).
KATA_VERSION=4.2.0
KATA_URL=https://github.com/kata-containers/kata-containers/releases/download/${KATA_VERSION}/kata-go-static-${KATA_VERSION}-amd64.tar.zst
KATA_SHA256=7dda31ca54b397cbf8165f620d6041872d3c45b7a77383ce0e86f76a06e103d0

STORAGE_LIMIT_MB=${STORAGE_LIMIT_MB:-24576}
POOL_DATA_SIZE=${POOL_DATA_SIZE:-200G}
POOL_META_SIZE=${POOL_META_SIZE:-2G}
POOL_NAME=aries-vmd-pool
ARIES_GROUP=${ARIES_GROUP:-aries}
DEVMAPPER_DIR=/var/lib/aries-vmd/devmapper
KATA_DIR=/opt/kata
CONFIG_DIR=/etc/aries-vmd
LIBEXEC=/usr/local/libexec/aries

manual=false
case ${1:-} in
--manual) manual=true ;;
"") ;;
*)
	echo "usage: $0 [--manual]" >&2
	exit 64
	;;
esac

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/../.." && pwd)

die() {
	echo "install: $*" >&2
	exit 1
}

[[ $(id -u) -eq 0 ]] || die "must run as root"
[[ $(uname -m) == x86_64 ]] || die "the pinned Kata release is amd64 only"
[[ $STORAGE_LIMIT_MB =~ ^[1-9][0-9]*$ ]] || die "STORAGE_LIMIT_MB must be a positive integer"
[[ -e /dev/kvm ]] || die "/dev/kvm is missing; enable hardware virtualization (or nested virtualization)"
for tool in curl sha256sum tar zstd losetup dmsetup blockdev ip nft; do
	command -v "$tool" >/dev/null || die "required tool '$tool' is not installed"
done
CONTAINERD=${CONTAINERD:-$(command -v containerd || true)}
[[ -x $CONTAINERD ]] || die "containerd 2.x is required (set CONTAINERD=/path/to/containerd)"
for binary in aries-vmd aries-vmd-helper; do
	[[ -x $repo_dir/bin/$binary ]] || die "missing $repo_dir/bin/$binary; run 'make build' first"
done

render() {
	sed -e "s|@STORAGE_LIMIT_MB@|$STORAGE_LIMIT_MB|g" \
		-e "s|@POOL_NAME@|$POOL_NAME|g" \
		-e "s|@DEVMAPPER_DIR@|$DEVMAPPER_DIR|g" \
		-e "s|@GROUP@|$ARIES_GROUP|g" \
		-e "s|@CONTAINERD@|$CONTAINERD|g" "$1" >"$2"
}

echo "== Kata Containers $KATA_VERSION"
if [[ ! -x $KATA_DIR/bin/containerd-shim-kata-v2 || $(cat "$KATA_DIR/.aries-version" 2>/dev/null || true) != "$KATA_VERSION" ]]; then
	work=$(mktemp -d)
	trap 'rm -rf "$work"' EXIT
	curl --fail --location --silent --show-error --output "$work/kata.tar.zst" "$KATA_URL"
	echo "$KATA_SHA256  $work/kata.tar.zst" | sha256sum --check --quiet
	mkdir "$work/root"
	tar --zstd -xf "$work/kata.tar.zst" -C "$work/root"
	shim=$(find "$work/root" -path '*/bin/containerd-shim-kata-v2' -type f | head -n 1)
	[[ -n $shim ]] || die "the Kata archive has no containerd-shim-kata-v2"
	kata_root=$(dirname "$(dirname "$shim")")
	rm -rf "$KATA_DIR"
	mkdir -p "$(dirname "$KATA_DIR")"
	cp -a "$kata_root" "$KATA_DIR"
	echo "$KATA_VERSION" >"$KATA_DIR/.aries-version"
	rm -rf "$work"
	trap - EXIT
fi
for binary in containerd-shim-kata-v2 firecracker jailer; do
	[[ -x $KATA_DIR/bin/$binary ]] || die "$KATA_DIR/bin/$binary is missing from the Kata release"
done

echo "== Kata Firecracker configuration"
mkdir -p "$CONFIG_DIR"
fc_config=$(find "$KATA_DIR" -name configuration-fc.toml -type f | head -n 1)
[[ -n $fc_config ]] || die "configuration-fc.toml not found under $KATA_DIR"
cp "$fc_config" "$CONFIG_DIR/configuration-fc.toml"
# Allow only per-sandbox vCPU and memory sizing through annotations.
sed -i -E 's|^#?[[:space:]]*enable_annotations[[:space:]]*=.*|enable_annotations = ["default_vcpus", "default_memory"]|' "$CONFIG_DIR/configuration-fc.toml"
grep -q '^enable_annotations = \["default_vcpus", "default_memory"\]' "$CONFIG_DIR/configuration-fc.toml" ||
	die "could not set enable_annotations in configuration-fc.toml"
sed -i -E "s|^path[[:space:]]*=.*firecracker.*|path = \"$KATA_DIR/bin/firecracker\"|; s|^jailer_path[[:space:]]*=.*|jailer_path = \"$KATA_DIR/bin/jailer\"|" "$CONFIG_DIR/configuration-fc.toml"
cat >/usr/local/bin/containerd-shim-kata-fc-v2 <<EOF
#!/bin/sh
KATA_CONF_FILE=$CONFIG_DIR/configuration-fc.toml exec $KATA_DIR/bin/containerd-shim-kata-v2 "\$@"
EOF
chmod 0755 /usr/local/bin/containerd-shim-kata-fc-v2

echo "== devmapper thin pool"
mkdir -p "$DEVMAPPER_DIR" "$LIBEXEC"
install -m 0755 "$script_dir/devmapper-pool.sh" "$LIBEXEC/devmapper-pool.sh"
[[ -f $DEVMAPPER_DIR/data ]] || truncate -s "$POOL_DATA_SIZE" "$DEVMAPPER_DIR/data"
[[ -f $DEVMAPPER_DIR/meta ]] || truncate -s "$POOL_META_SIZE" "$DEVMAPPER_DIR/meta"

echo "== containerd and aries-vmd"
render "$script_dir/containerd.toml.in" "$CONFIG_DIR/containerd.toml"
install -m 0755 "$repo_dir/bin/aries-vmd" /usr/local/bin/aries-vmd
install -m 0755 "$repo_dir/bin/aries-vmd-helper" "$LIBEXEC/aries-vmd-helper"
groupadd -f "$ARIES_GROUP"
mkdir -p /var/lib/aries-vmd
chmod 0700 /var/lib/aries-vmd

if $manual; then
	"$LIBEXEC/devmapper-pool.sh" "$DEVMAPPER_DIR" "$POOL_NAME"
	cat <<EOF
Installed without systemd units. Start, as root:
  PATH=/usr/local/bin:\$PATH $CONTAINERD --config $CONFIG_DIR/containerd.toml
  /usr/local/bin/aries-vmd --group $ARIES_GROUP --storage-limit-mb $STORAGE_LIMIT_MB
EOF
else
	for unit in aries-vmd-devmapper aries-containerd aries-vmd; do
		render "$script_dir/$unit.service.in" "/etc/systemd/system/$unit.service"
	done
	systemctl daemon-reload
	systemctl enable --now aries-vmd-devmapper.service aries-containerd.service aries-vmd.service
	systemctl restart aries-vmd.service
fi

user=${SUDO_USER:-}
if [[ -n $user ]] && ! id -nG "$user" | tr ' ' '\n' | grep -qx "$ARIES_GROUP"; then
	echo "Add $user to the $ARIES_GROUP group to use the daemon: usermod -aG $ARIES_GROUP $user (then log in again)"
fi
echo "aries-vmd installed (Kata $KATA_VERSION, storage limit ${STORAGE_LIMIT_MB} MiB)."
