#!/usr/bin/env bash
# Attach the loop-backed devmapper thin pool used by aries-containerd.
# Usage: devmapper-pool.sh DIR POOL_NAME
# DIR holds the sparse "data" and "meta" files created by install.sh.
# Idempotent: existing loop devices and an existing pool are reused.
set -euo pipefail

dir=${1:?usage: devmapper-pool.sh DIR POOL_NAME}
pool=${2:?usage: devmapper-pool.sh DIR POOL_NAME}
data_file=$dir/data
meta_file=$dir/meta

attach() {
	local file=$1 device
	device=$(losetup --associated "$file" --noheadings --output NAME | head -n 1)
	if [[ -z $device ]]; then
		device=$(losetup --find --show "$file")
	fi
	printf '%s\n' "$device"
}

[[ -f $data_file && -f $meta_file ]] || {
	echo "missing $data_file or $meta_file; run install.sh first" >&2
	exit 1
}

data_device=$(attach "$data_file")
meta_device=$(attach "$meta_file")

if dmsetup status "$pool" >/dev/null 2>&1; then
	exit 0
fi

# 64 KiB data blocks; low-water mark of 32768 blocks.
sectors=$(blockdev --getsz "$data_device")
dmsetup create "$pool" --table "0 $sectors thin-pool $meta_device $data_device 128 32768 1 skip_block_zeroing"
