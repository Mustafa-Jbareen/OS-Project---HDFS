#!/bin/bash
################################################################################
# SCRIPT: setup-loopback-fs.sh
# DESCRIPTION: Creates and mounts k loopback filesystems on a single node.
#              Each loopback image is a file formatted as ext4, mounted via
#              the loop driver. This gives each DataNode its own independent
#              filesystem (own journal, inode table, free-space tracking).
#              Every image is always reformatted, so no filesystem from an
#              earlier run (possibly made with other options) is reused.
#
# USAGE: bash setup-loopback-fs.sh <k> [image_size_mb] [image_dir] [mount_base] [direct_io] [mkfs_mode]
#   k              - Number of loopback filesystems to create (required)
#   image_size_mb  - Size of each disk image in MB (default: 30720 = 30GB)
#   image_dir      - Directory to store .img files (default: /scratch/loop_images)
#   mount_base     - Base mount point (default: /scratch/hdfs_loop)
#   direct_io      - 1 = loop devices with direct I/O: the image file is not
#                    cached a second time in the page cache (default: 0)
#   mkfs_mode      - fixed (default): the same ext4 layout for every image
#                    size (4 KB blocks, one inode per 16 KB);
#                    default: mkfs.ext4's own choice, which depends on the
#                    image size (below 512 MB it may use 1 KB blocks and more
#                    inodes) -- what all runs before September 2026 used
#
# REQUIRES: sudo for mkfs.ext4, mount, umount, losetup, fallocate, mkdir, chmod, rm
################################################################################

set -euo pipefail

K=${1:?Usage: setup-loopback-fs.sh <k> [image_size_mb] [image_dir] [mount_base] [direct_io] [mkfs_mode]}
IMAGE_SIZE_MB=${2:-30720}
IMAGE_DIR=${3:-/scratch/loop_images}
MOUNT_BASE=${4:-/scratch/hdfs_loop}
DIRECT_IO=${5:-0}
MKFS_MODE=${6:-fixed}

case "$MKFS_MODE" in
    fixed)   MKFS_ARGS=(-b 4096 -i 16384) ;;
    default) MKFS_ARGS=() ;;
    *) echo "ERROR: mkfs_mode must be fixed or default (got '$MKFS_MODE')" >&2; exit 1 ;;
esac

echo "========================================"
echo "Setting up $K loopback filesystems"
echo "  Image size:  ${IMAGE_SIZE_MB}MB each"
echo "  Image dir:   $IMAGE_DIR"
echo "  Mount base:  $MOUNT_BASE"
echo "  Direct I/O:  $DIRECT_IO"
echo "  mkfs:        $MKFS_MODE ${MKFS_ARGS[*]:-}"
echo "========================================"

# Create image directory
sudo mkdir -p "$IMAGE_DIR"

for ((i=1; i<=K; i++)); do
    IMG_FILE="$IMAGE_DIR/hdfs_dn${i}.img"
    MOUNT_POINT="$MOUNT_BASE/dn${i}"

    echo ""
    echo "--- Loopback FS #$i ---"

    if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        echo "  Unmounting the previous filesystem at $MOUNT_POINT"
        sudo umount "$MOUNT_POINT"
    fi

    # Step 1: Create (or recreate) the disk image file at the requested size
    if [[ -f "$IMG_FILE" ]]; then
        current_img_size_mb=$(($(stat -c%s "$IMG_FILE" 2>/dev/null || echo 0) / 1024 / 1024))
        if (( current_img_size_mb != IMAGE_SIZE_MB )); then
            sudo rm -f "$IMG_FILE"
        fi
    fi
    if [[ ! -f "$IMG_FILE" ]]; then
        echo "  Creating ${IMAGE_SIZE_MB}MB image: $IMG_FILE"
        sudo fallocate -l "${IMAGE_SIZE_MB}M" "$IMG_FILE"
    fi

    # Step 2: Format with ext4 (always, so every run starts from a fresh fs)
    #   -F  = force (don't ask for confirmation on non-block-device)
    #   -m0 = reserve 0% for root (we want all space for HDFS)
    echo "  Formatting as ext4..."
    sudo mkfs.ext4 -F -m0 -q "${MKFS_ARGS[@]}" "$IMG_FILE"

    # Make the .img file world-readable so `filefrag` and the cache step can
    # open it without sudo (Tapuz's sudo list has neither).
    sudo chmod 644 "$IMG_FILE"

    # Step 3: Create mount point and mount via loop driver
    sudo mkdir -p "$MOUNT_POINT"
    echo "  Mounting at $MOUNT_POINT..."
    if [[ "$DIRECT_IO" == "1" ]]; then
        LOOP_DEV=$(sudo losetup --find --show --direct-io=on "$IMG_FILE")
        sudo mount "$LOOP_DEV" "$MOUNT_POINT"
    else
        sudo mount -o loop "$IMG_FILE" "$MOUNT_POINT"
    fi

    # Step 4: Fix permissions so the Hadoop user can write
    sudo chmod 777 "$MOUNT_POINT"

    # Verify
    echo "  Mounted: $(df -h "$MOUNT_POINT" | tail -1)"
done

# Direct I/O can be refused by the kernel (e.g. sector-size mismatch) without
# losetup failing, so check every loop device of this experiment.
if [[ "$DIRECT_IO" == "1" ]]; then
    not_dio=$(losetup --list --noheadings --output DIO,BACK-FILE 2>/dev/null \
        | awk -v d="$IMAGE_DIR/hdfs_dn" 'index($2, d) == 1 && $1 != "1"' | wc -l)
    if (( not_dio > 0 )); then
        echo "ERROR: direct I/O is not active on $not_dio loop device(s) (kernel refused it)." >&2
        exit 1
    fi
    echo ""
    echo "Direct I/O verified on all $K loop devices."
fi

echo ""
echo "All $K loopback filesystems ready ($(stat -f -c '%S' "$MOUNT_BASE/dn1" 2>/dev/null || echo '?')-byte blocks)."
