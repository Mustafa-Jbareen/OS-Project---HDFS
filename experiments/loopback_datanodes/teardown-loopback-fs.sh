#!/bin/bash
################################################################################
# SCRIPT: teardown-loopback-fs.sh
# DESCRIPTION: Unmounts and removes all loopback filesystems created by
#              setup-loopback-fs.sh.
#
# USAGE: bash teardown-loopback-fs.sh [max_k] [image_dir] [mount_base]
#   max_k      - Maximum k to clean up (default: 16)
#   image_dir  - Directory where .img files are stored (default: /data/loop_images)
#   mount_base - Base mount point (default: /mnt/hdfs_loop)
#
# REQUIRES: sudo access
################################################################################

set -euo pipefail

MAX_K=${1:-16}
IMAGE_DIR=${2:-/data/loop_images}
MOUNT_BASE=${3:-/mnt/hdfs_loop}

echo "========================================"
echo "Tearing down loopback filesystems"
echo "  Checking up to $MAX_K mounts"
echo "  Image dir:   $IMAGE_DIR"
echo "  Mount base:  $MOUNT_BASE"
echo "========================================"

UNMOUNTED=0
FAILED_UNMOUNTS=0

try_unmount() {
    local mount_point=$1

    # Try a normal unmount first.
    if sudo umount "$mount_point" 2>/dev/null; then
        return 0
    fi

    echo "  Mount busy at $mount_point, attempting to release holders..."

    # Kill processes holding files under the mount, if fuser is available.
    if command -v fuser >/dev/null 2>&1; then
        sudo fuser -km "$mount_point" 2>/dev/null || true
    fi

    sleep 1

    # Retry normal unmount after cleanup.
    if sudo umount "$mount_point" 2>/dev/null; then
        return 0
    fi

    # Last resort: lazy unmount so teardown can proceed.
    if sudo umount -l "$mount_point" 2>/dev/null; then
        echo "  Lazy-unmounted $mount_point"
        return 0
    fi

    return 1
}

for ((i=1; i<=MAX_K; i++)); do
    MOUNT_POINT="$MOUNT_BASE/dn${i}"
    IMG_FILE="$IMAGE_DIR/hdfs_dn${i}.img"

    # Unmount if mounted
    if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        echo "Unmounting $MOUNT_POINT..."
        if try_unmount "$MOUNT_POINT"; then
            UNMOUNTED=$((UNMOUNTED + 1))
        else
            echo "  WARNING: Failed to unmount $MOUNT_POINT; keeping related image for safety."
            FAILED_UNMOUNTS=$((FAILED_UNMOUNTS + 1))
            continue
        fi
    fi

    # Remove mount point directory
    if [[ -d "$MOUNT_POINT" ]]; then
        sudo rmdir "$MOUNT_POINT" 2>/dev/null || true
    fi

    # Remove disk image
    if [[ -f "$IMG_FILE" ]]; then
        echo "Removing $IMG_FILE..."
        sudo rm -f "$IMG_FILE"
    fi
done

# Clean up empty directories
sudo rmdir "$MOUNT_BASE" 2>/dev/null || true
sudo rmdir "$IMAGE_DIR" 2>/dev/null || true

# Detach any remaining loop devices associated with our images
for loop_dev in $(losetup -a 2>/dev/null | grep "hdfs_dn" | cut -d: -f1); do
    echo "Detaching stale loop device: $loop_dev"
    sudo losetup -d "$loop_dev" 2>/dev/null || true
done

echo ""
echo "Teardown complete. Unmounted $UNMOUNTED filesystems."
if (( FAILED_UNMOUNTS > 0 )); then
    echo "WARNING: $FAILED_UNMOUNTS mount(s) could not be unmounted cleanly."
fi
