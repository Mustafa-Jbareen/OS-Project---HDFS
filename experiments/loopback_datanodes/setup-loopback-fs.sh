#!/bin/bash
################################################################################
# SCRIPT: setup-loopback-fs.sh
# DESCRIPTION: Creates and mounts k loopback filesystems on a single node.
#              Each loopback image is a file formatted as ext4, mounted via
#              the loop driver. This gives each DataNode its own independent
#              filesystem (own journal, inode table, free-space tracking).
#
# USAGE: bash setup-loopback-fs.sh <k> [image_size_gb] [image_dir] [mount_base]
#   k              - Number of loopback filesystems to create (required)
#   image_size_gb  - Size of each disk image in GB (default: 30)
#   image_dir      - Directory to store .img files (default: /data/loop_images)
#   mount_base     - Base mount point (default: /mnt/hdfs_loop)
#
# REQUIRES: sudo access (for mkfs, mount, losetup)
#
# EXAMPLE:
#   bash setup-loopback-fs.sh 4          # Create 4 × 30GB loopback FSes
#   bash setup-loopback-fs.sh 8 20       # Create 8 × 20GB loopback FSes
################################################################################

set -euo pipefail

K=${1:?Usage: setup-loopback-fs.sh <k> [image_size_gb] [image_dir] [mount_base]}
IMAGE_SIZE_GB=${2:-30}
IMAGE_DIR=${3:-/data/loop_images}
MOUNT_BASE=${4:-/mnt/hdfs_loop}

echo "========================================"
echo "Setting up $K loopback filesystems"
echo "  Image size:  ${IMAGE_SIZE_GB}GB each"
echo "  Image dir:   $IMAGE_DIR"
echo "  Mount base:  $MOUNT_BASE"
echo "========================================"

# Create image directory
sudo mkdir -p "$IMAGE_DIR"

for ((i=1; i<=K; i++)); do
    IMG_FILE="$IMAGE_DIR/hdfs_dn${i}.img"
    MOUNT_POINT="$MOUNT_BASE/dn${i}"
    RECREATE_IMAGE=false

    echo ""
    echo "--- Loopback FS #$i ---"

    # If already mounted, verify size matches requested image size.
    if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        current_size_gb=$(df -BG --output=size "$MOUNT_POINT" 2>/dev/null | tail -1 | tr -dc '0-9' || echo "")
        if [[ -n "$current_size_gb" && "$current_size_gb" == "$IMAGE_SIZE_GB" ]]; then
            echo "  Already mounted at $MOUNT_POINT with correct size (${current_size_gb}GB), keeping."
            continue
        fi
        echo "  Mounted size (${current_size_gb:-unknown}GB) != requested (${IMAGE_SIZE_GB}GB); remounting."
        sudo umount "$MOUNT_POINT" 2>/dev/null || true
        RECREATE_IMAGE=true
    fi

    # Step 1: Create or recreate disk image file at requested size
    if [[ -f "$IMG_FILE" ]]; then
        current_img_size_gb=$(($(stat -c%s "$IMG_FILE" 2>/dev/null || echo 0) / 1024 / 1024 / 1024))
        if (( current_img_size_gb != IMAGE_SIZE_GB )); then
            echo "  Existing image size (${current_img_size_gb}GB) != requested (${IMAGE_SIZE_GB}GB), recreating."
            RECREATE_IMAGE=true
        fi
    fi

    if [[ ! -f "$IMG_FILE" || "$RECREATE_IMAGE" == "true" ]]; then
        sudo rm -f "$IMG_FILE"
        echo "  Creating ${IMAGE_SIZE_GB}GB image: $IMG_FILE"
        sudo fallocate -l "${IMAGE_SIZE_GB}G" "$IMG_FILE"
    else
        echo "  Image already exists with correct size: $IMG_FILE"
    fi

    # Step 2: Format with ext4
    #   -F  = force (don't ask for confirmation on non-block-device)
    #   -m0 = reserve 0% for root (we want all space for HDFS)
    #   -O ^has_journal = optionally disable journal for slightly more space
    #     (we keep the journal here for crash safety)
    echo "  Formatting as ext4..."
    sudo mkfs.ext4 -F -m0 -q "$IMG_FILE"

    # Step 3: Create mount point and mount via loop driver
    sudo mkdir -p "$MOUNT_POINT"
    echo "  Mounting at $MOUNT_POINT..."
    sudo mount -o loop "$IMG_FILE" "$MOUNT_POINT"

    # Step 4: Fix permissions so the Hadoop user can write
    sudo chmod 777 "$MOUNT_POINT"

    # Verify
    echo "  Mounted: $(df -h "$MOUNT_POINT" | tail -1)"
done

echo ""
echo "All $K loopback filesystems ready."
echo ""
echo "Mounts:"
for ((i=1; i<=K; i++)); do
    echo "  $MOUNT_BASE/dn${i} -> $IMAGE_DIR/hdfs_dn${i}.img"
done
