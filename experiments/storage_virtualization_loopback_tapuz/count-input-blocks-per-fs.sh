#!/bin/bash
################################################################################
# SCRIPT: count-input-blocks-per-fs.sh
# DESCRIPTION: Counts how many blocks from a list are stored in each loopback
#              filesystem on this DataNode.
#
# USAGE: bash count-input-blocks-per-fs.sh <block_ids_file> <k> <mount_base>
#   block_ids_file - File containing block IDs (one per line, e.g., blk_1073741825_1001)
#   k              - Number of loopback filesystems
#   mount_base     - Base mount point (e.g., /scratch/hdfs_loop)
#
# OUTPUT: Semicolon-separated counts, e.g., "4;6;3;5" for k=4
################################################################################

set -euo pipefail

BLOCK_IDS_FILE=${1:?Usage: count-input-blocks-per-fs.sh <block_ids_file> <k> <mount_base>}
K=${2:?Usage: count-input-blocks-per-fs.sh <block_ids_file> <k> <mount_base>}
MOUNT_BASE=${3:?Usage: count-input-blocks-per-fs.sh <block_ids_file> <k> <mount_base>}

# Check if block IDs file exists
if [[ ! -f "$BLOCK_IDS_FILE" ]]; then
    # If no block IDs file, output zeros
    OUTPUT=""
    for ((i=1; i<=K; i++)); do
        if [[ -n "$OUTPUT" ]]; then
            OUTPUT="${OUTPUT};0"
        else
            OUTPUT="0"
        fi
    done
    echo -n "$OUTPUT"
    exit 0
fi

# If block IDs file is empty, output zeros
if [[ ! -s "$BLOCK_IDS_FILE" ]]; then
    OUTPUT=""
    for ((i=1; i<=K; i++)); do
        if [[ -n "$OUTPUT" ]]; then
            OUTPUT="${OUTPUT};0"
        else
            OUTPUT="0"
        fi
    done
    echo -n "$OUTPUT"
    exit 0
fi

# For each loopback filesystem, count how many of these block IDs exist.
# Parallelized: all K FSes run concurrently, each writing its result to a
# temp file named by index. Results are assembled in order afterwards.
TMPDIR_RESULTS=$(mktemp -d)
trap 'rm -rf "$TMPDIR_RESULTS"' EXIT

MAX_JOBS=256
ACTIVE=0
for ((i=1; i<=K; i++)); do
    (
        FS_DIR="$MOUNT_BASE/dn${i}/hdfs_data"
        COUNT=0
        if [[ -d "$FS_DIR" ]]; then
            COUNT=$(find "$FS_DIR" -name "blk_*" -not -name "*.meta" -type f -printf "%f\n" 2>/dev/null \
                    | grep -Fxf "$BLOCK_IDS_FILE" | wc -l)
        fi
        echo "$COUNT" > "$TMPDIR_RESULTS/$i"
    ) &
    ACTIVE=$(( ACTIVE + 1 ))
    if (( ACTIVE >= MAX_JOBS )); then
        wait -n 2>/dev/null || wait
        ACTIVE=$(( ACTIVE - 1 ))
    fi
done
wait

# Assemble results in order
OUTPUT=""
for ((i=1; i<=K; i++)); do
    COUNT=$(cat "$TMPDIR_RESULTS/$i" 2>/dev/null || echo 0)
    if [[ -n "$OUTPUT" ]]; then
        OUTPUT="${OUTPUT};${COUNT}"
    else
        OUTPUT="${COUNT}"
    fi
done

echo -n "$OUTPUT"
