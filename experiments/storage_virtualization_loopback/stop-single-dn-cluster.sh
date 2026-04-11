#!/bin/bash
################################################################################
# SCRIPT: stop-single-dn-cluster.sh
# DESCRIPTION: Stops the single DataNode cluster and tears down loopback
#              filesystems on all nodes.
#
# USAGE: bash stop-single-dn-cluster.sh [max_k]
#   max_k - Maximum k value to clean up loopback FSes (default: 512)
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

MAX_K=${1:-512}

# Cluster nodes
MASTER_NODE="tapuz14"
ALL_NODES=("tapuz14" "tapuz10" "tapuz11" "tapuz12" "tapuz13")
MASTER_HAS_DN=${MASTER_HAS_DN:-0}

DATANODE_NODES=()
if [[ "$MASTER_HAS_DN" == "0" ]]; then
    WORKER_NODES=("tapuz10" "tapuz11" "tapuz12" "tapuz13")
    DATANODE_NODES=("${WORKER_NODES[@]}")
else
    DATANODE_NODES=("${ALL_NODES[@]}")
fi

IMAGE_DIR="/scratch/loop_images"
MOUNT_BASE="/scratch/hdfs_loop"

echo "============================================================"
echo "Stopping Single DataNode Cluster"
echo "============================================================"

# ============================================================================
# STEP 1: Stop YARN and HDFS
# ============================================================================
echo "=== STEP 1: Stopping YARN and HDFS ==="

stop-yarn.sh 2>/dev/null || true
mapred --daemon stop historyserver 2>/dev/null || true
stop-dfs.sh 2>/dev/null || true
sleep 2

# ============================================================================
# STEP 2: Kill any remaining DataNode/NameNode processes
# ============================================================================
echo "=== STEP 2: Killing remaining Hadoop processes ==="

for node in "${ALL_NODES[@]}"; do
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        pkill -f "org.apache.hadoop.hdfs.server.datanode.DataNode" 2>/dev/null || true
        pkill -f "org.apache.hadoop.hdfs.server.namenode.NameNode" 2>/dev/null || true
    else
        ssh "$node" "pkill -f 'org.apache.hadoop.hdfs.server.datanode.DataNode'" 2>/dev/null || true
        ssh "$node" "pkill -f 'org.apache.hadoop.hdfs.server.namenode.NameNode'" 2>/dev/null || true
    fi
done
sleep 2

echo "All Hadoop processes stopped."

# ============================================================================
# STEP 3: Tear down loopback filesystems on DataNode nodes (PARALLEL)
# ============================================================================
echo ""
echo "=== STEP 3: Tearing down loopback filesystems (parallel) ==="

# Copy script to all remote nodes first
for node in "${DATANODE_NODES[@]}"; do
    if [[ "$node" != "$(hostname)" && "$node" != "$MASTER_NODE" ]]; then
        scp -q "$SCRIPT_DIR/teardown-loopback-fs.sh" "$node:/tmp/teardown-loopback-fs.sh" &
    fi
done
wait

# Run teardown in parallel on all nodes
declare -A TEARDOWN_PIDS
for node in "${DATANODE_NODES[@]}"; do
    echo "--- Starting teardown on $node ---"
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        bash "$SCRIPT_DIR/teardown-loopback-fs.sh" "$MAX_K" "$IMAGE_DIR" "$MOUNT_BASE" > "/tmp/teardown_${node}.log" 2>&1 &
        TEARDOWN_PIDS[$node]=$!
    else
        ssh "$node" "bash /tmp/teardown-loopback-fs.sh $MAX_K $IMAGE_DIR $MOUNT_BASE" > "/tmp/teardown_${node}.log" 2>&1 &
        TEARDOWN_PIDS[$node]=$!
    fi
done

# Wait for all teardown processes
for node in "${DATANODE_NODES[@]}"; do
    if wait "${TEARDOWN_PIDS[$node]}"; then
        echo "--- $node: teardown complete ---"
    else
        echo "--- $node: teardown FAILED ---"
        cat "/tmp/teardown_${node}.log"
    fi
done

echo ""
echo "============================================================"
echo "Cluster stopped and loopback filesystems torn down."
echo "============================================================"
