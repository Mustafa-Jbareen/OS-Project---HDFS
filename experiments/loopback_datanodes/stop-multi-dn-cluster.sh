#!/bin/bash
################################################################################
# SCRIPT: stop-multi-dn-cluster.sh
# DESCRIPTION: Stops all DataNode processes, the NameNode, YARN, and optionally
#              tears down the loopback filesystems.
#
# USAGE: bash stop-multi-dn-cluster.sh [--keep-loops]
#   --keep-loops  - Don't unmount/delete loopback images (faster restarts)
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

KEEP_LOOPS=false
if [[ "${1:-}" == "--keep-loops" ]]; then
    KEEP_LOOPS=true
fi

MASTER_NODE="tapuz14"
ALL_NODES=("tapuz14" "tapuz10" "tapuz11" "tapuz12" "tapuz13")
HADOOP_HOME="/home/mostufa.j/hadoop"
CONFIG_BASE="/tmp/hadoop_multi_dn"

wait_for_datanodes_exit() {
    local node=$1
    local wait_seconds=${2:-20}

    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        for ((t=0; t<wait_seconds; t++)); do
            if ! pgrep -f "org.apache.hadoop.hdfs.server.datanode.DataNode" >/dev/null 2>&1; then
                return 0
            fi
            sleep 1
        done
        return 1
    fi

    if ssh "$node" "for t in \$(seq 1 $wait_seconds); do if ! pgrep -f 'org.apache.hadoop.hdfs.server.datanode.DataNode' >/dev/null 2>&1; then exit 0; fi; sleep 1; done; exit 1" >/dev/null 2>&1; then
        return 0
    fi

    return 1
}

echo "============================================================"
echo "Stopping Multi-DataNode Cluster"
echo "============================================================"

# ============================================================================
# STEP 1: Stop YARN
# ============================================================================
echo ""
echo "=== Stopping YARN ==="
export HADOOP_CONF_DIR="$CONFIG_BASE/dn1" 2>/dev/null || true
stop-yarn.sh 2>/dev/null || true
mapred --daemon stop historyserver 2>/dev/null || true
pkill -f "org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer" 2>/dev/null || true

# ============================================================================
# STEP 2: Kill all DataNode processes on all nodes
# ============================================================================
echo ""
echo "=== Stopping DataNodes on all nodes ==="

for node in "${ALL_NODES[@]}"; do
    echo "  Stopping DataNodes on $node..."
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        pkill -f "org.apache.hadoop.hdfs.server.datanode.DataNode" 2>/dev/null || true
    else
        ssh "$node" "pkill -f 'org.apache.hadoop.hdfs.server.datanode.DataNode'" 2>/dev/null || true
    fi
done

echo ""
echo "=== Waiting for DataNode JVMs to exit ==="
for node in "${ALL_NODES[@]}"; do
    echo "  Waiting on $node..."
    if ! wait_for_datanodes_exit "$node" 20; then
        echo "  WARNING: DataNode still running on $node after timeout; forcing kill..."
        if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
            pkill -9 -f "org.apache.hadoop.hdfs.server.datanode.DataNode" 2>/dev/null || true
        else
            ssh "$node" "pkill -9 -f 'org.apache.hadoop.hdfs.server.datanode.DataNode'" 2>/dev/null || true
        fi
    fi
done

# ============================================================================
# STEP 3: Stop NameNode
# ============================================================================
echo ""
echo "=== Stopping NameNode ==="
pkill -f "org.apache.hadoop.hdfs.server.namenode.NameNode" 2>/dev/null || true

sleep 3

# ============================================================================
# STEP 4: Optionally tear down loopback filesystems
# ============================================================================
if [[ "$KEEP_LOOPS" == false ]]; then
    echo ""
    echo "=== Tearing down loopback filesystems ==="
    for node in "${ALL_NODES[@]}"; do
        echo "  Teardown on $node..."
        if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
            bash "$SCRIPT_DIR/teardown-loopback-fs.sh" 16
        else
            scp -q "$SCRIPT_DIR/teardown-loopback-fs.sh" "$node:/tmp/teardown-loopback-fs.sh"
            ssh "$node" "bash /tmp/teardown-loopback-fs.sh 16"
        fi
    done
else
    echo ""
    echo "Keeping loopback filesystems (--keep-loops specified)."
fi

# ============================================================================
# STEP 5: Clean up temp config directories
# ============================================================================
echo ""
echo "=== Cleaning up temp configs ==="
for node in "${ALL_NODES[@]}"; do
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        rm -rf "$CONFIG_BASE" 2>/dev/null || true
        rm -rf /tmp/hadoop_dn_logs 2>/dev/null || true
        rm -rf /tmp/hadoop_dn_pids 2>/dev/null || true
    else
        ssh "$node" "rm -rf $CONFIG_BASE /tmp/hadoop_dn_logs /tmp/hadoop_dn_pids" 2>/dev/null || true
    fi
done

echo ""
echo "Multi-DataNode cluster stopped."
echo "============================================================"
