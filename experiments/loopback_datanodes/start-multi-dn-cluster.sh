#!/bin/bash
################################################################################
# SCRIPT: start-multi-dn-cluster.sh
# DESCRIPTION: Starts a Hadoop cluster with k DataNode instances per physical
#              node, each using a separate loopback filesystem and unique ports.
#
# WORKFLOW:
#   1. Set up loopback filesystems on all nodes (k per node)
#   2. Generate per-DN config directories on all nodes
#   3. Format and start the NameNode
#   4. Start k DataNode processes on each node
#   5. Start YARN ResourceManager + NodeManagers
#   6. Wait for all DataNodes to register
#
# USAGE: bash start-multi-dn-cluster.sh <k> [image_size_gb] [dn_heap_mb] [replication]
#   k              - Number of DataNodes per physical node
#   image_size_gb  - Size of each loopback image (default: 30)
#   dn_heap_mb     - DataNode JVM heap in MB (default: auto)
#   replication    - HDFS replication factor (default: 3)
#
# NOTE: Requires sudo on all nodes for loopback mount operations.
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

K=${1:?Usage: start-multi-dn-cluster.sh <k> [image_size_gb] [dn_heap_mb] [replication]}
IMAGE_SIZE_GB=${2:-30}
DN_HEAP_MB=${3:-0}   # 0 = auto-calculate in config generator
REPLICATION=${4:-3}

# Cluster nodes
MASTER_NODE="tapuz14"
ALL_NODES=("tapuz14" "tapuz10" "tapuz11" "tapuz12" "tapuz13")
WORKER_NODES=("tapuz10" "tapuz11" "tapuz12" "tapuz13")
MASTER_HAS_DN=${MASTER_HAS_DN:-1}

DATANODE_NODES=()
if [[ "$MASTER_HAS_DN" == "0" ]]; then
    DATANODE_NODES=("${WORKER_NODES[@]}")
else
    DATANODE_NODES=("${ALL_NODES[@]}")
fi

# Paths
HADOOP_HOME="/home/mostufa.j/hadoop"
HADOOP_DATA_DIR="/home/mostufa.j/hadoop_data"
CONFIG_BASE="/tmp/hadoop_multi_dn"
IMAGE_DIR="/data/loop_images"
MOUNT_BASE="/mnt/hdfs_loop"
NAMENODE_PORT=9000

EXPECTED_DATANODES=$(( ${#DATANODE_NODES[@]} * K ))

ensure_fd_limit() {
    local target=8192
    local hard_limit
    hard_limit=$(ulimit -Hn 2>/dev/null || echo 1024)
    local new_limit=$target
    if (( hard_limit < target )); then
        new_limit=$hard_limit
    fi
    ulimit -n "$new_limit" 2>/dev/null || true
}

ensure_fd_limit

get_node_fd_hard_limit() {
    local node=$1
    local raw
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        raw=$(ulimit -Hn 2>/dev/null || echo "1024")
    else
        raw=$(ssh "$node" "ulimit -Hn 2>/dev/null || echo 1024" 2>/dev/null || echo "1024")
    fi
    if [[ "$raw" == "unlimited" ]]; then
        echo 1048576
    elif [[ "$raw" =~ ^[0-9]+$ ]]; then
        echo "$raw"
    else
        echo 1024
    fi
}

echo "============================================================"
echo "Starting Multi-DataNode Cluster"
echo "============================================================"
echo "  k = $K DataNodes per physical node"
echo "  Physical nodes: ${ALL_NODES[*]}"
echo "  DataNode hosts: ${DATANODE_NODES[*]}"
echo "  Expected total DataNodes: $EXPECTED_DATANODES"
echo "  Replication factor: $REPLICATION"
echo "  Image size: ${IMAGE_SIZE_GB}GB per loopback FS"
echo "  DN heap: ${DN_HEAP_MB}MB (0=auto)"
echo "  Open files limit: $(ulimit -Sn 2>/dev/null || echo '?') (soft), $(ulimit -Hn 2>/dev/null || echo '?') (hard)"
echo "============================================================"
echo ""

echo "Node hard open-files limits (ulimit -Hn):"
for node in "${ALL_NODES[@]}"; do
    fd_hard=$(get_node_fd_hard_limit "$node")
    echo "  $node: $fd_hard"
    if (( fd_hard < 4096 )); then
        echo "  WARNING: $node hard open-files limit is low (<4096). High k may fail."
    fi
done
echo ""

# ============================================================================
# STEP 1: Set up loopback filesystems on ALL nodes
# ============================================================================
echo "=== STEP 1: Setting up loopback filesystems ==="

for node in "${DATANODE_NODES[@]}"; do
    echo ""
    echo "--- Node: $node ---"
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        # Local node
        bash "$SCRIPT_DIR/setup-loopback-fs.sh" "$K" "$IMAGE_SIZE_GB" "$IMAGE_DIR" "$MOUNT_BASE"
    else
        # Remote node - copy script and run
        scp -q "$SCRIPT_DIR/setup-loopback-fs.sh" "$node:/tmp/setup-loopback-fs.sh"
        ssh "$node" "bash /tmp/setup-loopback-fs.sh $K $IMAGE_SIZE_GB $IMAGE_DIR $MOUNT_BASE"
    fi
done

echo ""
echo "All loopback filesystems ready on DataNode hosts."

# ============================================================================
# STEP 2: Generate per-DN config directories on ALL nodes
# ============================================================================
echo ""
echo "=== STEP 2: Generating DataNode configurations ==="

for node in "${ALL_NODES[@]}"; do
    echo ""
    echo "--- Node: $node ---"
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        bash "$SCRIPT_DIR/generate-multi-dn-configs.sh" "$K" "$CONFIG_BASE" "$MOUNT_BASE" "$DN_HEAP_MB" "$REPLICATION"
    else
        scp -q "$SCRIPT_DIR/generate-multi-dn-configs.sh" "$node:/tmp/generate-multi-dn-configs.sh"
        ssh "$node" "HADOOP_HOME=$HADOOP_HOME bash /tmp/generate-multi-dn-configs.sh $K $CONFIG_BASE $MOUNT_BASE $DN_HEAP_MB $REPLICATION"
    fi
done

# ============================================================================
# STEP 3: Stop any existing Hadoop processes
# ============================================================================
echo ""
echo "=== STEP 3: Stopping any existing Hadoop processes ==="

# Stop YARN and HDFS gracefully first
stop-yarn.sh 2>/dev/null || true
stop-dfs.sh 2>/dev/null || true
sleep 2

# Kill any remaining DataNode/NameNode processes on all nodes
for node in "${ALL_NODES[@]}"; do
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        pkill -f "org.apache.hadoop.hdfs.server.datanode.DataNode" 2>/dev/null || true
        pkill -f "org.apache.hadoop.hdfs.server.namenode.NameNode" 2>/dev/null || true
    else
        ssh "$node" "pkill -f 'org.apache.hadoop.hdfs.server.datanode.DataNode'" 2>/dev/null || true
        ssh "$node" "pkill -f 'org.apache.hadoop.hdfs.server.namenode.NameNode'" 2>/dev/null || true
    fi
done
sleep 3

# ============================================================================
# STEP 3.5: Clean DataNode storage directories on ALL nodes (both regular and loopback)
# ============================================================================
echo ""
echo "=== STEP 3.5: Cleaning DataNode storage directories ==="

for node in "${ALL_NODES[@]}"; do
    clean_loopback=1
    if [[ "$MASTER_HAS_DN" == "0" && "$node" == "$MASTER_NODE" ]]; then
        clean_loopback=0
    fi

    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        # Clean standard DataNode storage
        rm -rf "$HADOOP_DATA_DIR/datanode/current" 2>/dev/null || true
        if (( clean_loopback == 1 )); then
            # Clean loopback filesystem storage directories.
            # IMPORTANT: remove full hdfs_data dir (not only current/) to clear
            # stale VERSION files and clusterID from previous NameNode formats.
            for ((i=1; i<=K; i++)); do
                rm -rf "$MOUNT_BASE/dn${i}/hdfs_data" 2>/dev/null || true
                mkdir -p "$MOUNT_BASE/dn${i}/hdfs_data"
            done
        fi
    else
        ssh "$node" "bash -s" -- "$HADOOP_DATA_DIR" "$MOUNT_BASE" "$K" "$clean_loopback" <<'REMOTE_CLEAN_DN'
set -euo pipefail

hadoop_data_dir="$1"
mount_base="$2"
k="$3"
clean_loopback="$4"

rm -rf "$hadoop_data_dir/datanode/current" 2>/dev/null || true
if [ "$clean_loopback" = "1" ]; then
    for ((j=1; j<=k; j++)); do
        rm -rf "$mount_base/dn${j}/hdfs_data" 2>/dev/null || true
        mkdir -p "$mount_base/dn${j}/hdfs_data"
    done
fi
REMOTE_CLEAN_DN
    fi
done
echo "DataNode storage cleaned on all nodes (loopback cleaned on DataNode hosts only)."

# ============================================================================
# STEP 4: Clean NameNode data and format
# ============================================================================
echo ""
echo "=== STEP 4: Formatting NameNode ==="

rm -rf "$HADOOP_DATA_DIR/namenode/current" 2>/dev/null || true

# Use DN1's config for NameNode (it has the right core-site.xml + hdfs-site.xml)
export HADOOP_CONF_DIR="$CONFIG_BASE/dn1"
hdfs namenode -format -force -nonInteractive 2>/dev/null
echo "NameNode formatted."

# ============================================================================
# STEP 5: Start NameNode
# ============================================================================
echo ""
echo "=== STEP 5: Starting NameNode on $MASTER_NODE ==="

# Start NameNode using DN1's config (has the correct NN settings)
export HADOOP_CONF_DIR="$CONFIG_BASE/dn1"
hdfs --daemon start namenode
echo "NameNode started."

echo "Waiting for NameNode RPC (:${NAMENODE_PORT}) to become ready..."
NN_WAIT_MAX=60
NN_WAIT_ELAPSED=0
NN_WAIT_STEP=2
while (( NN_WAIT_ELAPSED < NN_WAIT_MAX )); do
    if hdfs dfsadmin -report >/dev/null 2>&1; then
        echo "NameNode RPC is ready."
        break
    fi
    sleep "$NN_WAIT_STEP"
    NN_WAIT_ELAPSED=$((NN_WAIT_ELAPSED + NN_WAIT_STEP))
done
if (( NN_WAIT_ELAPSED >= NN_WAIT_MAX )); then
    echo "ERROR: NameNode did not become ready within ${NN_WAIT_MAX}s"
    exit 1
fi

# ============================================================================
# STEP 6: Start k DataNodes on DataNode hosts
# ============================================================================
echo ""
echo "=== STEP 6: Starting $K DataNodes on DataNode hosts ==="

for node in "${DATANODE_NODES[@]}"; do
    echo ""
    echo "--- Node: $node ---"
    for ((i=1; i<=K; i++)); do
        DN_CONF_DIR="$CONFIG_BASE/dn${i}"
        DN_LOG_DIR="/tmp/hadoop_dn_logs/dn${i}"
        DN_PID_DIR="/tmp/hadoop_dn_pids/dn${i}"

        if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
            # Create data directory on the loopback FS
            mkdir -p "$MOUNT_BASE/dn${i}/hdfs_data"
            mkdir -p "$DN_LOG_DIR"
            mkdir -p "$DN_PID_DIR"

            # Raise soft open-files limit if possible (important for many DNs).
            ulimit -n "$(ulimit -Hn 2>/dev/null || echo 1024)" 2>/dev/null || true

            # Source the per-DN env override (sets HADOOP_DATANODE_OPTS with heap)
            source "$DN_CONF_DIR/dn-env-override.sh" 2>/dev/null || true

            # Start DataNode with unique PID and log directories
            HADOOP_CONF_DIR="$DN_CONF_DIR" \
            HADOOP_LOG_DIR="$DN_LOG_DIR" \
            HADOOP_PID_DIR="$DN_PID_DIR" \
                hdfs --daemon start datanode
            sleep 1
            LOCAL_DN_COUNT=$(pgrep -fc "org.apache.hadoop.hdfs.server.datanode.DataNode" || true)
            if (( LOCAL_DN_COUNT < i )); then
                echo "  ERROR: DN #$i failed to stay up on $node (running=$LOCAL_DN_COUNT)"
                exit 1
            fi
            echo "  Started DN #$i (local)"
        else
            if ! ssh "$node" "bash -s" -- "$MOUNT_BASE" "$i" "$DN_LOG_DIR" "$DN_PID_DIR" "$DN_CONF_DIR" "$HADOOP_HOME" "$node" <<'REMOTE_DN_START'
set -euo pipefail

mount_base="$1"
idx="$2"
dn_log_dir="$3"
dn_pid_dir="$4"
dn_conf_dir="$5"
hadoop_home="$6"
node_name="$7"

ulimit -n "$(ulimit -Hn 2>/dev/null || echo 1024)" 2>/dev/null || true

mkdir -p "$mount_base/dn${idx}/hdfs_data"
mkdir -p "$dn_log_dir"
mkdir -p "$dn_pid_dir"

if [ -f "$dn_conf_dir/dn-env-override.sh" ]; then
    . "$dn_conf_dir/dn-env-override.sh"
fi

HADOOP_CONF_DIR="$dn_conf_dir" \
HADOOP_LOG_DIR="$dn_log_dir" \
HADOOP_PID_DIR="$dn_pid_dir" \
    "$hadoop_home/bin/hdfs" --daemon start datanode

sleep 1
count=$(pgrep -fc "org.apache.hadoop.hdfs.server.datanode.DataNode" || true)
if [ "$count" -lt "$idx" ]; then
    echo "ERROR: DN #$idx failed to stay up on $node_name (running=$count)" >&2
    exit 1
fi
REMOTE_DN_START
            then
                echo "  ERROR: Failed to start DN #$i on $node"
                exit 1
            fi
            echo "  Started DN #$i on $node"
        fi
    done
done

# ============================================================================
# STEP 7: Start YARN (ResourceManager + NodeManagers)
# ============================================================================
echo ""
echo "=== STEP 7: Starting YARN ==="

# Restore standard config for YARN (unset HADOOP_CONF_DIR to use /etc/hadoop)
unset HADOOP_CONF_DIR
start-yarn.sh 2>/dev/null || true
mapred --daemon start historyserver 2>/dev/null || true

echo "Waiting for JobHistory RPC (:10020) to become ready..."
JHS_WAIT_MAX=60
JHS_WAIT_ELAPSED=0
JHS_WAIT_STEP=2
while (( JHS_WAIT_ELAPSED < JHS_WAIT_MAX )); do
    if jps -l 2>/dev/null | grep -q "org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer"; then
        echo "JobHistory RPC is ready."
        break
    fi
    sleep "$JHS_WAIT_STEP"
    JHS_WAIT_ELAPSED=$((JHS_WAIT_ELAPSED + JHS_WAIT_STEP))
done
if (( JHS_WAIT_ELAPSED >= JHS_WAIT_MAX )); then
    echo "WARNING: JobHistoryServer did not appear within ${JHS_WAIT_MAX}s; jobs may still finish but status polling can fail."
fi
echo "YARN started."

# ============================================================================
# STEP 8: Wait for DataNodes to register
# ============================================================================
echo ""
echo "=== STEP 8: Waiting for DataNodes to register ==="

MAX_WAIT=120  # seconds
ELAPSED=0
INTERVAL=5

while (( ELAPSED < MAX_WAIT )); do
    LIVE=$(hdfs dfsadmin -report 2>/dev/null | grep -i "Live datanodes" | grep -o '[0-9]*' || echo "0")
    echo "  Live DataNodes: $LIVE / $EXPECTED_DATANODES  (waited ${ELAPSED}s)"

    if (( LIVE >= EXPECTED_DATANODES )); then
        echo ""
        echo "All $EXPECTED_DATANODES DataNodes are live!"
        break
    fi

    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

FINAL_LIVE=$(hdfs dfsadmin -report 2>/dev/null | grep -i "Live datanodes" | grep -o '[0-9]*' || echo "0")
if (( FINAL_LIVE < EXPECTED_DATANODES )); then
    echo ""
    echo "WARNING: Only $FINAL_LIVE / $EXPECTED_DATANODES DataNodes registered after ${MAX_WAIT}s"
    echo "Collecting quick diagnostics (JPS + last DataNode log lines per node)..."
    for node in "${ALL_NODES[@]}"; do
        echo "--- Diagnostics: $node ---"
        if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
            jps -lm 2>/dev/null | grep -E "DataNode|NameNode|ResourceManager|NodeManager" || true
            tail -n 25 /tmp/hadoop_dn_logs/dn*/hadoop-*-datanode-*.log 2>/dev/null || true
        else
            ssh "$node" "jps -lm 2>/dev/null | grep -E 'DataNode|NameNode|ResourceManager|NodeManager' || true" 2>/dev/null || true
            ssh "$node" "tail -n 25 /tmp/hadoop_dn_logs/dn*/hadoop-*-datanode-*.log 2>/dev/null || true" 2>/dev/null || true
        fi
    done
    echo "ERROR: DataNode registration is incomplete; stopping startup to avoid invalid experiment results."
    exit 1
fi

echo ""
echo "============================================================"
echo "Multi-DataNode Cluster Running"
echo "  NameNode:   $MASTER_NODE:$NAMENODE_PORT"
echo "  DataNodes:  $FINAL_LIVE live (k=$K per DataNode host, ${#DATANODE_NODES[@]} hosts)"
echo "  HDFS Web UI: http://$MASTER_NODE:9870"
echo "============================================================"
