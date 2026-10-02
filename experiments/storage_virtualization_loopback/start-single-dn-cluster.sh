#!/bin/bash
################################################################################
# SCRIPT: start-single-dn-cluster.sh
# DESCRIPTION: Starts a Hadoop cluster with ONE DataNode per physical node,
#              but each DataNode uses k loopback filesystems in its data dir.
#              This tests storage virtualization (k virtual disks per DN).
#
# WORKFLOW:
#   1. Set up k loopback filesystems on all nodes
#   2. Generate single DataNode config with k storage dirs
#   3. Format and start the NameNode
#   4. Start one DataNode per node (using k loopback dirs)
#   5. Start YARN ResourceManager + NodeManagers, verify the YARN capacity
#   6. Wait for all DataNodes to register
#
# USAGE: bash start-single-dn-cluster.sh <k> [image_size_mb] [dn_heap_mb] [replication]
#   k              - Number of loopback storage dirs per DataNode
#   image_size_mb  - Size of each loopback image in MB (default: 30720 = 30GB)
#   dn_heap_mb     - DataNode JVM heap in MB or "auto" (default: DN_HEAP_MB from cluster conf)
#   replication    - HDFS replication factor (default: 3)
#
# YARN pool per node: SLOTS_PER_NODE x CONTAINER_MB (cluster conf, env overrides).
#
# NOTE: Requires sudo on all nodes for loopback mount operations.
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# cluster.conf provides MASTER_NODE, ALL_NODES, WORKER_NODES,
# HADOOP_HOME, HADOOP_DATA_DIR, CONFIG_DIR, IMAGE_DIR, MOUNT_BASE,
# SLOTS_PER_NODE, CONTAINER_MB, DN_HEAP_MB.
source "$SCRIPT_DIR/cluster.conf"

K=${1:?Usage: start-single-dn-cluster.sh <k> [image_size_mb] [dn_heap_mb] [replication]}
IMAGE_SIZE_MB=${2:-30720}
DN_HEAP_MB=${3:-$DN_HEAP_MB}
REPLICATION=${4:-3}

MASTER_HAS_DN=${MASTER_HAS_DN:-0}

DATANODE_NODES=()
if [[ "$MASTER_HAS_DN" == "0" ]]; then
    DATANODE_NODES=("${WORKER_NODES[@]}")
else
    DATANODE_NODES=("${ALL_NODES[@]}")
fi

if [[ "$MASTER_HAS_DN" == "0" ]]; then
    echo "Master is configured as NameNode-only (no DataNode on $MASTER_NODE)."
fi

NAMENODE_PORT=9000

EXPECTED_DATANODES=${#DATANODE_NODES[@]}

echo "============================================================"
echo "Starting Single DataNode per Node with K Storage Dirs"
echo "============================================================"
echo "  k = $K loopback storage dirs per DataNode"
echo "  Physical nodes: ${ALL_NODES[*]}"
echo "  DataNode hosts: ${DATANODE_NODES[*]}"
echo "  Expected DataNodes: $EXPECTED_DATANODES (1 per host)"
echo "  Replication factor: $REPLICATION"
echo "  Image size: ${IMAGE_SIZE_MB}MB per loopback FS"
echo "  DN heap: ${DN_HEAP_MB}MB"
echo "  YARN pool: ${SLOTS_PER_NODE} x ${CONTAINER_MB}MB per node"
echo "  Loop devices: direct I/O=${LOOP_DIRECT_IO}, mkfs=${MKFS_MODE}"
echo "============================================================"
echo ""

# ============================================================================
# STEP 1: Set up k loopback filesystems on ALL DataNode hosts (PARALLEL)
# ============================================================================
echo "=== STEP 1: Setting up $K loopback filesystems per DataNode host (parallel) ==="

# Copy script to all remote nodes first
for node in "${DATANODE_NODES[@]}"; do
    if [[ "$node" != "$(hostname)" && "$node" != "$MASTER_NODE" ]]; then
        scp -q "$SCRIPT_DIR/setup-loopback-fs.sh" "$node:/tmp/setup-loopback-fs.sh" &
    fi
done
wait

# Run setup in parallel on all nodes
declare -A SETUP_PIDS
for node in "${DATANODE_NODES[@]}"; do
    echo "--- Starting setup on $node ---"
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        bash "$SCRIPT_DIR/setup-loopback-fs.sh" "$K" "$IMAGE_SIZE_MB" "$IMAGE_DIR" "$MOUNT_BASE" "$LOOP_DIRECT_IO" "$MKFS_MODE" > "/tmp/setup_${node}.log" 2>&1 &
        SETUP_PIDS[$node]=$!
    else
        ssh "$node" "bash /tmp/setup-loopback-fs.sh $K $IMAGE_SIZE_MB $IMAGE_DIR $MOUNT_BASE $LOOP_DIRECT_IO $MKFS_MODE" > "/tmp/setup_${node}.log" 2>&1 &
        SETUP_PIDS[$node]=$!
    fi
done

# Wait for all setup processes and check results
SETUP_FAILED=0
for node in "${DATANODE_NODES[@]}"; do
    if wait "${SETUP_PIDS[$node]}"; then
        echo "--- $node: setup complete ---"
    else
        echo "--- $node: setup FAILED ---"
        cat "/tmp/setup_${node}.log"
        SETUP_FAILED=1
    fi
done

if [[ "$SETUP_FAILED" == "1" ]]; then
    echo "ERROR: Loopback setup failed on one or more nodes"
    exit 1
fi

echo ""
echo "All loopback filesystems ready on DataNode hosts."

# ============================================================================
# STEP 2: Generate single DataNode config with k storage dirs (PARALLEL)
# ============================================================================
echo ""
echo "=== STEP 2: Generating DataNode configurations (parallel) ==="

# Copy script to all remote nodes first
for node in "${ALL_NODES[@]}"; do
    if [[ "$node" != "$(hostname)" && "$node" != "$MASTER_NODE" ]]; then
        scp -q "$SCRIPT_DIR/generate-single-dn-configs.sh" "$node:/tmp/generate-single-dn-configs.sh" &
    fi
done
wait

# Run config generation in parallel. Every node gets the same YARN settings.
GEN_ENV="HADOOP_HOME=$HADOOP_HOME MASTER_NODE=$MASTER_NODE SLOTS_PER_NODE=$SLOTS_PER_NODE CONTAINER_MB=$CONTAINER_MB BLOCK_SIZE_MB=$BLOCK_SIZE_MB"
declare -A CONFIG_PIDS
for node in "${ALL_NODES[@]}"; do
    echo "--- Starting config on $node ---"
    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        # shellcheck disable=SC2086  # GEN_ENV is a list of VAR=value words
        env $GEN_ENV bash "$SCRIPT_DIR/generate-single-dn-configs.sh" "$K" "$CONFIG_DIR" "$MOUNT_BASE" "$DN_HEAP_MB" "$REPLICATION" > "/tmp/config_${node}.log" 2>&1 &
        CONFIG_PIDS[$node]=$!
    else
        ssh "$node" "$GEN_ENV bash /tmp/generate-single-dn-configs.sh $K $CONFIG_DIR $MOUNT_BASE $DN_HEAP_MB $REPLICATION" > "/tmp/config_${node}.log" 2>&1 &
        CONFIG_PIDS[$node]=$!
    fi
done

# Wait for all config processes
CONFIG_FAILED=0
for node in "${ALL_NODES[@]}"; do
    if wait "${CONFIG_PIDS[$node]}"; then
        echo "--- $node: config complete ---"
    else
        echo "--- $node: config FAILED ---"
        cat "/tmp/config_${node}.log"
        CONFIG_FAILED=1
    fi
done
if [[ "$CONFIG_FAILED" == "1" ]]; then
    echo "ERROR: Config generation failed on one or more nodes"
    exit 1
fi
grep -hE "NM pool|DN heap|Internal name" "/tmp/config_${MASTER_NODE}.log" 2>/dev/null || true

# NodeManagers run exactly on the DataNode hosts (start-yarn.sh reads this).
printf "%s\n" "${DATANODE_NODES[@]}" > "$CONFIG_DIR/workers"

# ============================================================================
# STEP 3: Stop any existing Hadoop processes
# ============================================================================
echo ""
echo "=== STEP 3: Stopping any existing Hadoop processes ==="

stop-yarn.sh 2>/dev/null || true
stop-dfs.sh 2>/dev/null || true
sleep 2

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
# STEP 3.5: Clean DataNode storage directories on ALL nodes
# ============================================================================
echo ""
echo "=== STEP 3.5: Cleaning DataNode storage directories ==="

for node in "${ALL_NODES[@]}"; do
    clean_loopback=1
    if [[ "$MASTER_HAS_DN" == "0" && "$node" == "$MASTER_NODE" ]]; then
        clean_loopback=0
    fi

    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        sudo rm -rf "$HADOOP_DATA_DIR/datanode/current" 2>/dev/null || true
        if (( clean_loopback == 1 )); then
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

sudo rm -rf "$hadoop_data_dir/datanode/current" 2>/dev/null || true
if [ "$clean_loopback" = "1" ]; then
    for ((j=1; j<=k; j++)); do
        rm -rf "$mount_base/dn${j}/hdfs_data" 2>/dev/null || true
        mkdir -p "$mount_base/dn${j}/hdfs_data"
    done
fi
REMOTE_CLEAN_DN
    fi
done
echo "DataNode storage cleaned on all nodes."

# ============================================================================
# STEP 4: Clean NameNode data and format
# ============================================================================
echo ""
echo "=== STEP 4: Formatting NameNode ==="

sudo rm -rf "$HADOOP_DATA_DIR/namenode/current" 2>/dev/null || true
sudo mkdir -p "$HADOOP_DATA_DIR/namenode"
sudo chmod -R 777 "$HADOOP_DATA_DIR"

export HADOOP_CONF_DIR="$CONFIG_DIR"
hdfs namenode -format -force -nonInteractive 2>/dev/null
echo "NameNode formatted."

# ============================================================================
# STEP 5: Start NameNode
# ============================================================================
echo ""
echo "=== STEP 5: Starting NameNode on $MASTER_NODE ==="

export HADOOP_CONF_DIR="$CONFIG_DIR"
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
# STEP 6: Start one DataNode per DataNode host
# ============================================================================
echo ""
echo "=== STEP 6: Starting ONE DataNode on each DataNode host ==="

for node in "${DATANODE_NODES[@]}"; do
    echo ""
    echo "--- Node: $node ---"
    DN_LOG_DIR="/scratch/tmp/hadoop_dn_logs"
    DN_PID_DIR="/scratch/tmp/hadoop_dn_pids"

    if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
        # Create data directories on all k loopback filesystems
        for ((i=1; i<=K; i++)); do
            mkdir -p "$MOUNT_BASE/dn${i}/hdfs_data"
        done
        sudo mkdir -p "$DN_LOG_DIR" "$DN_PID_DIR"
        sudo chown -R "$(id -u):$(id -g)" "$DN_LOG_DIR" "$DN_PID_DIR"

        # Source the DN env override (sets heap)
        source "$CONFIG_DIR/dn-env-override.sh" 2>/dev/null || true

        # Start DataNode
        HADOOP_CONF_DIR="$CONFIG_DIR" \
        HADOOP_LOG_DIR="$DN_LOG_DIR" \
        HADOOP_PID_DIR="$DN_PID_DIR" \
            hdfs --daemon start datanode
        sleep 1
        LOCAL_DN_COUNT=$(pgrep -fc "org.apache.hadoop.hdfs.server.datanode.DataNode" || true)
        if (( LOCAL_DN_COUNT < 1 )); then
            echo "  ERROR: DataNode failed to start on $node"
            exit 1
        fi
        echo "  Started DataNode (local)"
    else
        if ! ssh "$node" "bash -s" -- "$MOUNT_BASE" "$K" "$DN_LOG_DIR" "$DN_PID_DIR" "$CONFIG_DIR" "$HADOOP_HOME" "$node" <<'REMOTE_DN_START'
set -euo pipefail

mount_base="$1"
k="$2"
dn_log_dir="$3"
dn_pid_dir="$4"
dn_conf_dir="$5"
hadoop_home="$6"
node_name="$7"

for ((j=1; j<=k; j++)); do
    mkdir -p "$mount_base/dn${j}/hdfs_data"
done
sudo mkdir -p "$dn_log_dir" "$dn_pid_dir"
sudo chmod 777 "$dn_log_dir" "$dn_pid_dir"

if [ -f "$dn_conf_dir/dn-env-override.sh" ]; then
    . "$dn_conf_dir/dn-env-override.sh"
fi

HADOOP_CONF_DIR="$dn_conf_dir" \
HADOOP_LOG_DIR="$dn_log_dir" \
HADOOP_PID_DIR="$dn_pid_dir" \
    "$hadoop_home/bin/hdfs" --daemon start datanode

sleep 1
count=$(pgrep -fc "org.apache.hadoop.hdfs.server.datanode.DataNode" || true)
if [ "$count" -lt 1 ]; then
    echo "ERROR: DataNode failed to start on $node_name" >&2
    exit 1
fi
REMOTE_DN_START
        then
            echo "  ERROR: Failed to start DataNode on $node"
            exit 1
        fi
        echo "  Started DataNode on $node"
    fi
done

# ============================================================================
# STEP 7: Start YARN
# ============================================================================
echo ""
echo "=== STEP 7: Starting YARN ==="
# Keep HADOOP_CONF_DIR pointing at the auto-generated $CONFIG_DIR so start-yarn.sh
# reads the yarn-site.xml that has yarn.resourcemanager.hostname=$MASTER_NODE
# and the workers list. Unsetting it makes start-yarn.sh fall back to
# $HADOOP_HOME/etc/hadoop, which may reference stale hostnames -- ResourceManager
# then never binds to the right address and clients get "Connection refused" on :8032.
export HADOOP_CONF_DIR="$CONFIG_DIR"
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
    echo "WARNING: JobHistoryServer did not appear within ${JHS_WAIT_MAX}s"
fi
echo "YARN started."

export HADOOP_CONF_DIR="$CONFIG_DIR"

# ============================================================================
# STEP 8: Wait for DataNodes to register
# ============================================================================
echo ""
echo "=== STEP 8: Waiting for DataNodes to register ==="

MAX_WAIT=$(( K > 64 ? 800 : 200 ))
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
    echo "Collecting quick diagnostics..."
    for node in "${ALL_NODES[@]}"; do
        echo "--- Diagnostics: $node ---"
        if [[ "$node" == "$(hostname)" || "$node" == "$MASTER_NODE" ]]; then
            jps -lm 2>/dev/null | grep -E "DataNode|NameNode" || true
            tail -n 25 /scratch/tmp/hadoop_dn_logs/hadoop-*-datanode-*.log 2>/dev/null || true
        else
            ssh "$node" "jps -lm 2>/dev/null | grep -E 'DataNode|NameNode' || true" 2>/dev/null || true
            ssh "$node" "tail -n 25 /scratch/tmp/hadoop_dn_logs/hadoop-*-datanode-*.log 2>/dev/null || true" 2>/dev/null || true
        fi
    done
    echo "ERROR: DataNode registration incomplete"
    exit 1
fi

# ============================================================================
# STEP 9: Verify the YARN pool
# ============================================================================
# Every DataNode host must run a NodeManager with exactly
# SLOTS_PER_NODE x CONTAINER_MB. In May 2026 runs silently went ahead with one
# usable container per node, or with NodeManagers that did not match the
# DataNode names (0% data-local maps); this check stops that early.
echo ""
echo "=== STEP 9: Verifying YARN capacity ==="

EXPECTED_NM_MB=$(( SLOTS_PER_NODE * CONTAINER_MB ))
EXPECTED_TOTAL_MB=$(( EXPECTED_NM_MB * EXPECTED_DATANODES ))
RM_METRICS_URL="http://${MASTER_NODE}:8088/ws/v1/cluster/metrics"

rm_metrics() {
    curl -s --max-time 5 "$RM_METRICS_URL" 2>/dev/null | python3 -c '
import json, sys
try:
    m = json.load(sys.stdin)["clusterMetrics"]
    print(m.get("activeNodes", 0), m.get("totalMB", 0), m.get("totalVirtualCores", 0))
except Exception:
    print(0, 0, 0)
' 2>/dev/null || echo "0 0 0"
}

YARN_OK=0
ACTIVE_NMS=0; TOTAL_MB=0; TOTAL_VCORES=0
for ((waited=0; waited<=180; waited+=5)); do
    read -r ACTIVE_NMS TOTAL_MB TOTAL_VCORES <<< "$(rm_metrics)"
    if (( ACTIVE_NMS == EXPECTED_DATANODES && TOTAL_MB == EXPECTED_TOTAL_MB )); then
        YARN_OK=1
        break
    fi
    sleep 5
done

echo "  NodeManagers: $ACTIVE_NMS / $EXPECTED_DATANODES active"
echo "  Pool:         ${TOTAL_MB}MB / ${TOTAL_VCORES} vcores (expected ${EXPECTED_TOTAL_MB}MB = $EXPECTED_DATANODES x $SLOTS_PER_NODE x ${CONTAINER_MB}MB)"
if [[ "$YARN_OK" != "1" ]]; then
    echo ""
    echo "ERROR: YARN capacity does not match the configuration."
    yarn node -list -all 2>/dev/null || true
    echo "Check the NodeManager logs under $HADOOP_HOME/logs on the workers."
    exit 1
fi

echo ""
echo "============================================================"
echo "Single DataNode Cluster Running with K Storage Dirs"
echo "  NameNode:   $MASTER_NODE:$NAMENODE_PORT"
echo "  DataNodes:  $FINAL_LIVE live (1 per host, each with $K storage dirs)"
echo "  YARN:       $ACTIVE_NMS NodeManagers x $SLOTS_PER_NODE containers of ${CONTAINER_MB}MB"
echo "  HDFS Web UI: http://$MASTER_NODE:9870"
echo "============================================================"
