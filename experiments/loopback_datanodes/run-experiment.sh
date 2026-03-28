#!/bin/bash
################################################################################
# SCRIPT: run-experiment.sh
# DESCRIPTION: Main experiment runner for the loopback DataNodes experiment.
#              Tests WordCount (20GB, 128MB blocks, replication=3) performance
#              as the number of DataNodes per physical node (k) scales.
#
#              Each DataNode runs on its own loopback filesystem to simulate
#              independent storage devices.
#
#              Also monitors NameNode heap memory usage via JMX to study how
#              metadata management overhead scales with the number of DataNodes.
#
# USAGE: bash run-experiment.sh [K_REPS]
#   K_REPS - Repetitions per configuration (default: 3)
#
# OUTPUT: results/loopback-datanodes/run_<timestamp>/ with CSVs, memory data, metadata
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."
WORDCOUNT_DIR="$SCRIPT_DIR/../wordcount"

RESULTS_BASE="${RESULTS_BASE:-$PROJECT_ROOT/results/loopback-datanodes}"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_BASE/run_$TIMESTAMP"
mkdir -p "$RUN_DIR"

# ============================================================================
# PARAMETERS
# ============================================================================
K_REPS=${1:-3}                      # Repetitions per config

INPUT_SIZE_GB=1                    # 1GB input
BLOCK_SIZE=$((128 * 1024 * 1024))   # 128MB in bytes
BLOCK_SIZE_HUMAN="128MB"
REPLICATION=3                       # Standard HDFS replication

# Loopback sizing policy:
# Use a per-node loopback budget and split it across k DataNodes.
# This keeps runs realistic even for small INPUT_SIZE_GB smoke tests.
LOOPBACK_BUDGET_PER_NODE_GB=40
MIN_IMAGE_SIZE_GB=2

# k values to test: number of DataNodes per physical node
# Resource-safe values for 8GB-RAM / 4-core nodes.
#
# Memory budget per node:
#   OS + system overhead:  ~1.5 GB
#   NameNode (master only): ~1.0 GB heap
#   YARN RM (master only):  ~0.5 GB
#   YARN NM (all nodes):    ~0.5 GB
#   ────────────────────────────────────
#   Available for DataNodes on master: ~4.5 GB
#   Available for DataNodes on workers: ~5.5 GB
#
# DN heap is auto-calculated:
#   k=1  → 1024MB/DN (1.1GB total)
#   k=2  →  512MB/DN (1.2GB total)
#   k=4  →  384MB/DN (1.9GB total)
#   k=8  →  256MB/DN (2.8GB total)
#   k=16 →  200MB/DN (4.8GB total) ← tight on master, safe on workers
#
# Disk budget per node (20GB × replication 3 = 60GB total HDFS data):
#   k=1  → 12.0 GB/DN → 20GB images × 1 = 20 GB disk
#   k=2  →  6.0 GB/DN → 10GB images × 2 = 20 GB disk
#   k=4  →  3.0 GB/DN →  5GB images × 4 = 20 GB disk
#   k=8  →  1.5 GB/DN →  3GB images × 8 = 24 GB disk
#   k=16 →  0.75GB/DN →  2GB images ×16 = 32 GB disk
K_VALUES=(1 2 4 8 16)
ACTIVE_K_VALUES=()
SKIPPED_K_VALUES=()

# Conservative FD planning (per node)
FD_RESERVED=256   # shell + Hadoop daemons + OS/user processes
FD_PER_DN=64      # estimated open files/sockets per DataNode process

# Cluster info
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

NUM_PHYSICAL_NODES=${#ALL_NODES[@]}
NUM_DATANODE_HOSTS=${#DATANODE_NODES[@]}

HADOOP_HOME="/home/mostufa.j/hadoop"
CONFIG_BASE="/tmp/hadoop_multi_dn"

# NameNode JMX endpoint for memory monitoring
NAMENODE_HOST="$MASTER_NODE"
NAMENODE_HTTP_PORT=9870
JMX_URL="http://${NAMENODE_HOST}:${NAMENODE_HTTP_PORT}/jmx"

LOG_FILE="$RUN_DIR/experiment.log"
CSV_FILE="$RUN_DIR/results.csv"
NN_MEMORY_DIR="$RUN_DIR/namenode_memory"
mkdir -p "$NN_MEMORY_DIR"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

log() {
    echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

compute_avg() {
    local -n arr=$1
    local sum=0
    local n=${#arr[@]}
    for v in "${arr[@]}"; do
        sum=$(echo "$sum + $v" | bc)
    done
    echo "scale=2; $sum / $n" | bc
}

compute_stddev() {
    local -n arr=$1
    local avg=$2
    local n=${#arr[@]}
    if (( n < 2 )); then
        echo "0"
        return
    fi
    local sum_sq=0
    for v in "${arr[@]}"; do
        local diff=$(echo "$v - $avg" | bc)
        sum_sq=$(echo "$sum_sq + ($diff * $diff)" | bc)
    done
    echo "scale=2; sqrt($sum_sq / ($n - 1))" | bc
}

# ── Auto-calculate image size for each k value ──
# Primary policy: split LOOPBACK_BUDGET_PER_NODE_GB across k DataNodes.
# Safety floor: ensure size is still large enough for expected data per DN.
calc_image_size_gb() {
    local k=$1
    # Budget-based sizing (what we want to use in practice).
    local budget_based_gb=$(( LOOPBACK_BUDGET_PER_NODE_GB / k ))

    # Data-based minimum with integer ceil + 2.5x margin.
    local total_dns=$(( NUM_DATANODE_HOSTS * k ))
    local data_per_dn_gb=$(( (INPUT_SIZE_GB * REPLICATION + total_dns - 1) / total_dns ))
    local data_based_min_gb=$(( (data_per_dn_gb * 25 + 9) / 10 ))

    local image_gb=$budget_based_gb
    if (( image_gb < data_based_min_gb )); then
        image_gb=$data_based_min_gb
    fi
    if (( image_gb < MIN_IMAGE_SIZE_GB )); then
        image_gb=$MIN_IMAGE_SIZE_GB
    fi
    echo "$image_gb"
}

# ── Query NameNode JMX for memory + metadata stats ──
# Returns: heap_used_mb heap_max_mb block_count file_count live_datanodes
query_namenode_jmx() {
    local jmx_data
    jmx_data=$(curl -sL --connect-timeout 5 --max-time 10 "$JMX_URL" 2>/dev/null || echo "{}")

    if command -v python3 &>/dev/null; then
        echo "$jmx_data" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    heap_used = heap_max = 0
    block_count = file_count = live_dns = 0
    for bean in data.get('beans', []):
        name = bean.get('name', '')
        if name == 'java.lang:type=Memory':
            heap = bean.get('HeapMemoryUsage', {})
            heap_used = heap.get('used', 0) // (1024*1024)
            heap_max = heap.get('max', 0) // (1024*1024)
        elif 'FSNamesystem' in name and 'State' not in name:
            block_count = bean.get('BlocksTotal', 0)
            file_count = bean.get('FilesTotal', 0)
            live_dns = bean.get('NumLiveDataNodes', 0)
    print(f'{heap_used} {heap_max} {block_count} {file_count} {live_dns}')
except:
    print('0 0 0 0 0')
" 2>/dev/null || echo "0 0 0 0 0"
    else
        echo "0 0 0 0 0"
    fi
}

# ── Check loopback filesystem health and free space ──
# Ensures loopback filesystems have adequate free space before data upload.
# Returns 0 if healthy, 1 if problems found.
check_loopback_health() {
    local k=$1
    local image_size_gb=$2
    # Dynamic free-space floor:
    # - keep previous 5GB floor for large images
    # - allow small-image smoke tests (e.g., 2GB image)
    local min_free_gb=$(( image_size_gb - 1 ))
    if (( min_free_gb > 5 )); then
        min_free_gb=5
    fi
    if (( min_free_gb < 1 )); then
        min_free_gb=1
    fi
    local all_healthy=0

    log "  Checking loopback filesystem health (min free: ${min_free_gb}GB)..."

    for node in "${DATANODE_NODES[@]}"; do
        for ((i=1; i<=k; i++)); do
            local mount="/mnt/hdfs_loop/dn${i}"
            
            if [[ "$node" == "$MASTER_NODE" || "$node" == "$(hostname)" ]]; then
                # Local check
                if ! df "$mount" &>/dev/null; then
                    log "    ERROR: Loopback mount $mount not accessible on $node"
                    all_healthy=1
                    continue
                fi
                local free_gb
                free_gb=$(df -BG --output=avail "$mount" 2>/dev/null | tail -n 1 | tr -dc '0-9')
                if [[ -z "$free_gb" ]] || ! [[ "$free_gb" =~ ^[0-9]+$ ]]; then
                    log "    ERROR: Could not parse free space for $node:$mount"
                    all_healthy=1
                    continue
                fi
                log "    $node:$mount  free: ${free_gb}GB"
                if (( free_gb < min_free_gb )); then
                    log "    ERROR: Not enough free space on $node:$mount (${free_gb}GB < ${min_free_gb}GB)"
                    all_healthy=1
                fi
            else
                # Remote check
                if ! ssh "$node" "df $mount" &>/dev/null 2>&1; then
                    log "    ERROR: Loopback mount $mount not accessible on $node"
                    all_healthy=1
                    continue
                fi
                local free_gb
                free_gb=$(ssh "$node" "df -BG --output=avail $mount 2>/dev/null | tail -n 1 | tr -dc '0-9'" 2>/dev/null || true)
                if [[ -z "$free_gb" ]] || ! [[ "$free_gb" =~ ^[0-9]+$ ]]; then
                    log "    ERROR: Could not parse free space for $node:$mount"
                    all_healthy=1
                    continue
                fi
                log "    $node:$mount  free: ${free_gb}GB"
                if (( free_gb < min_free_gb )); then
                    log "    ERROR: Not enough free space on $node:$mount (${free_gb}GB < ${min_free_gb}GB)"
                    all_healthy=1
                fi
            fi
        done
    done

    return $all_healthy
}

# ── Start background NameNode memory monitor ──
# Writes samples to a CSV file every INTERVAL seconds.
# Usage: start_nn_monitor <output_csv> [interval_seconds]
# Sets NN_MONITOR_PID global variable.
NN_MONITOR_PID=""
start_nn_monitor() {
    local output_csv=$1
    local interval=${2:-5}

    echo "timestamp,heap_used_mb,heap_max_mb,block_count,file_count,live_datanodes" > "$output_csv"

    (
        while true; do
            local ts=$(date +"%Y-%m-%d %H:%M:%S")
            local stats=$(query_namenode_jmx)
            local heap_used=$(echo "$stats" | awk '{print $1}')
            local heap_max=$(echo "$stats" | awk '{print $2}')
            local blocks=$(echo "$stats" | awk '{print $3}')
            local files=$(echo "$stats" | awk '{print $4}')
            local live=$(echo "$stats" | awk '{print $5}')
            echo "$ts,$heap_used,$heap_max,$blocks,$files,$live" >> "$output_csv"
            sleep "$interval"
        done
    ) &
    NN_MONITOR_PID=$!
    log "  NameNode memory monitor started (PID=$NN_MONITOR_PID, interval=${interval}s)"
}

stop_nn_monitor() {
    if [[ -n "$NN_MONITOR_PID" ]] && kill -0 "$NN_MONITOR_PID" 2>/dev/null; then
        kill "$NN_MONITOR_PID" 2>/dev/null || true
        wait "$NN_MONITOR_PID" 2>/dev/null || true
        log "  NameNode memory monitor stopped."
    fi
    NN_MONITOR_PID=""
}

# Probe hard open-files limit (ulimit -Hn) on a node.
# Returns numeric value; if unlimited/unknown, returns 1048576.
get_node_fd_hard_limit() {
    local node=$1
    local raw

    if [[ "$node" == "$MASTER_NODE" || "$node" == "$(hostname)" ]]; then
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

# Filter K_VALUES using the minimum hard FD limit across nodes.
# Safe k cap per node: floor((fd_hard - FD_RESERVED) / FD_PER_DN)
apply_fd_based_k_filter() {
    local min_k_cap=99999

    log "Preflight: checking DataNode-host hard open-files limits (ulimit -Hn)..."
    for node in "${DATANODE_NODES[@]}"; do
        local fd_hard
        fd_hard=$(get_node_fd_hard_limit "$node")

        local k_cap=1
        if (( fd_hard > FD_RESERVED )); then
            k_cap=$(( (fd_hard - FD_RESERVED) / FD_PER_DN ))
            if (( k_cap < 1 )); then
                k_cap=1
            fi
        fi

        log "  $node: fd_hard=$fd_hard => estimated safe k<=${k_cap}"
        if (( k_cap < min_k_cap )); then
            min_k_cap=$k_cap
        fi
    done

    ACTIVE_K_VALUES=()
    SKIPPED_K_VALUES=()
    for k in "${K_VALUES[@]}"; do
        if (( k <= min_k_cap )); then
            ACTIVE_K_VALUES+=("$k")
        else
            SKIPPED_K_VALUES+=("$k")
        fi
    done

    if (( ${#ACTIVE_K_VALUES[@]} == 0 )); then
        log "ERROR: No safe k values remain after FD-based filtering."
        log "       Raise hard open-files limit (ulimit -Hn) or reduce FD_PER_DN assumption."
        exit 1
    fi

    if (( ${#SKIPPED_K_VALUES[@]} > 0 )); then
        log "WARNING: Skipping k values due to FD constraints: ${SKIPPED_K_VALUES[*]}"
    fi
}

# Extract peak heap from a monitor CSV
get_peak_heap_mb() {
    local csv_file=$1
    if [[ -f "$csv_file" ]] && command -v python3 &>/dev/null; then
        python3 -c "
import csv
peak = 0
with open('$csv_file') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            used = int(row['heap_used_mb'])
            if used > peak:
                peak = used
        except (ValueError, KeyError):
            pass
print(peak)
" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Get average heap from a monitor CSV
get_avg_heap_mb() {
    local csv_file=$1
    if [[ -f "$csv_file" ]] && command -v python3 &>/dev/null; then
        python3 -c "
import csv
values = []
with open('$csv_file') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            values.append(int(row['heap_used_mb']))
        except (ValueError, KeyError):
            pass
print(int(sum(values)/len(values)) if values else 0)
" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# ============================================================================
# CLEANUP TRAP
# ============================================================================
cleanup() {
    echo ""
    log "Caught interrupt, cleaning up..."
    stop_nn_monitor
    pkill -P $$ 2>/dev/null || true
    log "Cleanup complete. Partial results in: $RUN_DIR"
    exit 1
}
trap cleanup SIGINT SIGTERM

# ============================================================================
# MAIN EXPERIMENT
# ============================================================================

echo "============================================================"
echo "Loopback DataNodes Experiment"
echo "============================================================"
apply_fd_based_k_filter
echo "Run ID:           $TIMESTAMP"
echo "Input size:       ${INPUT_SIZE_GB}GB"
echo "Block size:       $BLOCK_SIZE_HUMAN"
echo "Replication:      $REPLICATION"
echo "Master has DN:    $MASTER_HAS_DN"
echo "Loopback budget:  ${LOOPBACK_BUDGET_PER_NODE_GB}GB/node (min ${MIN_IMAGE_SIZE_GB}GB/image)"
echo "Physical nodes:   $NUM_PHYSICAL_NODES (${ALL_NODES[*]})"
echo "DataNode hosts:   $NUM_DATANODE_HOSTS (${DATANODE_NODES[*]})"
echo "k values:         ${ACTIVE_K_VALUES[*]}"
echo "Repetitions (K):  $K_REPS"
echo "Results:          $RUN_DIR"
echo ""
echo "Resource plan per k value:"
for k in "${ACTIVE_K_VALUES[@]}"; do
    img=$(calc_image_size_gb $k)
    total_dns=$(( NUM_DATANODE_HOSTS * k ))
    total_disk=$(( img * k ))
    echo "  k=$k: ${total_dns} DataNodes, ${img}GB images x $k = ${total_disk}GB disk/node"
done
echo "============================================================"
echo ""

# Save metadata (write via Python to guarantee valid JSON)
NODE_NAMES_CSV=$(IFS=,; echo "${ALL_NODES[*]}")
DN_HOST_NAMES_CSV=$(IFS=,; echo "${DATANODE_NODES[*]}")
ACTIVE_K_CSV=$(IFS=,; echo "${ACTIVE_K_VALUES[*]}")
SKIPPED_K_CSV=$(IFS=,; echo "${SKIPPED_K_VALUES[*]:-}")

export RUN_DIR TIMESTAMP INPUT_SIZE_GB BLOCK_SIZE BLOCK_SIZE_HUMAN REPLICATION
export NUM_PHYSICAL_NODES NODE_NAMES_CSV ACTIVE_K_CSV SKIPPED_K_CSV
export NUM_DATANODE_HOSTS DN_HOST_NAMES_CSV MASTER_HAS_DN
export LOOPBACK_BUDGET_PER_NODE_GB MIN_IMAGE_SIZE_GB FD_RESERVED FD_PER_DN K_REPS

python3 - <<'PY' 2>/dev/null || true
import json
import os
from datetime import datetime

def parse_int_list(csv_text: str):
    csv_text = (csv_text or "").strip()
    if not csv_text:
        return []
    values = []
    for token in csv_text.split(','):
        token = token.strip()
        if token:
            values.append(int(token))
    return values

meta = {
    "run_id": os.environ["TIMESTAMP"],
    "input_size_gb": int(os.environ["INPUT_SIZE_GB"]),
    "block_size_bytes": int(os.environ["BLOCK_SIZE"]),
    "block_size_human": os.environ["BLOCK_SIZE_HUMAN"],
    "replication": int(os.environ["REPLICATION"]),
    "physical_nodes": int(os.environ["NUM_PHYSICAL_NODES"]),
    "node_names": [x for x in os.environ.get("NODE_NAMES_CSV", "").split(',') if x],
    "datanode_hosts": int(os.environ["NUM_DATANODE_HOSTS"]),
    "datanode_host_names": [x for x in os.environ.get("DN_HOST_NAMES_CSV", "").split(',') if x],
    "master_has_datanode": os.environ.get("MASTER_HAS_DN", "1") != "0",
    "k_values": parse_int_list(os.environ.get("ACTIVE_K_CSV", "")),
    "skipped_k_values": parse_int_list(os.environ.get("SKIPPED_K_CSV", "")),
    "loopback_budget_per_node_gb": int(os.environ["LOOPBACK_BUDGET_PER_NODE_GB"]),
    "min_image_size_gb": int(os.environ["MIN_IMAGE_SIZE_GB"]),
    "fd_reserved_per_node": int(os.environ["FD_RESERVED"]),
    "fd_per_datanode_assumption": int(os.environ["FD_PER_DN"]),
    "repetitions": int(os.environ["K_REPS"]),
    "start_time": datetime.now().astimezone().isoformat(timespec="seconds"),
}

out_path = os.path.join(os.environ["RUN_DIR"], "metadata.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(meta, f, indent=4)
PY

# Initialize CSV with NameNode memory columns
echo "k_per_node,total_datanodes,avg_runtime_seconds,stddev_runtime,individual_runtimes,live_datanodes,nn_heap_before_mb,nn_heap_peak_mb,nn_heap_avg_mb,nn_block_count" > "$CSV_FILE"

# ============================================================================
# Iterate over k values
# ============================================================================

for k in "${ACTIVE_K_VALUES[@]}"; do
    TOTAL_DNS=$(( NUM_DATANODE_HOSTS * k ))
    IMAGE_SIZE_GB=$(calc_image_size_gb $k)

    log ""
    log "============================================================"
    log "TESTING k=$k  ($k DataNodes per node, $TOTAL_DNS total)"
    log "  Image size: ${IMAGE_SIZE_GB}GB each, replication=$REPLICATION"
    log "============================================================"

    # -- Step 1: Start the multi-DN cluster --
    log "Starting cluster with k=$k..."
    bash "$SCRIPT_DIR/start-multi-dn-cluster.sh" "$k" "$IMAGE_SIZE_GB" "0" "$REPLICATION" 2>&1 | tee -a "$LOG_FILE"

    # Record actual live DataNodes
    export HADOOP_CONF_DIR="$CONFIG_BASE/dn1"
    LIVE_DNS=$(hdfs dfsadmin -report 2>/dev/null | grep -i "Live datanodes" | grep -o '[0-9]*' || echo "0")
    log "Live DataNodes: $LIVE_DNS (expected $TOTAL_DNS)"

    # -- Preflight check: Verify loopback filesystems are healthy --
    if ! check_loopback_health "$k" "$IMAGE_SIZE_GB"; then
        log "ERROR: Loopback filesystem health check failed. Skipping k=$k."
        log "Stopping cluster before moving to next k..."
        bash "$SCRIPT_DIR/stop-multi-dn-cluster.sh" 2>&1 | tee -a "$LOG_FILE"
        continue
    fi
    log "Generating ${INPUT_SIZE_GB}GB input..."

    hdfs dfs -mkdir -p /user/$USER/wordcount/input 2>/dev/null || true
    bash "$WORDCOUNT_DIR/generate-input.sh" "$((INPUT_SIZE_GB * 1024))" "$BLOCK_SIZE" 2>&1 | tee -a "$LOG_FILE"

    log "Input uploaded. HDFS status:"
    hdfs dfs -ls /user/$USER/wordcount/input 2>&1 | tee -a "$LOG_FILE"

    # -- Step 2b: Snapshot NameNode memory BEFORE WordCount --
    log "Querying NameNode memory (before WordCount)..."
    sleep 5  # Let NN stabilize after upload
    NN_BEFORE=$(query_namenode_jmx)
    NN_HEAP_BEFORE=$(echo "$NN_BEFORE" | awk '{print $1}')
    NN_BLOCK_COUNT=$(echo "$NN_BEFORE" | awk '{print $3}')
    log "  NN heap before: ${NN_HEAP_BEFORE}MB, blocks: $NN_BLOCK_COUNT"

    # -- Step 3: Start NameNode memory monitor --
    NN_MONITOR_CSV="$NN_MEMORY_DIR/nn_memory_k${k}.csv"
    start_nn_monitor "$NN_MONITOR_CSV" 5

    # -- Step 4: Run WordCount K_REPS times --
    declare -a runtimes=()

    for ((run_i=1; run_i<=K_REPS; run_i++)); do
        log ""
        log "  Run $run_i/$K_REPS (k=$k)..."

        # Remove output from previous run
        hdfs dfs -rm -r -f /user/$USER/wordcount/output 2>/dev/null || true

        # Time the WordCount job
        START_TIME=$(date +%s.%N)

        hadoop jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-"*.jar \
            wordcount \
            -D mapreduce.jobhistory.address=${MASTER_NODE}:10020 \
            -D mapreduce.jobhistory.webapp.address=${MASTER_NODE}:19888 \
            /user/$USER/wordcount/input \
            /user/$USER/wordcount/output 2>&1 | tee -a "$LOG_FILE"

        END_TIME=$(date +%s.%N)
        RUNTIME=$(echo "scale=2; $END_TIME - $START_TIME" | bc)

        log "  Runtime: ${RUNTIME}s"
        runtimes+=("$RUNTIME")
    done

    # -- Step 5: Stop NN monitor and collect stats --
    stop_nn_monitor
    NN_HEAP_PEAK=$(get_peak_heap_mb "$NN_MONITOR_CSV")
    NN_HEAP_AVG=$(get_avg_heap_mb "$NN_MONITOR_CSV")
    log "  NN heap peak during WordCount: ${NN_HEAP_PEAK}MB"
    log "  NN heap avg during WordCount:  ${NN_HEAP_AVG}MB"

    # -- Step 6: Compute timing stats --
    avg=$(compute_avg runtimes)
    stddev=$(compute_stddev runtimes "$avg")
    individual=$(IFS=";"; echo "${runtimes[*]}")

    log ""
    log "  k=$k  Average: ${avg}s  StdDev: ${stddev}s  Runs: $individual"
    log "  k=$k  NN: heap_before=${NN_HEAP_BEFORE}MB peak=${NN_HEAP_PEAK}MB avg=${NN_HEAP_AVG}MB blocks=$NN_BLOCK_COUNT"

    # Record to CSV (includes NameNode memory columns)
    echo "$k,$TOTAL_DNS,$avg,$stddev,$individual,$LIVE_DNS,$NN_HEAP_BEFORE,$NN_HEAP_PEAK,$NN_HEAP_AVG,$NN_BLOCK_COUNT" >> "$CSV_FILE"

    unset runtimes

    # -- Step 7: Clean up HDFS data --
    log "Cleaning HDFS data..."
    hdfs dfs -rm -r -f /user/$USER/wordcount 2>/dev/null || true

    # -- Step 8: Stop the cluster and tear down loopback FSes --
    log "Stopping cluster..."
    bash "$SCRIPT_DIR/stop-multi-dn-cluster.sh" 2>&1 | tee -a "$LOG_FILE"

    log "k=$k complete."
    log ""
done

# ============================================================================
# RESTORE NORMAL CLUSTER
# ============================================================================
log ""
log "============================================================"
log "Restoring normal single-DataNode cluster..."
log "============================================================"

# Unset our custom config dir so standard scripts use the default
unset HADOOP_CONF_DIR

# Format and start the standard cluster
rm -rf /home/mostufa.j/hadoop_data/namenode/current 2>/dev/null || true
rm -rf /home/mostufa.j/hadoop_data/datanode/current 2>/dev/null || true
for node in "${ALL_NODES[@]}"; do
    if [[ "$node" != "$(hostname)" && "$node" != "$MASTER_NODE" ]]; then
        ssh "$node" "rm -rf /home/mostufa.j/hadoop_data/datanode/current" 2>/dev/null || true
    fi
done

hdfs namenode -format -force -nonInteractive > /dev/null 2>&1
start-dfs.sh > /dev/null 2>&1
start-yarn.sh > /dev/null 2>&1
sleep 10

log "Normal cluster restored."

# ============================================================================
# SUMMARY
# ============================================================================
echo ""
echo "============================================================"
echo "Experiment Complete!"
echo "============================================================"
echo ""
echo "Results saved to: $RUN_DIR"
echo ""
echo "Files:"
echo "  - results.csv                     : Main results (runtime + NN memory)"
echo "  - metadata.json                   : Experiment configuration"
echo "  - experiment.log                  : Detailed log"
echo "  - namenode_memory/nn_memory_k*.csv : Per-k NameNode memory time series"
echo ""
echo "CSV preview:"
column -t -s, "$CSV_FILE" 2>/dev/null || cat "$CSV_FILE"
echo ""
echo "Generate plots with:"
echo "  python3 $SCRIPT_DIR/plot-results.py $RUN_DIR"
echo "============================================================"

# Create symlink to latest run
ln -sfn "$RUN_DIR" "$RESULTS_BASE/latest"

# Update metadata with end time
python3 -c "
import json, sys
with open('$RUN_DIR/metadata.json', 'r') as f:
    meta = json.load(f)
meta['end_time'] = '$(date -Iseconds)'
with open('$RUN_DIR/metadata.json', 'w') as f:
    json.dump(meta, f, indent=4)
" 2>/dev/null || true
