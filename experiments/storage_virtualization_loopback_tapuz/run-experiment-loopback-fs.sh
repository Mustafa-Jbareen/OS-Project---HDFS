#!/bin/bash
################################################################################
# SCRIPT: run-experiment-loopback-fs.sh
# DESCRIPTION: Main experiment runner for the storage virtualization (loopback
#              filesystem) scaling experiment. Tests WordCount performance as
#              the number of storage directories (k loopback filesystems) per
#              DataNode scales from 2 to 512 (doubling each time).
#
#              Each DataNode remains a single process but stores data across
#              k loopback-mounted filesystems to test storage virtualization.
#
# USAGE: bash run-experiment-loopback-fs.sh [K_REPS]
#   K_REPS - Repetitions per k value (default: 5)
#
# OUTPUT: results/storage_virtualization_loopback/run_<timestamp>/ with CSVs and plots
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."
WORDCOUNT_DIR="$SCRIPT_DIR/../wordcount"

RESULTS_BASE="${RESULTS_BASE:-$PROJECT_ROOT/results/storage_virtualization_loopback}"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_BASE/run_$TIMESTAMP"
mkdir -p "$RUN_DIR"

# ============================================================================
# PARAMETERS
# ============================================================================
K_REPS=${1:-5}  # Number of repetitions per k value

INPUT_SIZE_GB=1   # 50GB input (Tapuz HDD baseline)
BLOCK_SIZE=$((32 * 1024 * 1024))    # 32MB in bytes (must match dfs.blocksize in generate-single-dn-configs.sh)
BLOCK_SIZE_HUMAN="32MB"
REPLICATION=3                       # Standard HDFS replication

# k values to test: number of loopback storage dirs per DataNode (doubling progression)
K_VALUES=(1 512)

# Loopback sizing policy
LOOPBACK_BUDGET_PER_NODE_GB=80
MIN_IMAGE_SIZE_MB=100  # Minimum image size is 100MB

# Cluster info (Tapuz HDD nodes)
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

MASTER_NODE="tapuz14"
ALL_NODES=("tapuz14" "tapuz10" "tapuz11" "tapuz12" "tapuz13")
WORKER_NODES=("tapuz10" "tapuz11" "tapuz12" "tapuz13")
MASTER_HAS_DN=${MASTER_HAS_DN:-0}

DATANODE_NODES=()
if [[ "$MASTER_HAS_DN" == "0" ]]; then
    DATANODE_NODES=("${WORKER_NODES[@]}")
else
    DATANODE_NODES=("${ALL_NODES[@]}")
fi

NUM_PHYSICAL_NODES=${#ALL_NODES[@]}
HADOOP_HOME="/home/mostufa.j/hadoop"

NUM_DATANODE_HOSTS=${#DATANODE_NODES[@]}

CONFIG_DIR="/scratch/tmp/hadoop_single_dn_k_dirs"

# NameNode JMX endpoint for memory monitoring
NAMENODE_HOST="$MASTER_NODE"
NAMENODE_HTTP_PORT=9870
JMX_URL="http://${NAMENODE_HOST}:${NAMENODE_HTTP_PORT}/jmx"

LOG_FILE="$RUN_DIR/experiment.log"
CSV_FILE="$RUN_DIR/results.csv"
NN_MEMORY_DIR="$RUN_DIR/namenode_memory"
mkdir -p "$NN_MEMORY_DIR"

IOSTAT_DIR="$RUN_DIR/iostat"
mkdir -p "$IOSTAT_DIR"

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

# Query NameNode JMX for memory + metadata stats.
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

# NameNode memory monitor state
NN_MONITOR_PID=""

# Start background NameNode memory monitor.
# Usage: start_nn_monitor <output_csv> [interval_seconds]
start_nn_monitor() {
    local output_csv=$1
    local interval=${2:-5}

    echo "timestamp,heap_used_mb,heap_max_mb,block_count,file_count,live_datanodes" > "$output_csv"

    (
        while true; do
            local ts
            ts=$(date +"%Y-%m-%d %H:%M:%S")
            local stats
            stats=$(query_namenode_jmx)
            local heap_used
            local heap_max
            local blocks
            local files
            local live
            heap_used=$(echo "$stats" | awk '{print $1}')
            heap_max=$(echo "$stats" | awk '{print $2}')
            blocks=$(echo "$stats" | awk '{print $3}')
            files=$(echo "$stats" | awk '{print $4}')
            live=$(echo "$stats" | awk '{print $5}')
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

# ---- iostat disk I/O monitor ----
IOSTAT_PIDS=()

# Start iostat on all DataNode nodes.
# Usage: start_iostat_monitor <k_value>
start_iostat_monitor() {
    local k=$1
    IOSTAT_PIDS=()
    for node in "${DATANODE_NODES[@]}"; do
        local outfile="$IOSTAT_DIR/iostat_k${k}_${node}.log"

                # Detect the device backing /scratch (Tapuz uses sda1/sda2 partitions, with fallbacks).
                local scratch_dev
                scratch_dev=$(ssh "$node" bash -s << 'DEVEOF'
set +e  # Don't exit on error; we have fallbacks

# Method 1: Try findmnt to get the source device
src=$(findmnt -no SOURCE /scratch 2>/dev/null)

# Method 2: If findmnt fails, try df
if [[ -z "$src" ]]; then
    src=$(df /scratch 2>/dev/null | awk 'NR==2{print $1}')
fi

# Method 3: Try to resolve to a real /dev/ path
if [[ -n "$src" && "$src" != /* ]]; then
    # If src is not a /dev/ path, try to find it via lsblk
    resolved=$(lsblk -n -o NAME,MOUNTPOINT 2>/dev/null | grep "/scratch" | awk '{print $1}')
    if [[ -n "$resolved" ]]; then
        src="/dev/$resolved"
    fi
fi

# Fallback to readlink to resolve symlinks/mountpoints
if [[ -n "$src" ]]; then
    src=$(readlink -f "$src" 2>/dev/null || echo "$src")
fi

# Method 4: Use lsblk to find any mounted device at /scratch
if [[ -z "$src" ]] || [[ ! -b "$src" ]]; then
    src=$(lsblk -rn -o NAME,MOUNTPOINT 2>/dev/null | awk '$2 == "/scratch" {print "/dev/" $1}' | head -1)
fi

base=""
if [[ -n "$src" && -b "$src" ]]; then
    # src is now a valid /dev/ path
    base=$(basename "$src")
    
    # Remove partition suffix (e.g., "sda1" -> "sda", "nvme0n1p1" -> "nvme0n1")
    if [[ "$base" =~ ^([a-z]+[0-9]*)p?[0-9]+$ ]]; then
        base="${BASH_REMATCH[1]}"
    fi
    
    # If it's a dm device, try to find the backing device
    if [[ "$base" =~ ^dm- ]]; then
        parent=$(lsblk -rn -o NAME,TYPE /dev/"$base" 2>/dev/null | grep -v "^$base" | head -1 | awk '{print $1}')
        # If parent found, use it; else keep dm device
        if [[ -n "$parent" ]]; then
            if [[ "$parent" =~ ^([a-z]+[0-9]*)p?[0-9]*$ ]]; then
                base="${BASH_REMATCH[1]}"
            else
                base="$parent"
            fi
        fi
    fi
fi

# Final fallback: if device is still empty or not valid, use sda
if [[ -z "$base" ]]; then
    # Last resort: check if any of these common devices exist
    for candidate in sda nvme0n1 vda; do
        if [[ -b /dev/$candidate ]]; then
            base="$candidate"
            break
        fi
    done
    [[ -z "$base" ]] && base="sda"  # Ultimate fallback
fi

echo "$base"
DEVEOF
                )
                [[ -z "$scratch_dev" ]] && scratch_dev="sda"
                log "  Device detection: /scratch backed by $scratch_dev"

        log "  iostat on $node: monitoring device=${scratch_dev} (/scratch)"
        # LANG=C forces MM/DD/YYYY HH:MM:SS AM/PM timestamp format regardless of locale.
        # Passing the device name restricts iostat to /scratch I/O only (no OS disk noise).
        ssh "$node" "LANG=C iostat -dxyt 5 ${scratch_dev}" > "$outfile" 2>/dev/null &
        IOSTAT_PIDS+=($!)
    done
    log "  iostat monitor started on ${#DATANODE_NODES[@]} nodes (k=$k)"
}

stop_iostat_monitor() {
    # Kill local SSH tunnel processes
    for pid in "${IOSTAT_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    # Kill remote iostat processes
    for node in "${DATANODE_NODES[@]}"; do
        ssh "$node" "pkill -f 'iostat.*-dxyt'" 2>/dev/null || true
    done
    IOSTAT_PIDS=()
    log "  iostat monitor stopped."
}

# Parse raw iostat logs into a summary CSV for a given k value.
# Usage: parse_iostat_logs <k_value>
#
# Filtering: if $IOSTAT_DIR/wc_windows_k${k}.txt exists (one "start_epoch end_epoch"
# line per WordCount run), only iostat samples whose 5 s report window overlaps a
# WC window are kept. This drops idle cleanup time between runs so averages reflect
# real workload I/O, not HDFS rm gaps.
parse_iostat_logs() {
    local k=$1
    local summary_csv="$IOSTAT_DIR/iostat_summary_k${k}.csv"

    python3 - "$k" "$IOSTAT_DIR" <<'IOSTAT_PY'
import sys, re, os, glob
from datetime import datetime

k = sys.argv[1]
iostat_dir = sys.argv[2]
summary_path = os.path.join(iostat_dir, f"iostat_summary_k{k}.csv")

# Load WordCount run windows (epoch seconds) if recorded by the runner.
wc_windows = []
windows_path = os.path.join(iostat_dir, f"wc_windows_k{k}.txt")
if os.path.exists(windows_path):
    with open(windows_path) as wf:
        for line in wf:
            parts = line.strip().split()
            if len(parts) == 2:
                try:
                    wc_windows.append((int(parts[0]), int(parts[1])))
                except ValueError:
                    pass

def _parse_ts(ts_str):
    ts_str = (ts_str or "").strip()
    for fmt in (
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%y %H:%M:%S",
    ):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            pass
    return None

def in_wc_window(ts_str):
    # No windows recorded -> include everything (backward compatible).
    if not wc_windows:
        return True
    dt = _parse_ts(ts_str)
    if dt is None:
        return True
    ts_epoch = int(dt.timestamp())
    # iostat -dxyt 5 averages over the prior 5 s, so the report at ts covers [ts-5, ts].
    for s, e in wc_windows:
        if ts_epoch - 5 <= e and ts_epoch >= s:
            return True
    return False

pattern = os.path.join(iostat_dir, f"iostat_k{k}_*.log")
log_files = sorted(glob.glob(pattern))

kept = dropped = 0
with open(summary_path, "w") as out:
    out.write("timestamp,node,device,r_per_s,w_per_s,rkB_per_s,wkB_per_s,r_await,w_await,rareq_sz,wareq_sz,aqu_sz,util\n")

    for log_file in log_files:
        basename = os.path.basename(log_file)
        node = basename.replace(f"iostat_k{k}_", "").replace(".log", "")

        current_ts = ""
        header_indices = {}

        with open(log_file) as f:
            parts = []  # current data-line fields (set before get_col is called)

            def get_col(name, default="0"):
                idx = header_indices.get(name)
                if idx is not None and idx < len(parts):
                    return parts[idx]
                return default

            for line in f:
                line = line.strip()
                if not line:
                    continue

                ts_match = re.match(r"(\d{2}/\d{2}/\d{2,4}\s+\d{2}:\d{2}:\d{2}(?:\s+[AP]M)?)", line)
                if ts_match:
                    current_ts = ts_match.group(1)
                    continue

                if line.startswith("Device"):
                    cols = line.split()
                    for i, col in enumerate(cols):
                        header_indices[col] = i
                    continue

                parts = line.split()
                if not parts or len(parts) < 2:
                    continue
                device = parts[0]
                if not device.startswith("sd"):
                    continue

                if not in_wc_window(current_ts):
                    dropped += 1
                    continue

                row = [
                    current_ts, node, device,
                    get_col("r/s"), get_col("w/s"),
                    get_col("rkB/s"), get_col("wkB/s"),
                    get_col("r_await"), get_col("w_await"),
                    get_col("rareq-sz"), get_col("wareq-sz"),
                    get_col("aqu-sz"), get_col("%util"),
                ]
                out.write(",".join(row) + "\n")
                kept += 1

print(f"Parsed iostat summary: {summary_path} (kept={kept}, dropped_outside_wc={dropped}, windows={len(wc_windows)})")
IOSTAT_PY

    log "  iostat logs parsed: $summary_csv"
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

# Calculate image size for each k value
calc_image_size_mb() {
    local k=$1
    # Budget-based sizing (split budget across k loopback FSes)
    # Work in MB from the start to avoid integer truncation (200GB/256 = 0GB)
    local budget_based_mb=$(( (LOOPBACK_BUDGET_PER_NODE_GB * 1024) / k ))

    # Use budget-based size, but enforce minimum
    local image_mb=$budget_based_mb
    if (( image_mb < MIN_IMAGE_SIZE_MB )); then
        image_mb=$MIN_IMAGE_SIZE_MB
    fi
    echo "$image_mb"
}

# ============================================================================
# CLEANUP TRAP
# ============================================================================
cleanup() {
    echo ""
    log "Caught interrupt, cleaning up..."
    stop_iostat_monitor
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
echo "Storage Virtualization Loopback Filesystem Experiment"
echo "============================================================"
echo "Run ID:           $TIMESTAMP"
echo "Input size:       ${INPUT_SIZE_GB}GB"
echo "Block size:       $BLOCK_SIZE_HUMAN"
echo "Replication:      $REPLICATION"
echo "Master has DN:    $MASTER_HAS_DN"
echo "Loopback budget:  ${LOOPBACK_BUDGET_PER_NODE_GB}GB/node (min ${MIN_IMAGE_SIZE_MB}MB/image)"
echo "Physical nodes:   $NUM_PHYSICAL_NODES (${ALL_NODES[*]})"
echo "DataNode hosts:   $NUM_DATANODE_HOSTS (${DATANODE_NODES[*]})"
echo "k values:         ${K_VALUES[*]}"
echo "Repetitions (K):  $K_REPS"
echo "Results:          $RUN_DIR"
echo ""
echo "Resource plan per k value:"
for k in "${K_VALUES[@]}"; do
    img_mb=$(calc_image_size_mb $k)
    img_gb=$(awk "BEGIN {printf \"%.2f\", $img_mb/1024}")
    total_dirs=$(( NUM_DATANODE_HOSTS * k ))
    total_disk_mb=$(( img_mb * k ))
    total_disk_gb=$(awk "BEGIN {printf \"%.2f\", $total_disk_mb/1024}")
    echo "  k=$k: $NUM_DATANODE_HOSTS DataNodes (1 per host), $k storage dirs each = ${total_dirs} total dirs, ${img_mb}MB (${img_gb}GB) images x $k = ${total_disk_mb}MB (${total_disk_gb}GB) disk/node"
done
echo "============================================================"
echo ""

# Save metadata

# Export new MB-based minimum
export LOOPBACK_BUDGET_PER_NODE_GB MIN_IMAGE_SIZE_MB K_REPS
export TIMESTAMP INPUT_SIZE_GB BLOCK_SIZE BLOCK_SIZE_HUMAN REPLICATION
export NUM_PHYSICAL_NODES RUN_DIR NUM_DATANODE_HOSTS MASTER_HAS_DN

K_VALUES_CSV=$(IFS=,; echo "${K_VALUES[*]}")
NODE_NAMES_CSV=$(IFS=,; echo "${ALL_NODES[*]}")
DN_HOST_NAMES_CSV=$(IFS=,; echo "${DATANODE_NODES[*]}")
export K_VALUES_CSV NODE_NAMES_CSV DN_HOST_NAMES_CSV

python3 - <<'PY' 2>/dev/null || true
import json
import os
from datetime import datetime

def parse_int_list(csv_text: str):
    csv_text = (csv_text or "").strip()
    if not csv_text:
        return []
    return [int(x.strip()) for x in csv_text.split(',') if x.strip()]

meta = {
    "run_id": os.environ["TIMESTAMP"],
    "experiment_type": "storage_virtualization_loopback",
    "input_size_gb": int(os.environ["INPUT_SIZE_GB"]),
    "block_size_bytes": int(os.environ["BLOCK_SIZE"]),
    "block_size_human": os.environ["BLOCK_SIZE_HUMAN"],
    "replication": int(os.environ["REPLICATION"]),
    "physical_nodes": int(os.environ["NUM_PHYSICAL_NODES"]),
    "node_names": [x for x in os.environ.get("NODE_NAMES_CSV", "").split(',') if x],
    "datanode_hosts": int(os.environ["NUM_DATANODE_HOSTS"]),
    "datanode_host_names": [x for x in os.environ.get("DN_HOST_NAMES_CSV", "").split(',') if x],
    "master_has_datanode": os.environ.get("MASTER_HAS_DN", "1") != "0",
    "k_values": parse_int_list(os.environ.get("K_VALUES_CSV", "")),
    "loopback_budget_per_node_gb": int(os.environ["LOOPBACK_BUDGET_PER_NODE_GB"]),
    "min_image_size_mb": int(os.environ["MIN_IMAGE_SIZE_MB"]),
    "repetitions": int(os.environ["K_REPS"]),
    "start_time": datetime.now().astimezone().isoformat(timespec="seconds"),
}

out_path = os.path.join(os.environ["RUN_DIR"], "metadata.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(meta, f, indent=4)
PY

export K_VALUES_CSV

# Initialize CSV (runtime + NameNode memory columns + per-FS block counts + input block distribution + FS capacity)
echo "k_storage_dirs,total_storage_dirs,datanodes,avg_runtime_seconds,stddev_runtime,individual_runtimes,nn_heap_before_mb,nn_heap_peak_mb,nn_heap_avg_mb,nn_block_count,block_counts_per_fs,input_block_counts_per_fs,fs_used_mb_per_fs" > "$CSV_FILE"

# ============================================================================
# Iterate over k values
# ============================================================================

for k in "${K_VALUES[@]}"; do
    TOTAL_STORAGE_DIRS=$(( NUM_DATANODE_HOSTS * k ))
    IMAGE_SIZE_MB=$(calc_image_size_mb $k)

    log "Running experiment for k=$k with $TOTAL_STORAGE_DIRS total storage dirs..."

    # -- Step 1: Start the single-DN cluster with k storage dirs --
    log "Starting cluster with k=$k storage dirs per DataNode..."
    # Pass image size in MB to avoid integer truncation (200MB / 1024 = 0GB)
    bash "$SCRIPT_DIR/start-single-dn-cluster.sh" "$k" "$IMAGE_SIZE_MB" "5500" "$REPLICATION" 2>&1 | tee -a "$LOG_FILE"

    # Record actual live DataNodes
    export HADOOP_CONF_DIR="$CONFIG_DIR"
    LIVE_DNS=$(hdfs dfsadmin -report 2>/dev/null | grep -i "Live datanodes" | grep -o '[0-9]*' || echo "0")
    log "Live DataNodes: $LIVE_DNS (expected $NUM_DATANODE_HOSTS)"

    # -- Step 2: Generate and upload input data --
    log "Generating ${INPUT_SIZE_GB}GB input..."
    hdfs dfs -mkdir -p /user/$USER/wordcount/input 2>/dev/null || true
    bash "$WORDCOUNT_DIR/generate-input.sh" "$((INPUT_SIZE_GB * 1024))" "$BLOCK_SIZE" 2>&1 | tee -a "$LOG_FILE"

    log "Input uploaded. HDFS status:"
    hdfs dfs -ls /user/$USER/wordcount/input 2>&1 | tee -a "$LOG_FILE"

    # Snapshot NameNode memory before WordCount
    log "Querying NameNode memory (before WordCount)..."
    sleep 5
    NN_BEFORE=$(query_namenode_jmx)
    NN_HEAP_BEFORE=$(echo "$NN_BEFORE" | awk '{print $1}')
    NN_BLOCK_COUNT=$(echo "$NN_BEFORE" | awk '{print $3}')
    log "  NN heap before: ${NN_HEAP_BEFORE}MB, blocks: $NN_BLOCK_COUNT"

    # Monitor NameNode memory while WordCount runs
    NN_MONITOR_CSV="$NN_MEMORY_DIR/nn_memory_k${k}.csv"
    start_nn_monitor "$NN_MONITOR_CSV" 5
    start_iostat_monitor "$k"

    # Per-run WC windows (epoch seconds). parse_iostat_logs uses this file to
    # drop iostat samples captured during `hdfs dfs -rm` gaps between runs.
    WC_WINDOWS_FILE="$IOSTAT_DIR/wc_windows_k${k}.txt"
    : > "$WC_WINDOWS_FILE"

    # -- Step 3: Run WordCount K_REPS times --
    declare -a runtimes=()

    for ((run_i=1; run_i<=K_REPS; run_i++)); do
        log ""
        log "  Run $run_i/$K_REPS (k=$k)..."

        # Remove output from previous run (outside the timed/measured window)
        hdfs dfs -rm -r -f /user/$USER/wordcount/output 2>/dev/null || true

        # Flush HDFS caches and DataNode kernel caches to force disk reads (captures iostat)
        log "  Flushing HDFS data and OS caches..."
        
        # 1. Clear HDFS read-ahead cache by dropping HDFS data
        hdfs dfs -rm -r -f /user/$USER/wordcount/input 2>/dev/null || true
        
        # 2. Flush and drop OS caches on all DataNodes
        for node in "${DATANODE_NODES[@]}"; do
            ssh "$node" "sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" 2>/dev/null || \
            ssh "$node" "sync && echo 3 > /proc/sys/vm/drop_caches" 2>/dev/null || true
        done
        sleep 1
        
        # 3. Re-upload input data (will be fresh on disk, not in cache)
        log "  Re-uploading input data..."
        hdfs dfs -mkdir -p /user/$USER/wordcount/input 2>/dev/null || true
        bash "$WORDCOUNT_DIR/generate-input.sh" "$((INPUT_SIZE_GB * 1024))" "$BLOCK_SIZE" 2>/dev/null || true
        sleep 2

        # Time the WordCount job
        START_TIME=$(date +%s.%N)
        WC_START_EPOCH=$(date +%s)

        hadoop jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-"*.jar \
            wordcount \
            -D mapreduce.jobhistory.address=${MASTER_NODE}:10020 \
            -D mapreduce.jobhistory.webapp.address=${MASTER_NODE}:19888 \
            /user/$USER/wordcount/input \
            /user/$USER/wordcount/output 2>&1 | tee -a "$LOG_FILE"

        WC_END_EPOCH=$(date +%s)
        END_TIME=$(date +%s.%N)
        RUNTIME=$(echo "scale=2; $END_TIME - $START_TIME" | bc)

        echo "$WC_START_EPOCH $WC_END_EPOCH" >> "$WC_WINDOWS_FILE"

        log "  Runtime: ${RUNTIME}s"
        runtimes+=("$RUNTIME")
    done

    # -- Step 4: Stop monitors and collect stats --
    stop_iostat_monitor
    parse_iostat_logs "$k"
    stop_nn_monitor
    NN_HEAP_PEAK=$(get_peak_heap_mb "$NN_MONITOR_CSV")
    NN_HEAP_AVG=$(get_avg_heap_mb "$NN_MONITOR_CSV")
    log "  NN heap peak during WordCount: ${NN_HEAP_PEAK}MB"
    log "  NN heap avg during WordCount:  ${NN_HEAP_AVG}MB"

    # -- Step 5: Compute timing stats --
    avg=$(compute_avg runtimes)
    stddev=$(compute_stddev runtimes "$avg")
    individual=$(IFS=";"; echo "${runtimes[*]}")

    log ""
    log "  k=$k  Average: ${avg}s  StdDev: ${stddev}s  Runs: $individual"
    log "  k=$k  NN: heap_before=${NN_HEAP_BEFORE}MB peak=${NN_HEAP_PEAK}MB avg=${NN_HEAP_AVG}MB blocks=$NN_BLOCK_COUNT"

    # -- Step 4.5: Flush caches and stabilize before block collection --
    log "Flushing caches and stabilizing DataNode storage..."
    # Remove WordCount output so it doesn't pollute block counts (input-only measurement)
    hdfs dfs -rm -r -f /user/$USER/wordcount/output 2>/dev/null || true
    sleep 3  # Let async block writes complete
    for NODE in "${DATANODE_NODES[@]}"; do
        ssh "$NODE" "sync" 2>/dev/null || true
    done
    sleep 1

    # Collect per-filesystem block counts (after WordCount output removed, input still present)
    log "Collecting per-filesystem block counts..."
    block_counts_per_fs=""
    > "$RUN_DIR/block_counts_tmp.txt"
    for NODE in "${DATANODE_NODES[@]}"; do
        # Count block DATA files only (exclude .meta checksum files; each block has both)
        # Path: /scratch/hdfs_loop/dn<i>/hdfs_data/current/BP-<pool-id>/current/.../blk_*
        ssh "$NODE" 'for i in {1..'"$k"'}; do cnt=$(find /scratch/hdfs_loop/dn"$i"/hdfs_data -name "blk_*" -not -name "*.meta" -type f 2>/dev/null | wc -l); echo -n "$cnt;"; done' 2>/dev/null >> "$RUN_DIR/block_counts_tmp.txt"
    done
    if [[ -f "$RUN_DIR/block_counts_tmp.txt" && -s "$RUN_DIR/block_counts_tmp.txt" ]]; then
        block_counts_per_fs=$(cat "$RUN_DIR/block_counts_tmp.txt")
        # Remove trailing semicolon if present
        block_counts_per_fs="${block_counts_per_fs%;}"
        rm -f "$RUN_DIR/block_counts_tmp.txt"
    fi
    log "Block counts collected: $block_counts_per_fs"

    # Collect per-filesystem used capacity in MB
    log "Collecting per-filesystem used capacity..."
    fs_used_mb_per_fs=""
    > "$RUN_DIR/fs_used_mb_tmp.txt"
    for NODE in "${DATANODE_NODES[@]}"; do
        # Get used space (in MB) for each loopback filesystem mount point
        # Using df --output=used -BM to get used space in MB
        ssh "$NODE" 'for i in {1..'"$k"'}; do
            used=$(df --output=used -BM "/scratch/hdfs_loop/dn$i" 2>/dev/null | tail -1 | tr -dc "0-9")
            echo -n "${used:-0};"
        done' 2>/dev/null >> "$RUN_DIR/fs_used_mb_tmp.txt"
    done
    if [[ -f "$RUN_DIR/fs_used_mb_tmp.txt" && -s "$RUN_DIR/fs_used_mb_tmp.txt" ]]; then
        fs_used_mb_per_fs=$(cat "$RUN_DIR/fs_used_mb_tmp.txt")
        # Remove trailing semicolon if present
        fs_used_mb_per_fs="${fs_used_mb_per_fs%;}"
        rm -f "$RUN_DIR/fs_used_mb_tmp.txt"
    fi
    log "Filesystem capacity collected: $fs_used_mb_per_fs"


    # Collect HDFS block information specifically for INPUT FILES
    log "Collecting HDFS block information for input files..."
    HDFS_FSCK_OUTPUT="$RUN_DIR/hdfs_fsck_k${k}.txt"
    hdfs fsck /user/$USER/wordcount/input -files -blocks -locations > "$HDFS_FSCK_OUTPUT" 2>&1
    log "HDFS input block information saved to: $HDFS_FSCK_OUTPUT"

    # Extract input block IDs from FSCK output
    # IMPORTANT: FSCK shows "blk_1073741825_1001" but disk stores "blk_1073741825" (no gen stamp)
    # So we extract just the base block ID (blk_NNNN) without the generation stamp
    log "Extracting input block IDs..."
    INPUT_BLOCK_IDS="$RUN_DIR/input_block_ids_k${k}.txt"
    # Extract base block ID only: blk_1073741825 (not blk_1073741825_1001)
    grep -oP 'blk_\d+(?=_)' "$HDFS_FSCK_OUTPUT" | sort -u > "$INPUT_BLOCK_IDS" 2>/dev/null || touch "$INPUT_BLOCK_IDS"
    NUM_UNIQUE_BLOCK_IDS=$(wc -l < "$INPUT_BLOCK_IDS" 2>/dev/null || echo "0")
    log "Found $NUM_UNIQUE_BLOCK_IDS unique block IDs in input files"

    # Debug: show first few block IDs
    if (( NUM_UNIQUE_BLOCK_IDS > 0 )); then
        log "  Sample block IDs: $(head -3 "$INPUT_BLOCK_IDS" | tr '\n' ',' | sed 's/,$//')"
    fi

    # Count input blocks per loopback filesystem (using helper script)
    log "Counting input blocks per loopback filesystem..."
    input_block_counts_per_fs=""
    > "$RUN_DIR/input_block_counts_tmp.txt"
    for NODE in "${DATANODE_NODES[@]}"; do
        # Copy block IDs file and helper script to remote node
        scp -q "$INPUT_BLOCK_IDS" "$NODE:/tmp/input_block_ids_k${k}.txt" 2>/dev/null || true
        scp -q "$SCRIPT_DIR/count-input-blocks-per-fs.sh" "$NODE:/tmp/count-input-blocks-per-fs.sh" 2>/dev/null || true

        # Run helper script on remote node - pass block IDs file instead of HDFS path
        NODE_OUTPUT=$(ssh "$NODE" "bash /tmp/count-input-blocks-per-fs.sh /tmp/input_block_ids_k${k}.txt $k /scratch/hdfs_loop" 2>/dev/null || echo "")
        if [[ -n "$NODE_OUTPUT" ]]; then
            echo -n "${NODE_OUTPUT};" >> "$RUN_DIR/input_block_counts_tmp.txt"
        fi
    done

    if [[ -f "$RUN_DIR/input_block_counts_tmp.txt" && -s "$RUN_DIR/input_block_counts_tmp.txt" ]]; then
        input_block_counts_per_fs=$(cat "$RUN_DIR/input_block_counts_tmp.txt")
        # Remove trailing semicolon
        input_block_counts_per_fs="${input_block_counts_per_fs%;}"
        rm -f "$RUN_DIR/input_block_counts_tmp.txt"
    fi
    log "Input block counts per FS: $input_block_counts_per_fs"

    # Parse FSCK to extract input file block distribution summary
    log "Generating input block distribution summary..."
    INPUT_BLOCK_OUTPUT="$RUN_DIR/input_block_dist_k${k}.txt"
    # Use an unquoted heredoc so shell variables expand into the Python script.
    # The Python analyzer will catch exceptions and exit 0 so the main script continues.
    python3 > "$INPUT_BLOCK_OUTPUT" 2>&1 <<PARSE_BLOCKS
import re
import sys
from collections import defaultdict
try:
    fsck_file = "${HDFS_FSCK_OUTPUT}"
    input_size_gb = int(${INPUT_SIZE_GB})
    replication = int(${REPLICATION})
    k = int(${k})
    block_size_bytes = int(${BLOCK_SIZE})

    expected_blocks = (input_size_gb * 1024 * 1024 * 1024) // block_size_bytes
    expected_replicas = expected_blocks * replication

    print(f"Expected blocks for {input_size_gb}GB input: {expected_blocks}")
    print(f"Expected total replicas (rep={replication}): {expected_replicas}")
    print()

    # Parse FSCK to count input file blocks and replicas
    total_replicas_found = 0
    input_blocks_info = []

    try:
        with open(fsck_file) as f:
            content = f.read()
    except Exception as e:
        print(f"Could not read FSCK file '{fsck_file}': {e}")
        content = ''

    # Extract block entries from FSCK content
    # Matches lines containing: 0. BP-...:blk_12345_1 len=134217728 Live_repl=3 [DatanodeInfoWithStorage[...DS-...]
    block_entries = re.findall(r"(\d+)\.\s+BP-[^:]+:blk_(\d+_\d+).*?len=(\d+).*?Live_repl=(\d+)\s+\[(.*?)\]", content, re.DOTALL)

    for entry in block_entries:
        block_num = entry[0]
        block_id = entry[1]
        block_len = int(entry[2])
        live_repl = int(entry[3])
        replicas_str = entry[4]

        print(f"Block {block_num}: {block_id} ({block_len} bytes), replicas: {live_repl}")
        total_replicas_found += live_repl

        # Extract DataNode + storage ID info for each replica
        replica_matches = re.findall(r"DatanodeInfoWithStorage\[([^:]+):[^,]+,DS-([a-f0-9\-]+)", replicas_str)
        for dn_ip, storage_id in replica_matches:
            input_blocks_info.append({
                'block_id': block_id,
                'dn_ip': dn_ip,
                'storage_id': storage_id
            })

    print(f"Total replicas found: {total_replicas_found}")
    print()

    # Since mapping storage IDs to specific loopback mounts requires DataNode-side lookup,
    # fall back to an even distribution estimate across the k filesystems if we couldn't map.
    if total_replicas_found == 0 or k <= 0:
        per_fs_counts = [0] * max(1, k)
    else:
        per_fs_count = total_replicas_found // k
        remainder = total_replicas_found % k
        per_fs_counts = [per_fs_count] * k
        for i in range(remainder):
            per_fs_counts[i] += 1

    output = ';'.join(str(c) for c in per_fs_counts)
    print(f"Distribution per FS (estimated): {output}")
    print(output)
except Exception as e:
    print("ERROR parsing FSCK:", e)
    import traceback
    traceback.print_exc()
    # Ensure we exit with success so the main script continues
    sys.exit(0)
PARSE_BLOCKS

    # Record to CSV (runtime + NameNode memory columns + per-FS block counts + input block counts + FS capacity)
    echo "$k,$TOTAL_STORAGE_DIRS,$LIVE_DNS,$avg,$stddev,$individual,$NN_HEAP_BEFORE,$NN_HEAP_PEAK,$NN_HEAP_AVG,$NN_BLOCK_COUNT,$block_counts_per_fs,$input_block_counts_per_fs,$fs_used_mb_per_fs" >> "$CSV_FILE"

    unset runtimes

    # -- Step 5: Clean up HDFS data (output already removed above; this clears input too) --
    log "Cleaning HDFS data..."
    hdfs dfs -rm -r -f /user/$USER/wordcount 2>/dev/null || true

    # -- Step 6: Stop the cluster and tear down loopback FSes --
    log "Stopping cluster..."
    bash "$SCRIPT_DIR/stop-single-dn-cluster.sh" "$k" 2>&1 | tee -a "$LOG_FILE"

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

printf "%s\n" "${WORKER_NODES[@]}" > "$CONFIG_DIR/workers"
export HADOOP_CONF_DIR="$CONFIG_DIR"

rm -rf /scratch/hadoop_data/namenode/current 2>/dev/null || true
rm -rf /scratch/hadoop_data/datanode/current 2>/dev/null || true
for node in "${ALL_NODES[@]}"; do
    if [[ "$node" != "$(hostname)" && "$node" != "$MASTER_NODE" ]]; then
        ssh "$node" "rm -rf /scratch/hadoop_data/datanode/current" 2>/dev/null || true
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
echo "  - iostat/iostat_k*_*.log           : Per-k per-node raw iostat logs"
echo "  - iostat/iostat_summary_k*.csv     : Per-k parsed iostat summaries"
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
