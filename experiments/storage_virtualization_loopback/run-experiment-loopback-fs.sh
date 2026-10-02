#!/bin/bash
################################################################################
# SCRIPT: run-experiment-loopback-fs.sh
# DESCRIPTION: Main experiment runner for the storage virtualization (loopback
#              filesystem) experiment. For each k, builds a cluster where every
#              DataNode stores its blocks on k loopback ext4 filesystems (k
#              "virtual disks" carved out of one physical disk), uploads the
#              WordCount input once, and times WordCount K_REPS times for every
#              condition (load level x page-cache state).
#
# USAGE: [VAR=value ...] bash run-experiment-loopback-fs.sh [K_REPS]
#   K_REPS - repetitions per k and condition (default: 5)
#
# SETTINGS (environment variables; defaults, the same for every cluster, are
# in experiment.conf):
#   CLUSTER            tapuz | c6620 (default: guessed from the hostname)
#   K_VALUES           k values, e.g. "1 256 1024" (default "1 256 1024")
#   K_ORDER            given | random (default given)
#   INPUT_SIZE_GB      WordCount input size in GB (default 8)
#   INPUT_SIZE_MB      the same in MB, for inputs below 1 GB (overrides INPUT_SIZE_GB)
#   BLOCK_SIZE_MB      HDFS block size of the input in MB (default 32)
#   CONDITIONS         space-separated list of "maps=N,cache=cold|warm":
#                        maps  = map tasks per node running at once
#                                (1..SLOTS_PER_NODE, or "all")
#                        cache = cold: input evicted from the page cache
#                                      before each job, so it is read from disk
#                                warm: input read into the page cache first
#                      Default: "maps=all,cache=cold". Within each repetition
#                      the conditions run in a random (seeded) order.
#   SEED               seed for all random orders (default: current time)
#   SLOTS_PER_NODE     YARN containers per node
#   CONTAINER_MB       size of one container in MB
#   DN_HEAP_MB         DataNode heap in MB or "auto"
#   LOOPBACK_BUDGET_PER_NODE_GB  disk space for all k images of a node
#   SETTLE_SECONDS     pause before every measured job (default 10)
#   WARMUP_JOBS        untimed jobs after each cluster start (default 1)
#   REUPLOAD_EACH_REP  1 = delete and re-upload the input before every job (default 0)
#   WORDCOUNT_MODE     real | trivial (default real)
#   MASTER_HAS_DN      1 = also run a DataNode on the master (default 0)
#
# PER-JOB PROTOCOL: every measured job -- any condition, any k, any cluster --
# goes through the same steps (prepare_job + run_one_job below). Only the cache
# action and the map container size depend on the condition.
#
# OUTPUT: results/storage_virtualization_loopback_<cluster>/run_<timestamp>/
#   runs.csv       one row per WordCount job: k, condition, runtime, disk
#                  bytes read/written during the job, Hadoop job counters
#   summary.txt    per condition and k: mean runtime, change vs the smallest k,
#                  load (maps running at once), locality, disk reads
#   results.csv    per-k summary: runtimes, NameNode memory, blocks per filesystem (first condition;
#                  results_<condition>.csv for each condition when several)
#   dn_metrics.csv per job: DataNode blocks served, time to serve one block,
#                  packet transfer time (from the DataNodes' own metrics)
#   server_metrics.csv  per k and host: DataNode threads / memory / open files,
#                  kernel loop threads, fs block size, start-up and block-report
#                  time, cluster setup and upload time
#   metadata.json  all settings, code version, node hardware
#   configs/k<k>/  the generated Hadoop configs (master + one DataNode host)
#   jobs/          full output of every WordCount job
#   namenode_memory/, iostat/, sysstat/ (DataNode pidstat + mpstat),
#   fragmentation/, hdfs_fsck_k*.txt, input_block_*_k*.txt
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORDCOUNT_DIR="$SCRIPT_DIR/../wordcount"

# cluster.conf defines: CLUSTER, MASTER_NODE, ALL_NODES, WORKER_NODES,
# STORAGE_BASE, HADOOP_HOME, IMAGE_DIR, MOUNT_BASE, HADOOP_DATA_DIR,
# TMP_BASE, CONFIG_DIR and the experiment defaults (SLOTS_PER_NODE, ...).
source "$SCRIPT_DIR/cluster.conf"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
# HDFS paths below use $USER; it is unset in some non-login shells (cron, ...).
export USER="${USER:-$(id -un)}"

RESULTS_BASE="${RESULTS_BASE:-$PROJECT_ROOT/results/storage_virtualization_loopback_${CLUSTER}}"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_BASE/run_$TIMESTAMP"
mkdir -p "$RUN_DIR"

# ============================================================================
# PARAMETERS
# ============================================================================
K_REPS=${1:-${K_REPS:-5}}
INPUT_SIZE_MB=${INPUT_SIZE_MB:-$(( INPUT_SIZE_GB * 1024 ))}
BLOCK_SIZE=$(( BLOCK_SIZE_MB * 1024 * 1024 ))
BLOCK_SIZE_HUMAN="${BLOCK_SIZE_MB}MB"
REPLICATION=3
read -r -a K_VALUES <<< "${K_VALUES:-1 256 1024}"
K_ORDER=${K_ORDER:-given}
SEED=${SEED:-$(date +%s)}
MIN_IMAGE_SIZE_MB=100
REUPLOAD_EACH_REP=${REUPLOAD_EACH_REP:-0}

# WordCount mode:
#   real    -> stock hadoop-mapreduce-examples wordcount (full CPU work)
#   trivial -> custom no-tokenization mapper, isolates I/O+framework cost
WORDCOUNT_MODE=${WORDCOUNT_MODE:-real}
TRIVIAL_WC_JAR="${TRIVIAL_WC_JAR:-$WORDCOUNT_DIR/trivial/trivial-wordcount.jar}"

# Older runs used DROP_CACHES=0/1; map it onto the cache setting.
if [[ -z "${CACHE_MODE:-}" && -n "${DROP_CACHES:-}" ]]; then
    if [[ "$DROP_CACHES" == "0" ]]; then CACHE_MODE=warm; else CACHE_MODE=cold; fi
fi
CACHE_MODE=${CACHE_MODE:-cold}
CONDITIONS=${CONDITIONS:-"maps=all,cache=$CACHE_MODE"}

MASTER_HAS_DN=${MASTER_HAS_DN:-0}
DATANODE_NODES=()
if [[ "$MASTER_HAS_DN" == "0" ]]; then
    DATANODE_NODES=("${WORKER_NODES[@]}")
else
    DATANODE_NODES=("${ALL_NODES[@]}")
fi

NUM_PHYSICAL_NODES=${#ALL_NODES[@]}
NUM_DATANODE_HOSTS=${#DATANODE_NODES[@]}

# YARN pool per node, and the heap of every task JVM (fixed for all conditions)
NM_MEM_MB=$(( SLOTS_PER_NODE * CONTAINER_MB ))
TASK_HEAP_MB=$(( CONTAINER_MB * 8 / 10 ))

# NameNode JMX endpoint for memory monitoring
NAMENODE_HOST="$MASTER_NODE"
NAMENODE_HTTP_PORT=9870
JMX_URL="http://${NAMENODE_HOST}:${NAMENODE_HTTP_PORT}/jmx"

LOG_FILE="$RUN_DIR/experiment.log"
CSV_FILE="$RUN_DIR/results.csv"
RUNS_CSV="$RUN_DIR/runs.csv"
NN_MEMORY_DIR="$RUN_DIR/namenode_memory"
IOSTAT_DIR="$RUN_DIR/iostat"
SYSSTAT_DIR="$RUN_DIR/sysstat"
JOBS_DIR="$RUN_DIR/jobs"
mkdir -p "$NN_MEMORY_DIR" "$IOSTAT_DIR" "$SYSSTAT_DIR" "$JOBS_DIR"

# ============================================================================
# CONDITIONS
# ============================================================================
# "maps=N" is applied per job by enlarging the map container so that exactly N
# maps fit into one NodeManager (the task heap stays TASK_HEAP_MB). Nodes that
# also host the ApplicationMaster or the reducer may fit one map fewer.
COND_NAMES=()
COND_MAPS=()
COND_CACHE=()
COND_MAP_MB=()
read -r -a _COND_SPECS <<< "$CONDITIONS"
for spec in "${_COND_SPECS[@]}"; do
    maps=""
    cache=""
    IFS=',' read -r -a _kvs <<< "$spec"
    for kv in "${_kvs[@]}"; do
        case "$kv" in
            maps=*)  maps=${kv#maps=} ;;
            cache=*) cache=${kv#cache=} ;;
            *) echo "ERROR: unknown setting '$kv' in condition '$spec'" >&2; exit 1 ;;
        esac
    done
    maps=${maps:-all}
    cache=${cache:-cold}
    if [[ "$maps" == "all" ]]; then
        maps=$SLOTS_PER_NODE
    fi
    if ! [[ "$maps" =~ ^[0-9]+$ ]] || (( maps < 1 || maps > SLOTS_PER_NODE )); then
        echo "ERROR: condition '$spec': maps must be 1..$SLOTS_PER_NODE or 'all'" >&2
        exit 1
    fi
    if [[ "$cache" != "cold" && "$cache" != "warm" ]]; then
        echo "ERROR: condition '$spec': cache must be cold or warm" >&2
        exit 1
    fi
    map_mb=$(( NM_MEM_MB / maps / 512 * 512 ))
    if (( map_mb < CONTAINER_MB )); then
        map_mb=$CONTAINER_MB
    fi
    name="maps${maps}_${cache}"
    for existing in "${COND_NAMES[@]}"; do
        if [[ "$existing" == "$name" ]]; then
            echo "ERROR: condition $name listed twice" >&2
            exit 1
        fi
    done
    COND_NAMES+=("$name")
    COND_MAPS+=("$maps")
    COND_CACHE+=("$cache")
    COND_MAP_MB+=("$map_mb")
done
NUM_CONDITIONS=${#COND_NAMES[@]}

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
    if (( n == 0 )); then
        echo "0"
        return
    fi
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
        local diff
        diff=$(echo "$v - $avg" | bc)
        sum_sq=$(echo "$sum_sq + ($diff * $diff)" | bc)
    done
    echo "scale=2; sqrt($sum_sq / ($n - 1))" | bc
}

# Print the arguments in a random order that depends only on the seed $1.
seeded_shuffle() {
    python3 -c 'import random, sys
items = sys.argv[2:]
random.Random(int(sys.argv[1])).shuffle(items)
print(" ".join(items))' "$@"
}

# Query NameNode JMX for memory + metadata stats.
# Returns: heap_used_mb heap_max_mb block_count file_count live_datanodes
query_namenode_jmx() {
    local jmx_data
    jmx_data=$(curl -sL --connect-timeout 5 --max-time 10 "$JMX_URL" 2>/dev/null || echo "{}")

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
            local ts stats heap_used heap_max blocks files live
            ts=$(date +"%Y-%m-%d %H:%M:%S")
            stats=$(query_namenode_jmx)
            read -r heap_used heap_max blocks files live <<< "$stats"
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

# ---- Node hardware + the physical device behind $STORAGE_BASE ----
# Fills SCRATCH_DEV[node] and writes $RUN_DIR/hardware.txt, one line per node
# from node-info.sh: node|cores|mem_mb|kernel|device|rotational|model|java|hadoop
declare -A SCRATCH_DEV
collect_hardware() {
    local hw_file="$RUN_DIR/hardware.txt"
    : > "$hw_file"
    local node line
    for node in "${ALL_NODES[@]}"; do
        line=$(ssh "$node" "bash -s" -- "$node" "$STORAGE_BASE" "$HADOOP_HOME" \
            < "$SCRIPT_DIR/node-info.sh" 2>/dev/null || true)
        if [[ -z "$line" ]]; then
            log "  WARNING: could not read hardware info from $node"
            line="$node|?|?|?|sda|?|?|?|?"
        fi
        echo "$line" >> "$hw_file"
        SCRATCH_DEV[$node]=$(echo "$line" | cut -d'|' -f5)
        log "  $node: $(echo "$line" | awk -F'|' -v sb="$STORAGE_BASE" \
            '{printf "%s cores, %s MB RAM, %s on /dev/%s (rotational=%s, model %s); %s; %s", $2, $3, sb, $5, $6, $7, $8, $9}')"
    done
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
        # LANG=C fixes timestamp format. -k forces KB/s units; -y skips the boot-time report.
        # stdbuf ensures output is flushed even if the SSH session is stopped.
        ssh "$node" "LANG=C stdbuf -oL -eL iostat -dxkty 5 ${SCRATCH_DEV[$node]}" > "$outfile" 2>/dev/null &
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
        ssh "$node" "pkill -f '[i]ostat -dxkty'" 2>/dev/null || true
    done
    IOSTAT_PIDS=()
    log "  iostat monitor stopped."
}

# ---- DataNode process (pidstat) + whole-node CPU (mpstat) monitors ----
# Where does the extra time at high k go? These show the DataNode JVM's CPU,
# memory and I/O, and the node's %usr/%sys/%iowait, every 5 s. pidstat prints
# epoch timestamps (-H) and vmstat UTC ones, so final-report.py can assign
# every sample to the job that was running.
SYSSTAT_PIDS=()

start_sysstat_monitors() {
    local k=$1
    SYSSTAT_PIDS=()
    for node in "${DATANODE_NODES[@]}"; do
        ssh "$node" 'command -v pidstat >/dev/null || exit 0
dnpid=$(pgrep -f "[o]rg.apache.hadoop.hdfs.server.datanode.DataNode" | head -1)
[ -n "$dnpid" ] || exit 0
LANG=C exec stdbuf -oL pidstat -H -h -u -r -d -p "$dnpid" 5' \
            > "$SYSSTAT_DIR/pidstat_datanode_k${k}_${node}.log" 2>/dev/null &
        SYSSTAT_PIDS+=($!)
        ssh "$node" 'command -v mpstat >/dev/null || exit 0
LANG=C exec stdbuf -oL mpstat 5' \
            > "$SYSSTAT_DIR/mpstat_k${k}_${node}.log" 2>/dev/null &
        SYSSTAT_PIDS+=($!)
        # Memory: free / page cache / swap in+out -- shows whether warm runs
        # really fit in RAM and whether anything swaps (7.7 GB on Tapuz).
        ssh "$node" 'LANG=C TZ=UTC exec stdbuf -oL vmstat -n -t 5' \
            > "$SYSSTAT_DIR/vmstat_k${k}_${node}.log" 2>/dev/null &
        SYSSTAT_PIDS+=($!)
    done
    log "  pidstat (DataNode) + mpstat + vmstat monitors started (k=$k)"
}

stop_sysstat_monitors() {
    for pid in "${SYSSTAT_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    for node in "${DATANODE_NODES[@]}"; do
        ssh "$node" "pkill -f '[p]idstat -H -h -u -r -d -p'; pkill -f '[m]pstat 5'; pkill -f '[v]mstat -n -t 5'" 2>/dev/null || true
    done
    SYSSTAT_PIDS=()
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

WINDOW_SLACK_SECONDS = 15

def in_wc_window(ts_str, windows):
    # No windows recorded -> include everything (backward compatible).
    if not windows:
        return True
    dt = _parse_ts(ts_str)
    if dt is None:
        return True
    ts_epoch = int(dt.timestamp())
    # iostat -dxyt 5 averages over the prior 5 s, so the report at ts covers [ts-5, ts].
    for s, e in windows:
        if ts_epoch - 5 - WINDOW_SLACK_SECONDS <= e and ts_epoch + WINDOW_SLACK_SECONDS >= s:
            return True
    return False

pattern = os.path.join(iostat_dir, f"iostat_k{k}_*.log")
log_files = sorted(glob.glob(pattern))

def parse_logs(filter_windows=True):
    kept = dropped = 0
    with open(summary_path, "w") as out:
        out.write("timestamp,node,device,r_per_s,w_per_s,rkB_per_s,wkB_per_s,r_await,w_await,rareq_sz,wareq_sz,aqu_sz,util\n")

        for log_file in log_files:
            basename = os.path.basename(log_file)
            node = basename.replace(f"iostat_k{k}_", "").replace(".log", "")

            current_ts = ""
            header_indices = {}

            with open(log_file) as f:
                parts = []  # current data-line fields (set before get_num is called)

                def get_num(names):
                    for name in names:
                        idx = header_indices.get(name)
                        if idx is not None and idx < len(parts):
                            try:
                                return float(parts[idx])
                            except ValueError:
                                return None
                    return None

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
                    if device.startswith("avg-cpu") or device.startswith("Linux"):
                        continue

                    if filter_windows and not in_wc_window(current_ts, wc_windows):
                        dropped += 1
                        continue

                    r_per_s = get_num(["r/s"]) or 0.0
                    w_per_s = get_num(["w/s"]) or 0.0

                    rkB = get_num(["rkB/s", "rKB/s"])
                    if rkB is None:
                        rMB = get_num(["rMB/s"])
                        rkB = rMB * 1024 if rMB is not None else 0.0
                    wkB = get_num(["wkB/s", "wKB/s"])
                    if wkB is None:
                        wMB = get_num(["wMB/s"])
                        wkB = wMB * 1024 if wMB is not None else 0.0

                    row = [
                        current_ts, node, device,
                        f"{r_per_s}", f"{w_per_s}",
                        f"{rkB}", f"{wkB}",
                        f"{get_num(['r_await']) or 0.0}", f"{get_num(['w_await']) or 0.0}",
                        f"{get_num(['rareq-sz']) or 0.0}", f"{get_num(['wareq-sz']) or 0.0}",
                        f"{get_num(['aqu-sz']) or 0.0}", f"{get_num(['%util']) or 0.0}",
                    ]
                    out.write(",".join(row) + "\n")
                    kept += 1

    return kept, dropped

try:
    kept, dropped = parse_logs(filter_windows=True)
    if wc_windows and kept == 0:
        # If clocks are skewed or timestamps were missing, fall back to no filtering.
        kept, dropped = parse_logs(filter_windows=False)
        print(f"WARNING: no iostat samples matched WordCount windows for k={k}; wrote unfiltered data")

    print(f"Parsed iostat summary: {summary_path} (kept={kept}, dropped_outside_wc={dropped}, windows={len(wc_windows)})")
except Exception as e:
    import traceback
    print(f"ERROR parsing iostat for k={k}: {e}")
    traceback.print_exc()
    sys.exit(1)
IOSTAT_PY

    log "  iostat logs parsed: $summary_csv"
}

# Extract peak heap from a monitor CSV
get_peak_heap_mb() {
    local csv_file=$1
    if [[ -f "$csv_file" ]]; then
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
    if [[ -f "$csv_file" ]]; then
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
    local image_mb=$(( (LOOPBACK_BUDGET_PER_NODE_GB * 1024) / k ))
    if (( image_mb < MIN_IMAGE_SIZE_MB )); then
        image_mb=$MIN_IMAGE_SIZE_MB
    fi
    echo "$image_mb"
}

# Generate the WordCount input and upload it to HDFS (replaces any old input).
upload_input() {
    hdfs dfs -rm -r -f /user/$USER/wordcount/input >/dev/null 2>&1 || true
    hdfs dfs -mkdir -p /user/$USER/wordcount/input
    bash "$WORDCOUNT_DIR/generate-input.sh" "$INPUT_SIZE_MB" "$BLOCK_SIZE" >> "$LOG_FILE" 2>&1
}

# ============================================================================
# PER-JOB PROTOCOL
# ============================================================================
# Every job -- measured or warm-up, any condition, any k, any cluster -- is
# prepared by exactly these steps (prepare_job); only the cache action in
# step 3 depends on the condition:
#   1. wait until YARN is idle: no application running, the whole pool free
#      (the previous job's containers are gone)
#   2. delete the previous job's output (re-upload the input if REUPLOAD_EACH_REP=1)
#   3. on every DataNode host, in parallel: sync (flush the previous job's
#      writes), then
#        cold: evict the input from the page cache -- the HDFS block files and
#              the loopback images, i.e. both cached copies -- with
#              posix_fadvise(DONTNEED)
#        warm: read every local block file once, so the job reads from RAM
#      Same method on every cluster. It needs no root (Tapuz allows no sudo for
#      drop_caches) and in both modes leaves the OS, the Hadoop jars and the
#      filesystem metadata cached, so the modes differ only in the input data.
#   4. pause SETTLE_SECONDS
# run_one_job then records how much of the input is in the page cache and the
# disk counters, and times the job.

wait_for_idle_yarn() {
    local waited=0 running=-1 avail=0 total=0
    while (( waited < 300 )); do
        read -r running avail total <<< "$(curl -s --max-time 5 "http://${MASTER_NODE}:8088/ws/v1/cluster/metrics" 2>/dev/null | python3 -c '
import json, sys
try:
    m = json.load(sys.stdin)["clusterMetrics"]
    print(m.get("appsRunning", -1), m.get("availableMB", 0), m.get("totalMB", 0))
except Exception:
    print(-1, 0, 0)
' 2>/dev/null || echo "-1 0 0")"
        if [[ "$running" == "0" && "$total" != "0" && "$avail" == "$total" ]]; then
            return 0
        fi
        sleep 2
        waited=$(( waited + 2 ))
    done
    log "  WARNING: YARN not idle after 300s (apps running: $running, free ${avail}/${total}MB); continuing"
}

# Copy the node-side helpers to /tmp on every DataNode host (once per run).
copy_node_helpers() {
    local node
    for node in "${DATANODE_NODES[@]}"; do
        scp -q "$SCRIPT_DIR/cache-step.py" "$node:/tmp/cache-step.py"
    done
}

# Step 3 of the protocol on all DataNode hosts in parallel (cache-step.py:
# sync, then cold = evict the block files and images, warm = read the blocks).
cache_step() {
    local mode=$1
    local node pid
    local -a pids=()
    for node in "${DATANODE_NODES[@]}"; do
        ssh "$node" "python3 /tmp/cache-step.py $mode $MOUNT_BASE $IMAGE_DIR" \
            > "$RUN_DIR/.cache_${node}.out" 2>&1 &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    local summary=""
    for node in "${DATANODE_NODES[@]}"; do
        summary+=" $node: $(tr '\n' ' ' < "$RUN_DIR/.cache_${node}.out" 2>/dev/null)"
        rm -f "$RUN_DIR/.cache_${node}.out"
    done
    log "  Cache $mode:$summary"
}

prepare_job() {
    local cache=$1
    wait_for_idle_yarn
    hdfs dfs -rm -r -f /user/$USER/wordcount/output >/dev/null 2>&1 || true
    if [[ "$REUPLOAD_EACH_REP" == "1" ]]; then
        log "  Re-uploading input..."
        upload_input
    fi
    cache_step "$cache"
    sleep "$SETTLE_SECONDS"
}

# MB of HDFS block files (all replicas, all DataNode hosts) in the page cache
# right now (cache-step.py measure: mincore, the same on every cluster; it only
# looks, nothing is read or evicted); -1 if a host could not measure.
cached_input_mb() {
    local total=0 node mb
    for node in "${DATANODE_NODES[@]}"; do
        mb=$(ssh "$node" "python3 /tmp/cache-step.py measure $MOUNT_BASE" 2>/dev/null || true)
        if [[ ! "$mb" =~ ^[0-9]+$ ]]; then
            echo "-1"
            return
        fi
        total=$(( total + mb ))
    done
    echo "$total"
}

# Total sectors read and written on the scratch disks of all DataNode hosts.
# Prints "read_sectors write_sectors" (512-byte sectors, from /proc/diskstats).
disk_counters() {
    local total_r=0 total_w=0 r w node
    for node in "${DATANODE_NODES[@]}"; do
        read -r r w <<< "$(ssh "$node" "awk -v d=${SCRATCH_DEV[$node]} '\$3==d {print \$6, \$10}' /proc/diskstats" 2>/dev/null || echo "0 0")"
        total_r=$(( total_r + ${r:-0} ))
        total_w=$(( total_w + ${w:-0} ))
    done
    echo "$total_r $total_w"
}

# %CPU used by other people's processes (not $USER, not root) on the busiest
# DataNode host, measured over 1 s right before a job. Tapuz is shared: this
# flags jobs that ran next to someone else's work. -1 if pidstat is missing.
other_users_cpu() {
    local node pid max=0 v
    local -a pids=()
    for node in "${DATANODE_NODES[@]}"; do
        ssh "$node" "bash -s" -- "$USER" > "$RUN_DIR/.ocpu_${node}.out" 2>/dev/null <<'OCEOF' &
me=$1
command -v pidstat >/dev/null 2>&1 || { echo -1; exit 0; }
LANG=C pidstat -u -U 1 1 2>/dev/null | awk -v me="$me" '
    /^Average:/ && / USER / { for (i = 1; i <= NF; i++) { if ($i == "USER") u = i; if ($i == "%CPU") c = i }; next }
    /^Average:/ && u && c { if ($u != me && $u != "root") s += $c }
    END { printf "%d\n", s }'
OCEOF
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    for node in "${DATANODE_NODES[@]}"; do
        v=$(head -1 "$RUN_DIR/.ocpu_${node}.out" 2>/dev/null)
        rm -f "$RUN_DIR/.ocpu_${node}.out"
        if [[ "$v" == "-1" ]]; then max=-1; break; fi
        if [[ "$v" =~ ^[0-9]+$ ]] && (( v > max )); then max=$v; fi
    done
    echo "$max"
}

# ---- DataNode metrics per job ----
# Snapshot of every DataNode's DataNodeActivity metrics (JMX) right before
# each job. The difference to the next snapshot (taken >= SETTLE_SECONDS after
# the job, so the DataNode's 10 s metrics cache has refreshed) is that job's
# DataNode work: blocks and bytes served, and the average time to serve one
# block (ReadBlockOp) and to push one packet to the network. Written to
# dn_metrics.csv by analyze-counters.py --dn-delta.
DN_PREV_SNAPSHOT=""
DN_PREV_JOB=""
JOB_SEQ=0
DN_METRICS_CSV="$RUN_DIR/dn_metrics.csv"

dn_snapshot() {
    local out=$1 node js first=1
    {
        echo "{"
        for node in "${DATANODE_NODES[@]}"; do
            js=$(curl -s --max-time 5 "http://${node}:9864/jmx?qry=Hadoop:service=DataNode,name=DataNodeActivity-*" 2>/dev/null || true)
            [[ -z "$js" ]] && js='{}'
            (( first )) || echo ","
            first=0
            printf '"%s": %s\n' "$node" "$js"
        done
        echo "}"
    } > "$out"
}

# Close the previous job's DataNode metrics with snapshot $1.
dn_metrics_checkpoint() {
    local snap=$1 delta
    if [[ -n "$DN_PREV_SNAPSHOT" && -n "$DN_PREV_JOB" ]]; then
        delta=$(python3 "$SCRIPT_DIR/analyze-counters.py" --dn-delta "$DN_PREV_SNAPSHOT" "$snap" 2>/dev/null \
            || echo "0,0,0,-1,-1,-1,-1")
        echo "$DN_PREV_JOB,$delta" >> "$DN_METRICS_CSV"
    fi
    DN_PREV_SNAPSHOT=$snap
    DN_PREV_JOB=""
}

# ---- Server cost of k virtual disks (once per k, cluster idle) ----
# Per DataNode host: DataNode threads, memory (RSS, heap), open files; kernel
# loop/jbd2 threads; mounted loopback filesystems and their block size; from
# the DataNode log of this start: seconds from start to the first full block
# report, and that report's size and timings. Plus cluster setup and input
# upload time. One row per host in server_metrics.csv.
SERVER_CSV="$RUN_DIR/server_metrics.csv"
collect_server_metrics() {
    local k=$1 setup_s=$2 upload_s=$3 node line heap
    for node in "${DATANODE_NODES[@]}"; do
        line=$(ssh "$node" "bash -s" -- "$MOUNT_BASE" <<'SMEOF' 2>/dev/null || true
set +e
mount_base=$1
dnpid=$(pgrep -f '[o]rg.apache.hadoop.hdfs.server.datanode.DataNode' | head -1)
threads=$(ls "/proc/$dnpid/task" 2>/dev/null | wc -l)
rss_kb=$(awk '/^VmRSS:/ {print $2}' "/proc/$dnpid/status" 2>/dev/null)
fds=$(ls "/proc/$dnpid/fd" 2>/dev/null | wc -l)
kthreads=$(ps -e -o comm= 2>/dev/null | grep -cE '^(loop[0-9]+|jbd2/loop[0-9]+)')
mounts=$(grep -c " $mount_base/" /proc/mounts 2>/dev/null)
fsinfo=$(stat -f -c '%S %b %c' "$mount_base/dn1" 2>/dev/null)
log=$(ls -t /scratch/tmp/hadoop_dn_logs/hadoop-*-datanode-*.log 2>/dev/null | head -1)
report=""
if [ -n "$log" ]; then
    report=$(awk '
        $1 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/ { ts = $1 " " $2 }
        /STARTUP_MSG: Starting DataNode/ { start = ts; rep = ""; line = ""; next }
        /Successfully sent block report/ && start != "" && rep == "" { rep = ts; line = $0 }
        END { gsub(/\|/, " ", line); print start "|" rep "|" line }' "$log")
fi
echo "${threads:-0}|${rss_kb:-0}|${fds:-0}|${kthreads:-0}|${mounts:-0}|${fsinfo:-? ? ?}|${report:-||}"
SMEOF
        )
        heap=$(curl -s --max-time 5 "http://${node}:9864/jmx?qry=java.lang:type=Memory" 2>/dev/null | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin)["beans"][0]["HeapMemoryUsage"]["used"] // 1048576)
except Exception:
    print(-1)
' 2>/dev/null || echo -1)
        python3 - "$SERVER_CSV" "$k" "$node" "$setup_s" "$upload_s" "$INPUT_SIZE_MB" "$heap" "$line" <<'PY' || true
import csv, os, re, sys
from datetime import datetime
path, k, node, setup_s, upload_s, input_mb, heap, line = sys.argv[1:9]
f = (line.split("|") + [""] * 8)[:8]
threads, rss_kb, fds, kthreads, mounts, fsinfo, start, rep = f[:8]
report_line = line.split("|", 8)[8] if line.count("|") >= 8 else ""
fs = (fsinfo.split() + ["", "", ""])[:3]
def ts(s):
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S,%f")
    except ValueError:
        return None
t0, t1 = ts(start), ts(rep)
m = re.search(r"containing (\d+) storage report", report_line)
g = re.search(r"took (\d+) msecs? to generate and (\d+) msecs? for RPC", report_line)
upload = float(upload_s or 0)
row = {
    "k": k, "node": node, "cluster_setup_s": setup_s, "upload_s": upload_s,
    "upload_mb_per_s": f"{int(input_mb) / upload:.1f}" if upload > 0 else "",
    "fs_block_size": fs[0], "fs_blocks": fs[1], "fs_inodes": fs[2],
    "dn_threads": threads, "dn_rss_mb": int(rss_kb or 0) // 1024, "dn_fds": fds,
    "dn_heap_used_mb": heap, "kernel_loop_threads": kthreads, "loop_mounts": mounts,
    "dn_start_to_block_report_s": f"{(t1 - t0).total_seconds():.1f}" if t0 and t1 else "",
    "block_report_storages": m.group(1) if m else "",
    "block_report_generate_ms": g.group(1) if g else "",
    "block_report_rpc_ms": g.group(2) if g else "",
}
new = not os.path.exists(path)
with open(path, "a", newline="") as out:
    w = csv.DictWriter(out, fieldnames=list(row))
    if new:
        w.writeheader()
    w.writerow(row)
PY
    done
    log "  Server metrics recorded for k=$k (server_metrics.csv)"
}

EXAMPLES_JAR=$(compgen -G "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar" | head -1 || true)

# Run one WordCount job with map containers of $1 MB; full output to $2.
run_wordcount() {
    local map_mb=$1
    local job_log=$2
    local -a opts=(
        -D "mapreduce.jobhistory.address=${MASTER_NODE}:10020"
        -D "mapreduce.jobhistory.webapp.address=${MASTER_NODE}:19888"
        -D "mapreduce.map.memory.mb=${map_mb}"
        -D "mapreduce.map.java.opts=-Xmx${TASK_HEAP_MB}m"
    )
    if [[ "$WORDCOUNT_MODE" == "trivial" ]]; then
        hadoop jar "$TRIVIAL_WC_JAR" TrivialWordCount "${opts[@]}" \
            /user/$USER/wordcount/input /user/$USER/wordcount/output 2>&1 \
            | tee "$job_log" >> "$LOG_FILE"
    else
        hadoop jar "$EXAMPLES_JAR" wordcount "${opts[@]}" \
            /user/$USER/wordcount/input /user/$USER/wordcount/output 2>&1 \
            | tee "$job_log" >> "$LOG_FILE"
    fi
}

# One job through the per-job protocol, recorded in runs.csv.
# Usage: run_one_job <k> <rep> <pos> <name> <maps> <map_mb> <cache> <ci>
#   rep 0 = untimed warm-up job (ci -1): recorded with status "warmup" and
#   left out of all averages.
declare -A COND_RUNTIMES
FAILED_JOBS=0
run_one_job() {
    local k=$1 rep=$2 pos=$3 name=$4 maps=$5 map_mb=$6 cache=$7 ci=$8
    local job_log

    log ""
    if (( rep == 0 )); then
        job_log="$JOBS_DIR/k${k}_warmup${pos}.log"
        log "  Warm-up job $pos/$WARMUP_JOBS  k=$k  (not measured; maps/node=$maps, cache=$cache)"
    else
        job_log="$JOBS_DIR/k${k}_rep${rep}_${name}.log"
        log "  Rep $rep/$K_REPS  k=$k  condition=$name (maps/node=$maps, map container=${map_mb}MB, cache=$cache)  position $pos/$NUM_CONDITIONS"
    fi

    prepare_job "$cache"

    local rs0 ws0 rs1 ws1 t0 t1 start_epoch end_epoch cached_mb other_cpu snap status=ok
    cached_mb=$(cached_input_mb)
    other_cpu=$(other_users_cpu)
    JOB_SEQ=$(( JOB_SEQ + 1 ))
    snap="$JOBS_DIR/dnjmx_$(printf '%04d' "$JOB_SEQ").json"
    dn_snapshot "$snap"
    dn_metrics_checkpoint "$snap"
    read -r rs0 ws0 <<< "$(disk_counters)"
    start_epoch=$(date +%s)
    t0=$(date +%s.%N)
    if ! run_wordcount "$map_mb" "$job_log"; then
        status=failed
    fi
    t1=$(date +%s.%N)
    end_epoch=$(date +%s)
    read -r rs1 ws1 <<< "$(disk_counters)"
    if ! grep -q "completed successfully" "$job_log"; then
        status=failed
    fi

    if (( rep == 0 )); then
        if [[ "$status" == "ok" ]]; then status=warmup; else status=warmup-failed; fi
    fi
    DN_PREV_JOB="$k,$rep,$pos,$name,$status"

    local runtime disk_read_mb disk_write_mb counters
    local maps_launched data_local map_ms red_ms cpu_ms gc_ms conc avg_map_s
    runtime=$(echo "scale=2; $t1 - $t0" | bc)
    disk_read_mb=$(( (rs1 - rs0) / 2048 ))
    disk_write_mb=$(( (ws1 - ws0) / 2048 ))

    counters=$(python3 "$SCRIPT_DIR/analyze-counters.py" --job-log "$job_log" 2>/dev/null || echo "0 0 0 0 0 0")
    read -r maps_launched data_local map_ms red_ms cpu_ms gc_ms <<< "$counters"
    conc=$(awk -v m="$map_ms" -v r="$runtime" 'BEGIN {printf "%.1f", (r > 0) ? m / 1000 / r : 0}')
    avg_map_s=$(awk -v m="$map_ms" -v n="$maps_launched" 'BEGIN {printf "%.2f", (n > 0) ? m / 1000 / n : 0}')

    echo "$TIMESTAMP,$k,$rep,$pos,$name,$maps,$map_mb,$cache,$status,$start_epoch,$end_epoch,$runtime,$cached_mb,$other_cpu,$disk_read_mb,$disk_write_mb,$maps_launched,$data_local,$map_ms,$red_ms,$cpu_ms,$gc_ms,$conc,$avg_map_s" >> "$RUNS_CSV"
    log "  Runtime: ${runtime}s [$status]  maps: $maps_launched ($data_local data-local), $conc running at once on average, ${avg_map_s}s each; input in page cache at start: ${cached_mb}MB; disk read during job: ${disk_read_mb}MB; other users' CPU before: ${other_cpu}%"

    if (( rep == 0 )); then
        if [[ "$status" != "warmup" ]]; then
            log "  WARNING: warm-up job failed; see $job_log"
        fi
        return 0
    fi
    echo "$start_epoch $end_epoch" >> "$IOSTAT_DIR/wc_windows_k${k}.txt"

    if [[ "$status" == "ok" ]]; then
        COND_RUNTIMES[$ci]+="${runtime};"
    else
        FAILED_JOBS=$(( FAILED_JOBS + 1 ))
        log "  WARNING: job failed; see $job_log"
    fi
}

# Copy the generated Hadoop configs (master + first DataNode host) into the run dir.
save_configs() {
    local k=$1
    local dest="$RUN_DIR/configs/k$k"
    local dn=${DATANODE_NODES[0]}
    mkdir -p "$dest/$MASTER_NODE" "$dest/$dn"
    cp "$CONFIG_DIR"/*.xml "$CONFIG_DIR"/dn-env-override.sh "$CONFIG_DIR"/workers "$dest/$MASTER_NODE/" 2>/dev/null || true
    scp -q "$dn:$CONFIG_DIR/*.xml" "$dn:$CONFIG_DIR/dn-env-override.sh" "$dest/$dn/" 2>/dev/null || true
}

# One results.csv-format row for condition index $1 into file $2.
write_results_row() {
    local ci=$1 csv=$2
    local -a rts=()
    local raw=${COND_RUNTIMES[$ci]:-}
    if [[ -n "$raw" ]]; then
        IFS=';' read -r -a rts <<< "${raw%;}"
    fi
    local avg stddev individual
    avg=$(compute_avg rts)
    stddev=$(compute_stddev rts "$avg")
    individual=$(IFS=";"; echo "${rts[*]}")
    echo "$k,$TOTAL_STORAGE_DIRS,$LIVE_DNS,$avg,$stddev,$individual,$NN_HEAP_BEFORE,$NN_HEAP_PEAK,$NN_HEAP_AVG,$NN_BLOCK_COUNT,$block_counts_per_fs,$input_block_counts_per_fs,$fs_used_mb_per_fs" >> "$csv"
    log "  k=$k  ${COND_NAMES[$ci]}: average ${avg}s  stddev ${stddev}s  runs: $individual"
}

# Stop early if the cluster cannot hold the run.
preflight() {
    local fail=0 tool node avail_gb
    for tool in bc python3 curl hdfs hadoop yarn; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            echo "  missing on $(hostname): $tool"
            fail=1
        fi
    done
    if [[ "$WORDCOUNT_MODE" == "trivial" && ! -f "$TRIVIAL_WC_JAR" ]]; then
        echo "  WORDCOUNT_MODE=trivial but the jar is missing: $TRIVIAL_WC_JAR (run wordcount/trivial/build.sh)"
        fail=1
    fi
    if [[ "$WORDCOUNT_MODE" != "trivial" && -z "$EXAMPLES_JAR" ]]; then
        echo "  hadoop-mapreduce-examples jar not found under $HADOOP_HOME/share/hadoop/mapreduce"
        fail=1
    fi
    # At the largest k, every image must keep at least two blocks free once the
    # input is in: the job's own files and its output need new blocks. (A 90 GB
    # input at k=1024 left ~110 MB per 200 MB image; with 128 MB blocks for
    # those files every job failed at submission.)
    local kmax=0 k img_mb per_vol_mb usable_mb
    for k in "${K_VALUES[@]}"; do (( k > kmax )) && kmax=$k; done
    img_mb=$(calc_image_size_mb "$kmax")
    per_vol_mb=$(( INPUT_SIZE_MB * REPLICATION / ${#DATANODE_NODES[@]} / kmax ))
    usable_mb=$(( img_mb * 88 / 100 ))   # ext4 journal, inode tables and bitmaps take ~10%
    if (( usable_mb - per_vol_mb < 2 * BLOCK_SIZE_MB )); then
        echo "  input too large for k=$kmax: ~${per_vol_mb} MB of blocks per ${img_mb} MB image leaves"
        echo "    less than two ${BLOCK_SIZE_MB} MB blocks free (lower the input or the largest k,"
        echo "    or raise LOOPBACK_BUDGET_PER_NODE_GB)"
        fail=1
    fi
    local need_gb=$(( LOOPBACK_BUDGET_PER_NODE_GB + 5 ))
    for node in "${DATANODE_NODES[@]}"; do
        avail_gb=$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$node" \
            "df -BG --output=avail '$STORAGE_BASE' | tail -1 | tr -dc '0-9'" 2>/dev/null || echo "")
        if [[ -z "$avail_gb" ]]; then
            echo "  cannot reach $node over passwordless ssh"
            fail=1
        elif (( avail_gb < need_gb )); then
            echo "  $node: only ${avail_gb}GB free on $STORAGE_BASE, need ${need_gb}GB"
            echo "    (leftover loop images from an aborted run? bash stop-single-dn-cluster.sh 1024)"
            fail=1
        fi
    done
    return $fail
}

# ============================================================================
# CLEANUP TRAP
# ============================================================================
cleanup() {
    echo ""
    log "Caught interrupt, cleaning up..."
    stop_iostat_monitor
    stop_sysstat_monitors
    stop_nn_monitor
    pkill -P $$ 2>/dev/null || true
    log "Cleanup complete. Partial results in: $RUN_DIR"
    log "The cluster is still up; stop it with: bash $SCRIPT_DIR/stop-single-dn-cluster.sh 1024"
    exit 1
}
trap cleanup SIGINT SIGTERM

# ============================================================================
# MAIN EXPERIMENT
# ============================================================================

case "$K_ORDER" in
    given) ;;
    random) read -r -a K_VALUES <<< "$(seeded_shuffle "$SEED" "${K_VALUES[@]}")" ;;
    *) echo "ERROR: K_ORDER must be given or random" >&2; exit 1 ;;
esac

if [[ -f "$PROJECT_ROOT/VERSION" ]]; then
    CODE_VERSION=$(head -1 "$PROJECT_ROOT/VERSION")
elif git -C "$PROJECT_ROOT" rev-parse --short HEAD >/dev/null 2>&1; then
    CODE_VERSION=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD)
    git -C "$PROJECT_ROOT" diff --quiet 2>/dev/null || CODE_VERSION+="-dirty"
else
    CODE_VERSION="unknown"
fi

echo "============================================================"
echo "Storage Virtualization Loopback Filesystem Experiment"
echo "============================================================"
echo "Run ID:           $TIMESTAMP   (code $CODE_VERSION)"
echo "Cluster:          $CLUSTER"
echo "Input size:       ${INPUT_SIZE_MB}MB in ${BLOCK_SIZE_HUMAN} blocks, uploaded $( [[ "$REUPLOAD_EACH_REP" == "1" ]] && echo "before every job" || echo "once per k")"
echo "Replication:      $REPLICATION"
echo "WordCount mode:   $WORDCOUNT_MODE"
echo "YARN pool:        $SLOTS_PER_NODE x ${CONTAINER_MB}MB per node, task heap ${TASK_HEAP_MB}MB"
echo "Per job:          wait for idle YARN, sync, cache step, ${SETTLE_SECONDS}s pause; $WARMUP_JOBS untimed warm-up job(s) per k"
echo "Loop devices:     mkfs=$MKFS_MODE, direct I/O=$LOOP_DIRECT_IO"
echo "DataNode heap:    ${DN_HEAP_MB}MB"
echo "Conditions:       ${COND_NAMES[*]}"
echo "Master has DN:    $MASTER_HAS_DN"
echo "Storage base:     $STORAGE_BASE  (HADOOP_HOME=$HADOOP_HOME)"
echo "Loopback budget:  ${LOOPBACK_BUDGET_PER_NODE_GB}GB/node (min ${MIN_IMAGE_SIZE_MB}MB/image)"
echo "Physical nodes:   $NUM_PHYSICAL_NODES (${ALL_NODES[*]})"
echo "DataNode hosts:   $NUM_DATANODE_HOSTS (${DATANODE_NODES[*]})"
echo "k values:         ${K_VALUES[*]}  (order: $K_ORDER, seed $SEED)"
echo "Repetitions:      $K_REPS per k and condition"
echo "Results:          $RUN_DIR"
echo ""
echo "Resource plan per k value:"
for k in "${K_VALUES[@]}"; do
    img_mb=$(calc_image_size_mb "$k")
    img_gb=$(awk "BEGIN {printf \"%.2f\", $img_mb/1024}")
    total_dirs=$(( NUM_DATANODE_HOSTS * k ))
    total_disk_mb=$(( img_mb * k ))
    total_disk_gb=$(awk "BEGIN {printf \"%.2f\", $total_disk_mb/1024}")
    echo "  k=$k: $NUM_DATANODE_HOSTS DataNodes (1 per host), $k storage dirs each = ${total_dirs} total dirs, ${img_mb}MB (${img_gb}GB) images x $k = ${total_disk_mb}MB (${total_disk_gb}GB) disk/node"
done
echo "============================================================"
echo ""

log "Pre-flight checks..."
if ! preflight 2>&1 | tee -a "$LOG_FILE"; then
    log "Pre-flight checks failed; nothing was started."
    exit 1
fi

log "Node hardware:"
collect_hardware
copy_node_helpers

# Save metadata
export LOOPBACK_BUDGET_PER_NODE_GB MIN_IMAGE_SIZE_MB K_REPS
export TIMESTAMP INPUT_SIZE_MB BLOCK_SIZE BLOCK_SIZE_MB BLOCK_SIZE_HUMAN REPLICATION
export NUM_PHYSICAL_NODES RUN_DIR NUM_DATANODE_HOSTS MASTER_HAS_DN
export WORDCOUNT_MODE STORAGE_BASE CLUSTER CODE_VERSION SEED K_ORDER REUPLOAD_EACH_REP
export SLOTS_PER_NODE CONTAINER_MB TASK_HEAP_MB DN_HEAP_MB SETTLE_SECONDS WARMUP_JOBS MKFS_MODE LOOP_DIRECT_IO

K_VALUES_CSV=$(IFS=,; echo "${K_VALUES[*]}")
NODE_NAMES_CSV=$(IFS=,; echo "${ALL_NODES[*]}")
DN_HOST_NAMES_CSV=$(IFS=,; echo "${DATANODE_NODES[*]}")
COND_SPEC_CSV=""
for ((ci=0; ci<NUM_CONDITIONS; ci++)); do
    COND_SPEC_CSV+="${COND_NAMES[$ci]}:${COND_MAPS[$ci]}:${COND_MAP_MB[$ci]}:${COND_CACHE[$ci]},"
done
export K_VALUES_CSV NODE_NAMES_CSV DN_HOST_NAMES_CSV COND_SPEC_CSV

python3 - <<'PY' 2>/dev/null || true
import json
import os
from datetime import datetime

def parse_int_list(csv_text: str):
    csv_text = (csv_text or "").strip()
    if not csv_text:
        return []
    return [int(x.strip()) for x in csv_text.split(',') if x.strip()]

conditions = []
for item in os.environ.get("COND_SPEC_CSV", "").split(","):
    if item:
        name, maps, map_mb, cache = item.split(":")
        conditions.append({"name": name, "maps_per_node": int(maps),
                           "map_container_mb": int(map_mb), "cache": cache})

hardware = []
hw_path = os.path.join(os.environ["RUN_DIR"], "hardware.txt")
if os.path.exists(hw_path):
    for line in open(hw_path):
        f = line.strip().split("|")
        if len(f) >= 7:
            hardware.append(dict(zip(
                ["node", "cores", "mem_mb", "kernel", "scratch_device", "rotational", "model",
                 "java", "hadoop"], f)))

meta = {
    "run_id": os.environ["TIMESTAMP"],
    "experiment_type": "storage_virtualization_loopback",
    "cluster": os.environ["CLUSTER"],
    "code_version": os.environ["CODE_VERSION"],
    "input_size_mb": int(os.environ["INPUT_SIZE_MB"]),
    "input_size_gb": round(int(os.environ["INPUT_SIZE_MB"]) / 1024, 3),
    "block_size_bytes": int(os.environ["BLOCK_SIZE"]),
    "block_size_human": os.environ["BLOCK_SIZE_HUMAN"],
    "replication": int(os.environ["REPLICATION"]),
    "physical_nodes": int(os.environ["NUM_PHYSICAL_NODES"]),
    "node_names": [x for x in os.environ.get("NODE_NAMES_CSV", "").split(',') if x],
    "datanode_hosts": int(os.environ["NUM_DATANODE_HOSTS"]),
    "datanode_host_names": [x for x in os.environ.get("DN_HOST_NAMES_CSV", "").split(',') if x],
    "master_has_datanode": os.environ.get("MASTER_HAS_DN", "1") != "0",
    "k_values": parse_int_list(os.environ.get("K_VALUES_CSV", "")),
    "k_order": os.environ["K_ORDER"],
    "seed": int(os.environ["SEED"]),
    "loopback_budget_per_node_gb": int(os.environ["LOOPBACK_BUDGET_PER_NODE_GB"]),
    "min_image_size_mb": int(os.environ["MIN_IMAGE_SIZE_MB"]),
    "repetitions": int(os.environ["K_REPS"]),
    "conditions": conditions,
    "yarn_slots_per_node": int(os.environ["SLOTS_PER_NODE"]),
    "yarn_container_mb": int(os.environ["CONTAINER_MB"]),
    "task_heap_mb": int(os.environ["TASK_HEAP_MB"]),
    "datanode_heap_mb": os.environ["DN_HEAP_MB"],
    "input_uploaded": "every job" if os.environ["REUPLOAD_EACH_REP"] == "1" else "once per k",
    "per_job_protocol": "wait for idle YARN; delete output; sync; cold = posix_fadvise(DONTNEED) on block files + loopback images, warm = read all block files; pause",
    "settle_seconds": int(os.environ["SETTLE_SECONDS"]),
    "warmup_jobs_per_k": int(os.environ["WARMUP_JOBS"]),
    "speculative_execution": False,
    "mkfs_mode": os.environ["MKFS_MODE"],
    "loop_direct_io": os.environ["LOOP_DIRECT_IO"] == "1",
    "wordcount_mode": os.environ.get("WORDCOUNT_MODE", "real"),
    "storage_base": os.environ.get("STORAGE_BASE", "/scratch"),
    "hardware": hardware,
    "start_time": datetime.now().astimezone().isoformat(timespec="seconds"),
}

out_path = os.path.join(os.environ["RUN_DIR"], "metadata.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(meta, f, indent=4)
PY

# Initialize CSVs
RESULTS_HEADER="k_storage_dirs,total_storage_dirs,datanodes,avg_runtime_seconds,stddev_runtime,individual_runtimes,nn_heap_before_mb,nn_heap_peak_mb,nn_heap_avg_mb,nn_block_count,block_counts_per_fs,input_block_counts_per_fs,fs_used_mb_per_fs"
echo "$RESULTS_HEADER" > "$CSV_FILE"
if (( NUM_CONDITIONS > 1 )); then
    for name in "${COND_NAMES[@]}"; do
        echo "$RESULTS_HEADER" > "$RUN_DIR/results_${name}.csv"
    done
fi
echo "run_id,k,rep,order_pos,condition,maps_per_node,map_container_mb,cache,status,start_epoch,end_epoch,runtime_s,cached_input_mb,other_users_cpu_pct,disk_read_mb,disk_write_mb,launched_maps,data_local_maps,total_map_ms,total_reduce_ms,cpu_ms,gc_ms,avg_concurrent_maps,avg_map_s" > "$RUNS_CSV"
echo "k,rep,order_pos,condition,status,dn_read_ops,dn_blocks_read,dn_mb_read,dn_read_block_avg_ms,dn_packet_transfer_avg_us,dn_packet_blocked_on_network_avg_us,dn_total_read_ms" > "$DN_METRICS_CSV"

# ============================================================================
# Iterate over k values
# ============================================================================

for k in "${K_VALUES[@]}"; do
    TOTAL_STORAGE_DIRS=$(( NUM_DATANODE_HOSTS * k ))
    IMAGE_SIZE_MB=$(calc_image_size_mb "$k")
    COND_RUNTIMES=()

    log "Running experiment for k=$k with $TOTAL_STORAGE_DIRS total storage dirs..."

    # -- Step 1: Start the single-DN cluster with k storage dirs --
    log "Starting cluster with k=$k storage dirs per DataNode..."
    # Pass image size in MB to avoid integer truncation (200MB / 1024 = 0GB)
    t_setup0=$(date +%s.%N)
    bash "$SCRIPT_DIR/start-single-dn-cluster.sh" "$k" "$IMAGE_SIZE_MB" "$DN_HEAP_MB" "$REPLICATION" 2>&1 | tee -a "$LOG_FILE"
    SETUP_S=$(echo "scale=1; ($(date +%s.%N) - $t_setup0) / 1" | bc)
    log "Cluster setup for k=$k took ${SETUP_S}s"

    # Record actual live DataNodes
    export HADOOP_CONF_DIR="$CONFIG_DIR"
    LIVE_DNS=$(hdfs dfsadmin -report 2>/dev/null | grep -i "Live datanodes" | grep -o '[0-9]*' || echo "0")
    log "Live DataNodes: $LIVE_DNS (expected $NUM_DATANODE_HOSTS)"
    save_configs "$k"

    # -- Step 2: Generate and upload input data (once for all jobs of this k) --
    log "Generating and uploading ${INPUT_SIZE_MB}MB input..."
    t_up0=$(date +%s.%N)
    upload_input
    UPLOAD_S=$(echo "scale=1; ($(date +%s.%N) - $t_up0) / 1" | bc)
    log "Input uploaded in ${UPLOAD_S}s (generation + HDFS write). HDFS status:"
    hdfs dfs -ls /user/$USER/wordcount/input 2>&1 | tee -a "$LOG_FILE"

    # Snapshot NameNode memory before WordCount
    log "Querying NameNode memory (before WordCount)..."
    sleep 5
    read -r NN_HEAP_BEFORE _ NN_BLOCK_COUNT _ _ <<< "$(query_namenode_jmx)"
    log "  NN heap before: ${NN_HEAP_BEFORE}MB, blocks: $NN_BLOCK_COUNT"

    # -- Step 2.5: Fragmentation snapshot (mentor's filefrag check) --
    # Captures extent counts of loopback images + sampled HDFS block files
    # AFTER input is on disk, BEFORE any MapReduce work touches it.
    log "Measuring fragmentation (filefrag) for k=$k..."
    bash "$SCRIPT_DIR/measure-fragmentation.sh" "$k" "$RUN_DIR/fragmentation" "${DATANODE_NODES[@]}" 2>&1 | tee -a "$LOG_FILE" || \
        log "  WARNING: fragmentation step failed for k=$k (continuing)"

    # -- Step 2.7: untimed warm-up job(s) on the fresh cluster (JIT-compiles the
    #    new DataNode JVMs, first-job YARN overheads), so no condition pays
    #    for being first. Same for every k; full load, cold cache.
    for ((w=1; w<=WARMUP_JOBS; w++)); do
        run_one_job "$k" 0 "$w" warmup "$SLOTS_PER_NODE" "$CONTAINER_MB" cold -1
    done

    # -- Step 2.8: server cost of this k (cluster idle, all loop disks mounted) --
    collect_server_metrics "$k" "$SETUP_S" "$UPLOAD_S"

    # Monitor NameNode memory, disks and the DataNode processes while WordCount runs
    NN_MONITOR_CSV="$NN_MEMORY_DIR/nn_memory_k${k}.csv"
    start_nn_monitor "$NN_MONITOR_CSV" 5
    start_iostat_monitor "$k"
    start_sysstat_monitors "$k"

    # Per-run WC windows (epoch seconds). parse_iostat_logs uses this file to
    # drop iostat samples captured between jobs.
    : > "$IOSTAT_DIR/wc_windows_k${k}.txt"

    # -- Step 3: K_REPS repetitions; all conditions in every repetition, in a
    #    random order (seeded by SEED, k and the repetition number).
    for ((rep=1; rep<=K_REPS; rep++)); do
        read -r -a ORDER <<< "$(seeded_shuffle "$(( SEED + k * 1000 + rep ))" "${!COND_NAMES[@]}")"
        pos=0
        for ci in "${ORDER[@]}"; do
            pos=$(( pos + 1 ))
            run_one_job "$k" "$rep" "$pos" "${COND_NAMES[$ci]}" "${COND_MAPS[$ci]}" \
                "${COND_MAP_MB[$ci]}" "${COND_CACHE[$ci]}" "$ci"
        done
    done

    # Close the last job's DataNode metrics: wait until the DataNodes' 10 s
    # metrics cache has refreshed, then take the final snapshot.
    sleep $(( SETTLE_SECONDS > 12 ? SETTLE_SECONDS : 12 ))
    JOB_SEQ=$(( JOB_SEQ + 1 ))
    final_snap="$JOBS_DIR/dnjmx_$(printf '%04d' "$JOB_SEQ").json"
    dn_snapshot "$final_snap"
    dn_metrics_checkpoint "$final_snap"
    DN_PREV_SNAPSHOT=""

    # -- Step 4: Stop monitors and collect stats --
    stop_iostat_monitor
    stop_sysstat_monitors
    parse_iostat_logs "$k"
    stop_nn_monitor
    NN_HEAP_PEAK=$(get_peak_heap_mb "$NN_MONITOR_CSV")
    NN_HEAP_AVG=$(get_avg_heap_mb "$NN_MONITOR_CSV")
    log "  NN heap peak during WordCount: ${NN_HEAP_PEAK}MB"
    log "  NN heap avg during WordCount:  ${NN_HEAP_AVG}MB"

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
    : > "$RUN_DIR/block_counts_tmp.txt"
    for NODE in "${DATANODE_NODES[@]}"; do
        # Count block DATA files only (exclude .meta checksum files; each block has both)
        # Path: ${MOUNT_BASE}/dn<i>/hdfs_data/current/BP-<pool-id>/current/.../blk_*
        ssh "$NODE" "for i in {1..$k}; do cnt=\$(find ${MOUNT_BASE}/dn\$i/hdfs_data -name 'blk_*' -not -name '*.meta' -type f 2>/dev/null | wc -l); echo -n \"\$cnt;\"; done" 2>/dev/null >> "$RUN_DIR/block_counts_tmp.txt"
    done
    if [[ -f "$RUN_DIR/block_counts_tmp.txt" && -s "$RUN_DIR/block_counts_tmp.txt" ]]; then
        block_counts_per_fs=$(cat "$RUN_DIR/block_counts_tmp.txt")
        # Remove trailing semicolon if present
        block_counts_per_fs="${block_counts_per_fs%;}"
    fi
    rm -f "$RUN_DIR/block_counts_tmp.txt"
    log "Block counts collected: $block_counts_per_fs"

    # Collect per-filesystem used capacity in MB
    log "Collecting per-filesystem used capacity..."
    fs_used_mb_per_fs=""
    : > "$RUN_DIR/fs_used_mb_tmp.txt"
    for NODE in "${DATANODE_NODES[@]}"; do
        # Get used space (in MB) for each loopback filesystem mount point
        ssh "$NODE" "for i in {1..$k}; do
            used=\$(df --output=used -BM \"${MOUNT_BASE}/dn\$i\" 2>/dev/null | tail -1 | tr -dc '0-9')
            echo -n \"\${used:-0};\"
        done" 2>/dev/null >> "$RUN_DIR/fs_used_mb_tmp.txt"
    done
    if [[ -f "$RUN_DIR/fs_used_mb_tmp.txt" && -s "$RUN_DIR/fs_used_mb_tmp.txt" ]]; then
        fs_used_mb_per_fs=$(cat "$RUN_DIR/fs_used_mb_tmp.txt")
        # Remove trailing semicolon if present
        fs_used_mb_per_fs="${fs_used_mb_per_fs%;}"
    fi
    rm -f "$RUN_DIR/fs_used_mb_tmp.txt"
    log "Filesystem capacity collected: $fs_used_mb_per_fs"


    # Collect HDFS block information specifically for INPUT FILES
    log "Collecting HDFS block information for input files..."
    HDFS_FSCK_OUTPUT="$RUN_DIR/hdfs_fsck_k${k}.txt"
    hdfs fsck /user/$USER/wordcount/input -files -blocks -locations > "$HDFS_FSCK_OUTPUT" 2>&1 || true
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
    : > "$RUN_DIR/input_block_counts_tmp.txt"
    for NODE in "${DATANODE_NODES[@]}"; do
        # Copy block IDs file and helper script to remote node
        scp -q "$INPUT_BLOCK_IDS" "$NODE:/tmp/input_block_ids_k${k}.txt" 2>/dev/null || true
        scp -q "$SCRIPT_DIR/count-input-blocks-per-fs.sh" "$NODE:/tmp/count-input-blocks-per-fs.sh" 2>/dev/null || true

        # Run helper script on remote node - pass block IDs file instead of HDFS path
        NODE_OUTPUT=$(ssh "$NODE" "bash /tmp/count-input-blocks-per-fs.sh /tmp/input_block_ids_k${k}.txt $k ${MOUNT_BASE}" 2>/dev/null || echo "")
        if [[ -n "$NODE_OUTPUT" ]]; then
            echo -n "${NODE_OUTPUT};" >> "$RUN_DIR/input_block_counts_tmp.txt"
        fi
    done

    if [[ -f "$RUN_DIR/input_block_counts_tmp.txt" && -s "$RUN_DIR/input_block_counts_tmp.txt" ]]; then
        input_block_counts_per_fs=$(cat "$RUN_DIR/input_block_counts_tmp.txt")
        # Remove trailing semicolon
        input_block_counts_per_fs="${input_block_counts_per_fs%;}"
    fi
    rm -f "$RUN_DIR/input_block_counts_tmp.txt"
    log "Input block counts per FS: $input_block_counts_per_fs"

    # Parse FSCK to extract input file block distribution summary
    log "Generating input block distribution summary..."
    INPUT_BLOCK_OUTPUT="$RUN_DIR/input_block_dist_k${k}.txt"
    # Use an unquoted heredoc so shell variables expand into the Python script.
    # The Python analyzer will catch exceptions and exit 0 so the main script continues.
    python3 > "$INPUT_BLOCK_OUTPUT" 2>&1 <<PARSE_BLOCKS || true
import re
import sys
from collections import defaultdict
try:
    fsck_file = "${HDFS_FSCK_OUTPUT}"
    input_size_mb = int(${INPUT_SIZE_MB})
    replication = int(${REPLICATION})
    k = int(${k})
    block_size_bytes = int(${BLOCK_SIZE})

    expected_blocks = (input_size_mb * 1024 * 1024) // block_size_bytes
    expected_replicas = expected_blocks * replication

    print(f"Expected blocks for {input_size_mb}MB input: {expected_blocks}")
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

    # -- Step 5: Record per-k results (runtime + NameNode memory + block/FS stats) --
    log ""
    log "  k=$k  NN: heap_before=${NN_HEAP_BEFORE}MB peak=${NN_HEAP_PEAK}MB avg=${NN_HEAP_AVG}MB blocks=$NN_BLOCK_COUNT"
    write_results_row 0 "$CSV_FILE"
    if (( NUM_CONDITIONS > 1 )); then
        for ((ci=0; ci<NUM_CONDITIONS; ci++)); do
            write_results_row "$ci" "$RUN_DIR/results_${COND_NAMES[$ci]}.csv"
        done
    fi

    # -- Step 6: Clean up HDFS data (output already removed above; this clears input too) --
    log "Cleaning HDFS data..."
    hdfs dfs -rm -r -f /user/$USER/wordcount 2>/dev/null || true

    # -- Step 7: Stop the cluster and tear down loopback FSes --
    log "Stopping cluster..."
    bash "$SCRIPT_DIR/stop-single-dn-cluster.sh" "$k" 2>&1 | tee -a "$LOG_FILE"

    log "k=$k complete."
    log ""
done

# ============================================================================
# RESTORE NORMAL CLUSTER
# ============================================================================
if [[ "$RESTORE_BASE_CLUSTER" == "1" ]]; then
    log ""
    bash "$SCRIPT_DIR/restore-base-cluster.sh" 2>&1 | tee -a "$LOG_FILE" || true
fi

# ============================================================================
# SUMMARY
# ============================================================================
ln -sfn "$RUN_DIR" "$RESULTS_BASE/latest"

python3 - "$RUN_DIR/metadata.json" "$(date -Iseconds)" "$FAILED_JOBS" <<'PY' 2>/dev/null || true
import json, sys
path, end_time, failed = sys.argv[1], sys.argv[2], int(sys.argv[3])
with open(path) as f:
    meta = json.load(f)
meta["end_time"] = end_time
meta["failed_jobs"] = failed
with open(path, "w") as f:
    json.dump(meta, f, indent=4)
PY

python3 "$SCRIPT_DIR/summarize-runs.py" "$RUN_DIR" 2>&1 | tee "$RUN_DIR/summary.txt" || true

echo ""
echo "============================================================"
echo "Experiment Complete!  ($FAILED_JOBS failed jobs)"
echo "============================================================"
echo ""
echo "Results saved to: $RUN_DIR"
echo "  - summary.txt / runs.csv          : per-job results and the comparison table"
echo "  - results.csv                     : per-k summary (NameNode memory, blocks per filesystem)"
echo "  - metadata.json, configs/, jobs/  : settings, Hadoop configs, job output"
echo "  - namenode_memory/, iostat/, sysstat/ : monitors"
echo ""
REL_RUN=${RUN_DIR#"$PROJECT_ROOT/results/"}
echo "Report with figures (needs matplotlib):"
echo "  here:    python3 $SCRIPT_DIR/final-report.py $RUN_DIR"
echo "  laptop:  .\\sync-cluster.ps1 pull, then in PowerShell (in my_scripts):"
echo "           python experiments\\storage_virtualization_loopback\\final-report.py ..\\${REL_RUN//\//\\}"
echo "============================================================"

# Missing cells make the comparison incomplete: say so with the exit status,
# so run-all.sh stops (or marks an extra stage failed) instead of going on.
if (( FAILED_JOBS > 0 )); then
    echo "WARNING: $FAILED_JOBS measured job(s) failed (status 'failed' in runs.csv; see jobs/*.log)."
    exit 3
fi
