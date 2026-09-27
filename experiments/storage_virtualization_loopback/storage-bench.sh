#!/bin/bash
################################################################################
# SCRIPT: storage-bench.sh
# DESCRIPTION: The storage stack alone, without Hadoop. Does the slowdown at
#              large k already come from the operating system (loop devices,
#              k ext4 filesystems on one disk), or from HDFS / the DataNode?
#
#   On every DataNode host in parallel, for each k:
#     1. set up k loopback filesystems exactly like the HDFS runs (same image
#        sizes, mkfs mode, direct-I/O setting)
#     2. write the data one DataNode holds in the 2x2 run (input x 3 replicas
#        / hosts) as block-sized files, spread round-robin over the k
#        filesystems, like the DataNode's volume choice
#     3. read it back with 1 and with SLOTS_PER_NODE parallel readers (like 1
#        and 8 map tasks per node), cold and warm (same cache step as the
#        HDFS runs), REPS times each, in a random order
#     4. tear down
#   Per host and read: wall time, MB/s, CPU split (user / system / iowait)
#   and MB read from disk.
#
# USAGE: [VAR=value ...] bash storage-bench.sh [REPS]
#   REPS           repetitions per cell (default 3)
#   K_VALUES       default "1 256 1024"
#   READERS        default "1 $SLOTS_PER_NODE"
#   CACHES         default "cold warm"; "cold" when the data cannot fit in RAM
#   BENCH_DATA_MB  data per host (default: MATRIX_INPUT_GB x 1024 x 3 / hosts)
#
# OUTPUT: results/storage_bench_<cluster>/run_<timestamp>/ (or RESULTS_BASE):
#   bench.csv (one row per host and read), writes.csv, summary.txt,
#   metadata.json, hardware.txt, bench.log
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/cluster.conf"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
export USER="${USER:-$(id -un)}"

REPS=${1:-${REPS:-3}}
read -r -a K_VALUES <<< "${K_VALUES:-1 256 1024}"
read -r -a READERS <<< "${READERS:-1 $SLOTS_PER_NODE}"
read -r -a CACHES <<< "${CACHES:-cold warm}"
for c in "${CACHES[@]}"; do
    [[ "$c" == "cold" || "$c" == "warm" ]] || { echo "CACHES: '$c' is not cold or warm" >&2; exit 1; }
done
MIN_IMAGE_SIZE_MB=100
SEED=${SEED:-$(date +%s)}

MASTER_HAS_DN=${MASTER_HAS_DN:-0}
if [[ "$MASTER_HAS_DN" == "0" ]]; then
    DATANODE_NODES=("${WORKER_NODES[@]}")
else
    DATANODE_NODES=("${ALL_NODES[@]}")
fi
NUM_HOSTS=${#DATANODE_NODES[@]}
BENCH_DATA_MB=${BENCH_DATA_MB:-$(( MATRIX_INPUT_GB * 1024 * 3 / NUM_HOSTS ))}
FILE_MB=$BLOCK_SIZE_MB
NUM_FILES=$(( BENCH_DATA_MB / FILE_MB ))

RESULTS_BASE="${RESULTS_BASE:-$PROJECT_ROOT/results/storage_bench_${CLUSTER}}"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_BASE/run_$TIMESTAMP"
mkdir -p "$RUN_DIR"
LOG_FILE="$RUN_DIR/bench.log"
CSV="$RUN_DIR/bench.csv"
WCSV="$RUN_DIR/writes.csv"

log() {
    echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

calc_image_size_mb() {
    local image_mb=$(( (LOOPBACK_BUDGET_PER_NODE_GB * 1024) / $1 ))
    if (( image_mb < MIN_IMAGE_SIZE_MB )); then
        image_mb=$MIN_IMAGE_SIZE_MB
    fi
    echo "$image_mb"
}

seeded_shuffle() {
    python3 -c 'import random, sys
items = sys.argv[2:]
random.Random(int(sys.argv[1])).shuffle(items)
print(" ".join(items))' "$@"
}

# Run "$@" (a command string) on every host in parallel via ssh; output of
# host N goes to $RUN_DIR/.out_<host>. Returns non-zero if any host failed.
on_all_hosts() {
    local cmd=$1 node pid rc=0
    local -a pids=()
    for node in "${DATANODE_NODES[@]}"; do
        ssh "$node" "$cmd" > "$RUN_DIR/.out_${node}" 2>&1 &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" || rc=1
    done
    return $rc
}

# Node side: write NUM_FILES files of FILE_MB, round-robin over the k filesystems.
# Prints the seconds taken (including sync).
WRITE_SCRIPT='k=$1; base=$2; n=$3; fmb=$4
t0=$(date +%s.%N)
for ((i=0; i<n; i++)); do
    d="$base/dn$(( i % k + 1 ))/bench"
    mkdir -p "$d"
    dd if=/dev/zero of="$d/f_$(printf "%05d" "$i")" bs=1M count="$fmb" status=none
done
sync
awk -v a="$t0" -v b="$(date +%s.%N)" "BEGIN {printf \"%.2f\\n\", b - a}"'

# Node side: read all bench files with R parallel readers (reader r reads
# files r, r+R, r+2R, ... in write order, so consecutive files come from
# different filesystems). Prints: seconds files user sys iowait idle
# (CPU ticks over all cores) disk_read_mb clk_tck.
READ_SCRIPT='readers=$1; base=$2; dev=$3
mapfile -t files < <(find "$base"/dn*/bench -type f -name "f_*" -printf "%f %p\n" 2>/dev/null | sort | cut -d" " -f2)
n=${#files[@]}
cpu() { awk "/^cpu / {print \$2+\$3, \$4+\$7+\$8, \$6, \$5}" /proc/stat; }
disk() { awk -v d="$dev" "\$3==d {print \$6}" /proc/diskstats; }
read -r u0 s0 w0 i0 <<< "$(cpu)"; d0=$(disk)
t0=$(date +%s.%N)
pids=()
for ((r=0; r<readers; r++)); do
    ( for ((i=r; i<n; i+=readers)); do dd if="${files[$i]}" of=/dev/null bs=4M status=none; done ) &
    pids+=($!)
done
for p in "${pids[@]}"; do wait "$p"; done
t1=$(date +%s.%N)
read -r u1 s1 w1 i1 <<< "$(cpu)"; d1=$(disk)
echo "$(awk -v a="$t0" -v b="$t1" "BEGIN {printf \"%.3f\", b - a}") $n $((u1-u0)) $((s1-s0)) $((w1-w0)) $((i1-i0)) $(( (${d1:-0} - ${d0:-0}) / 2048 )) $(getconf CLK_TCK 2>/dev/null || echo 100)"'

trap 'log "Interrupted; tearing down"; bash "$SCRIPT_DIR/stop-single-dn-cluster.sh" 1024 >> "$LOG_FILE" 2>&1; exit 1' SIGINT SIGTERM

echo "============================================================"
echo "Storage-only benchmark (no Hadoop)"
echo "============================================================"
echo "Cluster:        $CLUSTER   hosts: ${DATANODE_NODES[*]}"
echo "k values:       ${K_VALUES[*]}"
echo "Data per host:  ${BENCH_DATA_MB}MB = $NUM_FILES files x ${FILE_MB}MB"
echo "Readers:        ${READERS[*]}   cache: ${CACHES[*]}   reps: $REPS   seed: $SEED"
echo "Loop devices:   mkfs=$MKFS_MODE, direct I/O=$LOOP_DIRECT_IO, budget ${LOOPBACK_BUDGET_PER_NODE_GB}GB/host"
echo "Results:        $RUN_DIR"
echo "============================================================"

log "Stopping any Hadoop cluster and removing old loopback disks..."
bash "$SCRIPT_DIR/stop-single-dn-cluster.sh" 1024 >> "$LOG_FILE" 2>&1 || true

log "Hosts:"
declare -A SCRATCH_DEV
: > "$RUN_DIR/hardware.txt"
for node in "${DATANODE_NODES[@]}"; do
    line=$(ssh "$node" "bash -s" -- "$node" "$STORAGE_BASE" "$HADOOP_HOME" < "$SCRIPT_DIR/node-info.sh" 2>/dev/null || true)
    if [[ -z "$line" ]]; then
        log "ERROR: cannot reach $node"
        exit 1
    fi
    echo "$line" >> "$RUN_DIR/hardware.txt"
    SCRATCH_DEV[$node]=$(echo "$line" | cut -d'|' -f5)
    log "  $line"
    avail_gb=$(ssh "$node" "df -BG --output=avail '$STORAGE_BASE' | tail -1 | tr -dc '0-9'" 2>/dev/null || echo 0)
    if (( ${avail_gb:-0} < LOOPBACK_BUDGET_PER_NODE_GB + 5 )); then
        log "ERROR: $node has only ${avail_gb}GB free on $STORAGE_BASE, need $(( LOOPBACK_BUDGET_PER_NODE_GB + 5 ))GB"
        exit 1
    fi
    scp -q "$SCRIPT_DIR/setup-loopback-fs.sh" "$SCRIPT_DIR/teardown-loopback-fs.sh" "$SCRIPT_DIR/cache-step.py" "$node:/tmp/"
done

K_CSV=$(IFS=,; echo "${K_VALUES[*]}")
R_CSV=$(IFS=,; echo "${READERS[*]}")
python3 - "$RUN_DIR" "$CLUSTER" "$K_CSV" "$R_CSV" "$REPS" "$SEED" "$BENCH_DATA_MB" "$FILE_MB" \
    "$MKFS_MODE" "$LOOP_DIRECT_IO" "$LOOPBACK_BUDGET_PER_NODE_GB" "$SETTLE_SECONDS" "${CACHES[*]}" <<'PY'
import json, os, sys
from datetime import datetime
(run_dir, cluster, ks, readers, reps, seed, data_mb, file_mb,
 mkfs, dio, budget, settle, caches) = sys.argv[1:14]
hardware = []
for line in open(os.path.join(run_dir, "hardware.txt")):
    f = line.strip().split("|")
    hardware.append(dict(zip(["node", "cores", "mem_mb", "kernel", "scratch_device",
                              "rotational", "model", "java", "hadoop"], f)))
meta = {
    "experiment_type": "storage_bench", "cluster": cluster,
    "k_values": [int(x) for x in ks.split(",")], "readers": [int(x) for x in readers.split(",")],
    "caches": caches.split(), "repetitions": int(reps), "seed": int(seed), "data_mb_per_host": int(data_mb),
    "file_mb": int(file_mb), "mkfs_mode": mkfs, "loop_direct_io": dio == "1",
    "loopback_budget_per_node_gb": int(budget), "settle_seconds": int(settle),
    "cache_step": "same as the HDFS runs (cache-step.py)", "hardware": hardware,
    "start_time": datetime.now().astimezone().isoformat(timespec="seconds"),
}
json.dump(meta, open(os.path.join(run_dir, "metadata.json"), "w"), indent=4)
PY

echo "node,k,readers,cache,rep,order_pos,seconds,mb,mb_per_s,user_ticks,sys_ticks,iowait_ticks,idle_ticks,clk_tck,disk_read_mb" > "$CSV"
echo "node,k,seconds,mb,mb_per_s" > "$WCSV"

CELLS=()
for r in "${READERS[@]}"; do
    for c in "${CACHES[@]}"; do
        CELLS+=("${r}:${c}")
    done
done

for k in "${K_VALUES[@]}"; do
    image_mb=$(calc_image_size_mb "$k")
    log ""
    log "=== k=$k: $k loopback filesystems of ${image_mb}MB per host ==="
    if ! on_all_hosts "bash /tmp/setup-loopback-fs.sh $k $image_mb $IMAGE_DIR $MOUNT_BASE $LOOP_DIRECT_IO $MKFS_MODE"; then
        for node in "${DATANODE_NODES[@]}"; do tail -5 "$RUN_DIR/.out_${node}" | tee -a "$LOG_FILE"; done
        log "ERROR: loopback setup failed for k=$k"
        exit 1
    fi
    for node in "${DATANODE_NODES[@]}"; do cat "$RUN_DIR/.out_${node}" >> "$LOG_FILE"; done

    log "Writing ${BENCH_DATA_MB}MB per host..."
    on_all_hosts "bash -c '$WRITE_SCRIPT' _ $k $MOUNT_BASE $NUM_FILES $FILE_MB" || { log "ERROR: write failed"; exit 1; }
    for node in "${DATANODE_NODES[@]}"; do
        secs=$(tail -1 "$RUN_DIR/.out_${node}")
        echo "$node,$k,$secs,$BENCH_DATA_MB,$(awk -v s="$secs" -v m="$BENCH_DATA_MB" 'BEGIN {printf "%.1f", (s > 0) ? m / s : 0}')" >> "$WCSV"
        log "  $node: wrote in ${secs}s"
    done

    for ((rep=1; rep<=REPS; rep++)); do
        read -r -a ORDER <<< "$(seeded_shuffle "$(( SEED + k * 1000 + rep ))" "${CELLS[@]}")"
        pos=0
        for cell in "${ORDER[@]}"; do
            pos=$(( pos + 1 ))
            readers=${cell%%:*}
            cache=${cell##*:}
            on_all_hosts "python3 /tmp/cache-step.py $cache $MOUNT_BASE $IMAGE_DIR 'dn*/bench/f_*'" || true
            sleep "$SETTLE_SECONDS"
            # Each host reads its own disk; the device name differs per host.
            pids=()
            for node in "${DATANODE_NODES[@]}"; do
                ssh "$node" "bash -c '$READ_SCRIPT' _ $readers $MOUNT_BASE ${SCRATCH_DEV[$node]}" \
                    > "$RUN_DIR/.out_${node}" 2>&1 &
                pids+=($!)
            done
            for pid in "${pids[@]}"; do wait "$pid" || true; done
            secs_all=""
            for node in "${DATANODE_NODES[@]}"; do
                read -r secs n u s w i d hz < <(tail -1 "$RUN_DIR/.out_${node}") || true
                mb=$(( ${n:-0} * FILE_MB ))
                echo "$node,$k,$readers,$cache,$rep,$pos,$secs,$mb,$(awk -v s="$secs" -v m="$mb" 'BEGIN {printf "%.1f", (s > 0) ? m / s : 0}'),$u,$s,$w,$i,$hz,$d" >> "$CSV"
                secs_all+=" $secs"
            done
            log "  rep $rep  k=$k  readers=$readers  $cache: seconds per host:$secs_all"
        done
    done

    log "Tearing down k=$k..."
    on_all_hosts "bash /tmp/teardown-loopback-fs.sh $k $IMAGE_DIR $MOUNT_BASE" >> "$LOG_FILE" 2>&1 || true
done
rm -f "$RUN_DIR"/.out_*

python3 - "$RUN_DIR/metadata.json" "$(date -Iseconds)" <<'PY' || true
import json, sys
meta = json.load(open(sys.argv[1]))
meta["end_time"] = sys.argv[2]
json.dump(meta, open(sys.argv[1], "w"), indent=4)
PY

python3 "$SCRIPT_DIR/final-report.py" --bench-summary "$RUN_DIR" 2>&1 | tee "$RUN_DIR/summary.txt" || true
ln -sfn "$RUN_DIR" "$RESULTS_BASE/latest"
log "Done: $RUN_DIR"
