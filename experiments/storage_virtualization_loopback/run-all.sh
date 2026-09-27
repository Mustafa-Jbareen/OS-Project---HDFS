#!/bin/bash
################################################################################
# SCRIPT: run-all.sh
# DESCRIPTION: The whole experiment in one command. Stages run in order; the
#              smoke test must pass (READY) before anything long starts.
#
#   0 clean     clean-scratch.sh: stop any Hadoop cluster, remove leftover
#               loopback disks, input copies, YARN caches and old logs
#   1 smoke     small version of the main run (k=1 and 4, 1 repetition; same
#               input, conditions and protocol) + checks.
#               NOT READY -> print what failed and stop.
#   2 main      load (1 / middle / all maps per node) x cache (cold / warm) at
#               k = 1 64 256 512 1024 in random order -- the main result
#   3 bench     storage-only benchmark, no Hadoop (1 vs all readers x cold/warm)
#   4 directio  control: loop devices with direct I/O (no second cached copy),
#               all maps per node, cold + warm, k = 1 and 1024
#   5 mkfs      control: mkfs's default layout (what all runs before
#               September 2026 used), all maps per node, cold, k = 1 and 1024
#   6 report    FINAL_REPORT.md from all stages; restore the normal cluster
#
# USAGE (on the cluster master, inside screen):
#   bash run-all.sh                 all stages, 5 repetitions  (~13 h on tapuz)
#   bash run-all.sh --reps 3        3 repetitions               (~9 h)
#   bash run-all.sh --from 3        continue the latest pipeline at stage 3
#   bash run-all.sh --only 1        run a single stage (1 = just the smoke test)
#   bash run-all.sh --input-gb 100 --block-mb 16 --reps 3
#                                   the same pipeline with another input (below)
#
# Stages 1 and 2 are required: if either fails the pipeline stops. Stages 3-5
# are extras: a failure is noted and the pipeline goes on to the report.
#
# OTHER INPUTS: --input-gb and --block-mb set the input of stages 2, 4 and 5
# (default: 2 GB in 32 MB blocks); the bench writes the same amount per host
# in files of the block size. The smoke test keeps its 2 GB input (with the
# chosen block size), so it still takes ~30 min. If the block files per
# worker (input x 3 replicas / workers) exceed half of a worker's RAM, a warm
# cache cannot exist: every warm condition is dropped, so the main run is
# 1 / middle / all maps per node, all cold, and the bench and the direct-I/O
# control run cold only. The folder is then named
# pipeline_<timestamp>_<N>GB_<M>MB. --from and --only continue a pipeline
# with the input it was started with.
#
# OUTPUT: results/pipeline_<timestamp>/ with 1_smoke/ 2_main/ 3_bench/
#         4_directio/ 5_mkfs/ figures/ FINAL_REPORT.md pipeline.log stages.env
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/cluster.conf"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

# The stages never restore the normal cluster; that happens once at the end.
RESTORE_AT_END=$RESTORE_BASE_CLUSTER
export RESTORE_BASE_CLUSTER=0

REPS=5
FROM=0
ONLY=""
INPUT_GB=""
BLOCK_MB=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --reps) REPS=${2:?"--reps needs a number"}; shift 2 ;;
        --from) FROM=${2:?"--from needs a stage number"}; shift 2 ;;
        --only) ONLY=${2:?"--only needs a stage number"}; shift 2 ;;
        --input-gb) INPUT_GB=${2:?"--input-gb needs a size in GB"}; shift 2 ;;
        --block-mb) BLOCK_MB=${2:?"--block-mb needs a size in MB"}; shift 2 ;;
        -h|--help) sed -n '2,/^####/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//; /^###/d'; exit 0 ;;
        *) echo "unknown argument: $1 (see --help)" >&2; exit 1 ;;
    esac
done
for v in "$REPS" "$FROM" "${ONLY:-0}" "${INPUT_GB:-1}" "${BLOCK_MB:-1}"; do
    [[ "$v" =~ ^[0-9]+$ ]] || { echo "--reps/--from/--only/--input-gb/--block-mb need a number" >&2; exit 1; }
done

MAIN_K=${MAIN_K_VALUES:-"1 64 256 512 1024"}
CONTROL_K=${CONTROL_K_VALUES:-"1 1024"}
BENCH_K=${BENCH_K_VALUES:-"1 256 1024"}
SMOKE_K=${SMOKE_K_VALUES:-"1 4"}
BENCH_REPS=${BENCH_REPS:-3}

RESULTS_ROOT="$PROJECT_ROOT/results"
continue_latest=0
if (( FROM >= 2 )); then continue_latest=1; fi
if [[ -n "$ONLY" ]] && (( ONLY >= 2 )); then continue_latest=1; fi
if (( continue_latest )); then
    PIPE_DIR=$(ls -d "$RESULTS_ROOT"/pipeline_2* 2>/dev/null | sort | tail -1 || true)
    if [[ -z "$PIPE_DIR" ]]; then
        echo "No earlier pipeline in $RESULTS_ROOT to continue." >&2
        exit 1
    fi
    # Continue with the input the pipeline was started with.
    rec_in=$(grep -m1 '^PIPE_INPUT_GB=' "$PIPE_DIR/stages.env" 2>/dev/null | cut -d= -f2 || true)
    rec_blk=$(grep -m1 '^PIPE_BLOCK_MB=' "$PIPE_DIR/stages.env" 2>/dev/null | cut -d= -f2 || true)
    rec_in=${rec_in:-$MATRIX_INPUT_GB}
    rec_blk=${rec_blk:-$BLOCK_SIZE_MB}
    if [[ -n "$INPUT_GB" && "$INPUT_GB" != "$rec_in" ]] || [[ -n "$BLOCK_MB" && "$BLOCK_MB" != "$rec_blk" ]]; then
        echo "$PIPE_DIR was started with $rec_in GB in $rec_blk MB blocks;" >&2
        echo "continue it with the same --input-gb/--block-mb, or leave them out." >&2
        exit 1
    fi
    INPUT_GB=$rec_in
    BLOCK_MB=$rec_blk
else
    INPUT_GB=${INPUT_GB:-$MATRIX_INPUT_GB}
    BLOCK_MB=${BLOCK_MB:-$BLOCK_SIZE_MB}
    suffix=""
    if [[ "$INPUT_GB" != "$MATRIX_INPUT_GB" || "$BLOCK_MB" != "$BLOCK_SIZE_MB" ]]; then
        suffix="_${INPUT_GB}GB_${BLOCK_MB}MB"
    fi
    PIPE_DIR="$RESULTS_ROOT/pipeline_$(date +%Y-%m-%d_%H-%M-%S)$suffix"
fi

# A warm cache needs the block files of a worker (input x 3 replicas spread
# over the workers) to fit in its RAM with room to spare.
NUM_WORKERS=${#WORKER_NODES[@]}
DATA_PER_WORKER_MB=$(( INPUT_GB * 1024 * 3 / NUM_WORKERS ))
WORKER_RAM_MB=$(ssh -o BatchMode=yes -o ConnectTimeout=10 "${WORKER_NODES[0]}" \
    "awk '/^MemTotal/ {print int(\$2 / 1024)}' /proc/meminfo" 2>/dev/null || true)
[[ "$WORKER_RAM_MB" =~ ^[0-9]+$ ]] || WORKER_RAM_MB=8192
COLD_ONLY=0
if (( DATA_PER_WORKER_MB * 2 > WORKER_RAM_MB )); then
    COLD_ONLY=1
fi

# Conditions of the main run: the 2x2 (1 vs all maps per node x cold vs warm)
# plus a middle load level, cold only -- or only the cold ones when the input
# cannot stay in RAM.
MID=$(( SLOTS_PER_NODE / 2 ))
MID_COND=""
if (( MID > 1 && MID < SLOTS_PER_NODE )); then
    MID_COND=" maps=$MID,cache=cold"
fi
if (( COLD_ONLY )); then
    CONDITIONS_MAIN="maps=1,cache=cold${MID_COND} maps=all,cache=cold"
    CONDITIONS_DIO="maps=all,cache=cold"
    BENCH_CACHES="cold"
else
    CONDITIONS_MAIN="maps=1,cache=cold maps=1,cache=warm${MID_COND} maps=all,cache=cold maps=all,cache=warm"
    CONDITIONS_DIO="maps=all,cache=cold maps=all,cache=warm"
    BENCH_CACHES="cold warm"
fi

mkdir -p "$PIPE_DIR"
touch "$PIPE_DIR/stages.env"
PLOG="$PIPE_DIR/pipeline.log"
ln -sfn "$PIPE_DIR" "$RESULTS_ROOT/pipeline_latest"

plog() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$PLOG"
}
set_stage() {
    { grep -v "^$1=" "$PIPE_DIR/stages.env" || true; echo "$1=$2"; } > "$PIPE_DIR/stages.env.tmp"
    mv "$PIPE_DIR/stages.env.tmp" "$PIPE_DIR/stages.env"
}
latest_run() {
    ls -d "$1"/run_* 2>/dev/null | sort | tail -1 || true
}
want() {
    if [[ -n "$ONLY" ]]; then [[ "$1" == "$ONLY" ]]; else (( $1 >= FROM )); fi
}
# Cleaning is cheap and every Hadoop stage needs it (leftover loopback images
# of an aborted run would fail the free-space check), so it runs before
# stages 1 and 2 however they are started.
want_clean() {
    if [[ -n "$ONLY" ]]; then (( ONLY <= 2 )); else (( FROM <= 2 )); fi
}
# Run a stage command with its output on screen and in pipeline.log.
# Sets STAGE_RC.
run_logged() {
    STAGE_RC=0
    if "$@" 2>&1 | tee -a "$PLOG"; then STAGE_RC=0; else STAGE_RC=$?; fi
}
stage_header() {
    plog ""
    plog "================================================================"
    plog "$1"
    plog "================================================================"
}

set_stage PIPE_INPUT_GB "$INPUT_GB"
set_stage PIPE_BLOCK_MB "$BLOCK_MB"

plog "Pipeline: $PIPE_DIR"
plog "Cluster $CLUSTER; repetitions $REPS; main k: $MAIN_K; controls k: $CONTROL_K; bench k: $BENCH_K"
plog "Input: $INPUT_GB GB in $BLOCK_MB MB blocks = $DATA_PER_WORKER_MB MB of block files per worker (RAM $WORKER_RAM_MB MB); smoke test: $MATRIX_INPUT_GB GB"
if (( COLD_ONLY )); then
    plog "That cannot stay in RAM, so every condition is cold (main run, bench, direct-I/O control)."
fi
plog "Main conditions: $CONDITIONS_MAIN"
if [[ "$INPUT_GB" == "$MATRIX_INPUT_GB" ]]; then
    plog "Rough duration on tapuz: smoke ~35 min, main ~$(( REPS * 5 / 3 + 1 )) h, bench ~45 min, controls ~2-3 h"
fi
PIPE_T0=$(date +%s)

# ---------------------------------------------------------------- stage 0
if want_clean; then
    stage_header "Stage 0: clean -- stop Hadoop, remove loopback disks and old leftovers in $STORAGE_BASE"
    run_logged bash "$SCRIPT_DIR/clean-scratch.sh"
    plog "Stage 0 done."
fi

# ---------------------------------------------------------------- stage 1
if want 1; then
    stage_header "Stage 1: smoke test (k = $SMOKE_K, 1 repetition, all main conditions)"
    run_logged env RESULTS_BASE="$PIPE_DIR/1_smoke" CONDITIONS="$CONDITIONS_MAIN" K_VALUES="$SMOKE_K" \
        INPUT_SIZE_GB="$MATRIX_INPUT_GB" BLOCK_SIZE_MB="$BLOCK_MB" \
        bash "$SCRIPT_DIR/run-2x2.sh" smoke
    run_dir=$(latest_run "$PIPE_DIR/1_smoke")
    set_stage SMOKE_RUN "${run_dir#"$PIPE_DIR"/}"
    if (( STAGE_RC != 0 )); then
        set_stage SMOKE_STATUS failed
        plog ""
        plog "STOPPED: the smoke test is NOT READY, so the long stages were not started."
        if [[ -n "$run_dir" && -f "$run_dir/checks.txt" ]]; then
            plog "Failed checks:"
            grep -E "^ *FAIL" "$run_dir/checks.txt" | tee -a "$PLOG" || true
        else
            plog "The smoke run itself stopped with an error; see the messages above and $PLOG."
        fi
        plog "Results of the smoke test: ${run_dir:-$PIPE_DIR/1_smoke}"
        plog "After fixing the problem, start again with: bash run-all.sh"
        exit 1
    fi
    set_stage SMOKE_STATUS ok
    plog "Stage 1 passed: READY."
fi

# ---------------------------------------------------------------- stage 2
if want 2; then
    stage_header "Stage 2: main run (k = $MAIN_K, random order, $REPS repetitions)"
    run_logged env RESULTS_BASE="$PIPE_DIR/2_main" CONDITIONS="$CONDITIONS_MAIN" K_VALUES="$MAIN_K" K_ORDER=random \
        INPUT_SIZE_GB="$INPUT_GB" BLOCK_SIZE_MB="$BLOCK_MB" \
        bash "$SCRIPT_DIR/run-2x2.sh" "$REPS"
    run_dir=$(latest_run "$PIPE_DIR/2_main")
    set_stage MAIN_RUN "${run_dir#"$PIPE_DIR"/}"
    if (( STAGE_RC != 0 )) || [[ -z "$run_dir" ]]; then
        set_stage MAIN_STATUS failed
        plog "STOPPED: the main run failed (exit $STAGE_RC). See $PLOG and ${run_dir:-$PIPE_DIR/2_main}."
        plog "Continue later from this stage with: bash run-all.sh --from 2"
        exit 1
    fi
    set_stage MAIN_STATUS ok
    if python3 "$SCRIPT_DIR/summarize-runs.py" "$run_dir" --check > "$run_dir/checks.txt" 2>&1; then
        plog "Stage 2 done; all checks passed."
    else
        plog "Stage 2 done; some checks failed -- see $run_dir/checks.txt (the report lists them)."
    fi
fi

# ---------------------------------------------------------------- stages 3-5 (extras)
extra_stage() {
    local num=$1 key=$2 title=$3
    shift 3
    stage_header "Stage $num: $title"
    run_logged "$@"
    local run_dir
    run_dir=$(latest_run "$PIPE_DIR/$num"_*)
    run_dir=${run_dir:-}
    set_stage "${key}_RUN" "${run_dir#"$PIPE_DIR"/}"
    if (( STAGE_RC != 0 )) || [[ -z "$run_dir" ]]; then
        set_stage "${key}_STATUS" failed
        plog "Stage $num FAILED (exit $STAGE_RC); continuing. Rerun it with: bash run-all.sh --only $num"
        bash "$SCRIPT_DIR/stop-single-dn-cluster.sh" 1024 >> "$PLOG" 2>&1 || true
    else
        set_stage "${key}_STATUS" ok
        plog "Stage $num done."
    fi
}

if want 3; then
    extra_stage 3 BENCH "storage-only benchmark (k = $BENCH_K, $BENCH_REPS repetitions)" \
        env RESULTS_BASE="$PIPE_DIR/3_bench" K_VALUES="$BENCH_K" \
        BLOCK_SIZE_MB="$BLOCK_MB" BENCH_DATA_MB="${BENCH_DATA_MB:-$DATA_PER_WORKER_MB}" CACHES="$BENCH_CACHES" \
        bash "$SCRIPT_DIR/storage-bench.sh" "$BENCH_REPS"
fi
if want 4; then
    extra_stage 4 DIO "control: loop devices with direct I/O (k = $CONTROL_K)" \
        env RESULTS_BASE="$PIPE_DIR/4_directio" LOOP_DIRECT_IO=1 K_VALUES="$CONTROL_K" \
        CONDITIONS="$CONDITIONS_DIO" INPUT_SIZE_GB="$INPUT_GB" BLOCK_SIZE_MB="$BLOCK_MB" \
        bash "$SCRIPT_DIR/run-experiment-loopback-fs.sh" "$REPS"
fi
if want 5; then
    extra_stage 5 MKFS "control: default mkfs layout (k = $CONTROL_K)" \
        env RESULTS_BASE="$PIPE_DIR/5_mkfs" MKFS_MODE=default K_VALUES="$CONTROL_K" \
        CONDITIONS="maps=all,cache=cold" INPUT_SIZE_GB="$INPUT_GB" BLOCK_SIZE_MB="$BLOCK_MB" \
        bash "$SCRIPT_DIR/run-experiment-loopback-fs.sh" "$REPS"
fi

# ---------------------------------------------------------------- stage 6
if want 6; then
    stage_header "Stage 6: final report"
    if python3 "$SCRIPT_DIR/final-report.py" "$PIPE_DIR" 2>&1 | tee -a "$PLOG"; then
        plog "Report: $PIPE_DIR/FINAL_REPORT.md"
    else
        plog "The report could not be written (see above); the stage results are in $PIPE_DIR."
    fi
    if [[ "$RESTORE_AT_END" == "1" ]]; then
        bash "$SCRIPT_DIR/stop-single-dn-cluster.sh" 1024 >> "$PLOG" 2>&1 || true
        bash "$SCRIPT_DIR/restore-base-cluster.sh" >> "$PLOG" 2>&1 || true
        plog "Normal cluster restored."
    fi
fi

plog ""
plog "Finished after $(( ($(date +%s) - PIPE_T0) / 60 )) min. Stage status:"
grep "_STATUS=" "$PIPE_DIR/stages.env" | tee -a "$PLOG" || true
plog "Copy everything to the laptop with (on the laptop, in PowerShell): .\sync-cluster.ps1 pull"
