#!/bin/bash
################################################################################
# SCRIPT: run-all.sh
# DESCRIPTION: The whole experiment in one command. Stages run in order; the
#              smoke test must pass (READY) before anything long starts.
#
#   0 clean     stop any Hadoop cluster, remove leftover loopback disks
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
#
# Stages 1 and 2 are required: if either fails the pipeline stops. Stages 3-5
# are extras: a failure is noted and the pipeline goes on to the report.
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
while [[ $# -gt 0 ]]; do
    case "$1" in
        --reps) REPS=$2; shift 2 ;;
        --from) FROM=$2; shift 2 ;;
        --only) ONLY=$2; shift 2 ;;
        -h|--help) sed -n '2,/^####/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//; /^###/d'; exit 0 ;;
        *) echo "unknown argument: $1 (see --help)" >&2; exit 1 ;;
    esac
done
for v in "$REPS" "$FROM" "${ONLY:-0}"; do
    [[ "$v" =~ ^[0-9]+$ ]] || { echo "--reps/--from/--only need a number" >&2; exit 1; }
done

MAIN_K=${MAIN_K_VALUES:-"1 64 256 512 1024"}
CONTROL_K=${CONTROL_K_VALUES:-"1 1024"}
BENCH_K=${BENCH_K_VALUES:-"1 256 1024"}
SMOKE_K=${SMOKE_K_VALUES:-"1 4"}
BENCH_REPS=${BENCH_REPS:-3}

# Conditions of the main run: the 2x2 (1 vs all maps per node x cold vs warm)
# plus a middle load level, cold only.
MID=$(( SLOTS_PER_NODE / 2 ))
CONDITIONS_MAIN="maps=1,cache=cold maps=1,cache=warm"
if (( MID > 1 && MID < SLOTS_PER_NODE )); then
    CONDITIONS_MAIN+=" maps=$MID,cache=cold"
fi
CONDITIONS_MAIN+=" maps=all,cache=cold maps=all,cache=warm"

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
else
    PIPE_DIR="$RESULTS_ROOT/pipeline_$(date +%Y-%m-%d_%H-%M-%S)"
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

plog "Pipeline: $PIPE_DIR"
plog "Cluster $CLUSTER; repetitions $REPS; main k: $MAIN_K; controls k: $CONTROL_K; bench k: $BENCH_K"
plog "Main conditions: $CONDITIONS_MAIN"
plog "Rough duration on tapuz: smoke ~35 min, main ~$(( REPS * 5 / 3 + 1 )) h, bench ~45 min, controls ~2-3 h"
PIPE_T0=$(date +%s)

# ---------------------------------------------------------------- stage 0
if want_clean; then
    stage_header "Stage 0: clean -- stop Hadoop, remove loopback disks, old input copies"
    bash "$SCRIPT_DIR/stop-single-dn-cluster.sh" 1024 >> "$PLOG" 2>&1 || true
    rm -f "$TMP_BASE"/wordcount_*MB.txt 2>/dev/null || true
    plog "Stage 0 done."
fi

# ---------------------------------------------------------------- stage 1
if want 1; then
    stage_header "Stage 1: smoke test (k = $SMOKE_K, 1 repetition, all main conditions)"
    run_logged env RESULTS_BASE="$PIPE_DIR/1_smoke" CONDITIONS="$CONDITIONS_MAIN" K_VALUES="$SMOKE_K" \
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
        bash "$SCRIPT_DIR/storage-bench.sh" "$BENCH_REPS"
fi
if want 4; then
    extra_stage 4 DIO "control: loop devices with direct I/O (k = $CONTROL_K)" \
        env RESULTS_BASE="$PIPE_DIR/4_directio" LOOP_DIRECT_IO=1 K_VALUES="$CONTROL_K" \
        CONDITIONS="maps=all,cache=cold maps=all,cache=warm" INPUT_SIZE_GB="$MATRIX_INPUT_GB" \
        bash "$SCRIPT_DIR/run-experiment-loopback-fs.sh" "$REPS"
fi
if want 5; then
    extra_stage 5 MKFS "control: default mkfs layout (k = $CONTROL_K)" \
        env RESULTS_BASE="$PIPE_DIR/5_mkfs" MKFS_MODE=default K_VALUES="$CONTROL_K" \
        CONDITIONS="maps=all,cache=cold" INPUT_SIZE_GB="$MATRIX_INPUT_GB" \
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
