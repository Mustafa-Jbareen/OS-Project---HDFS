#!/bin/bash
################################################################################
# SCRIPT: run-2x2.sh
# DESCRIPTION: The 2x2 test -- which factor makes the k slowdown appear?
#
#   Factor 1, load:        1 map task per node   vs  SLOTS_PER_NODE per node
#   Factor 2, page cache:  cold (reads hit disk) vs  warm (reads hit RAM)
#
#   All 4 combinations run at every k on the same cluster and the same
#   uploaded data, through the same per-job protocol; within every repetition
#   the 4 conditions run in a random order, so slow drift of the machines hits
#   all of them alike. Past runs changed both factors (plus others) at once,
#   so they cannot tell which one hid the slowdown.
#
# USAGE:
#   bash run-2x2.sh smoke      Tiny version (~25 min on tapuz): k=1 and k=4,
#                              1 repetition, 1 GB input. Checks that every
#                              part works (load levels, cold/warm cache,
#                              counters, locality, swapping) and prints
#                              READY / NOT READY. Results go to
#                              results/storage_virtualization_loopback_<cluster>_smoke/.
#   bash run-2x2.sh [K_REPS]   Full test: k = 1 64 256 512 1024 in random
#                              order, K_REPS repetitions (default 5), 2 GB input.
#                              About 7-8 hours on tapuz (K_REPS=3: ~5 hours).
#
# Any setting can be overridden, e.g.
#   K_VALUES="1 1024" bash run-2x2.sh 3
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Read the cluster name and the shared defaults in a subshell, so that the
# values set below still act as defaults for the runner.
CLUSTER_NAME=$(source "$SCRIPT_DIR/cluster.conf" && echo "$CLUSTER")
MATRIX_INPUT_GB_DEFAULT=$(source "$SCRIPT_DIR/cluster.conf" && echo "$MATRIX_INPUT_GB")

export CONDITIONS="${CONDITIONS:-maps=1,cache=cold maps=1,cache=warm maps=all,cache=cold maps=all,cache=warm}"

if [[ "${1:-}" == "smoke" ]]; then
    # 1 GB in 16 MB blocks = 64 map tasks: more than the 8 x 4 = 32 slots, so
    # the full-load condition really fills the nodes. Small k values keep the
    # cluster setup short (k=1024 alone takes ~25 min on tapuz).
    export K_VALUES="${K_VALUES:-1 4}"
    export INPUT_SIZE_MB="${INPUT_SIZE_MB:-1024}"
    export BLOCK_SIZE_MB="${BLOCK_SIZE_MB:-16}"
    export LOOPBACK_BUDGET_PER_NODE_GB="${LOOPBACK_BUDGET_PER_NODE_GB:-20}"
    export RESULTS_BASE="${RESULTS_BASE:-$PROJECT_ROOT/results/storage_virtualization_loopback_${CLUSTER_NAME}_smoke}"

    before=$(ls -d "$RESULTS_BASE"/run_* 2>/dev/null | sort | tail -1 || true)
    rc=0
    bash "$SCRIPT_DIR/run-experiment-loopback-fs.sh" 1 || rc=$?
    run_dir=$(ls -d "$RESULTS_BASE"/run_* 2>/dev/null | sort | tail -1 || true)
    if [[ -z "$run_dir" || "$run_dir" == "$before" ]]; then
        echo "Smoke test: the run did not start (see the messages above)."
        exit 1
    fi
    echo ""
    echo "============================================================"
    echo "Smoke test checks: $run_dir"
    echo "============================================================"
    python3 "$SCRIPT_DIR/summarize-runs.py" "$run_dir" --check | tee "$run_dir/checks.txt" || rc=1
    exit "$rc"
fi

export K_VALUES="${K_VALUES:-1 64 256 512 1024}"
export K_ORDER="${K_ORDER:-random}"
export INPUT_SIZE_GB="${INPUT_SIZE_GB:-$MATRIX_INPUT_GB_DEFAULT}"

exec bash "$SCRIPT_DIR/run-experiment-loopback-fs.sh" "${1:-5}"
