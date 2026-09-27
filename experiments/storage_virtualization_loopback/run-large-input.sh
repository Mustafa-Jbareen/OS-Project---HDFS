#!/bin/bash
################################################################################
# SCRIPT: run-large-input.sh
# DESCRIPTION: The April-style experiment with the new protocol: a large
#              WordCount input read from disk under full load. Does the k
#              slowdown hold when the data cannot fit in RAM?
#
#   - Input 100 GB in 16 MB blocks (6400 map tasks per job), all map slots of
#     every node busy (maps=all), cold cache only: 100 GB x 3 replicas is
#     75 GB per worker, far more than its 7.7 GB of RAM, so "warm" cannot
#     exist at this size.
#   - k = 1 64 256 512 1024 in a random order; the per-job protocol is the one
#     of run-all.sh (idle YARN, sync, cache step, pause, measurements).
#   - No warm-up job (WARMUP_JOBS=0): at ~80 min per job, the first-job
#     overheads it absorbs are a few seconds, and it would add ~6 h. Each
#     job's DataNode metrics still start at a snapshot taken just before it.
#   - Starts with clean-scratch.sh; ends with the checks (checks.txt) and a
#     text report (FINAL_REPORT.md; the laptop adds the figures on pull).
#
# USAGE (on the cluster master, inside screen):
#   bash run-large-input.sh [REPS]                   default: 3 repetitions
#   K_VALUES="1 512 1024" bash run-large-input.sh    fewer k values (~15 h)
#   INPUT_SIZE_GB=50 BLOCK_SIZE_MB=32 bash run-large-input.sh
#   INPUT_SIZE_GB=2 K_VALUES="1 4" bash run-large-input.sh 1    ~20 min dry run
#
# TIME on tapuz, scaled from the April 40 GB / 16 MB run (30 min per job at
# k=1, 12 min upload): input generation ~25 min once; per k an upload of
# ~30 min plus REPS jobs of ~75-90 min each. Default: about 24 hours.
#
# OUTPUT: results/storage_virtualization_loopback_<cluster>_<input>GB_<block>MB/run_<timestamp>/
#         (pulled by .\sync-cluster.ps1 pull with the default patterns)
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLUSTER_NAME=$(source "$SCRIPT_DIR/cluster.conf" && echo "$CLUSTER")

REPS=${1:-3}
export INPUT_SIZE_GB="${INPUT_SIZE_GB:-100}"
export BLOCK_SIZE_MB="${BLOCK_SIZE_MB:-16}"
export K_VALUES="${K_VALUES:-1 64 256 512 1024}"
export K_ORDER="${K_ORDER:-random}"
export CONDITIONS="${CONDITIONS:-maps=all,cache=cold}"
export WARMUP_JOBS="${WARMUP_JOBS:-0}"
export RESULTS_BASE="${RESULTS_BASE:-$PROJECT_ROOT/results/storage_virtualization_loopback_${CLUSTER_NAME}_${INPUT_SIZE_GB}GB_${BLOCK_SIZE_MB}MB}"

echo "Large-input run: ${INPUT_SIZE_GB} GB in ${BLOCK_SIZE_MB} MB blocks, k = $K_VALUES, $REPS repetitions,"
echo "conditions: $CONDITIONS; results in $RESULTS_BASE"
echo ""

bash "$SCRIPT_DIR/clean-scratch.sh"

before=$(ls -d "$RESULTS_BASE"/run_* 2>/dev/null | sort | tail -1 || true)
rc=0
bash "$SCRIPT_DIR/run-experiment-loopback-fs.sh" "$REPS" || rc=$?
run_dir=$(ls -d "$RESULTS_BASE"/run_* 2>/dev/null | sort | tail -1 || true)
if [[ -z "$run_dir" || "$run_dir" == "$before" ]]; then
    echo "The run did not start (see the messages above)."
    exit 1
fi

echo ""
echo "============================================================"
echo "Checks and report: $run_dir"
echo "============================================================"
python3 "$SCRIPT_DIR/summarize-runs.py" "$run_dir" --check > "$run_dir/checks.txt" 2>&1 || true
grep -E "^ *(PASS|WARN|FAIL)" "$run_dir/checks.txt" || true
python3 "$SCRIPT_DIR/final-report.py" "$run_dir" || true
echo ""
echo "On the laptop (PowerShell, in my_scripts): .\\sync-cluster.ps1 pull"
exit "$rc"
