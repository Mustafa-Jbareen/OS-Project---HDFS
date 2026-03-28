#!/bin/bash
################################################################################
# SCRIPT: run-experiment-nn-only.sh
# DESCRIPTION: Runs the loopback DataNodes experiment with a NameNode-only
#              master (tapuz14 runs NameNode/YARN RM, no DataNode process).
#
# USAGE: bash run-experiment-nn-only.sh [K_REPS]
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."

export MASTER_HAS_DN=0
export RESULTS_BASE="$PROJECT_ROOT/results/loopback-datanodes-nn-only"

bash "$SCRIPT_DIR/run-experiment.sh" "$@"
