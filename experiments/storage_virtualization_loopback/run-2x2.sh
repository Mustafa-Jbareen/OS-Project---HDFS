#!/bin/bash
################################################################################
# SCRIPT: run-2x2.sh
# DESCRIPTION: The 2x2 test -- which factor makes the k slowdown appear?
#
#   Factor 1, load:        1 map task per node   vs  SLOTS_PER_NODE per node
#   Factor 2, page cache:  cold (reads hit disk) vs  warm (reads hit RAM)
#
#   All 4 combinations are measured at k=1 and at k=1024 on the same cluster
#   and the same uploaded data; within every repetition the 4 conditions run
#   in a random order, so slow drift of the machines hits all of them alike.
#
#   Past runs changed both factors (plus others) at once, so they cannot tell
#   which one hid the slowdown. Their job counters point at load (the runs
#   without slowdown ran 1-13 maps at once); the cache was the suspect at the
#   time. This test separates the two.
#
# USAGE: [VAR=value ...] bash run-2x2.sh [K_REPS]
#   K_REPS  repetitions per k and condition (default 5)
#   Any setting of run-experiment-loopback-fs.sh can be overridden, e.g.
#     K_VALUES="1 256 1024" INPUT_SIZE_GB=4 bash run-2x2.sh 3
#
# DURATION (tapuz, defaults): roughly 3-4 hours.
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster.conf"

export CONDITIONS="${CONDITIONS:-maps=1,cache=cold maps=1,cache=warm maps=all,cache=cold maps=all,cache=warm}"
export K_VALUES="${K_VALUES:-1 1024}"
export INPUT_SIZE_GB="${INPUT_SIZE_GB:-$MATRIX_INPUT_GB}"

exec bash "$SCRIPT_DIR/run-experiment-loopback-fs.sh" "${1:-5}"
