#!/bin/bash
################################################################################
# SCRIPT: stop-hdfs.sh
# DESCRIPTION: Wrapper script - calls scripts/hdfs/stop-hdfs.sh
#              Stops all Hadoop services (HDFS + YARN).
################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/scripts/hdfs/stop-hdfs.sh" "$@"
