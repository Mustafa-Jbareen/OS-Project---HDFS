#!/bin/bash
################################################################################
# SCRIPT: clear-hdfs.sh
# DESCRIPTION: Wrapper script - calls scripts/hdfs/clear-hdfs.sh
#              Clears HDFS data directories on all nodes.
################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/scripts/hdfs/clear-hdfs.sh" "$@"
