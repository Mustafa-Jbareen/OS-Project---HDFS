#!/bin/bash
################################################################################
# SCRIPT: reset-hdfs-contents.sh
# DESCRIPTION: Wrapper script - calls scripts/hdfs/reset-hdfs-contents.sh
#              Removes all files from HDFS while keeping services running.
################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/scripts/hdfs/reset-hdfs-contents.sh" "$@"
