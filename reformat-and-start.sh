#!/bin/bash
################################################################################
# SCRIPT: reformat-and-start.sh
# DESCRIPTION: Wrapper script - calls scripts/hdfs/reformat-and-start.sh
#              Reformats NameNode and starts the Hadoop cluster.
################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/scripts/hdfs/reformat-and-start.sh" "$@"
