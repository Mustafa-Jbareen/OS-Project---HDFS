#!/bin/bash
################################################################################
# SCRIPT: start-hdfs.sh
# DESCRIPTION: Wrapper script - calls scripts/hdfs/start-hdfs.sh
#              Starts HDFS and YARN services.
################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/scripts/hdfs/start-hdfs.sh" "$@"
