#!/bin/bash
################################################################################
# SCRIPT: generate-workers-file.sh
# DESCRIPTION: Generates the workers file for Hadoop
# USAGE: bash generate-workers-file.sh <output_directory>
# PREREQUISITES: None
################################################################################

set -e

OUTPUT_DIR=${1:-/home/mostufa.j/hadoop/etc/hadoop}
MASTER_NODE="tapuz14"
WORKER_NODES=("tapuz13")

printf "%s\n" "$MASTER_NODE" "${WORKER_NODES[@]}" > "$OUTPUT_DIR/workers"

echo "workers file generated at $OUTPUT_DIR"