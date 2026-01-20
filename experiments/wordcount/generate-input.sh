#!/bin/bash
################################################################################
# SCRIPT: generate-input.sh
# DESCRIPTION: Generates input data for the WordCount experiment and uploads it to HDFS.
# USAGE: bash generate-input.sh <size_in_MB>
# PREREQUISITES:
#   - Hadoop cluster running.
# OUTPUT: Random text file of specified size uploaded to HDFS.
################################################################################

set -e

SIZE_MB=${1:-100}

LOCAL_FILE=/tmp/wordcount_${SIZE_MB}MB.txt
HDFS_INPUT=/user/$USER/wordcount/input

# Generate input file
if ! base64 /dev/urandom | head -c ${SIZE_MB}M > "$LOCAL_FILE"; then
  echo "Error: Failed to generate input file."
  exit 1
fi

echo "Uploading input to HDFS..."
hdfs dfs -mkdir -p "$HDFS_INPUT"
hdfs dfs -put -f "$LOCAL_FILE" "$HDFS_INPUT"

echo "HDFS input contents:"
hdfs dfs -ls "$HDFS_INPUT"
