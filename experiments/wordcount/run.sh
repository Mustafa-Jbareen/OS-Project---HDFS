#!/bin/bash
################################################################################
# SCRIPT: run.sh
# DESCRIPTION: Runs the WordCount MapReduce job on the Hadoop cluster.
# USAGE: bash run.sh
# PREREQUISITES:
#   - Hadoop cluster running.
#   - Input data uploaded to HDFS.
# OUTPUT: WordCount job results stored in HDFS and logged locally.
################################################################################

set -e
set -o pipefail

# Ensure files have Unix line endings
sed -i 's/\r$//' /home/mostufa.j/my_scripts/experiments/common/cluster.conf
sed -i 's/\r$//' /home/mostufa.j/my_scripts/experiments/common/utils.sh

source /home/mostufa.j/my_scripts/experiments/common/cluster.conf
source /home/mostufa.j/my_scripts/experiments/common/utils.sh

INPUT=$HDFS_BASE/wordcount/input
OUTPUT=$HDFS_BASE/wordcount/output
JAR=$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar

RUN_ID=$(timestamp)
RUN_DIR=$RESULTS_DIR/wordcount/$RUN_ID
mkdir -p "$RUN_DIR"

log "Starting WordCount run: $RUN_ID"

hdfs_rm_if_exists "$OUTPUT"

START=$(date +%s)

hadoop jar "$JAR" wordcount "$INPUT" "$OUTPUT" \
  2>&1 | tee "$RUN_DIR/job.log"

if ! hdfs dfs -test -d "$OUTPUT"; then
    log "ERROR: Output directory not created"
    exit 1
fi

END=$(date +%s)
echo $((END - START)) > "$RUN_DIR/runtime_seconds.txt"

log "Run completed successfully"
