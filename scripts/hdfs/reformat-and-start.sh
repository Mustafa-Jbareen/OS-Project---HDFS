#!/bin/bash
################################################################################
# SCRIPT: reformat-and-start.sh
# DESCRIPTION: Reformats the NameNode and starts the entire Hadoop cluster.
# USAGE: bash reformat-and-start.sh
# PREREQUISITES:
#   - Hadoop installed and configured.
#   - HDFS data cleared (use clear-hdfs.sh first).
# OUTPUT: NameNode formatted, HDFS and YARN services running.
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Hadoop installation directory
HADOOP_HOME=/home/mostufa.j/hadoop
HADOOP_DATA_DIR=/home/mostufa.j/hadoop_data

echo "[*] Checking if NameNode needs formatting"
# Check if NameNode has been formatted before
if [ ! -d "$HADOOP_DATA_DIR/namenode/current" ]; then
  echo "[*] Formatting NameNode (initializing filesystem)"
  # Format the NameNode filesystem
  # -force: Bypass safety checks and format even if already formatted
  hdfs namenode -format -force
else
  echo "[*] NameNode already formatted, skipping reformat"
fi

echo "[*] Starting Hadoop cluster..."
bash "$SCRIPT_DIR/start-hdfs.sh"

echo "[*] Hadoop cluster is now running"
