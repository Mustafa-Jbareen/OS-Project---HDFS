#!/bin/bash
################################################################################
# SCRIPT: restart-hdfs.sh
# DESCRIPTION: Restarts HDFS services while preserving data.
# USAGE: bash restart-hdfs.sh
# PREREQUISITES:
#   - Hadoop installed and previously started.
# OUTPUT: HDFS services stopped and restarted.
################################################################################

set -e

# Hadoop installation directory
HADOOP_HOME=/home/mostufa.j/hadoop

echo "[*] Restarting HDFS (NameNode and DataNodes)"

echo "[*] Stopping HDFS daemons"
# Gracefully stop all HDFS services
bash stop-hdfs.sh

echo "[*] Starting HDFS daemons"
# Start HDFS services again (NameNode on this node, DataNodes on workers)
start-dfs.sh

echo "[*] HDFS restarted successfully"
