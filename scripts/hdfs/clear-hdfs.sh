#!/bin/bash
################################################################################
# SCRIPT: clear-hdfs.sh
# DESCRIPTION: Clears all HDFS data from NameNode and DataNode directories
#              on ALL nodes (master + workers). Stops HDFS and removes all
#              filesystem state.
# USAGE: bash clear-hdfs.sh
# PREREQUISITES:
#   - Hadoop installed and configured.
#   - SSH passwordless access to worker nodes.
# OUTPUT: HDFS directories cleared on all nodes, ready for reformatting.
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Hadoop installation directory
HADOOP_HOME=/home/mostufa.j/hadoop
HADOOP_DATA_DIR=/home/mostufa.j/hadoop_data

# Worker nodes - must match your cluster configuration
WORKER_NODES=("tapuz10" "tapuz11" "tapuz12" "tapuz13")

echo "[*] Stopping Hadoop before clearing data"
# Call the stop script to gracefully shutdown all HDFS services
bash "$SCRIPT_DIR/stop-hdfs.sh"

echo "[*] Removing HDFS directories on master (namenode & datanode)"
# Remove NameNode metadata directory - contains edit logs and image files
rm -rf "$HADOOP_DATA_DIR/namenode/current"
# Remove DataNode data directory - contains actual block replicas
rm -rf "$HADOOP_DATA_DIR/datanode/current"

echo "[*] Removing DataNode directories on worker nodes"
for node in "${WORKER_NODES[@]}"; do
    echo "    Cleaning $node..."
    ssh "$node" "rm -rf $HADOOP_DATA_DIR/datanode/current" 2>/dev/null || echo "    Warning: Could not clean $node"
done

echo "[*] HDFS data cleared on all nodes. You need to re-format the NameNode to restart HDFS."
