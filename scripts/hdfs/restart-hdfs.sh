#!/bin/bash
################################################################################
# SCRIPT: restart-hdfs.sh
# DESCRIPTION: Performs a full HDFS reset - stops all services, cleans state
#              on ALL nodes (master + workers), reformats NameNode, and starts
#              a fresh cluster. This is equivalent to a completely fresh setup.
# USAGE: bash restart-hdfs.sh
# PREREQUISITES:
#   - Hadoop installed and configured.
#   - SSH passwordless access to worker nodes.
# OUTPUT: Fresh HDFS cluster running from scratch.
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Hadoop installation directory
HADOOP_HOME=/home/mostufa.j/hadoop
HADOOP_DATA_DIR=/home/mostufa.j/hadoop_data

# Worker nodes - must match your cluster configuration
WORKER_NODES=("tapuz13")

echo "=============================================="
echo "HDFS Full Restart (Fresh Initialization)"
echo "=============================================="

echo ""
echo "[1/6] Stopping all Hadoop services..."
# Gracefully stop all HDFS and YARN services
bash "$SCRIPT_DIR/stop-hdfs.sh"

echo ""
echo "[2/6] Cleaning NameNode state on master..."
# Remove NameNode metadata directory - contains edit logs and image files
rm -rf "$HADOOP_DATA_DIR/namenode/current"
echo "      NameNode state cleared"

echo ""
echo "[3/6] Cleaning DataNode state on master..."
# Remove DataNode data directory on master
rm -rf "$HADOOP_DATA_DIR/datanode/current"
echo "      Master DataNode state cleared"

echo ""
echo "[4/6] Cleaning DataNode state on worker nodes..."
# Remove DataNode data directory on all worker nodes
for node in "${WORKER_NODES[@]}"; do
    echo "      Cleaning $node..."
    ssh "$node" "rm -rf $HADOOP_DATA_DIR/datanode/current" 2>/dev/null || echo "      Warning: Could not clean $node"
done
echo "      Worker DataNode states cleared"

echo ""
echo "[5/6] Formatting NameNode (initializing filesystem)..."
# Format the NameNode filesystem
# -force: Bypass safety checks and format even if already formatted
hdfs namenode -format -force

echo ""
echo "[6/6] Starting fresh HDFS cluster..."
bash "$SCRIPT_DIR/start-hdfs.sh"

echo ""
echo "=============================================="
echo "HDFS Full Restart Complete!"
echo "=============================================="
echo "The cluster is now running with a fresh filesystem."
echo "All previous data has been removed from all nodes."
echo "=============================================="
