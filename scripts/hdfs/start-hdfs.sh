#!/bin/bash
################################################################################
# SCRIPT: start-hdfs.sh
# DESCRIPTION: Starts HDFS and YARN services.
# USAGE: bash start-hdfs.sh
# PREREQUISITES:
#   - Hadoop installed and configured.
#   - NameNode formatted.
# OUTPUT: HDFS and YARN services running.
################################################################################

set -e

# Hadoop installation directory
HADOOP_HOME=/home/mostufa.j/hadoop

echo "[*] Starting HDFS (NameNode and DataNode daemons)"
# Start HDFS daemon processes (NameNode on this node, DataNodes on other nodes)
start-dfs.sh

echo "[*] Starting YARN (ResourceManager and NodeManager daemons)"
# Start YARN daemon processes (ResourceManager on this node, NodeManagers on workers)
start-yarn.sh

echo "[*] Hadoop cluster is now running"
