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

# Hadoop installation directory
HADOOP_HOME=/home/mostufa.j/hadoop

echo "[*] Checking if NameNode needs formatting"
# Check if NameNode has been formatted before
# Using local disk /home/mostufa.j instead of shared $HOME
if [ ! -d "/home/mostufa.j/hadoop_data/namenode/current" ]; then
  echo "[*] Formatting NameNode (initializing filesystem)"
  # Format the NameNode filesystem
  # -force: Bypass safety checks and format even if already formatted
  hdfs namenode -format -force
else
  echo "[*] NameNode already formatted, skipping reformat"
fi

echo "[*] Starting HDFS (NameNode and DataNode daemons)"
# Start HDFS daemon processes (NameNode on this node, DataNodes on other nodes)
start-dfs.sh

echo "[*] Starting YARN (ResourceManager and NodeManager daemons)"
# Start YARN daemon processes (ResourceManager on this node, NodeManagers on workers)
start-yarn.sh

echo "[*] Hadoop cluster is now running"
