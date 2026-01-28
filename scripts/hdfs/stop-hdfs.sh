#!/bin/bash
################################################################################
# SCRIPT: stop-hdfs.sh
# DESCRIPTION: Stops all Hadoop cluster services (HDFS and YARN).
# USAGE: bash stop-hdfs.sh
# PREREQUISITES:
#   - Hadoop cluster running.
# OUTPUT: All HDFS and YARN daemons stopped.
################################################################################

set -e

# Hadoop installation directory
HADOOP_HOME=/home/mostufa.j/hadoop

echo "[*] Stopping YARN services"
# Stop YARN daemons:
# - ResourceManager on this node (master)
# - NodeManagers on all worker nodes
# Use '|| true' to ignore errors if YARN is not running
stop-yarn.sh || true

echo "[*] Stopping HDFS services"
# Stop HDFS daemons:
# - NameNode on this node (master)
# - DataNodes on all worker nodes
# Use '|| true' to ignore errors if HDFS is not running
stop-dfs.sh || true

echo "[*] All Hadoop daemons stopped successfully"
