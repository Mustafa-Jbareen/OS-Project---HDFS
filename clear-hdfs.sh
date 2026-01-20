#!/bin/bash
################################################################################
# SCRIPT: clear-hdfs.sh
# DESCRIPTION: Clears all HDFS data from NameNode and DataNode directories.
# USAGE: bash clear-hdfs.sh
# PREREQUISITES:
#   - Hadoop installed and configured.
#   - HDFS services stopped (this script stops them automatically).
# OUTPUT: HDFS directories cleared, ready for reformatting.
################################################################################

set -e

# Hadoop installation directory
HADOOP_HOME=/home/mostufa.j/hadoop

echo "[*] Stopping Hadoop before clearing data"
# Call the stop script to gracefully shutdown all HDFS services
bash stop-hdfs.sh

echo "[*] Removing HDFS directories (namenode & datanode)"
# Remove NameNode metadata directory - contains edit logs and image files
# Using local disk /home/mostufa.j instead of shared $HOME
rm -rf /home/mostufa.j/hadoop_data/namenode/current
# Remove DataNode data directory - contains actual block replicas
rm -rf /home/mostufa.j/hadoop_data/datanode/current

echo "[*] HDFS data cleared. You need to re-format the NameNode to restart HDFS."
