#!/bin/bash
################################################################################
# SCRIPT: restore-base-cluster.sh
# DESCRIPTION: Re-formats and starts the normal (non-experiment) Hadoop
#              cluster with the base install's own configuration. The
#              experiment scripts call it at the end when the cluster conf
#              sets RESTORE_BASE_CLUSTER=1 (tapuz). Wipes the base HDFS.
#
# USAGE: bash restore-base-cluster.sh
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster.conf"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

echo "Restoring the normal Hadoop cluster (base configuration)..."
unset HADOOP_CONF_DIR

rm -rf "${HADOOP_DATA_DIR}/namenode/current" 2>/dev/null || true
rm -rf "${HADOOP_DATA_DIR}/datanode/current" 2>/dev/null || true
for node in "${ALL_NODES[@]}"; do
    if [[ "$node" != "$(hostname)" && "$node" != "$MASTER_NODE" ]]; then
        ssh "$node" "rm -rf ${HADOOP_DATA_DIR}/datanode/current" 2>/dev/null || true
    fi
done

hdfs namenode -format -force -nonInteractive > /dev/null 2>&1 || true
start-dfs.sh > /dev/null 2>&1 || true
start-yarn.sh > /dev/null 2>&1 || true
sleep 10

echo "Normal cluster restored."
