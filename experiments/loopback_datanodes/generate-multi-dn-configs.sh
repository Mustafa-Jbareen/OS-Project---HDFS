#!/bin/bash
################################################################################
# SCRIPT: generate-multi-dn-configs.sh
# DESCRIPTION: Generates a separate Hadoop configuration directory for each
#              DataNode instance on a node. Each DataNode gets unique ports
#              and a unique loopback-mounted data directory.
#
# USAGE: bash generate-multi-dn-configs.sh <k> [config_base] [mount_base] [dn_heap_mb] [replication]
#   k           - Number of DataNode instances per physical node
#   config_base - Where to store per-DN config dirs (default: /tmp/hadoop_multi_dn)
#   mount_base  - Loopback mount base dir (default: /mnt/hdfs_loop)
#   dn_heap_mb  - DataNode JVM heap size in MB (default: auto-calculated)
#   replication - HDFS replication factor (default: 3)
#
# OUTPUT: Creates directories config_base/dn{1..k}/ each containing a full
#         Hadoop config (hdfs-site.xml, core-site.xml, etc.)
#
# PORT ALLOCATION SCHEME (per DataNode instance i on a given node):
#   dfs.datanode.address          = 0.0.0.0 : (9866 + (i-1)*10)
#   dfs.datanode.http.address     = 0.0.0.0 : (9864 + (i-1)*10)
#   dfs.datanode.ipc.address      = 0.0.0.0 : (9867 + (i-1)*10)
#
# For k=4 on one node, DataNodes use ports:
#   DN1: 9866, 9864, 9867  (standard defaults)
#   DN2: 9876, 9874, 9877
#   DN3: 9886, 9884, 9887
#   DN4: 9896, 9894, 9897
################################################################################

set -euo pipefail

K=${1:?Usage: generate-multi-dn-configs.sh <k> [config_base] [mount_base] [dn_heap_mb] [replication]}
CONFIG_BASE=${2:-/tmp/hadoop_multi_dn}
MOUNT_BASE=${3:-/mnt/hdfs_loop}
DN_HEAP_MB=${4:-0}  # 0 = auto-calculate
REPLICATION=${5:-3}

HADOOP_HOME=${HADOOP_HOME:-/home/mostufa.j/hadoop}
HADOOP_CONF="$HADOOP_HOME/etc/hadoop"
MASTER_NODE="tapuz14"
NAMENODE_PORT=9000
JOBHISTORY_RPC_PORT=10020
JOBHISTORY_WEB_PORT=19888

# ── Auto-calculate DataNode heap size if not provided ──
# Policy: fixed total DN heap budget per node divided across k DataNodes.
# Example with 4000MB budget: k=1->4000MB, k=2->2000MB, k=4->1000MB, ...
DN_HEAP_BUDGET_PER_NODE_MB=4000
DN_HEAP_MIN_MB=200
if (( DN_HEAP_MB == 0 )); then
  DN_HEAP_MB=$(( DN_HEAP_BUDGET_PER_NODE_MB / K ))
  if (( DN_HEAP_MB < DN_HEAP_MIN_MB )); then
    DN_HEAP_MB=$DN_HEAP_MIN_MB
  fi
fi

echo "Generating configs for $K DataNode instances..."
echo "  Config base:   $CONFIG_BASE"
echo "  Mount base:    $MOUNT_BASE"
echo "  DN Heap:       ${DN_HEAP_MB}MB per DataNode"
echo "  Replication:   $REPLICATION"

# Clean previous configs
rm -rf "$CONFIG_BASE"

for ((i=1; i<=K; i++)); do
    DN_CONF_DIR="$CONFIG_BASE/dn${i}"
    mkdir -p "$DN_CONF_DIR"

    # Port offsets: each DN gets a 10-port range
    local_offset=$(( (i - 1) * 10 ))
    DN_DATA_PORT=$(( 9866 + local_offset ))
    DN_HTTP_PORT=$(( 9864 + local_offset ))
    DN_IPC_PORT=$((  9867 + local_offset ))

    # Data directory on the loopback filesystem
    DN_DATA_DIR="$MOUNT_BASE/dn${i}/hdfs_data"

    # ── core-site.xml ──
    cat > "$DN_CONF_DIR/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://$MASTER_NODE:$NAMENODE_PORT</value>
  </property>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.datanode.use.datanode.hostname</name>
    <value>true</value>
  </property>
</configuration>
EOF

    # ── hadoop-env.sh override for DataNode heap ──
    # Use HDFS_DATANODE_OPTS (HADOOP_DATANODE_OPTS is deprecated).
    cat > "$DN_CONF_DIR/dn-env-override.sh" <<ENVEOF
  export HDFS_DATANODE_OPTS="-Xmx${DN_HEAP_MB}m -Xms${DN_HEAP_MB}m \${HDFS_DATANODE_OPTS:-}"
ENVEOF

    # ── hdfs-site.xml ──
    cat > "$DN_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>$REPLICATION</value>
  </property>

  <!-- DataNode data directory on the loopback filesystem -->
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>$DN_DATA_DIR</value>
  </property>

  <!-- Unique ports for this DataNode instance -->
  <property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:$DN_DATA_PORT</value>
  </property>
  <property>
    <name>dfs.datanode.http.address</name>
    <value>0.0.0.0:$DN_HTTP_PORT</value>
  </property>
  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>0.0.0.0:$DN_IPC_PORT</value>
  </property>

  <!-- Block size: 128MB -->
  <property>
    <name>dfs.blocksize</name>
    <value>134217728</value>
  </property>

  <!-- NameNode settings (only relevant on NameNode, harmless elsewhere) -->
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/home/mostufa.j/hadoop_data/namenode</value>
  </property>

  <!-- Minimum block size (keep low for flexibility) -->
  <property>
    <name>dfs.namenode.fs-limits.min-block-size</name>
    <value>131072</value>
  </property>
</configuration>
EOF

    # ── mapred-site.xml ──
    # Explicitly set JobHistory endpoint to avoid Hadoop default 0.0.0.0:10020
    # on clients (which causes completion-status lookup failures).
    cat > "$DN_CONF_DIR/mapred-site.xml" <<EOF
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>$MASTER_NODE:$JOBHISTORY_RPC_PORT</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>$MASTER_NODE:$JOBHISTORY_WEB_PORT</value>
  </property>
</configuration>
EOF

    # ── Copy remaining configs from the base Hadoop install ──
    # (yarn-site.xml, log4j, hadoop-env.sh, etc.)
    for f in "$HADOOP_CONF"/yarn-site.xml \
             "$HADOOP_CONF"/log4j.properties \
             "$HADOOP_CONF"/hadoop-env.sh \
             "$HADOOP_CONF"/workers; do
        if [[ -f "$f" ]]; then
            cp "$f" "$DN_CONF_DIR/"
        fi
    done

    echo "  DN #$i: ports data=$DN_DATA_PORT http=$DN_HTTP_PORT ipc=$DN_IPC_PORT  dir=$DN_DATA_DIR  heap=${DN_HEAP_MB}MB"
done

echo ""
echo "Generated $K DataNode configs in $CONFIG_BASE/"
