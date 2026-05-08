#!/bin/bash
################################################################################
# SCRIPT: generate-single-dn-configs.sh
# DESCRIPTION: Generates Hadoop configuration for a single DataNode per node
#              with k loopback filesystem directories (storage virtualization).
#              Unlike generate-multi-dn-configs.sh, this creates only ONE
#              DataNode config per node, but with dfs.datanode.data.dir set
#              to a comma-separated list of k mount points.
#
# USAGE: bash generate-single-dn-configs.sh <k> [config_dir] [mount_base] [dn_heap_mb] [replication]
#   k           - Number of loopback storage directories per DataNode
#   config_dir  - Where to store DataNode config (default: /scratch/tmp/hadoop_single_dn_k_dirs)
#   mount_base  - Loopback mount base dir (default: /scratch/hdfs_loop)
#   dn_heap_mb  - DataNode JVM heap size in MB (default: 2048)
#   replication - HDFS replication factor (default: 3)
#
# OUTPUT: Creates config_dir/ containing Hadoop config with dfs.datanode.data.dir
#         set to: /scratch/hdfs_loop/dn1/hdfs_data,/scratch/hdfs_loop/dn2/hdfs_data,...
################################################################################

set -euo pipefail

K=${1:?Usage: generate-single-dn-configs.sh <k> [config_dir] [mount_base] [dn_heap_mb] [replication]}
CONFIG_DIR=${2:-/scratch/tmp/hadoop_single_dn_k_dirs}
MOUNT_BASE=${3:-/scratch/hdfs_loop}
DN_HEAP_MB=${4:-4096}
REPLICATION=${5:-3}

# Honor caller-passed env (start-single-dn-cluster.sh sets these).
HADOOP_HOME="${HADOOP_HOME:-/scratch/hadoop/hadoop-3.3.1}"
HADOOP_CONF="$HADOOP_HOME/etc/hadoop"
MASTER_NODE="${MASTER_NODE:?MASTER_NODE must be set (export from caller)}"
NAMENODE_PORT=9000
JOBHISTORY_RPC_PORT=10020
JOBHISTORY_WEB_PORT=19888

echo "Generating config for single DataNode with $K storage directories..."
echo "  Config dir:    $CONFIG_DIR"
echo "  Mount base:    $MOUNT_BASE"
echo "  DN Heap:       ${DN_HEAP_MB}MB"
echo "  Replication:   $REPLICATION"

# Clean previous config and create directory
# Use sudo + chmod 777 pattern (same as setup-loopback-fs.sh) to ensure user can write
sudo rm -rf "$CONFIG_DIR" 2>/dev/null || true
sudo mkdir -p "$CONFIG_DIR"
sudo chmod 777 "$CONFIG_DIR"

# Build comma-separated list of data directories
DATA_DIRS=""
for ((i=1; i<=K; i++)); do
    if [[ -n "$DATA_DIRS" ]]; then
        DATA_DIRS="${DATA_DIRS},"
    fi
    DATA_DIRS="${DATA_DIRS}${MOUNT_BASE}/dn${i}/hdfs_data"
done

echo "  Data dirs: $DATA_DIRS"

# ── core-site.xml ──
cat > "$CONFIG_DIR/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://$MASTER_NODE:$NAMENODE_PORT</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/scratch/tmp/hadoop</value>
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
cat > "$CONFIG_DIR/dn-env-override.sh" <<ENVEOF
export HDFS_DATANODE_OPTS="-Xmx${DN_HEAP_MB}m -Xms${DN_HEAP_MB}m \${HDFS_DATANODE_OPTS:-}"
# Raise open-file limit: 1024 storage dirs each need lock FDs + JVM system FDs
ulimit -n 65536 2>/dev/null || true
ENVEOF

# ── hdfs-site.xml ──
cat > "$CONFIG_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>$REPLICATION</value>
  </property>

  <!-- DataNode data directories (comma-separated list of k loopback filesystems) -->
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>$DATA_DIRS</value>
  </property>

  <!-- Standard DataNode ports -->
  <property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:9866</value>
  </property>
  <property>
    <name>dfs.datanode.http.address</name>
    <value>0.0.0.0:9864</value>
  </property>
  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>0.0.0.0:9867</value>
  </property>

  <!-- Default block size: 32MB.
       generate-input.sh passes BLOCK_SIZE as -D dfs.blocksize at write time
       (overriding this default for input files).  WordCount output and any
       other writes without an explicit -D flag use this 32MB default. -->
  <property>
    <name>dfs.blocksize</name>
    <value>33554432</value>
  </property>

  <!-- NameNode settings -->
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/scratch/hadoop_data/namenode</value>
  </property>

  <!-- Minimum block size -->
  <property>
    <name>dfs.namenode.fs-limits.min-block-size</name>
    <value>131072</value>
  </property>

  <!-- Performance: larger write packet for sequential bulk writes.
       Default is 64KB; 8MB = quarter of the 32MB block size (4 packets per
       block).  Good balance: reduces per-packet overhead vs. the default
       while keeping pipeline stall time per hop manageable (~65ms on 1Gbps). -->
  <property>
    <name>dfs.client.write.packet.size</name>
    <value>8388608</value>
  </property>

  <!-- Performance: more DataNode handler threads (default 10;
       useful when one DN has many concurrent readers at high k) -->
  <property>
    <name>dfs.datanode.handler.count</name>
    <value>16</value>
  </property>
</configuration>
EOF

# ── mapred-site.xml ──
cat > "$CONFIG_DIR/mapred-site.xml" <<EOF
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
for f in "$HADOOP_CONF"/yarn-site.xml \
         "$HADOOP_CONF"/log4j.properties \
         "$HADOOP_CONF"/hadoop-env.sh \
         "$HADOOP_CONF"/workers; do
    if [[ -f "$f" ]]; then
        cp "$f" "$CONFIG_DIR/"
    fi
done

echo ""
echo "Generated single DataNode config with $K storage directories in $CONFIG_DIR/"
echo "  dfs.datanode.data.dir = $DATA_DIRS"
