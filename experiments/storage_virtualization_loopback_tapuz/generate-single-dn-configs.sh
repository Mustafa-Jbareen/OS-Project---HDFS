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
DN_HEAP_MB=${4:-auto}
REPLICATION=${5:-3}

# Honor caller-passed env (start-single-dn-cluster.sh sets these).
HADOOP_HOME="${HADOOP_HOME:-/scratch/hadoop/hadoop-3.3.1}"
HADOOP_CONF="$HADOOP_HOME/etc/hadoop"
MASTER_NODE="${MASTER_NODE:?MASTER_NODE must be set (export from caller)}"
NAMENODE_PORT=9000
JOBHISTORY_RPC_PORT=10020
JOBHISTORY_WEB_PORT=19888

# ---- Auto-scale resources to local machine -----------------------------------
# This script runs on every node (master + each worker), so each generates its
# own yarn-site.xml sized for its own hardware. The c6620 cluster has ~54GB
# RAM/64 vcores, the tapuz nodes have ~8GB/8 cores -- both end up using almost
# all available RAM/CPU for YARN containers without any per-cluster tuning.
TOTAL_MEM_MB=$(awk '/^MemTotal:/ {printf "%d", $2/1024}' /proc/meminfo)
TOTAL_CORES=$(nproc)

# ---- Detect this node's internal-network identity ----------------------------
# Without this, NodeManager calls getCanonicalHostName() which reverse-DNSs the
# eth0 (public) IP and registers itself as e.g. er113.utah.cloudlab.us. The RM
# (which only knows about node0..nodeN from cluster.conf) then marks the NM
# UNHEALTHY and cluster capacity drops to <memory:0, vCores:0>. Pin the NM to
# its 10.10.x.x identity so registration matches the names RM expects.
LOCAL_INTERNAL_IP=$(ip -o -4 addr show 2>/dev/null \
    | awk '/inet 10\.10\./ {split($4,a,"/"); print a[1]; exit}')
LOCAL_INTERNAL_NAME=$(awk -v ip="$LOCAL_INTERNAL_IP" \
    '$1==ip {print $NF; exit}' /etc/hosts 2>/dev/null)
# Fallback: short hostname if /etc/hosts lookup fails
LOCAL_INTERNAL_NAME=${LOCAL_INTERNAL_NAME:-$(hostname -s)}
LOCAL_INTERNAL_IP=${LOCAL_INTERNAL_IP:-0.0.0.0}

# DataNode JVM heap: clamp to [2GB, 8GB] based on machine size.
if [[ "$DN_HEAP_MB" == "auto" ]]; then
    DN_HEAP_MB=$(( TOTAL_MEM_MB / 8 ))
    (( DN_HEAP_MB < 2048 )) && DN_HEAP_MB=2048
    (( DN_HEAP_MB > 8192 )) && DN_HEAP_MB=8192
fi

# NodeManager pool: leave DN heap + 2GB for OS/agents/buffers.
OS_RESERVE_MB=2048
NM_MEM_MB=$(( TOTAL_MEM_MB - DN_HEAP_MB - OS_RESERVE_MB ))
(( NM_MEM_MB < 1024 )) && NM_MEM_MB=1024
NM_CORES=$TOTAL_CORES

# Per-container default size: roughly half the NM pool, capped sanely.
MR_CONTAINER_MB=$(( NM_MEM_MB / 2 ))
(( MR_CONTAINER_MB < 1024 )) && MR_CONTAINER_MB=1024
(( MR_CONTAINER_MB > 8192 )) && MR_CONTAINER_MB=8192
MR_HEAP_MB=$(( MR_CONTAINER_MB * 8 / 10 ))

echo "Generating config for single DataNode with $K storage directories..."
echo "  Host:          $(hostname)  (${TOTAL_MEM_MB}MB RAM, ${TOTAL_CORES} vcores)"
echo "  Internal name: $LOCAL_INTERNAL_NAME ($LOCAL_INTERNAL_IP)"
echo "  Config dir:    $CONFIG_DIR"
echo "  Mount base:    $MOUNT_BASE"
echo "  DN heap:       ${DN_HEAP_MB}MB"
echo "  NM pool:       ${NM_MEM_MB}MB / ${NM_CORES} vcores"
echo "  MR container:  ${MR_CONTAINER_MB}MB (heap ${MR_HEAP_MB}MB)"
echo "  Replication:   $REPLICATION"

# Clean previous config and create directory
# Use sudo + chmod 777 pattern (same as setup-loopback-fs.sh) to ensure user can write
sudo rm -rf "$CONFIG_DIR" 2>/dev/null || true
sudo mkdir -p "$CONFIG_DIR"
sudo chmod 777 "$CONFIG_DIR"

# Pre-create YARN/Hadoop scratch dirs that the NM and MR jobs need writable.
# /scratch/tmp gets made root:755 by `sudo mkdir -p $CONFIG_DIR`, so the NM
# (running as $USER) cannot create $hadoop.tmp.dir/nm-local-dir underneath it.
# That manifests as "No space available in any of the local directories" at
# AM container localization time. Explicitly chmod the parents and pre-create
# the YARN dirs we point yarn-site.xml at.
for d in /scratch/tmp /scratch/tmp/hadoop /scratch/yarn-local /scratch/yarn-logs; do
    sudo mkdir -p "$d"
    sudo chmod 1777 "$d"   # sticky-bit world-writable (like /tmp)
done

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

  <!-- Default block size: 128MB.
       generate-input.sh passes BLOCK_SIZE as -D dfs.blocksize at write time
       (overriding this default for input files).  WordCount output and any
       other writes without an explicit -D flag use this 128MB default. -->
  <property>
    <name>dfs.blocksize</name>
    <value>134217728</value>
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

  <!-- Container size auto-scaled to local machine; lets c6620 nodes use the
       full ~54GB RAM and tapuz nodes use ~4GB without per-cluster tuning. -->
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>$MR_CONTAINER_MB</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>$MR_CONTAINER_MB</value>
  </property>
  <property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx${MR_HEAP_MB}m</value>
  </property>
  <property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx${MR_HEAP_MB}m</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.resource.mb</name>
    <value>$MR_CONTAINER_MB</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.command-opts</name>
    <value>-Xmx${MR_HEAP_MB}m</value>
  </property>
</configuration>
EOF

# ── yarn-site.xml ──
# Written from scratch (instead of copying $HADOOP_CONF/yarn-site.xml) so that
# the NodeManager pool reflects the *local* machine's RAM/cores. Each worker
# generates its own copy via the SSH path in start-single-dn-cluster.sh.
cat > "$CONFIG_DIR/yarn-site.xml" <<EOF
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>$MASTER_NODE</value>
  </property>
  <property>
    <name>yarn.resourcemanager.bind-host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
  </property>

  <!-- Force NM to advertise its internal-network identity to RM. Without this,
       the NM registers itself via reverse-DNS of eth0 -> public hostname, the
       RM marks the node UNHEALTHY (it expects node0..nodeN), and cluster
       capacity reads <0, 0> so no AM container can be allocated. -->
  <property>
    <name>yarn.nodemanager.hostname</name>
    <value>$LOCAL_INTERNAL_NAME</value>
  </property>
  <property>
    <name>yarn.nodemanager.bind-host</name>
    <value>$LOCAL_INTERNAL_IP</value>
  </property>

  <!-- Relax disk-health checks: tiny loopback FSes at high k can drop below
       the default 90% free threshold, which would also flip NMs to UNHEALTHY.
       NM local-dirs live under /scratch/tmp on the host fs, so this only
       matters if /scratch itself fills up. -->
  <property>
    <name>yarn.nodemanager.disk-health-checker.min-healthy-disks</name>
    <value>0.0</value>
  </property>
  <property>
    <name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name>
    <value>99.0</value>
  </property>
  <property>
    <name>yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb</name>
    <value>0</value>
  </property>

  <!-- Resource pool sized to this node's hardware -->
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>$NM_MEM_MB</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>$NM_CORES</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>$NM_MEM_MB</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-vcores</name>
    <value>1</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>$NM_CORES</value>
  </property>

  <!-- Disable physical/virtual memory enforcement so a slightly oversized
       JVM doesn't get killed; we trust the heap settings in mapred-site.xml. -->
  <property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>

  <!-- Explicit scratch dirs (pre-created world-writable above). The default
       location is hadoop.tmp.dir/nm-local-dir, which lives under a root-owned
       /scratch/tmp parent and fails to localize jars => "No space available
       in any of the local directories". -->
  <property>
    <name>yarn.nodemanager.local-dirs</name>
    <value>/scratch/yarn-local</value>
  </property>
  <property>
    <name>yarn.nodemanager.log-dirs</name>
    <value>/scratch/yarn-logs</value>
  </property>
</configuration>
EOF

# ── Copy remaining configs from the base Hadoop install ──
# capacity-scheduler.xml is REQUIRED -- without it, ResourceManager dies at
# init with "Queue configuration missing child queue names for root".
for f in "$HADOOP_CONF"/log4j.properties \
         "$HADOOP_CONF"/hadoop-env.sh \
         "$HADOOP_CONF"/workers \
         "$HADOOP_CONF"/capacity-scheduler.xml; do
    if [[ -f "$f" ]]; then
        cp "$f" "$CONFIG_DIR/"
    fi
done

# Fallback: if the base install doesn't have capacity-scheduler.xml at all,
# write a minimal one with a single 'default' queue at 100% capacity.
if [[ ! -f "$CONFIG_DIR/capacity-scheduler.xml" ]]; then
    cat > "$CONFIG_DIR/capacity-scheduler.xml" <<'CAPSCHED_EOF'
<configuration>
  <property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>default</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.capacity</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.maximum-capacity</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
    <value>0.5</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.resource-calculator</name>
    <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
  </property>
</configuration>
CAPSCHED_EOF
fi

echo ""
echo "Generated single DataNode config with $K storage directories in $CONFIG_DIR/"
echo "  dfs.datanode.data.dir = $DATA_DIRS"
