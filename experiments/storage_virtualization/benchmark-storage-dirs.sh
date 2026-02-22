#!/bin/bash
################################################################################
# SCRIPT: benchmark-storage-dirs.sh
# DESCRIPTION: Tests HDFS performance with varying numbers of storage directories
#              per DataNode. This simulates virtual storage units.
#
# CONCEPT: HDFS allows multiple dfs.datanode.data.dir paths.
#          Each directory is treated as a separate "storage" with:
#          - Independent failure detection
#          - Separate capacity tracking
#          - Round-robin block placement
#
# USAGE: bash benchmark-storage-dirs.sh [max_dirs]
# OUTPUT: CSV with storage_dirs, write_throughput, read_throughput, block_report_time
################################################################################

set -euo pipefail

# ============================================================================
# CLEANUP TRAP - Ensure cluster is in good state on exit
# ============================================================================
cleanup() {
    echo ""
    echo "Caught interrupt, cleaning up..."
    # Kill any child processes
    pkill -P $$ 2>/dev/null || true
    # Clean up temp files
    rm -f /tmp/test_write.bin /tmp/test_read.bin /tmp/hdfs-site-multivol.xml 2>/dev/null || true
    echo "Cleanup complete."
    exit 1
}
trap cleanup SIGINT SIGTERM

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

MAX_DIRS=${1:-64}
TEST_FILE_SIZE_MB=${2:-1024}  # 1GB test file

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_DIR/storage_dirs_${TIMESTAMP}"
mkdir -p "$RUN_DIR"

CSV_FILE="$RUN_DIR/results.csv"
LOG_FILE="$RUN_DIR/benchmark.log"

# Hadoop configuration
HADOOP_HOME=${HADOOP_HOME:-/home/mostufa.j/hadoop}
HADOOP_DATA_BASE=${HADOOP_DATA_BASE:-/home/mostufa.j/hadoop_data}

# Worker nodes
WORKER_NODES=("tapuz12" "tapuz13")
MASTER_NODE="tapuz14"

log() {
    echo "$1" | tee -a "$LOG_FILE"
}

# ============================================================================
# FUNCTIONS
# ============================================================================

create_storage_dirs() {
    local NUM_DIRS=$1
    local NODE=$2
    
    log "Creating $NUM_DIRS storage directories on $NODE..."
    
    # Build comma-separated list of directories
    local DIR_LIST=""
    for ((i=1; i<=NUM_DIRS; i++)); do
        if [ -n "$DIR_LIST" ]; then
            DIR_LIST="${DIR_LIST},"
        fi
        DIR_LIST="${DIR_LIST}${HADOOP_DATA_BASE}/datanode/vol${i}"
    done
    
    # Create directories on the node
    ssh "$NODE" "
        for i in \$(seq 1 $NUM_DIRS); do
            mkdir -p ${HADOOP_DATA_BASE}/datanode/vol\${i}
        done
    " 2>/dev/null
    
    echo "$DIR_LIST"
}

update_hdfs_site() {
    local NUM_DIRS=$1
    local DIR_LIST=$2
    
    log "Updating hdfs-site.xml with $NUM_DIRS storage directories..."
    
    # Generate new hdfs-site.xml with multiple data dirs
    cat > /tmp/hdfs-site-multivol.xml <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>${HADOOP_DATA_BASE}/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>${DIR_LIST}</value>
  </property>
  <property>
    <name>dfs.namenode.fs-limits.min-block-size</name>
    <value>131072</value>
  </property>
  <!-- Report each storage directory separately -->
  <property>
    <name>dfs.datanode.failed.volumes.tolerated</name>
    <value>$((NUM_DIRS / 2))</value>
  </property>
</configuration>
EOF
    
    # Copy to Hadoop config
    cp /tmp/hdfs-site-multivol.xml "$HADOOP_HOME/etc/hadoop/hdfs-site.xml"
    
    # Distribute to workers
    for node in "${WORKER_NODES[@]}"; do
        scp /tmp/hdfs-site-multivol.xml "$node:$HADOOP_HOME/etc/hadoop/hdfs-site.xml" 2>/dev/null
    done
}

restart_cluster() {
    log "Restarting HDFS cluster..."
    
    # Stop HDFS
    stop-dfs.sh 2>/dev/null || true
    sleep 2
    
    # Clean old data (fresh start for each test)
    rm -rf ${HADOOP_DATA_BASE}/datanode/*/current 2>/dev/null || true
    rm -rf ${HADOOP_DATA_BASE}/namenode/current 2>/dev/null || true
    
    for node in "${WORKER_NODES[@]}"; do
        ssh "$node" "rm -rf ${HADOOP_DATA_BASE}/datanode/*/current" 2>/dev/null || true
    done
    
    # Format and start
    hdfs namenode -format -force 2>/dev/null
    start-dfs.sh 2>/dev/null
    
    # Wait for DataNodes to register
    log "Waiting for DataNodes to register..."
    sleep 10
    
    # Verify
    local LIVE_NODES=$(hdfs dfsadmin -report 2>/dev/null | grep -i "Live datanodes" | grep -o '[0-9]*' || echo "0")
    log "Live DataNodes: $LIVE_NODES"
}

measure_write_throughput() {
    local SIZE_MB=$1
    
    log "Measuring write throughput with ${SIZE_MB}MB file..."
    
    # Generate local test file
    dd if=/dev/urandom of=/tmp/test_write.bin bs=1M count="$SIZE_MB" 2>/dev/null
    
    # Time the write to HDFS
    local START=$(date +%s.%N)
    hdfs dfs -put -f /tmp/test_write.bin /test_write.bin 2>/dev/null
    local END=$(date +%s.%N)
    
    local DURATION=$(echo "$END - $START" | bc)
    local THROUGHPUT=$(echo "scale=2; $SIZE_MB / $DURATION" | bc)
    
    rm /tmp/test_write.bin
    echo "$THROUGHPUT"
}

measure_read_throughput() {
    local SIZE_MB=$1
    
    log "Measuring read throughput..."
    
    # Time the read from HDFS
    local START=$(date +%s.%N)
    hdfs dfs -get /test_write.bin /tmp/test_read.bin 2>/dev/null
    local END=$(date +%s.%N)
    
    local DURATION=$(echo "$END - $START" | bc)
    local THROUGHPUT=$(echo "scale=2; $SIZE_MB / $DURATION" | bc)
    
    rm -f /tmp/test_read.bin
    hdfs dfs -rm /test_write.bin 2>/dev/null || true
    
    echo "$THROUGHPUT"
}

measure_block_report_time() {
    log "Measuring block report time..."
    
    # Force a block report and time it
    local START=$(date +%s.%N)
    hdfs dfsadmin -triggerBlockReport localhost 2>/dev/null || true
    local END=$(date +%s.%N)
    
    local DURATION=$(echo "scale=3; ($END - $START) * 1000" | bc)
    echo "$DURATION"
}

get_namenode_heap() {
    # Get NameNode heap usage from JMX
    local JMX_DATA=$(curl -sL --connect-timeout 5 "http://${MASTER_NODE}:9870/jmx" 2>/dev/null || echo "{}")
    
    if command -v python3 &>/dev/null; then
        local HEAP_USED=$(echo "$JMX_DATA" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for bean in data.get('beans', []):
        if bean.get('name') == 'java.lang:type=Memory':
            print(bean.get('HeapMemoryUsage', {}).get('used', 0))
            break
    else:
        print(0)
except:
    print(0)
" 2>/dev/null || echo "0")
    else
        local HEAP_USED=$(echo "$JMX_DATA" | grep -o '"HeapMemoryUsage"[^}]*' | grep -o '"used":[0-9]*' | cut -d: -f2 || echo "0")
    fi
    
    if [ "$HEAP_USED" -gt 0 ] 2>/dev/null; then
        echo $((HEAP_USED / 1024 / 1024))
    else
        echo "0"
    fi
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

log "=============================================="
log "Storage Directory Scaling Benchmark"
log "=============================================="
log "Max directories: $MAX_DIRS"
log "Test file size: ${TEST_FILE_SIZE_MB}MB"
log "Results: $RUN_DIR"
log "=============================================="
log ""

# Save metadata
cat > "$RUN_DIR/metadata.json" <<EOF
{
    "run_id": "$TIMESTAMP",
    "max_dirs": $MAX_DIRS,
    "test_file_size_mb": $TEST_FILE_SIZE_MB,
    "worker_nodes": [$(printf '"%s",' "${WORKER_NODES[@]}" | sed 's/,$//')]
}
EOF

# CSV Header
echo "num_dirs,write_throughput_mbps,read_throughput_mbps,block_report_ms,namenode_heap_mb,storage_count" > "$CSV_FILE"

# Test with different numbers of storage directories: 1, 2, 4, 8, 16, 32, 64, ...
NUM_DIRS=1
while [ $NUM_DIRS -le $MAX_DIRS ]; do
    log ""
    log "=============================================="
    log "Testing with $NUM_DIRS storage directories"
    log "=============================================="
    
    # Create storage directories on all nodes
    DIR_LIST=$(create_storage_dirs $NUM_DIRS "$MASTER_NODE")
    for node in "${WORKER_NODES[@]}"; do
        create_storage_dirs $NUM_DIRS "$node" >/dev/null
    done
    
    # Update configuration
    update_hdfs_site $NUM_DIRS "$DIR_LIST"
    
    # Restart cluster
    restart_cluster
    
    # Run benchmarks
    WRITE_TP=$(measure_write_throughput $TEST_FILE_SIZE_MB)
    READ_TP=$(measure_read_throughput $TEST_FILE_SIZE_MB)
    BLOCK_REPORT=$(measure_block_report_time)
    HEAP_MB=$(get_namenode_heap)
    
    # Get storage count from HDFS
    STORAGE_COUNT=$(hdfs dfsadmin -report 2>/dev/null | grep -c "Storage" || echo "$NUM_DIRS")
    
    log ""
    log "Results for $NUM_DIRS directories:"
    log "  Write throughput: ${WRITE_TP} MB/s"
    log "  Read throughput:  ${READ_TP} MB/s"
    log "  Block report:     ${BLOCK_REPORT} ms"
    log "  NameNode heap:    ${HEAP_MB} MB"
    log "  Storage count:    ${STORAGE_COUNT}"
    
    # Save to CSV
    echo "$NUM_DIRS,$WRITE_TP,$READ_TP,$BLOCK_REPORT,$HEAP_MB,$STORAGE_COUNT" >> "$CSV_FILE"
    
    # Double the directory count
    NUM_DIRS=$((NUM_DIRS * 2))
done

log ""
log "=============================================="
log "Benchmark Complete!"
log "=============================================="
log "Results saved to: $CSV_FILE"
log ""
log "To plot results:"
log "  python3 plot-storage-dirs.py $CSV_FILE"
log "=============================================="

# Display summary
log ""
log "Summary:"
cat "$CSV_FILE"
