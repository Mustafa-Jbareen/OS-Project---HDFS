#!/bin/bash
################################################################################
# SCRIPT: benchmark-multinode-blocksize.sh
# DESCRIPTION: Comprehensive benchmark that tests WordCount performance across:
#              - Different node counts (2, 3, 4, 5 nodes)
#              - Different block sizes (128MB to 4GB)
#              - Fixed input size (5GB)
#
# This script reconfigures the cluster for each node count, then runs
# WordCount with various block sizes.
#
# USAGE: bash benchmark-multinode-blocksize.sh
# OUTPUT: results/multinode-benchmark/run_<timestamp>/ with CSV and plots
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."
RESULTS_BASE="$PROJECT_ROOT/results/multinode-benchmark"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_BASE/run_$TIMESTAMP"

mkdir -p "$RUN_DIR"

# ============================================================================
# CONFIGURATION
# ============================================================================

# All available nodes (master is always tapuz14)
MASTER_NODE="tapuz14"
ALL_WORKERS=("tapuz10" "tapuz11" "tapuz12" "tapuz13")

# Test configurations
NODE_COUNTS=(2 3 4 5)              # Number of nodes to test (includes master)
INPUT_SIZE_GB=20                    # 20GB input
HADOOP_HOME="/home/mostufa.j/hadoop"
HADOOP_DATA_DIR="/home/mostufa.j/hadoop_data"

# Block sizes: 128MB, 256MB, 512MB, 1GB, 2GB, 4GB
# Expressed as exponents: 2^N bytes
BLOCK_SIZE_EXPONENTS=(
    24    # 2^24 = 16 MB
    25   # 2^25 = 32 MB
    26    # 2^26 = 64 MB
    27    # 2^27 = 128 MB
    28    # 2^28 = 256 MB
    29    # 2^29 = 512 MB
    30    # 2^30 = 1 GB
    31    # 2^31 = 2 GB
    32    # 2^32 = 4 GB
)

LOG_FILE="$RUN_DIR/benchmark.log"
COMBINED_CSV="$RUN_DIR/all_results.csv"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

log() {
    echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

exp_to_bytes() {
    local exp=$1
    echo $((2**exp))
}

bytes_to_human() {
    local bytes=$1
    if (( bytes >= 1073741824 )); then
        echo "$((bytes / 1073741824))GB"
    elif (( bytes >= 1048576 )); then
        echo "$((bytes / 1048576))MB"
    else
        echo "${bytes}B"
    fi
}

# Get worker nodes for a given node count (total nodes = node_count, workers = node_count - 1)
get_workers_for_count() {
    local count=$1
    local workers_needed=$((count - 1))
    echo "${ALL_WORKERS[@]:0:$workers_needed}"
}

# Reconfigure cluster for specific number of nodes
reconfigure_cluster() {
    local node_count=$1
    local workers=($(get_workers_for_count $node_count))
    
    log "Reconfiguring cluster for $node_count nodes..."
    log "  Master: $MASTER_NODE"
    log "  Workers: ${workers[*]}"
    
    # Stop existing cluster
    stop-dfs.sh 2>/dev/null || true
    stop-yarn.sh 2>/dev/null || true
    sleep 2
    
    # Generate new workers file
    local WORKERS_FILE="$HADOOP_HOME/etc/hadoop/workers"
    echo "$MASTER_NODE" > "$WORKERS_FILE"
    for worker in "${workers[@]}"; do
        echo "$worker" >> "$WORKERS_FILE"
    done
    
    # Distribute to all nodes that will be used
    for worker in "${workers[@]}"; do
        scp "$WORKERS_FILE" "$worker:$HADOOP_HOME/etc/hadoop/workers" 2>/dev/null || true
    done
    
    # Clean data directories
    rm -rf "$HADOOP_DATA_DIR/namenode/current" 2>/dev/null || true
    rm -rf "$HADOOP_DATA_DIR/datanode/current" 2>/dev/null || true
    
    for worker in "${workers[@]}"; do
        ssh "$worker" "rm -rf $HADOOP_DATA_DIR/datanode/current" 2>/dev/null || true
    done
    
    # Format and start
    hdfs namenode -format -force -nonInteractive > /dev/null 2>&1
    start-dfs.sh > /dev/null 2>&1
    start-yarn.sh > /dev/null 2>&1
    
    # Wait for cluster to stabilize
    sleep 10
    
    # Verify node count
    local live_nodes=$(hdfs dfsadmin -report 2>/dev/null | grep -i "Live datanodes" | grep -o '[0-9]*' || echo "0")
    log "  Live DataNodes: $live_nodes (expected: $node_count)"
    
    if [[ "$live_nodes" -ne "$node_count" ]]; then
        log "  WARNING: Node count mismatch!"
    fi
}

# Generate input and upload to HDFS
generate_and_upload_input() {
    local size_gb=$1
    local block_size=$2
    local size_mb=$((size_gb * 1024))
    
    log "Generating ${size_gb}GB input with block size $(bytes_to_human $block_size)..."
    
    # Clean previous input
    hdfs dfs -rm -r -f /user/$USER/wordcount 2>/dev/null || true
    hdfs dfs -mkdir -p /user/$USER/wordcount/input 2>/dev/null || true
    
    # Generate input using the existing script
    bash "$SCRIPT_DIR/generate-input.sh" "$size_mb" "$block_size" 2>/dev/null
}

# Run WordCount and return runtime
run_wordcount() {
    local start_time=$(date +%s.%N)
    
    hdfs dfs -rm -r -f /user/$USER/wordcount/output 2>/dev/null || true
    
    hadoop jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-"*.jar \
        wordcount \
        /user/$USER/wordcount/input \
        /user/$USER/wordcount/output 2>/dev/null
    
    local end_time=$(date +%s.%N)
    echo "scale=2; $end_time - $start_time" | bc
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

echo "=============================================="
echo "Multi-Node Block Size Benchmark"
echo "=============================================="
echo "Run ID: $TIMESTAMP"
echo "Input size: ${INPUT_SIZE_GB}GB"
echo "Node counts: ${NODE_COUNTS[*]}"
echo "Block sizes: 128MB, 256MB, 512MB, 1GB, 2GB, 4GB"
echo "Results: $RUN_DIR"
echo "=============================================="
echo ""

# Save metadata
cat > "$RUN_DIR/metadata.json" <<EOF
{
    "run_id": "$TIMESTAMP",
    "input_size_gb": $INPUT_SIZE_GB,
    "node_counts": [$(IFS=,; echo "${NODE_COUNTS[*]}")],
    "block_size_exponents": [$(IFS=,; echo "${BLOCK_SIZE_EXPONENTS[*]}")],
    "master_node": "$MASTER_NODE",
    "all_workers": ["$(IFS='","'; echo "${ALL_WORKERS[*]}")"],
    "start_time": "$(date -Iseconds)"
}
EOF

# Initialize combined CSV
echo "node_count,block_size_exp,block_size_bytes,block_size_human,runtime_seconds" > "$COMBINED_CSV"

# Run benchmarks for each node count
for node_count in "${NODE_COUNTS[@]}"; do
    log ""
    log "========================================================"
    log "TESTING WITH $node_count NODES"
    log "========================================================"
    
    # Create per-node-count CSV
    NODE_CSV="$RUN_DIR/results_${node_count}nodes.csv"
    echo "block_size_exp,block_size_bytes,block_size_human,runtime_seconds" > "$NODE_CSV"
    
    # Reconfigure cluster
    reconfigure_cluster $node_count
    
    for exp in "${BLOCK_SIZE_EXPONENTS[@]}"; do
        block_size=$(exp_to_bytes $exp)
        human_size=$(bytes_to_human $block_size)
        
        log ""
        log "  Block Size: $human_size (2^$exp = $block_size bytes)"
        log "  ------------------------------------------------"
        
        # Generate and upload input
        generate_and_upload_input $INPUT_SIZE_GB $block_size
        
        # Run WordCount
        log "  Running WordCount..."
        runtime=$(run_wordcount)
        log "  Runtime: ${runtime}s"
        
        # Record results
        echo "$exp,$block_size,$human_size,$runtime" >> "$NODE_CSV"
        echo "$node_count,$exp,$block_size,$human_size,$runtime" >> "$COMBINED_CSV"
    done
    
    log ""
    log "Results for $node_count nodes saved to: $NODE_CSV"
done

# Clean up
log ""
log "Cleaning up..."
hdfs dfs -rm -r -f /user/$USER/wordcount 2>/dev/null || true

# Restore full cluster configuration (5 nodes)
log "Restoring full cluster configuration (5 nodes)..."
reconfigure_cluster 5

echo ""
echo "=============================================="
echo "Benchmark Complete!"
echo "=============================================="
echo "Results saved to: $RUN_DIR"
echo ""
echo "Files:"
echo "  - all_results.csv: Combined results for all node counts"
for nc in "${NODE_COUNTS[@]}"; do
    echo "  - results_${nc}nodes.csv: Results for $nc nodes"
done
echo "  - metadata.json: Run configuration"
echo "  - benchmark.log: Detailed log"
echo ""
echo "Generate plots with:"
echo "  python3 $SCRIPT_DIR/plot-multinode-results.py $RUN_DIR"
echo "=============================================="

# Create symlink to latest
ln -sfn "$RUN_DIR" "$RESULTS_BASE/latest"
