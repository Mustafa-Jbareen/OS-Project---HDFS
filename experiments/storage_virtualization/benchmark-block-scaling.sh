#!/bin/bash
################################################################################
# SCRIPT: benchmark-block-scaling.sh
# DESCRIPTION: Comprehensive block count scaling experiment.
#              Creates files with increasing block counts and measures:
#              - NameNode heap usage
#              - Block report latency
#              - File operation latency (ls, stat)
#
# CONCEPT: More blocks = more metadata = more NameNode memory
#          This helps understand scalability limits.
#
# USAGE: bash benchmark-block-scaling.sh [max_blocks]
# OUTPUT: CSV with block_count, heap_mb, block_report_ms, ls_latency_ms
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

MAX_BLOCKS=${1:-100000}
BLOCK_SIZE=${2:-$((128 * 1024))}  # 128KB blocks by default

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_DIR/block_scaling_${TIMESTAMP}"
mkdir -p "$RUN_DIR"

CSV_FILE="$RUN_DIR/results.csv"
LOG_FILE="$RUN_DIR/benchmark.log"

MASTER_NODE=${MASTER_NODE:-localhost}

log() {
    echo "$1" | tee -a "$LOG_FILE"
}

get_namenode_heap() {
    local JMX_DATA=$(curl -s "http://${MASTER_NODE}:9870/jmx" 2>/dev/null || echo "{}")
    local HEAP_USED=$(echo "$JMX_DATA" | grep -o '"HeapMemoryUsage"[^}]*' | grep -o '"used":[0-9]*' | cut -d: -f2 || echo "0")
    echo $((HEAP_USED / 1024 / 1024))
}

get_block_count() {
    hdfs dfsadmin -report 2>/dev/null | grep -i "blocks" | head -1 | grep -o '[0-9]*' | head -1 || echo "0"
}

measure_ls_latency() {
    local START=$(date +%s.%N)
    hdfs dfs -ls -R /block_test 2>/dev/null | wc -l >/dev/null
    local END=$(date +%s.%N)
    echo "scale=2; ($END - $START) * 1000" | bc
}

measure_fsck_latency() {
    local START=$(date +%s.%N)
    hdfs fsck /block_test -files -blocks 2>/dev/null | tail -1 >/dev/null
    local END=$(date +%s.%N)
    echo "scale=2; ($END - $START) * 1000" | bc
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

log "=============================================="
log "Block Count Scaling Benchmark"
log "=============================================="
log "Target max blocks: $MAX_BLOCKS"
log "Block size: $BLOCK_SIZE bytes"
log "Results: $RUN_DIR"
log "=============================================="
log ""

# Clean up previous test data
hdfs dfs -rm -r -f /block_test 2>/dev/null || true
hdfs dfs -mkdir -p /block_test 2>/dev/null

# Initial measurements
INITIAL_BLOCKS=$(get_block_count)
INITIAL_HEAP=$(get_namenode_heap)
log "Initial state: ${INITIAL_BLOCKS} blocks, ${INITIAL_HEAP}MB heap"
log ""

# Save metadata
cat > "$RUN_DIR/metadata.json" <<EOF
{
    "run_id": "$TIMESTAMP",
    "max_blocks": $MAX_BLOCKS,
    "block_size_bytes": $BLOCK_SIZE,
    "initial_blocks": $INITIAL_BLOCKS,
    "initial_heap_mb": $INITIAL_HEAP
}
EOF

# CSV Header
echo "target_blocks,actual_blocks,heap_mb,heap_delta_mb,ls_latency_ms,fsck_latency_ms,file_count" > "$CSV_FILE"

# Generate files to create blocks
# File size to create ~N blocks: N * BLOCK_SIZE bytes
BLOCK_SIZE_MB=$(echo "scale=4; $BLOCK_SIZE / 1024 / 1024" | bc)

# Test at exponential intervals: 100, 1000, 10000, 100000
BLOCK_TARGETS=(100 500 1000 2000 5000 10000 20000 50000 100000)

CURRENT_FILES=0
for TARGET in "${BLOCK_TARGETS[@]}"; do
    if [ "$TARGET" -gt "$MAX_BLOCKS" ]; then
        break
    fi
    
    log "----------------------------------------------"
    log "Creating blocks to reach target: $TARGET"
    log "----------------------------------------------"
    
    # Calculate how many more blocks we need
    CURRENT_BLOCKS=$(get_block_count)
    BLOCKS_NEEDED=$((TARGET - CURRENT_BLOCKS + INITIAL_BLOCKS))
    
    if [ "$BLOCKS_NEEDED" -le 0 ]; then
        log "Already at or past target, skipping..."
        continue
    fi
    
    # Create files to add blocks
    # Each file of size BLOCK_SIZE creates 1 block
    FILES_TO_CREATE=$BLOCKS_NEEDED
    
    log "Creating $FILES_TO_CREATE files (each creates 1 block)..."
    
    # Create files in batches for efficiency
    BATCH_SIZE=100
    for ((i=0; i<FILES_TO_CREATE; i+=BATCH_SIZE)); do
        BATCH_END=$((i + BATCH_SIZE))
        if [ "$BATCH_END" -gt "$FILES_TO_CREATE" ]; then
            BATCH_END=$FILES_TO_CREATE
        fi
        
        # Generate batch of small files
        for ((j=i; j<BATCH_END; j++)); do
            FILE_NUM=$((CURRENT_FILES + j))
            dd if=/dev/urandom bs=$BLOCK_SIZE count=1 2>/dev/null | \
                hdfs dfs -D dfs.blocksize=$BLOCK_SIZE -put - /block_test/file_${FILE_NUM}.bin 2>/dev/null &
        done
        wait
        
        printf "\r  Progress: %d/%d files" "$BATCH_END" "$FILES_TO_CREATE"
    done
    echo ""
    
    CURRENT_FILES=$((CURRENT_FILES + FILES_TO_CREATE))
    
    # Measure metrics
    sleep 2  # Let things settle
    
    ACTUAL_BLOCKS=$(get_block_count)
    HEAP_MB=$(get_namenode_heap)
    HEAP_DELTA=$((HEAP_MB - INITIAL_HEAP))
    LS_LATENCY=$(measure_ls_latency)
    FSCK_LATENCY=$(measure_fsck_latency)
    FILE_COUNT=$(hdfs dfs -ls /block_test 2>/dev/null | wc -l)
    
    log ""
    log "Results at target $TARGET:"
    log "  Actual blocks: $ACTUAL_BLOCKS"
    log "  NameNode heap: ${HEAP_MB}MB (+${HEAP_DELTA}MB)"
    log "  ls -R latency: ${LS_LATENCY}ms"
    log "  fsck latency:  ${FSCK_LATENCY}ms"
    log "  File count:    $FILE_COUNT"
    
    # Save to CSV
    echo "$TARGET,$ACTUAL_BLOCKS,$HEAP_MB,$HEAP_DELTA,$LS_LATENCY,$FSCK_LATENCY,$FILE_COUNT" >> "$CSV_FILE"
done

log ""
log "=============================================="
log "Benchmark Complete!"
log "=============================================="
log "Results saved to: $CSV_FILE"
log ""
log "Key findings:"

# Calculate memory per block
if [ "$(get_block_count)" -gt "$INITIAL_BLOCKS" ]; then
    TOTAL_NEW_BLOCKS=$(($(get_block_count) - INITIAL_BLOCKS))
    TOTAL_NEW_HEAP=$(($(get_namenode_heap) - INITIAL_HEAP))
    BYTES_PER_BLOCK=$((TOTAL_NEW_HEAP * 1024 * 1024 / TOTAL_NEW_BLOCKS))
    log "  Memory per block: ~${BYTES_PER_BLOCK} bytes"
    log "  Blocks per GB heap: ~$((1024 * 1024 * 1024 / BYTES_PER_BLOCK))"
fi

log ""
log "To plot results:"
log "  python3 plot-block-scaling.py $CSV_FILE"
log "=============================================="

# Cleanup
log ""
log "Cleaning up test files..."
hdfs dfs -rm -r -f /block_test 2>/dev/null || true

cat "$CSV_FILE"
