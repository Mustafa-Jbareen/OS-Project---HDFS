#!/bin/bash
################################################################################
# SCRIPT: simulate-virtual-storage-failure.sh
# DESCRIPTION: Simulates failure of individual virtual storage units.
#              Tests HDFS resilience when one of many storage dirs fails.
#
# CONCEPT: With 1000 virtual storages per DataNode, failures are more granular.
#          This script tests:
#          - How HDFS detects failed storage directories
#          - How blocks are re-replicated
#          - Recovery time and overhead
#
# USAGE: bash simulate-virtual-storage-failure.sh [num_dirs] [fail_count]
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

NUM_DIRS=${1:-16}        # Total storage directories
FAIL_COUNT=${2:-1}       # How many to "fail"
TEST_FILE_SIZE_MB=${3:-256}

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_DIR/failure_sim_${TIMESTAMP}"
mkdir -p "$RUN_DIR"

LOG_FILE="$RUN_DIR/simulation.log"
CSV_FILE="$RUN_DIR/results.csv"

HADOOP_DATA_BASE=${HADOOP_DATA_BASE:-/home/mostufa.j/hadoop_data}
MASTER_NODE=${MASTER_NODE:-tapuz14}

log() {
    echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# ============================================================================
# FUNCTIONS
# ============================================================================

get_block_count() {
    hdfs dfsadmin -report 2>/dev/null | grep -i "blocks" | head -1 | grep -o '[0-9]*' | head -1 || echo "0"
}

get_under_replicated_blocks() {
    hdfs fsck / 2>/dev/null | grep -i "Under-replicated" | grep -o '[0-9]*' | head -1 || echo "0"
}

get_storage_count() {
    hdfs dfsadmin -report 2>/dev/null | grep -c "Storage:" || echo "0"
}

wait_for_replication() {
    local MAX_WAIT=$1
    local START=$(date +%s)
    
    log "Waiting for block replication to complete..."
    
    while true; do
        local UNDER_REP=$(get_under_replicated_blocks)
        local ELAPSED=$(($(date +%s) - START))
        
        if [ "$UNDER_REP" -eq 0 ]; then
            log "Replication complete after ${ELAPSED}s"
            echo "$ELAPSED"
            return 0
        fi
        
        if [ "$ELAPSED" -gt "$MAX_WAIT" ]; then
            log "Timeout waiting for replication (${UNDER_REP} under-replicated)"
            echo "$MAX_WAIT"
            return 1
        fi
        
        printf "\r  Under-replicated: %d, elapsed: %ds" "$UNDER_REP" "$ELAPSED"
        sleep 2
    done
}

simulate_storage_failure() {
    local DIR_NUM=$1
    local NODE=$2
    
    log "Simulating failure of storage vol${DIR_NUM} on $NODE"
    
    # Option 1: Make directory read-only (soft failure)
    # ssh "$NODE" "chmod 000 ${HADOOP_DATA_BASE}/datanode/vol${DIR_NUM}"
    
    # Option 2: Delete the directory contents (hard failure)
    ssh "$NODE" "rm -rf ${HADOOP_DATA_BASE}/datanode/vol${DIR_NUM}/current/*" 2>/dev/null || true
    
    # Option 3: Rename directory (cleanest simulation)
    ssh "$NODE" "mv ${HADOOP_DATA_BASE}/datanode/vol${DIR_NUM} ${HADOOP_DATA_BASE}/datanode/vol${DIR_NUM}_failed" 2>/dev/null || true
}

recover_storage() {
    local DIR_NUM=$1
    local NODE=$2
    
    log "Recovering storage vol${DIR_NUM} on $NODE"
    
    ssh "$NODE" "mv ${HADOOP_DATA_BASE}/datanode/vol${DIR_NUM}_failed ${HADOOP_DATA_BASE}/datanode/vol${DIR_NUM}" 2>/dev/null || true
    ssh "$NODE" "mkdir -p ${HADOOP_DATA_BASE}/datanode/vol${DIR_NUM}/current" 2>/dev/null || true
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

log "=============================================="
log "Virtual Storage Failure Simulation"
log "=============================================="
log "Total storage dirs: $NUM_DIRS"
log "Dirs to fail: $FAIL_COUNT"
log "Test file size: ${TEST_FILE_SIZE_MB}MB"
log "Results: $RUN_DIR"
log "=============================================="
log ""

# Save metadata
cat > "$RUN_DIR/metadata.json" <<EOF
{
    "run_id": "$TIMESTAMP",
    "num_dirs": $NUM_DIRS,
    "fail_count": $FAIL_COUNT,
    "test_file_size_mb": $TEST_FILE_SIZE_MB
}
EOF

# CSV Header
echo "phase,storage_count,block_count,under_replicated,time_seconds" > "$CSV_FILE"

# Phase 1: Initial state
log ""
log "Phase 1: Recording initial state"
log "─────────────────────────────────"

INITIAL_STORAGE=$(get_storage_count)
INITIAL_BLOCKS=$(get_block_count)
INITIAL_UNDER_REP=$(get_under_replicated_blocks)

log "Initial storage count: $INITIAL_STORAGE"
log "Initial block count: $INITIAL_BLOCKS"
log "Initial under-replicated: $INITIAL_UNDER_REP"

echo "initial,$INITIAL_STORAGE,$INITIAL_BLOCKS,$INITIAL_UNDER_REP,0" >> "$CSV_FILE"

# Phase 2: Create test data
log ""
log "Phase 2: Creating test data"
log "─────────────────────────────────"

hdfs dfs -mkdir -p /failure_test 2>/dev/null || true

log "Uploading ${TEST_FILE_SIZE_MB}MB test file..."
dd if=/dev/urandom bs=1M count=$TEST_FILE_SIZE_MB 2>/dev/null | \
    hdfs dfs -put - /failure_test/testdata.bin 2>/dev/null

BLOCKS_AFTER_UPLOAD=$(get_block_count)
log "Blocks after upload: $BLOCKS_AFTER_UPLOAD"

echo "after_upload,$INITIAL_STORAGE,$BLOCKS_AFTER_UPLOAD,0,0" >> "$CSV_FILE"

# Phase 3: Simulate failures
log ""
log "Phase 3: Simulating storage failures"
log "─────────────────────────────────"

FAILURE_START=$(date +%s)

# Fail random storage directories on the master node
for ((i=1; i<=FAIL_COUNT; i++)); do
    # Pick a random directory to fail (1 to NUM_DIRS)
    DIR_TO_FAIL=$((RANDOM % NUM_DIRS + 1))
    simulate_storage_failure $DIR_TO_FAIL "$MASTER_NODE"
done

# Wait a bit for HDFS to detect the failure
sleep 5

STORAGE_AFTER_FAILURE=$(get_storage_count)
BLOCKS_AFTER_FAILURE=$(get_block_count)
UNDER_REP_AFTER_FAILURE=$(get_under_replicated_blocks)
FAILURE_ELAPSED=$(($(date +%s) - FAILURE_START))

log "Storage count after failure: $STORAGE_AFTER_FAILURE (was $INITIAL_STORAGE)"
log "Blocks after failure: $BLOCKS_AFTER_FAILURE"
log "Under-replicated after failure: $UNDER_REP_AFTER_FAILURE"

echo "after_failure,$STORAGE_AFTER_FAILURE,$BLOCKS_AFTER_FAILURE,$UNDER_REP_AFTER_FAILURE,$FAILURE_ELAPSED" >> "$CSV_FILE"

# Phase 4: Wait for recovery
log ""
log "Phase 4: Waiting for automatic recovery"
log "─────────────────────────────────"

RECOVERY_START=$(date +%s)
RECOVERY_TIME=$(wait_for_replication 300)
RECOVERY_ELAPSED=$(($(date +%s) - RECOVERY_START))

FINAL_BLOCKS=$(get_block_count)
FINAL_UNDER_REP=$(get_under_replicated_blocks)

echo "after_recovery,$STORAGE_AFTER_FAILURE,$FINAL_BLOCKS,$FINAL_UNDER_REP,$RECOVERY_ELAPSED" >> "$CSV_FILE"

# Phase 5: Cleanup and restore
log ""
log "Phase 5: Cleanup"
log "─────────────────────────────────"

# Restore failed directories
for ((i=1; i<=FAIL_COUNT; i++)); do
    DIR_TO_RESTORE=$((RANDOM % NUM_DIRS + 1))
    recover_storage $DIR_TO_RESTORE "$MASTER_NODE"
done

hdfs dfs -rm -r -f /failure_test 2>/dev/null || true

log ""
log "=============================================="
log "Simulation Complete!"
log "=============================================="
log ""
log "Summary:"
log "  Storages failed: $FAIL_COUNT of $NUM_DIRS"
log "  Storage reduction: $((INITIAL_STORAGE - STORAGE_AFTER_FAILURE))"
log "  Under-replicated blocks: $UNDER_REP_AFTER_FAILURE"
log "  Recovery time: ${RECOVERY_TIME}s"
log ""
log "Key insight: With ${NUM_DIRS} virtual storages, failing ${FAIL_COUNT}"
log "only affects $((100 * FAIL_COUNT / NUM_DIRS))% of local data capacity."
log ""
log "Results saved to: $CSV_FILE"
log "=============================================="

cat "$CSV_FILE"
