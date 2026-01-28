#!/bin/bash
################################################################################
# SCRIPT: monitor-namenode-memory.sh
# DESCRIPTION: Monitors NameNode heap usage in real-time.
#              Critical for understanding metadata overhead scaling.
# USAGE: bash monitor-namenode-memory.sh [interval_seconds] [duration_minutes]
# OUTPUT: CSV file with timestamp, heap_used, heap_max, block_count
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

INTERVAL=${1:-5}        # Sample every N seconds
DURATION=${2:-60}       # Run for N minutes
SAMPLES=$((DURATION * 60 / INTERVAL))

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
OUTPUT_FILE="$RESULTS_DIR/namenode_memory_${TIMESTAMP}.csv"

echo "=============================================="
echo "NameNode Memory Monitor"
echo "=============================================="
echo "Interval: ${INTERVAL}s"
echo "Duration: ${DURATION} minutes ($SAMPLES samples)"
echo "Output: $OUTPUT_FILE"
echo "=============================================="
echo ""

# CSV Header
echo "timestamp,heap_used_mb,heap_max_mb,heap_pct,block_count,file_count,total_capacity_gb,used_capacity_gb" > "$OUTPUT_FILE"

# Get NameNode JMX URL
NAMENODE_HOST=${NAMENODE_HOST:-localhost}
NAMENODE_HTTP_PORT=${NAMENODE_HTTP_PORT:-9870}
JMX_URL="http://${NAMENODE_HOST}:${NAMENODE_HTTP_PORT}/jmx"

echo "Fetching metrics from: $JMX_URL"
echo ""

for ((i=1; i<=SAMPLES; i++)); do
    CURRENT_TIME=$(date +"%Y-%m-%d %H:%M:%S")
    
    # Fetch JMX metrics (NameNode exposes metrics via HTTP)
    JMX_DATA=$(curl -s "$JMX_URL" 2>/dev/null || echo "{}")
    
    # Parse heap memory from JMX
    HEAP_USED=$(echo "$JMX_DATA" | grep -o '"HeapMemoryUsage"[^}]*' | grep -o '"used":[0-9]*' | cut -d: -f2 || echo "0")
    HEAP_MAX=$(echo "$JMX_DATA" | grep -o '"HeapMemoryUsage"[^}]*' | grep -o '"max":[0-9]*' | cut -d: -f2 || echo "0")
    
    # Convert to MB
    HEAP_USED_MB=$((HEAP_USED / 1024 / 1024))
    HEAP_MAX_MB=$((HEAP_MAX / 1024 / 1024))
    
    if [ "$HEAP_MAX_MB" -gt 0 ]; then
        HEAP_PCT=$((HEAP_USED_MB * 100 / HEAP_MAX_MB))
    else
        HEAP_PCT=0
    fi
    
    # Get block and file count from HDFS report
    HDFS_REPORT=$(hdfs dfsadmin -report 2>/dev/null || echo "")
    BLOCK_COUNT=$(echo "$HDFS_REPORT" | grep -i "blocks" | head -1 | grep -o '[0-9]*' | head -1 || echo "0")
    
    # Get file count
    FSCK_OUTPUT=$(hdfs fsck / -files 2>/dev/null | tail -5 || echo "")
    FILE_COUNT=$(echo "$FSCK_OUTPUT" | grep -i "Total files" | grep -o '[0-9]*' || echo "0")
    
    # Get capacity
    TOTAL_CAP=$(echo "$HDFS_REPORT" | grep -i "Present Capacity" | grep -o '[0-9]*' | head -1 || echo "0")
    USED_CAP=$(echo "$HDFS_REPORT" | grep -i "DFS Used" | head -1 | grep -o '[0-9]*' | head -1 || echo "0")
    TOTAL_CAP_GB=$((TOTAL_CAP / 1024 / 1024 / 1024))
    USED_CAP_GB=$((USED_CAP / 1024 / 1024 / 1024))
    
    # Log to CSV
    echo "$CURRENT_TIME,$HEAP_USED_MB,$HEAP_MAX_MB,$HEAP_PCT,$BLOCK_COUNT,$FILE_COUNT,$TOTAL_CAP_GB,$USED_CAP_GB" >> "$OUTPUT_FILE"
    
    # Console output
    printf "\r[%d/%d] Heap: %dMB/%dMB (%d%%) | Blocks: %s | Files: %s" \
           "$i" "$SAMPLES" "$HEAP_USED_MB" "$HEAP_MAX_MB" "$HEAP_PCT" "$BLOCK_COUNT" "$FILE_COUNT"
    
    sleep "$INTERVAL"
done

echo ""
echo ""
echo "=============================================="
echo "Monitoring Complete"
echo "=============================================="
echo "Results saved to: $OUTPUT_FILE"
echo ""
echo "To plot results:"
echo "  python3 plot-namenode-memory.py $OUTPUT_FILE"
echo "=============================================="
