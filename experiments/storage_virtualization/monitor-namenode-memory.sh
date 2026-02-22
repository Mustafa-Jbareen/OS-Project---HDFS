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

# Test JMX connectivity first
echo "Testing JMX connection..."
echo "  URL: $JMX_URL"

# First check if NameNode is running
if ! jps 2>/dev/null | grep -q "NameNode"; then
    echo "ERROR: NameNode process not found!"
    echo "Start HDFS with: start-dfs.sh"
    exit 1
fi
echo "  NameNode process: running ✓"

# Try to connect with verbose output
echo "  Attempting connection..."

# Write to temp file to avoid pipe blocking issues
TEMP_JMX_FILE=$(mktemp)
if curl -sL --connect-timeout 3 --max-time 10 "$JMX_URL" > "$TEMP_JMX_FILE" 2>/dev/null; then
    TEST_JMX=$(head -c 200 "$TEMP_JMX_FILE")
    rm -f "$TEMP_JMX_FILE"
else
    rm -f "$TEMP_JMX_FILE"
    echo ""
    echo "ERROR: curl failed to connect to NameNode JMX at $JMX_URL"
    echo ""
    echo "Debugging info:"
    echo "  1. Check if port 9870 is listening:"
    echo "     ss -tlnp | grep 9870"
    echo ""
    echo "  2. Try curl manually:"
    echo "     curl -v http://localhost:9870/ 2>&1 | head -20"
    echo ""
    echo "  3. Check NameNode logs:"
    echo "     tail -50 \$HADOOP_HOME/logs/hadoop-*-namenode-*.log"
    echo ""
    exit 1
fi

if [ -z "$TEST_JMX" ]; then
    echo ""
    echo "ERROR: Empty response from NameNode JMX at $JMX_URL"
    exit 1
fi

if echo "$TEST_JMX" | grep -q "beans"; then
    echo "  JMX response: valid ✓"
else
    echo "  WARNING: JMX response doesn't look right:"
    echo "  $TEST_JMX"
fi
echo ""

for ((i=1; i<=SAMPLES; i++)); do
    CURRENT_TIME=$(date +"%Y-%m-%d %H:%M:%S")
    
    # Fetch JMX metrics (NameNode exposes metrics via HTTP)
    JMX_DATA=$(curl -sL --connect-timeout 5 "$JMX_URL" 2>/dev/null || echo "{}")
    
    # Parse heap memory - look for java.lang:type=Memory bean
    # The format is: "HeapMemoryUsage":{"committed":xxx,"init":xxx,"max":xxx,"used":xxx}
    # Use Python for reliable JSON parsing if available, otherwise fall back to grep
    if command -v python3 &>/dev/null; then
        HEAP_INFO=$(echo "$JMX_DATA" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for bean in data.get('beans', []):
        if bean.get('name') == 'java.lang:type=Memory':
            heap = bean.get('HeapMemoryUsage', {})
            print(f\"{heap.get('used', 0)} {heap.get('max', 0)}\")
            break
    else:
        print('0 0')
except:
    print('0 0')
" 2>/dev/null || echo "0 0")
        HEAP_USED=$(echo "$HEAP_INFO" | awk '{print $1}')
        HEAP_MAX=$(echo "$HEAP_INFO" | awk '{print $2}')
    else
        # Fallback: use grep (less reliable)
        HEAP_USED=$(echo "$JMX_DATA" | grep -oP '"HeapMemoryUsage"\s*:\s*\{[^}]*"used"\s*:\s*\K[0-9]+' | head -1 || echo "0")
        HEAP_MAX=$(echo "$JMX_DATA" | grep -oP '"HeapMemoryUsage"\s*:\s*\{[^}]*"max"\s*:\s*\K[0-9]+' | head -1 || echo "0")
    fi
    
    # Ensure we have numbers
    HEAP_USED=${HEAP_USED:-0}
    HEAP_MAX=${HEAP_MAX:-0}
    
    # Convert to MB
    if [ "$HEAP_USED" -gt 0 ] 2>/dev/null; then
        HEAP_USED_MB=$((HEAP_USED / 1024 / 1024))
    else
        HEAP_USED_MB=0
    fi
    
    if [ "$HEAP_MAX" -gt 0 ] 2>/dev/null; then
        HEAP_MAX_MB=$((HEAP_MAX / 1024 / 1024))
    else
        HEAP_MAX_MB=0
    fi
    
    if [ "$HEAP_MAX_MB" -gt 0 ]; then
        HEAP_PCT=$((HEAP_USED_MB * 100 / HEAP_MAX_MB))
    else
        HEAP_PCT=0
    fi
    
    # Get block count from JMX (BlocksTotal in FSNamesystem bean)
    if command -v python3 &>/dev/null; then
        BLOCK_COUNT=$(echo "$JMX_DATA" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for bean in data.get('beans', []):
        if 'FSNamesystem' in bean.get('name', ''):
            print(bean.get('BlocksTotal', 0))
            break
    else:
        print(0)
except:
    print(0)
" 2>/dev/null || echo "0")
    else
        BLOCK_COUNT=$(echo "$JMX_DATA" | grep -oP '"BlocksTotal"\s*:\s*\K[0-9]+' | head -1 || echo "0")
    fi
    
    # Get file count from NameNode metrics (faster than fsck!)
    if command -v python3 &>/dev/null; then
        FILE_COUNT=$(echo "$JMX_DATA" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for bean in data.get('beans', []):
        if 'FSNamesystem' in bean.get('name', ''):
            print(bean.get('FilesTotal', 0))
            break
    else:
        print(0)
except:
    print(0)
" 2>/dev/null || echo "0")
    else
        FILE_COUNT=$(echo "$JMX_DATA" | grep -oP '"FilesTotal"\s*:\s*\K[0-9]+' | head -1 || echo "0")
    fi
    
    # Get capacity from JMX (faster than hdfs report)
    if command -v python3 &>/dev/null; then
        CAP_INFO=$(echo "$JMX_DATA" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for bean in data.get('beans', []):
        if 'FSNamesystem' in bean.get('name', ''):
            total = bean.get('CapacityTotal', 0)
            used = bean.get('CapacityUsed', 0)
            print(f'{total} {used}')
            break
    else:
        print('0 0')
except:
    print('0 0')
" 2>/dev/null || echo "0 0")
        TOTAL_CAP=$(echo "$CAP_INFO" | awk '{print $1}')
        USED_CAP=$(echo "$CAP_INFO" | awk '{print $2}')
    else
        TOTAL_CAP=$(echo "$HDFS_REPORT" | grep -i "Present Capacity" | grep -o '[0-9]*' | head -1 || echo "0")
        USED_CAP=$(echo "$HDFS_REPORT" | grep -i "DFS Used" | head -1 | grep -o '[0-9]*' | head -1 || echo "0")
    fi
    
    TOTAL_CAP=${TOTAL_CAP:-0}
    USED_CAP=${USED_CAP:-0}
    
    if [ "$TOTAL_CAP" -gt 0 ] 2>/dev/null; then
        TOTAL_CAP_GB=$((TOTAL_CAP / 1024 / 1024 / 1024))
    else
        TOTAL_CAP_GB=0
    fi
    
    if [ "$USED_CAP" -gt 0 ] 2>/dev/null; then
        USED_CAP_GB=$((USED_CAP / 1024 / 1024 / 1024))
    else
        USED_CAP_GB=0
    fi
    
    # Log to CSV
    echo "$CURRENT_TIME,$HEAP_USED_MB,$HEAP_MAX_MB,$HEAP_PCT,$BLOCK_COUNT,$FILE_COUNT,$TOTAL_CAP_GB,$USED_CAP_GB" >> "$OUTPUT_FILE"
    
    # Console output
    printf "\r[%d/%d] Heap: %dMB/%dMB (%d%%) | Blocks: %s | Files: %s" \
           "$i" "$SAMPLES" "$HEAP_USED_MB" "$HEAP_MAX_MB" "$HEAP_PCT" "${BLOCK_COUNT:-0}" "${FILE_COUNT:-0}"
    
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
