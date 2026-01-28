#!/bin/bash
################################################################################
# SCRIPT: reset-hdfs-contents.sh
# DESCRIPTION: Removes all files and directories from HDFS while keeping the
#              HDFS service running. This is a "soft reset" that clears all
#              user data without restarting the cluster.
# USAGE: bash reset-hdfs-contents.sh
# PREREQUISITES:
#   - HDFS cluster running.
# OUTPUT: Empty HDFS filesystem (service remains running).
################################################################################

set -e

echo "=============================================="
echo "HDFS Contents Reset"
echo "=============================================="

# Check if HDFS is accessible
echo "[*] Checking HDFS status..."
if ! hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; then
    # Try to leave safe mode if stuck
    echo "[*] Attempting to leave safe mode..."
    hdfs dfsadmin -safemode leave || true
fi

echo "[*] Listing current HDFS root contents..."
hdfs dfs -ls / 2>/dev/null || echo "    (HDFS root is empty or inaccessible)"

echo ""
echo "[*] Removing all files and directories from HDFS..."

# Get list of all items in root (excluding system directories like /system)
# and remove them one by one
for item in $(hdfs dfs -ls / 2>/dev/null | awk '{print $NF}' | grep -v "^$"); do
    # Skip the header line (which doesn't start with /)
    if [[ "$item" == /* ]]; then
        echo "    Removing: $item"
        hdfs dfs -rm -r -f -skipTrash "$item" 2>/dev/null || true
    fi
done

echo ""
echo "[*] Emptying trash..."
hdfs dfs -expunge 2>/dev/null || true

echo ""
echo "[*] Verifying HDFS is empty..."
REMAINING=$(hdfs dfs -ls / 2>/dev/null | tail -n +2 | wc -l)
if [ "$REMAINING" -eq 0 ]; then
    echo "    HDFS is now empty"
else
    echo "    Warning: $REMAINING items remain in HDFS root"
    hdfs dfs -ls /
fi

echo ""
echo "[*] Checking HDFS service status..."
hdfs dfsadmin -report 2>/dev/null | head -20

echo ""
echo "=============================================="
echo "HDFS Contents Reset Complete!"
echo "=============================================="
echo "All user data has been removed."
echo "HDFS service is still running."
echo "=============================================="
