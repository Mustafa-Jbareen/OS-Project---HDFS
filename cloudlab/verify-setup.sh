#!/bin/bash
set -euo pipefail

echo "=========================================="
echo "CloudLab Setup Verification"
echo "=========================================="
echo ""

FAILED=0

# 1. Check /scratch is mounted and bound to /mydata
echo "[1/7] Checking /scratch mount..."
if mountpoint -q /scratch; then
    SCRATCH_USED=$(df -h /scratch | tail -1 | awk '{print $3}')
    SCRATCH_AVAIL=$(df -h /scratch | tail -1 | awk '{print $4}')
    echo "✓ /scratch is mounted: $SCRATCH_USED used, $SCRATCH_AVAIL available"
else
    echo "✗ /scratch is NOT mounted!"
    FAILED=1
fi
echo ""

# 2. Check Hadoop installation
echo "[2/7] Checking Hadoop installation..."
if [ -d /scratch/hadoop/hadoop-3.3.1 ]; then
    echo "✓ Hadoop found at /scratch/hadoop/hadoop-3.3.1"
    if /scratch/hadoop/hadoop-3.3.1/bin/hadoop version > /dev/null 2>&1; then
        echo "✓ Hadoop command works"
    else
        echo "✗ Hadoop command failed"
        FAILED=1
    fi
else
    echo "✗ Hadoop NOT found at /scratch/hadoop/hadoop-3.3.1"
    FAILED=1
fi
echo ""

# 3. Check Java
echo "[3/7] Checking Java..."
if java -version > /dev/null 2>&1; then
    JAVA_VER=$(java -version 2>&1 | head -1)
    echo "✓ Java available: $JAVA_VER"
else
    echo "✗ Java NOT found"
    FAILED=1
fi
echo ""

# 4. Check HADOOP_HOME env var
echo "[4/7] Checking HADOOP_HOME..."
source /etc/profile.d/hadoop.sh 2>/dev/null || true
if [ -n "$HADOOP_HOME" ]; then
    echo "✓ HADOOP_HOME=$HADOOP_HOME"
else
    echo "⚠ HADOOP_HOME not set (will be set at login)"
fi
echo ""

# 5. Check experiment script edits
echo "[5/7] Checking experiment script edits..."
EXP_SCRIPT="$HOME/my_scripts/experiments/storage_virtualization_loopback/run-experiment-loopback-fs.sh"
if [ -f "$EXP_SCRIPT" ]; then
    BUDGET=$(grep "^LOOPBACK_BUDGET_PER_NODE_GB=" "$EXP_SCRIPT" | head -1 | awk -F= '{print $2}')
    if [ "$BUDGET" = "30" ]; then
        echo "✓ LOOPBACK_BUDGET_PER_NODE_GB is set to $BUDGET (correct)"
    else
        echo "✗ LOOPBACK_BUDGET_PER_NODE_GB is set to $BUDGET (should be 30)"
        FAILED=1
    fi
    
    K_COUNT=$(grep "^K_VALUES=(" "$EXP_SCRIPT" | grep -o "[0-9]*" | wc -l)
    echo "✓ K_VALUES has $K_COUNT test values"
else
    echo "✗ Experiment script not found at $EXP_SCRIPT"
    FAILED=1
fi
echo ""

# 6. Check nodes.txt
echo "[6/7] Checking nodes.txt..."
NODES_FILE="$HOME/my_scripts/cloudlab/nodes.txt"
if [ -f "$NODES_FILE" ]; then
    NODE_COUNT=$(wc -l < "$NODES_FILE")
    echo "✓ nodes.txt exists with $NODE_COUNT nodes"
    echo "  First node (master): $(head -1 $NODES_FILE)"
    echo "  All nodes:"
    sed 's/^/    /' "$NODES_FILE"
else
    echo "✗ nodes.txt not found"
    FAILED=1
fi
echo ""

# 7. Check passwordless SSH to worker nodes
echo "[7/7] Checking passwordless SSH to worker nodes..."
MASTER=$(head -1 "$NODES_FILE")
WORKERS=$(tail -n +2 "$NODES_FILE")
SSH_FAILED=0

for worker in $WORKERS; do
    if timeout 5 ssh -o BatchMode=yes -o ConnectTimeout=3 "$worker" hostname > /dev/null 2>&1; then
        echo "✓ SSH to $worker: OK"
    else
        echo "✗ SSH to $worker: FAILED (passwordless SSH not working)"
        SSH_FAILED=1
    fi
done

if [ "$SSH_FAILED" = "0" ]; then
    echo "✓ All worker nodes reachable via passwordless SSH"
else
    echo "⚠ Some SSH connections failed (cluster key may not be fully configured)"
    FAILED=1
fi
echo ""

echo "=========================================="
if [ "$FAILED" = "0" ]; then
    echo "✓ All checks PASSED! Ready to run experiment."
    echo ""
    echo "Next: bash ~/my_scripts/cloudlab/run-experiment-wrapper.sh 3"
else
    echo "✗ Some checks FAILED. See above for details."
    echo ""
    echo "Troubleshooting:"
    echo "  - If /scratch is not mounted: run 'sudo mount --bind /mydata /scratch'"
    echo "  - If Hadoop is missing: re-run bootstrap"
    echo "  - If SSH fails: check cluster SSH key is in ~/.ssh/authorized_keys on workers"
fi
echo "=========================================="

exit $FAILED
