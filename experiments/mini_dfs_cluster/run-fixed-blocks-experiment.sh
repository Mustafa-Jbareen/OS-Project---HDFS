#!/bin/bash
################################################################################
# SCRIPT: run-fixed-blocks-experiment.sh
# DESCRIPTION: Runs the MiniDFSCluster fixed-blocks distribution experiment.
#              Keeps total block count constant and increases DataNode count.
#              Tests how NameNode memory scales with block distribution.
#
# USAGE: bash run-fixed-blocks-experiment.sh [total_blocks] [max_datanodes]
#        total_blocks  = number of HDFS blocks to create (default: 256)
#        max_datanodes = upper bound for DataNode count (default: 4096)
#
# OUTPUT: CSV and PNG graphs in results/fixed_blocks/
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."
RESULTS_DIR="$SCRIPT_DIR/results"

TOTAL_BLOCKS=${1:-256}
MAX_DATANODES=${2:-512}
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_DIR/fixed_blocks/run_$TIMESTAMP"

mkdir -p "$RUN_DIR"

echo "=============================================="
echo "MiniDFS Fixed-Blocks Distribution Experiment"
echo "=============================================="
echo "Total Blocks:  $TOTAL_BLOCKS"
echo "Max DataNodes: $MAX_DATANODES"
echo "Results:       $RUN_DIR"
echo "=============================================="
echo ""

# Build the project
echo "[1/3] Building project..."
cd "$PROJECT_ROOT"
mvn -DskipTests package
BUILD_STATUS=$?

if [ $BUILD_STATUS -ne 0 ]; then
    echo "ERROR: Maven build failed with exit code $BUILD_STATUS"
    exit 1
fi

JAR_FILE="$PROJECT_ROOT/target/minidfscluster-experiment-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: JAR not found at $JAR_FILE"
    ls -la "$PROJECT_ROOT/target/" 2>/dev/null || true
    exit 1
fi
echo "Build complete: $JAR_FILE"
echo ""

# Run experiment
echo "[2/3] Running fixed-blocks experiment..."
echo "      DataNodes: 2, 4, 8, 16, ... up to $MAX_DATANODES"
echo "      Total blocks: $TOTAL_BLOCKS (each 1 KB, replication=1)"

# Ensure local tmpdir exists + has correct permissions
TMPDIR_OVERRIDE=/home/mostufa.j/tmp
mkdir -p "$TMPDIR_OVERRIDE"
chmod 700 "$TMPDIR_OVERRIDE"
# Remove stale MiniDFS temp directories from prior runs
find "$TMPDIR_OVERRIDE" -maxdepth 1 -type d -name 'minidfs-fixed-*' -prune -exec rm -rf {} +

# Raise OS limits -- CRITICAL for 4096 DataNodes.
# At 4096 DNs we need ~25k threads and ~25k file descriptors.
echo "Raising OS limits..."
ulimit -u "$(ulimit -Hu)" 2>/dev/null || echo "  WARNING: could not raise ulimit -u"
ulimit -n "$(ulimit -Hn)" 2>/dev/null || echo "  WARNING: could not raise ulimit -n. THIS IS THE MAIN BOTTLENECK."
echo "  ulimit -u (max processes):  $(ulimit -u)  [hard: $(ulimit -Hu)]"
echo "  ulimit -n (max open files): $(ulimit -n)  [hard: $(ulimit -Hn)]"
echo ""
if [ "$(ulimit -n)" -lt 65536 ] 2>/dev/null; then
    echo "  WARNING: ulimit -n is $(ulimit -n) which is too low for 4096 DataNodes."
    echo "           Ask your admin to add these lines to /etc/security/limits.conf:"
    echo "             $USER  soft  nofile  65536"
    echo "             $USER  hard  nofile  65536"
    echo ""
fi

# ===================== JVM FLAGS EXPLAINED =====================
# -Xmx16g:  16 GB max heap. At 4096 DNs each DN holds ~1-2 MiB of
#           metadata objects in heap; NameNode block maps add more.
# -Xms4g:   Pre-allocate 4 GB to avoid repeated heap expansion.
# -Xss256k: 256 KB stack per thread (default 1 MB).
#           At ~20k threads: 20k × 256KB = 5 GB stack space.
#           With default 1 MB: 20k × 1 MB = 20 GB -- won't fit.
# -XX:+UseG1GC + reduced parallelism: fewer GC background threads.
# -XX:-ShrinkHeapInSteps: prevent G1GC from de-committing heap
#           between iterations, which causes totalMemory() to shrink
#           and creates misleading memory-dip measurements.
# -Dio.netty.eventLoopThreads=1:
#   THE critical fix for EAGAIN/pthread_create failures.
#   Each DataNode's DatanodeHttpServer creates a Netty NioEventLoopGroup.
#   Default size = 2 × CPU cores (e.g. 16 on 8-core). At 4096 DataNodes
#   that would be 65536 threads from Netty alone, far exceeding ulimit -u.
#   Setting this to 1 caps EVERY EventLoopGroup in the JVM to 1 thread.
# -Dio.netty.allocator.type=unpooled:
#   Use unpooled allocator to avoid Netty's PooledByteBufAllocator
#   which creates per-thread caches and arena structures per DN.
# ================================================================
# Unset JAVA_TOOL_OPTIONS to avoid global interference
unset JAVA_TOOL_OPTIONS

java -Xmx4g -Xms2g \
    -Xss256k \
    -XX:+UseG1GC \
    -XX:ParallelGCThreads=2 \
    -XX:CICompilerCount=2 \
    -XX:ConcGCThreads=1 \
    -XX:-ShrinkHeapInSteps \
    -XX:MaxGCPauseMillis=500 \
    -Djdk.virtualThreadScheduler.parallelism=1 \
    -Dio.netty.eventLoopThreads=1 \
    -Dio.netty.recycler.maxCapacityPerThread=0 \
    -Dio.netty.allocator.type=unpooled \
    -Djava.io.tmpdir="$TMPDIR_OVERRIDE" \
    -Dhadoop.http.jetty.thread.pool.size=16 \
    -cp "$JAR_FILE" \
    com.example.MiniDFSFixedBlocksExperiment \
    "$TOTAL_BLOCKS" \
    "$MAX_DATANODES" \
    "$RUN_DIR/fixed_blocks_memory.csv"

echo ""
echo "Raw data:"
cat "$RUN_DIR/fixed_blocks_memory.csv"
echo ""

# Generate plots
echo "[3/3] Generating plots..."
if command -v python3 &>/dev/null; then
    cd "$SCRIPT_DIR"
    python3 plot_fixed_blocks.py "$RUN_DIR/fixed_blocks_memory.csv" \
        -o "$RUN_DIR/fixed_blocks"

    # Also with log scale
    python3 plot_fixed_blocks.py "$RUN_DIR/fixed_blocks_memory.csv" \
        -o "$RUN_DIR/fixed_blocks_log" --log-y

    echo "Plots saved to $RUN_DIR/"
else
    echo "WARNING: python3 not found, skipping plots"
fi

# Symlink to latest
ln -sfn "$RUN_DIR" "$RESULTS_DIR/fixed_blocks/latest"

echo ""
echo "=============================================="
echo "Experiment Complete!"
echo "=============================================="
echo "Results: $RUN_DIR"
echo "=============================================="
