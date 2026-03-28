#!/bin/bash
################################################################################
# SCRIPT: run-experiment.sh
# DESCRIPTION: Runs the MiniDFSCluster memory scaling experiment locally on tapuz14.
#              Tests memory usage as DataNode count grows from 2 to 4096+.
# USAGE: bash run-experiment.sh [max_datanodes]
# OUTPUT: CSV and PNG graph in results/mini_dfs_cluster/
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."
RESULTS_DIR="$SCRIPT_DIR/results"

MAX_DATANODES=${1:-4096}
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_DIR/run_$TIMESTAMP"

mkdir -p "$RUN_DIR"

echo "=============================================="
echo "MiniDFSCluster Memory Scaling Experiment"
echo "=============================================="
echo "Max DataNodes: $MAX_DATANODES"
echo "Results: $RUN_DIR"
echo "=============================================="
echo ""

# Build the project (pom.xml is in my_scripts root)
echo "[1/3] Building MiniDFSCluster experiment..."
cd "$PROJECT_ROOT"

# Run maven build and show output
mvn -DskipTests package
BUILD_STATUS=$?

if [ $BUILD_STATUS -ne 0 ]; then
    echo "ERROR: Maven build failed with exit code $BUILD_STATUS"
    exit 1
fi

# Note: Maven shade plugin replaces original JAR with shaded version (no -shaded suffix)
JAR_FILE="$PROJECT_ROOT/target/minidfscluster-experiment-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: Build completed but JAR not found at $JAR_FILE"
    echo "Checking target directory:"
    ls -la "$PROJECT_ROOT/target/" 2>/dev/null || echo "target/ directory does not exist"
    exit 1
fi
echo "Build complete: $JAR_FILE"
echo ""

# Run the experiment with increased heap
echo "[2/3] Running experiment (this may take a while)..."
echo "      Testing DataNodes: 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096..."

# Raise OS limits for thread/file-descriptor count
# These are necessary to support 500+ DataNodes (each creates ~10-15 threads + ~5-8 FDs)
echo "Raising OS limits for thread-heavy workload..."
ulimit -u "$(ulimit -Hu)" 2>/dev/null || echo "  WARNING: could not raise max user processes (ulimit -u)."
ulimit -n "$(ulimit -Hn)" 2>/dev/null || echo "  WARNING: could not raise max open files (ulimit -n). THIS IS THE MAIN BOTTLENECK."
echo "  Current ulimit -u (max processes): $(ulimit -u)  [hard: $(ulimit -Hu)]"
echo "  Current ulimit -n (max open files): $(ulimit -n)  [hard: $(ulimit -Hn)]"
echo ""
echo "  TIP: If ulimit -n is still 1024, ask your admin to add these lines to /etc/security/limits.conf:"
echo "        $USER  soft  nofile  65536"
echo "        $USER  hard  nofile  65536"
echo ""

# ===================== JVM FLAGS EXPLAINED =====================
# -Xmx16g:  16 GB max heap. At 4096 DNs each DN holds ~1-2 MiB of
#           metadata objects in heap; NameNode block maps add more.
# -Xms4g:   Pre-allocate 4 GB to avoid repeated heap expansion.
# -Xss256k: 256 KB stack per thread (default 1 MB).
#           At ~20k threads: 20k x 256KB = 5 GB stack space.
#           With default 1 MB: 20k x 1 MB = 20 GB -- won't fit.
# -XX:+UseG1GC + reduced parallelism: fewer GC background threads.
# -XX:-ShrinkHeapInSteps: prevent G1GC from de-committing heap
#           between iterations, which causes totalMemory() to shrink
#           and creates misleading memory-dip measurements.
# -XX:CICompilerCount=2: reduce JIT compiler threads.
# -Dio.netty.eventLoopThreads=1:
#   Caps every Netty NioEventLoopGroup in the JVM to 1 thread.
#   Without this, each DataNode's HTTP server spawns 2xCPU threads (e.g. 16
#   on 8 cores) via Netty, causing EAGAIN/pthread_create failures at scale.
#   At 4096 DNs without this flag: 4096 x 16 = 65536 Netty threads alone!
# -Dio.netty.allocator.type=unpooled:
#   Disable Netty's PooledByteBufAllocator per-thread caches.
# ================================================================
java -Xmx16g -Xms4g \
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
    -jar "$JAR_FILE" \
    "$MAX_DATANODES" \
    "$RUN_DIR/memory_usage.csv"

echo ""
echo "Experiment complete. Raw data:"
cat "$RUN_DIR/memory_usage.csv"
echo ""

# Generate the plot
echo "[3/3] Generating plot..."
if command -v python3 &>/dev/null; then
    cd "$SCRIPT_DIR"
    python3 plot_memory.py "$RUN_DIR/memory_usage.csv" -o "$RUN_DIR/memory_scaling.png"
    
    # Also generate log-scale version
    python3 plot_memory.py "$RUN_DIR/memory_usage.csv" -o "$RUN_DIR/memory_scaling_log.png" --log-y
    
    echo "Plots saved to:"
    echo "  - $RUN_DIR/memory_scaling.png (linear scale)"
    echo "  - $RUN_DIR/memory_scaling_log.png (log scale)"
else
    echo "WARNING: python3 not found, skipping plot generation"
    echo "Copy memory_usage.csv to a machine with Python and matplotlib to generate plots"
fi

# Create symlink to latest
ln -sfn "$RUN_DIR" "$RESULTS_DIR/latest"

echo ""
echo "=============================================="
echo "Experiment Complete!"
echo "=============================================="
echo "Results: $RUN_DIR"
echo "Latest symlink: $RESULTS_DIR/latest"
echo "=============================================="
