# MiniDFSCluster Deep Dive: Comprehensive Explanation

## 🎯 What This Project Does

This is a **Hadoop research toolkit** that:
1. **Manages multi-node real Hadoop clusters** (via shell scripts)
2. **Runs experiments** measuring performance with different configurations
3. **Studies NameNode memory scaling** using in-JVM `MiniDFSCluster` (the focus of this doc)

---

## 📚 Table of Contents

1. [MiniDFSCluster Overview](#minidfscluster-overview)
2. [Core Components & Architecture](#core-components--architecture)
3. [Deep Dive: MiniDFSClusterExperiment.java](#deep-dive-minidfsclusterexperimentjava)
4. [Deep Dive: MiniDFSFixedBlocksExperiment.java](#deep-dive-minidfssfixedblocksexperimentjava)
5. [The 512-DataNode Failure & Solutions](#the-512-datanode-failure--solutions)
6. [How to Extend & Modify](#how-to-extend--modify)
7. [Configuration Parameters Reference](#configuration-parameters-reference)

---

## MiniDFSCluster Overview

### What is MiniDFSCluster?

`MiniDFSCluster` is a **Hadoop testing utility** that:
- Spins up a **full HDFS cluster inside a single JVM**
- Runs NameNode + multiple DataNodes as Java threads (not separate processes)
- Allows testing cluster behavior **without provisioning real machines**
- Runs in-memory with temporary directories

### Why Use It?

✅ **Speed**: No network latency, no machine provisioning  
✅ **Memory control**: Measure heap usage as cluster grows  
✅ **Isolation**: Multiple clusters can run independently  
✅ **Research**: Study scaling behavior without physical hardware  
✅ **Cost**: Use a single powerful machine to simulate 512 DataNodes  

### Key Limitation

⚠️ **Single-process bottleneck**: All DataNodes run as threads in one JVM, so:
- System limitations apply: `ulimit -u` (max processes/threads), `ulimit -n` (max file descriptors)
- Memory is shared across all nodes
- Cannot reach true production scale (e.g., 1000+ DataNodes)

---

## Core Components & Architecture

### Project Structure

```
src/main/java/com/example/
├── MiniDFSClusterExperiment.java          ← Main experiment (memory scaling)
└── MiniDFSFixedBlocksExperiment.java      ← Alternative experiment (fixed blocks)

experiments/mini_dfs_cluster/
├── run-experiment.sh                       ← Execute memory scaling experiment
├── run-fixed-blocks-experiment.sh          ← Execute fixed-blocks experiment
├── plot_memory.py                          ← Visualize results
└── plot_fixed_blocks.py                    ← Visualize fixed-blocks results

pom.xml                                     ← Maven build config (Hadoop dependencies)
```

### Build & Execution Flow

```
┌─────────────────────────────────────┐
│ run-experiment.sh                   │
│ (bash script entry point)           │
└──────────────┬──────────────────────┘
               │
               ├─ mvn package
               │  (builds uber-JAR with all deps)
               │
               └─ java -jar minidfscluster-experiment-*.jar
                  (executes Java experiment)
                  │
                  ├─ For each DataNode count (2, 4, 8, ..., 4096)
                  │  ├─ Create Configuration
                  │  ├─ Apply resource-reduction settings
                  │  ├─ MiniDFSCluster.Builder.numDataNodes(N)
                  │  ├─ cluster.build() → spins up N DataNodes as threads
                  │  ├─ Write test file(s)
                  │  ├─ Measure stable JVM heap
                  │  ├─ cluster.shutdown() → stops all threads
                  │  └─ Record memory sample
                  │
                  └─ Write results to CSV
                     memory_usage.csv: DataNodes,MemoryUsed
```

---

## Deep Dive: MiniDFSClusterExperiment.java

### Purpose

**Measure how much JVM heap memory is consumed as the number of DataNodes grows.**

This answers: **"What is the per-DataNode infrastructure memory overhead?"**

### Main Execution Flow

```java
public static void main(String[] args) throws Exception {
    int maxDataNodes = 4096;      // param 1: scale up to this many DNs
    File outputFile = ...;         // param 2: CSV output path
    
    List<MemorySample> samples = new ArrayList<>();
    
    // Loop: 2 → 4 → 8 → 16 → ... → maxDataNodes (doubling each iteration)
    for (int numDataNodes = 2; numDataNodes <= maxDataNodes; numDataNodes *= 2) {
        runIteration(numDataNodes, baseDir, samples);
        // This writes ONE test file with replication=1
        // So only ONE DataNode gets the block
        // But ALL DataNodes are in memory (threads running)
        
        // Record memory + cleanup
        samples.add(new MemorySample(numDataNodes, usedMemory));
        
        // Cleanup + pause between iterations
    }
    
    saveSamples(outputFile, samples);  // CSV: "DataNodes,MemoryUsed"
}
```

### Step 1: Configuration Setup

The code creates a `Configuration` object with **aggressive resource-reduction settings**:

#### Why Reduce Resources?

The goal is to pack as many DataNodes as possible into a single machine. Each DataNode consumes:

- **10-15 threads** (scanners, handlers, IPC, Netty)
- **5-8 file descriptors** (storage dirs, sockets)
- **~50 MiB heap** (metadata, thread stacks)

At 512 DataNodes on a typical server:
- **Threads needed**: 512 × 15 ≈ **7,680 threads** vs system limit of ~31,000 ✓ (fits)
- **File descriptors**: 512 × 8 ≈ **4,096 FDs** vs limit of 1,024 ✗ (FAILS!)

The reduction settings keep these under control.

#### Key Configuration Settings

```java
Configuration conf = new Configuration();

// ============ DISABLE BACKGROUND SCANNERS ============
// Each DataNode runs a background block scanner
// Saves 2-4 threads per DN
conf.setLong("dfs.block.scanner.volume.bytes.per.second", 0);
conf.setLong("dfs.datanode.directoryscan.interval", -1);

// ============ MINIMIZE HANDLER THREADS ============
// IPC handlers: default 10 per DN
conf.setInt("dfs.datanode.handler.count", 1);

// Xceiver threads (block transfer): default 4096 per DN
conf.setInt("dfs.datanode.max.transfer.threads", 1);

// NameNode handlers: default 10
conf.setInt("dfs.namenode.handler.count", 2);
conf.setInt("dfs.namenode.service.handler.count", 1);
conf.setInt("dfs.namenode.lifeline.handler.count", 1);

// ============ MINIMIZE JETTY (HTTP) THREADS ============
// DataNode has internal HTTP server for block serving
// Default Jetty thread pool: ~200 threads per DN
conf.setInt("hadoop.http.max.threads", 1);
conf.set("dfs.datanode.http.address", "127.0.0.1:0");
conf.set("dfs.datanode.https.address", "127.0.0.1:0");
conf.setInt("dfs.datanode.http.internal-proxy.port", 0);

// ============ REDUCE HEARTBEAT OVERHEAD ============
// Default: 3 seconds, causes lots of RPC threads
conf.setLong("dfs.heartbeat.interval", 30);  // 30 seconds

// ============ SINGLE REPLICA (no replication) ============
conf.setInt("dfs.replication", 1);
conf.setInt("dfs.namenode.replication.min", 1);

// ============ DISABLE DISK SPACE CHECK (CRITICAL!) ============
// Default: dfs.du.interval = 600000 ms (10 minutes)
// Every check spawns a new 'du' shell process per volume
// At scale, these processes pile up and exhaust ulimit -u
// Fix: set to 600 billion ms (~7 days, effectively disabled)
conf.setLong("fs.du.interval", 600_000_000);
conf.setLong("fs.getspaceused.jitterMillis", 0);
```

**Example Savings at 512 DataNodes**:
- Block scanners disabled: -2 threads/DN = **-1,024 threads** ✓
- Jetty reduced from 200 to 1: -199/DN = **-101,888 threads** ✓
- Xceiver threads 1 instead of 4096: saves many FDs ✓
- `du` disabled: saves thread spawning overhead ✓

### Step 2: Build MiniDFSCluster

```java
MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
builder.numDataNodes(numDataNodes);
builder.storagesPerDatanode(1);  // 1 storage dir per DN (saves FDs)
MiniDFSCluster cluster = builder.build();
// ← This creates numDataNodes Java threads!
```

**What happens inside `.build()`**:
1. Starts **NameNode** (thread pool) in the JVM
2. Starts **N DataNode** threads, each with:
   - Block manager, storage, scanner threads
   - IPC server, RPC handlers
   - Jetty HTTP server with Netty EventLoop
   - Heartbeat manager
3. All DataNodes connect to NameNode via RPC (in-process)

### Step 3: Write Test File & Measure Memory

```java
FileSystem fs = cluster.getFileSystem();
Path testFile = new Path("/test-file-" + numDataNodes + ".txt");

try (FSDataOutputStream out = fs.create(testFile)) {
    out.write("Test data".getBytes(StandardCharsets.UTF_8));
}
// This creates 1 small block (replication=1)
// NameNode distributes it to ONE random DataNode
// Other 511 DataNodes have NO blocks, just infrastructure

long usedMemory = stableUsedMemory();
// ← KEY: multiple GC passes until memory converges
samples.add(new MemorySample(numDataNodes, usedMemory));
```

### Step 4: Stable Memory Measurement (Critical!)

```java
private static long stableUsedMemory() throws InterruptedException {
    long prev = Long.MAX_VALUE;
    for (int i = 0; i < 5; i++) {
        System.gc();                    // Hint to GC
        Thread.sleep(500);              // Wait for collection
        long used = Runtime.getRuntime().totalMemory()
                  - Runtime.getRuntime().freeMemory();
        
        // Check convergence: if delta < 1 MiB, we're stable
        if (Math.abs(used - prev) < 1024 * 1024) {
            return used;  // ← Return stable measurement
        }
        prev = used;
    }
    return prev;
}
```

#### Why Is This Necessary?

**Without stable measurement**, you see memory dips:

| DataNodes | Memory (unstable) | Memory (stable) | Issue |
|-----------|------------------|-----------------|-------|
| 2         | 150 MB           | 150 MB          | ✓ |
| 4         | 220 MB           | 220 MB          | ✓ |
| 8         | 280 MB           | 280 MB          | ✓ |
| 16        | **250 MB** ⚠️     | 340 MB          | Dip! |
| 32        | 390 MB           | 390 MB          | ✓ |

**Why dips occur**:

1. **Non-deterministic GC**: `System.gc()` is only a *hint*. The JVM may collect 30 MB or 100 MB of garbage on one call, 60 MB on another.

2. **Heap de-commitment**: G1GC (the collector used) can release committed heap regions back to the OS. `Runtime.totalMemory()` shrinks, making reported usage smaller.

3. **Lazy-init caches**: Hadoop caches DNS lookups, SecurityManager instances, class metadata. These persist unpredictably across cluster iterations.

4. **Thread-local storage**: Netty and Hadoop RPC handlers store thread-local data. When a cluster shuts down, these objects may survive in thread-locals that are recycled for the next cluster.

**Solution**: Run GC multiple times (5 iterations) and only record the measurement once the value converges (changes by < 1 MiB between consecutive calls). This ensures you're measuring the true baseline heap, not temporary garbage.

### Step 5: Cleanup & Repeat

```java
cluster.shutdown();  // Stop all DataNode threads, NameNode
deleteDirectory(baseDir);  // Remove temp files

// Between iterations: aggressive cleanup
System.gc();
Thread.sleep(5000);  // 5 seconds to let daemon threads die
System.gc();
Thread.sleep(1000);

// Monitor thread count
int activeThreads = Thread.activeCount();
if (activeThreads > 25_000) {
    System.err.println("Thread count too high, stopping");
    break;  // Avoid ulimit crash
}
```

### Output Format

```csv
DataNodes,MemoryUsed
2,157286400
4,219025408
8,281395200
16,340582400
32,389120000
64,481234944
128,584908800
256,702046208
```

---

## Deep Dive: MiniDFSFixedBlocksExperiment.java

### Purpose

**Keep total blocks constant, increase DataNodes, measure memory.**

This answers: **"What is the per-block memory overhead when that block is distributed across more DataNodes?"**

### Why This Is Different

| Aspect | Memory Scaling Experiment | Fixed-Blocks Experiment |
|--------|---------------------------|------------------------|
| **Blocks per iteration** | Always 1 block | Fixed (e.g., 256 blocks) |
| **DataNodes per iteration** | Increases (2, 4, 8, ...) | Increases (2, 4, 8, ...) |
| **Blocks per DataNode** | Decreases (1/2, 1/4, 1/8, ...) | Decreases (256/2, 256/4, ...) |
| **What it measures** | Infrastructure overhead per DN | Block metadata overhead per DN |
| **Usage** | Answer: "How many threads/memory per DN?" | Answer: "How much does NameNode scale with distributed blocks?" |

### Main Flow

```java
int totalBlocks = 256;      // Keep this constant
int maxDataNodes = 512;     // Vary this: 2, 4, 8, ..., 512

for (int numDataNodes = 2; numDataNodes <= maxDataNodes; numDataNodes *= 2) {
    // Write totalBlocks files (each 1 KB → 1 block)
    // NameNode distributes 256 blocks across numDataNodes
    // Sample: 256 blocks / 4 DNs = 64 blocks/DN
    
    // Measure heap and record:
    // (DataNodes=4, TotalBlocks=256, BlocksPerDN=64, Memory=...)
}
```

### Key Differences from Memory Scaling

#### 1. Write Many Files (Not Just One)

```java
byte[] data = new byte[FILE_SIZE_BYTES];  // 1 KB
for (int i = 0; i < totalBlocks; i++) {
    Path filePath = new Path("/data/block-" + i + ".dat");
    try (FSDataOutputStream out = fs.create(filePath, (short) 1)) {
        out.write(data);  // One 1 KB file → one block
    }
}
```

Each file is small (1 KB) and exactly one block (replication=1). So:
- 256 files → 256 blocks in NameNode's FsImage
- NameNode distributes these 256 blocks across available DataNodes

#### 2. Block Distribution

When you create 256 blocks in a 4-DataNode cluster:
- NameNode's block placement strategy distributes them round-robin
- Expected: 256 / 4 ≈ **64 blocks per DataNode**
- Reality: 64, 64, 64, 64 (exact if divisible) or 65, 65, 63, 63 (round-robin remainder)

This tests: **How does NameNode memory scale with block metadata?**

#### 3. CSV Output Includes Block-Per-DN Metric

```csv
DataNodes,TotalBlocks,BlocksPerDataNode,MemoryUsed
2,256,128,425238528
4,256,64,486539264
8,256,32,521834496
16,256,16,545607680
32,256,8,564019200
```

Notice: Memory grows even though blocks/DN decreases! This shows:
- NameNode **infrastructure overhead** scales with DataNode count (threads, metadata structures)
- Block metadata overhead is amortized across more DNs

---

## The 512-DataNode Failure & Solutions

### What Fails at 512 DataNodes?

```
Exception: java.io.IOException: error=24, Too many open files
or
Exception: error -1 EAGAIN: Resource temporarily unavailable
or
java.lang.OutOfMemoryError: unable to create new native thread
```

### Root Causes

#### Problem 1: File Descriptors Exhausted (`ulimit -n`)

Each DataNode opens:
- **Storage directory file handles**: 2-3 FDs
- **Socket pairs for inter-process communication**: 4-6 FDs
- **Total per DN**: ~8 FDs

At 512 DataNodes: 512 × 8 = **4,096 FDs needed**

**System limit** (typical): `ulimit -n = 1024` (Ubuntu default)

**Solution**:
```bash
# Raise file descriptor limit to hard limit
ulimit -n $(ulimit -Hn)  # Usually 65536

# Check if raised:
ulimit -n  # Should show 65536
```

**Permanent fix** (ask your sysadmin to add to `/etc/security/limits.conf`):
```
your_username  soft  nofile  65536
your_username  hard  nofile  65536
```

#### Problem 2: Threads Exhausted (`ulimit -u`)

Each DataNode spawns:
- IPC handler threads: 1
- Xceiver threads: 1
- Jetty HTTP thread pool: 1 (with our config)
- Block scanner: 0 (disabled)
- Netty EventLoop threads: 1 per (reduced from 16 default)
- **Total per DN**: ~10 threads (with our optimizations)

At 512 DataNodes: 512 × 10 = **5,120 threads**

**System limit** (typical): `ulimit -u = 31314` ✓ (Usually OK)

**Solution**: Same as above, but less critical.

#### Problem 3: Netty EventLoop Thread Explosion (Most Insidious!)

**Root cause**: Each DataNode has a `DatanodeHttpServer` that spins up a Netty `NioEventLoopGroup`.

**Default Netty behavior**: `EventLoopGroup` size = 2 × CPU cores

On an 8-core machine:
- Per DataNode: 2 × 8 = **16 Netty threads**
- At 512 DataNodes: 512 × 16 = **8,192 Netty threads alone!**

This exceeds `ulimit -u` by default → `EAGAIN` errors.

**Solution**: **JVM flag** `-Dio.netty.eventLoopThreads=1`

This caps **every** Netty `EventLoopGroup` in the JVM to exactly 1 thread.

**Set in run script**:
```bash
java -Dio.netty.eventLoopThreads=1 -jar ...
```

#### Problem 4: Thread Stack Space Exhaustion

Each thread needs stack space (default 1 MB per thread).

At 30,000 threads: 30,000 MB = **30 GB** of virtual memory

This may hit system limits or cause swapping.

**Solution**: Reduce stack size with `-Xss128k` (128 KB per thread)

At 30,000 threads: 30,000 × 128 KB = **3.75 GB** ✓

**Set in run script**:
```bash
java -Xss128k -jar ...
```

#### Problem 5: JVM Heap Fragmentation

As clusters spin up and down, heap becomes fragmented.

**Solution**: Use G1GC (Garbage First) garbage collector with parallelism reduced:

```bash
java -XX:+UseG1GC \
     -XX:ParallelGCThreads=2 \
     -XX:CICompilerCount=2 \
     -XX:ConcGCThreads=1 \
     -Djdk.virtualThreadScheduler.parallelism=1 \
     -jar ...
```

### The Complete Fix

See `experiments/mini_dfs_cluster/run-experiment.sh` for the full JVM command:

```bash
java -Xmx8g -Xms2g \
    -Xss512k \
    -XX:+UseG1GC \
    -XX:ParallelGCThreads=2 \
    -XX:CICompilerCount=2 \
    -XX:ConcGCThreads=1 \
    -Djdk.virtualThreadScheduler.parallelism=1 \
    -Dio.netty.eventLoopThreads=1 \
    -Dio.netty.recycler.maxCapacityPerThread=0 \
    -jar target/minidfscluster-experiment-1.0-SNAPSHOT.jar \
    4096 results/memory_usage.csv
```

**Explanation of each flag**:

| Flag | Purpose |
|------|---------|
| `-Xmx8g` | Max heap 8 GB (needed for metadata) |
| `-Xms2g` | Initial heap 2 GB (reduce startup time) |
| `-Xss512k` | Stack per thread 512 KB (saves 1.5 GB total) |
| `-XX:+UseG1GC` | Use low-pause GC suitable for many threads |
| `-XX:ParallelGCThreads=2` | Only 2 threads for GC (default would be 4 on 8 cores) |
| `-XX:CICompilerCount=2` | Only 2 JIT compiler threads (reduce background work) |
| `-XX:ConcGCThreads=1` | Only 1 concurrent GC marking thread |
| `-Djdk.virtualThreadScheduler.parallelism=1` | Pin Java virtual threads to 1 carrier thread |
| `-Dio.netty.eventLoopThreads=1` | **CRITICAL**: Cap Netty EventLoopGroup to 1 thread |
| `-Dio.netty.recycler.maxCapacityPerThread=0` | Disable Netty object recycler (saves memory) |

---

## How to Extend & Modify

### Scenario 1: Test with More DataNodes

```bash
# Default: up to 4096 DNs
bash experiments/mini_dfs_cluster/run-experiment.sh 4096

# Test up to 1024 DNs (faster)
bash experiments/mini_dfs_cluster/run-experiment.sh 1024

# Test up to 8192 DNs (requires admin to raise ulimit -n!)
bash experiments/mini_dfs_cluster/run-experiment.sh 8192
```

### Scenario 2: Measure Actual HDFS Block Metadata Overhead

Edit `MiniDFSClusterExperiment.java` to write **multiple files per iteration**:

```java
// OLD: Write 1 file per iteration
Path testFile = new Path("/test-file-" + numDataNodes + ".txt");
try (FSDataOutputStream out = fs.create(testFile)) {
    out.write("Test data".getBytes(StandardCharsets.UTF_8));
}

// NEW: Write 100 files per iteration
for (int f = 0; f < 100; f++) {
    Path testFile = new Path("/test-file-" + numDataNodes + "-" + f + ".txt");
    try (FSDataOutputStream out = fs.create(testFile)) {
        out.write(("Test data " + f).getBytes(StandardCharsets.UTF_8));
    }
}
```

This creates 100 blocks distributed across DNs, showing per-block overhead.

### Scenario 3: Test Different Block Sizes

Edit `MiniDFSFixedBlocksExperiment.java`:

```java
// OLD: 1 KB per file
private static final int FILE_SIZE_BYTES = 1024;

// NEW: 1 MB per file (still one block if block size > 1 MB)
private static final int FILE_SIZE_BYTES = 1024 * 1024;

// NEW: 256 MB per file
private static final int FILE_SIZE_BYTES = 256 * 1024 * 1024;
```

Also adjust replication to test block distribution:

```java
// OLD: replication=1
try (FSDataOutputStream out = fs.create(filePath, (short) 1)) {

// NEW: replication=3 (each block replicated to 3 DNs)
try (FSDataOutputStream out = fs.create(filePath, (short) 3)) {
```

### Scenario 4: Measure Memory Peak During File Write

Add memory monitoring **during** write phase:

```java
// NEW: Start memory monitoring thread
MemoryMonitor monitor = new MemoryMonitor();
Thread monitorThread = new Thread(monitor);
monitorThread.setDaemon(true);
monitorThread.start();

// Write files
for (int i = 0; i < totalBlocks; i++) {
    Path filePath = new Path("/data/block-" + i + ".dat");
    try (FSDataOutputStream out = fs.create(filePath, (short) 1)) {
        out.write(data);
    }
}

// Stop monitor and get peak
monitor.stop();
long peakMemory = monitor.getPeakMemory();
samples.add(new Sample(numDataNodes, totalBlocks, 
                       totalBlocks / numDataNodes, peakMemory));
```

### Scenario 5: Test with Replication

Add replication to test NameNode block placement:

```java
// MiniDFSClusterExperiment.java

// NEW: Configure replication
conf.setInt("dfs.replication", 3);                    // Each block on 3 DNs
conf.setInt("dfs.namenode.replication.min", 1);       // Minimum 1 replica

// When creating file:
// OLD: replication=1 (default from conf)
try (FSDataOutputStream out = fs.create(testFile)) {

// NEW: override to replication=3
try (FSDataOutputStream out = fs.create(testFile, (short) 3)) {
    out.write("Test data".getBytes(StandardCharsets.UTF_8));
}
// Now ONE file gets replicated to 3 DNs!
```

### Scenario 6: Add Latency/Throughput Metrics

```java
// Add to configuration
conf.setLong("dfs.heartbeat.interval", 10);  // More frequent heartbeats

// Measure heartbeat latency
long startTime = System.nanoTime();
// Wait for first heartbeats from all DNs
Thread.sleep(1000);
long latency = System.nanoTime() - startTime;

// Add to output CSV
samples.add(new MemorySampleWithLatency(numDataNodes, usedMemory, latency));
```

### Scenario 7: Test with Different Garbage Collectors

Modify `run-experiment.sh`:

```bash
# OLD: G1GC
java -XX:+UseG1GC ...

# NEW: ZGC (low latency, modern)
java -XX:+UseZGC ...

# NEW: Shenandoah (concurrent GC)
java -XX:+UseShenandoahGC ...

# NEW: Serial (single-threaded, minimal overhead)
java -XX:+UseSerialGC ...
```

### Scenario 8: Extend Configuration to Test Resource Trade-offs

Create a parameterized experiment:

```java
// Add config flags to control parameters
java -Dhandler.count=2 \
     -Dmax.transfer.threads=4 \
     -Dio.netty.eventLoopThreads=2 \
     -jar experiment.jar 512 results/custom_config.csv
```

Then in code:
```java
conf.setInt("dfs.datanode.handler.count", 
            Integer.getInteger("handler.count", 1));
```

---

## Configuration Parameters Reference

### Resource Reduction Parameters

```java
// ============ HANDLER THREADS ============
"dfs.datanode.handler.count"           // Default: 10      Recommended: 1
"dfs.datanode.max.transfer.threads"    // Default: 4096    Recommended: 1
"dfs.namenode.handler.count"           // Default: 10      Recommended: 2
"dfs.namenode.service.handler.count"   // Default: 10      Recommended: 1
"dfs.namenode.lifeline.handler.count"  // Default: 1       Recommended: 1

// ============ SCANNER / BACKGROUND JOBS ============
"dfs.block.scanner.volume.bytes.per.second"  // Default: 1MB/s  Recommended: 0
"dfs.datanode.directoryscan.interval"        // Default: 21600s Recommended: -1

// ============ HEARTBEAT / REPLICATION ============
"dfs.heartbeat.interval"               // Default: 3s      Recommended: 30s
"dfs.replication"                      // Default: 3       Recommended: 1
"dfs.namenode.replication.min"         // Default: 1       Recommended: 1

// ============ HTTP / JETTY ============
"hadoop.http.max.threads"              // Default: 200     Recommended: 1
"dfs.datanode.http.address"            // Default: 0.0.0.0 Recommended: 127.0.0.1:0
"dfs.datanode.https.address"           // Default: 0.0.0.0 Recommended: 127.0.0.1:0
"dfs.datanode.http.internal-proxy.port"// Default: auto    Recommended: 0

// ============ DISK SPACE CHECK (CRITICAL!) ============
"fs.du.interval"                       // Default: 600s    Recommended: 600000000s
"fs.getspaceused.jitterMillis"         // Default: random  Recommended: 0

// ============ BLOCK SIZE / PLACEMENT ============
"dfs.blocksize"                        // Default: 128MB
"dfs.namenode.replication.consider-load" // Default: true

// ============ STORAGE ============
"dfs.namenode.fs-limits.min-block-size" // Default: 1MB
```

### JVM Tuning Parameters

```bash
# Memory
-Xmx8g              # Max heap
-Xms2g              # Initial heap
-Xss512k            # Stack per thread

# Garbage Collection
-XX:+UseG1GC        # G1 collector
-XX:ParallelGCThreads=2   # GC parallelism
-XX:CICompilerCount=2     # JIT compiler threads
-XX:ConcGCThreads=1       # Concurrent marking threads

# Netty (Critical!)
-Dio.netty.eventLoopThreads=1              # Cap EventLoopGroup threads
-Dio.netty.recycler.maxCapacityPerThread=0 # Disable recycler

# Java Virtual Threads
-Djdk.virtualThreadScheduler.parallelism=1 # Limit virtual thread carriers

# Hadoop
-Dhadoop.tmp.dir=/tmp/hadoop-test          # Temp directory for MiniDFS
```

### Build & Runtime

```bash
# Build
mvn -DskipTests package

# Run with experiment parameters
java -jar experiment.jar [maxDataNodes] [outputFile]

# Run with fixed-blocks variant
java -cp experiment.jar com.example.MiniDFSFixedBlocksExperiment \
     [totalBlocks] [maxDataNodes] [outputFile]

# Check OS limits before running
ulimit -a
# ulimit -n should be 65536+
# ulimit -u should be 30000+
```

---

## Troubleshooting

### Error: "Too many open files"

```bash
# Fix:
ulimit -n $(ulimit -Hn)
# Then rerun experiment
```

### Error: "Unable to create new native thread"

```bash
# Cause: Netty creating too many threads
# Fix in run script:
java -Dio.netty.eventLoopThreads=1 ...

# Also check ulimit -u
ulimit -u  # Should be high (e.g., 31314)
```

### Error: "OutOfMemoryError: Java heap space"

```bash
# Increase max heap
java -Xmx16g -jar ...  # Instead of -Xmx8g

# Or reduce max DataNodes
java -jar experiment.jar 2048  # Instead of 4096
```

### Memory readings seem wrong / have dips

This is **expected and documented**. See [Stable Memory Measurement](#step-4-stable-memory-measurement-critical) section.

The code handles this via `stableUsedMemory()` which runs GC multiple times and waits for convergence.

### Experiment stops abruptly without error

Check:
1. Thread count exceeded: `if (activeThreads > 25_000) break;`
2. File descriptor exhaustion (silent failure in some cases)
3. System killed the process: check `dmesg` or `journalctl`

---

## Summary

| Aspect | Detail |
|--------|--------|
| **What** | Measure JVM heap memory as MiniDFSCluster scales to 512+ DataNodes |
| **Why** | Understand per-DataNode infrastructure overhead in Hadoop |
| **How** | Spin up clusters with increasing DN count, measure stable heap, record CSV |
| **Bottleneck at 512 DNs** | File descriptors, Netty threads, thread stack space |
| **Solution** | Raise `ulimit -n`, add `-Dio.netty.eventLoopThreads=1`, reduce stack size |
| **Key Code** | `stableUsedMemory()`, resource-reduction Configuration settings |
| **Extension** | Modify file count, block size, replication, or add custom metrics |

