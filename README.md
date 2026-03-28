# Hadoop Cluster Management & Experiments

A comprehensive toolkit for setting up, managing, and benchmarking a multi-node Hadoop HDFS/YARN cluster, plus in-JVM MiniDFSCluster experiments for NameNode memory scaling research.

---

## Table of Contents

1. [Directory Structure](#directory-structure)
2. [Quick Start](#quick-start)
3. [HDFS Management Scripts](#hdfs-management-scripts)
4. [Cluster Setup](#cluster-setup)
5. [Experiments](#experiments)
   - [WordCount Block-Size Benchmark (Single Node)](#experiment-1-wordcount-block-size-benchmark)
   - [WordCount Multi-Node Benchmark](#experiment-2-wordcount-multi-node-benchmark)
   - [MiniDFSCluster Memory Scaling](#experiment-3-minidfscluster-memory-scaling)
   - [MiniDFS Fixed-Blocks Distribution](#experiment-4-minidfs-fixed-blocks-distribution)
   - [Storage Virtualization](#experiment-5-storage-virtualization)
6. [Java Source Code](#java-source-code)
7. [Plotting & Visualization](#plotting--visualization)
8. [Known Issues & Notes](#known-issues--notes)

---

## Directory Structure

```
my_scripts/
├── README.md                              # This file
├── pom.xml                                # Maven project for MiniDFSCluster experiments
│
├── # === Root-Level HDFS Management ===
├── start-hdfs.sh                          # Start HDFS + YARN
├── stop-hdfs.sh                           # Stop HDFS + YARN
├── restart-hdfs.sh                        # Full restart (stop → clean → reformat → start)
├── clear-hdfs.sh                          # Clear HDFS data dirs (requires restart)
├── reformat-and-start.sh                  # Format NameNode and start
├── reset-hdfs-contents.sh                 # Remove HDFS files without restart
│
├── # === Cluster Setup ===
├── install-all-nodes.sh                   # Install Java, SSH, Hadoop on all nodes
├── setup-cluster-automated.sh             # Automated cluster setup
├── setup-ssh.sh                           # Configure passwordless SSH
│
├── scripts/
│   ├── hdfs/                              # HDFS management (mirrors root scripts)
│   │   ├── start-hdfs.sh
│   │   ├── stop-hdfs.sh
│   │   ├── restart-hdfs.sh
│   │   ├── clear-hdfs.sh
│   │   ├── reformat-and-start.sh
│   │   └── reset-hdfs-contents.sh
│   └── cluster/config/                    # Hadoop XML config generators
│       ├── generate-core-site-xml.sh
│       ├── generate-hdfs-site-xml.sh
│       ├── generate-mapred-site-xml.sh
│       ├── generate-yarn-site-xml.sh
│       └── generate-workers-file.sh
│
├── experiments/
│   ├── common/
│   │   ├── cluster.conf                   # Shared cluster config (nodes, paths)
│   │   └── utils.sh                       # Helper functions
│   │
│   ├── wordcount/                         # Real-cluster WordCount benchmarks
│   │   ├── benchmark-blocksize.sh         # Single-node block-size sweep
│   │   ├── benchmark-multinode-blocksize.sh  # Multi-node + K-run averaging
│   │   ├── plot-blocksize-results.py      # Plot single-node results
│   │   ├── plot-multinode-results.py      # Plot multi-node results (error bars)
│   │   ├── run.sh                         # Simple WordCount runner
│   │   ├── generate-input.sh              # Ultra-fast input generator (dd-based)
│   │   ├── collect-results.sh             # Collect results from HDFS
│   │   └── analyze.py                     # Multi-run analysis
│   │
│   ├── mini_dfs_cluster/                  # In-JVM MiniDFS experiments
│   │   ├── run-experiment.sh              # Run memory-scaling experiment
│   │   ├── run-fixed-blocks-experiment.sh # Run fixed-blocks distribution experiment
│   │   ├── plot_memory.py                 # Plot memory-vs-DataNodes
│   │   ├── plot_fixed_blocks.py           # Plot fixed-blocks results
│   │   └── requirements.txt              # Python dependencies
│   │
│   ├── storage_virtualization/            # Storage virtualization research
│   │   ├── README.md                      # Detailed experiment docs
│   │   ├── benchmark-block-scaling.sh
│   │   ├── benchmark-storage-dirs.sh
│   │   ├── monitor-namenode-memory.sh
│   │   ├── run-full-experiment.sh
│   │   ├── simulate-virtual-storage-failure.sh
│   │   └── plot-storage-virtualization.py
│   │
│   └── results/                           # Experiment output (gitignored)
│
├── src/main/java/com/example/
│   ├── MiniDFSClusterExperiment.java      # Memory scaling: increasing DataNodes
│   └── MiniDFSFixedBlocksExperiment.java  # Fixed blocks distributed across DataNodes
│
└── backup_unused_files/
    └── README.md
```

---

## Quick Start

### 1. Prepare the Cluster Configuration

On the **master node** (tapuz14), create the node list:

```bash
cat > /csl/mostufa.j/cluster << EOF
tapuz14
tapuz10
tapuz11
tapuz12
tapuz13
EOF
```

### 2. Install & Setup

```bash
bash install-all-nodes.sh        # Install Java, SSH, Hadoop on all nodes
bash setup-cluster-automated.sh  # Configure and start the cluster
```

### 3. Verify

```bash
hdfs dfsadmin -report            # Check live DataNodes
yarn node -list                  # Check YARN NodeManagers
```

---

## HDFS Management Scripts

| Script | Description |
|--------|-------------|
| `start-hdfs.sh` | Start all Hadoop services (HDFS + YARN) |
| `stop-hdfs.sh` | Stop all Hadoop services |
| `restart-hdfs.sh` | Full restart: stop → clean NameNode/DataNode state → reformat → start |
| `clear-hdfs.sh` | Clear HDFS data directories (requires manual restart after) |
| `reformat-and-start.sh` | Format NameNode and start fresh cluster |
| `reset-hdfs-contents.sh` | Delete all HDFS files but keep services running |

```bash
bash restart-hdfs.sh          # Full cluster reset
bash reset-hdfs-contents.sh   # Soft reset (files only)
```

---

## Cluster Setup

| Script | Description |
|--------|-------------|
| `install-all-nodes.sh` | Installs Java 11, configures SSH, deploys Hadoop 3.3.6 to all nodes |
| `setup-cluster-automated.sh` | Generates all Hadoop XML configs, distributes to workers, starts services |
| `setup-ssh.sh` | Sets up passwordless SSH between all cluster nodes |

Configuration generators in `scripts/cluster/config/` produce:
- `core-site.xml` — NameNode address, default filesystem
- `hdfs-site.xml` — Replication, block size, data directories
- `mapred-site.xml` — MapReduce framework (YARN)
- `yarn-site.xml` — ResourceManager settings
- `workers` — List of worker hostnames

### Cluster Topology

| Node | Role |
|------|------|
| tapuz14 | Master (NameNode, ResourceManager, DataNode) |
| tapuz10 | Worker (DataNode, NodeManager) |
| tapuz11 | Worker (DataNode, NodeManager) |
| tapuz12 | Worker (DataNode, NodeManager) |
| tapuz13 | Worker (DataNode, NodeManager) |

---

## Experiments

### Experiment 1: WordCount Block-Size Benchmark

**Purpose**: Measure how HDFS block size (128 KB → 256 MB) affects MapReduce WordCount performance on a single cluster configuration.

```bash
bash experiments/wordcount/benchmark-blocksize.sh 512   # 512 MB input
```

**What it does**:
- Tests 12 block sizes: 128KB, 256KB, 512KB, 1MB, 2MB, 4MB, 8MB, 16MB, 32MB, 64MB, 128MB, 256MB
- Records runtime and split count for each
- Saves results to a timestamped directory

**Plot**:
```bash
python3 experiments/wordcount/plot-blocksize-results.py results/blocksize-benchmark/latest
```

**Key insight**: Smaller block sizes create more map tasks (higher overhead), larger sizes reduce parallelism. The optimal block size depends on cluster size and workload.

---

### Experiment 2: WordCount Multi-Node Benchmark

**Purpose**: Measure WordCount performance across **varying node counts** (2–5 nodes) and **varying block sizes** (16 MB → 4 GB), running each configuration **K times** and reporting the **average runtime** with standard deviation.

```bash
# Run each config 5 times and average
bash experiments/wordcount/benchmark-multinode-blocksize.sh 5

# Default: 3 repetitions
bash experiments/wordcount/benchmark-multinode-blocksize.sh
```

**Parameters**:
- `K` (1st argument): Number of repetitions per (node_count, block_size) pair (default: 3)
- Input size: 20 GB (hardcoded)
- Node counts: 2, 3, 4, 5
- Block sizes: 16MB, 32MB, 64MB, 128MB, 256MB, 512MB, 1GB, 2GB, 4GB

**Output CSV columns**: `node_count, block_size_exp, block_size_bytes, block_size_human, avg_runtime_seconds, stddev_runtime, individual_runtimes`

**Plot** (includes error bars showing ±1 stddev):
```bash
python3 experiments/wordcount/plot-multinode-results.py results/multinode-benchmark/latest
```

**Generated visualizations**:
1. **Combined chart** — All node counts as lines with error bars
2. **Per-node bar charts** — One chart per node count with error bars
3. **Heatmap** — Runtime color-coded by (nodes × block size)
4. **Speedup chart** — Speedup relative to 2-node baseline

---

### Experiment 3: MiniDFSCluster Memory Scaling

**Purpose**: Measure JVM heap memory consumption as the number of DataNodes scales from 2 to 4096+ within a single JVM using Hadoop's `MiniDFSCluster`.

```bash
# On the remote machine (tapuz14):
bash experiments/mini_dfs_cluster/run-experiment.sh 4096
```

**How it works**:
- Starts a MiniDFSCluster with N DataNodes (N = 2, 4, 8, 16, …)
- Writes one small test file, measures heap usage, shuts down
- Doubles N and repeats until OOM or ulimit is hit

**Memory measurement improvement**: The code uses a **stable measurement** technique — it runs GC repeatedly and waits for the heap reading to converge (within 1 MiB) before recording. This greatly reduces non-deterministic dips.

#### Why Memory Dips Can Occur

If you observe non-monotonic memory growth (memory drops at some points before continuing to rise), this is caused by:

1. **Non-deterministic GC**: `System.gc()` is only a *hint*. The JVM may collect varying amounts of garbage between iterations, so stale objects from a previous cluster sometimes inflate the reading while other times they are collected → dip.
2. **Heap de-commit**: G1GC can release committed heap pages back to the OS, making `Runtime.totalMemory()` shrink between iterations.
3. **Lazy-init caches**: Hadoop's internal caches (DNS resolution, SecurityManager, class metadata) have non-deterministic lifetimes.
4. **Thread-local storage**: IPC/Netty thread-locals from Hadoop may survive across cluster iterations unpredictably.

The stable-measurement approach mitigates this, but small dips may still occur at scale.

#### Blocks Per DataNode

In this experiment, each iteration creates **1 tiny file → 1 block** (replication=1). This means only **1 DataNode** gets a block regardless of cluster size. The experiment measures the **per-DataNode infrastructure overhead** (threads, sockets, heartbeat handlers), not the block-metadata overhead. For a block-distribution experiment, see Experiment 4 below.

**Plot**:
```bash
python3 experiments/mini_dfs_cluster/plot_memory.py results/mini_dfs_cluster/latest/memory_usage.csv \
    -o results/mini_dfs_memory.png
# Add --log-y for logarithmic memory axis
```

---

### Experiment 4: MiniDFS Fixed-Blocks Distribution

**Purpose**: Keep the total number of HDFS blocks **constant** and increase the number of DataNodes, so the fixed data set gets **distributed across more nodes**. This isolates the memory overhead of *distributing* block metadata across DataNodes from the overhead of the DataNode infrastructure itself.

```bash
# 256 blocks across up to 512 DataNodes
bash experiments/mini_dfs_cluster/run-fixed-blocks-experiment.sh 256 512

# 1024 blocks across up to 256 DataNodes
bash experiments/mini_dfs_cluster/run-fixed-blocks-experiment.sh 1024 256
```

**Parameters**:
- `total_blocks` (1st arg): Fixed number of 1 KB files to create (default: 256)
- `max_datanodes` (2nd arg): Upper bound for DataNode count (default: 512)

**How it works**:
- For each DataNode count (2, 4, 8, …), spins up a MiniDFSCluster
- Writes `total_blocks` files (each 1 KB, replication=1) → `total_blocks` HDFS blocks
- The NameNode distributes blocks across available DataNodes
- Measures stable heap memory after all files are written

**Output CSV columns**: `DataNodes, TotalBlocks, BlocksPerDataNode, MemoryUsed`

As DataNodes double, `BlocksPerDataNode` halves (e.g., 256 blocks / 8 DataNodes = 32 blocks/DN).

**Plot** (two charts):
```bash
python3 experiments/mini_dfs_cluster/plot_fixed_blocks.py \
    results/fixed_blocks/latest/fixed_blocks_memory.csv \
    -o results/fixed_blocks_plots
```

1. **Memory vs DataNodes** — Primary y-axis: heap (MiB), secondary y-axis: blocks/DN
2. **Memory vs Blocks-per-DataNode** — Shows how memory changes as data gets more distributed

---

### Experiment 5: Storage Virtualization

**Purpose**: Research the impact of virtualizing storage in HDFS DataNodes for finer-grained fault tolerance, extended flash lifespan, and software-defined failure domains.

See [experiments/storage_virtualization/README.md](experiments/storage_virtualization/README.md) for full documentation.

**Sub-experiments**:

| Script | What it tests |
|--------|--------------|
| `benchmark-block-scaling.sh` | NameNode heap growth as block count increases |
| `benchmark-storage-dirs.sh` | DataNode performance with multiple storage directories |
| `monitor-namenode-memory.sh` | Real-time NameNode heap monitoring via JMX |
| `simulate-virtual-storage-failure.sh` | Failure detection and re-replication timing |
| `run-full-experiment.sh` | Runs all sub-experiments sequentially |

```bash
# Run everything
bash experiments/storage_virtualization/run-full-experiment.sh all

# Individual experiments
bash experiments/storage_virtualization/benchmark-block-scaling.sh 100000
bash experiments/storage_virtualization/benchmark-storage-dirs.sh 64
```

---

## Java Source Code

Built with Maven. The `pom.xml` uses the shade plugin to produce an uber-JAR with all Hadoop dependencies.

### Build

```bash
mvn -DskipTests package
```

Produces: `target/minidfscluster-experiment-1.0-SNAPSHOT.jar`

### Classes

| Class | Description |
|-------|-------------|
| `MiniDFSClusterExperiment` | Scales DataNode count (2 → 4096), measures per-DN memory overhead. Creates 1 block per iteration. |
| `MiniDFSFixedBlocksExperiment` | Fixed total blocks distributed across increasing DataNode counts. Measures block-distribution memory overhead. |

### Running Directly

```bash
# Memory scaling experiment (default main class in JAR manifest)
java -Xmx8g -jar target/minidfscluster-experiment-1.0-SNAPSHOT.jar 4096 results/memory_usage.csv

# Fixed-blocks experiment (needs -cp instead of -jar to specify class)
java -Xmx8g -cp target/minidfscluster-experiment-1.0-SNAPSHOT.jar \
    com.example.MiniDFSFixedBlocksExperiment 256 512 results/fixed_blocks.csv
```

### Resource Tuning

Both experiments apply aggressive resource-reduction settings to maximize the DataNode count on a single machine:
- Disable block scanners and directory scans
- Minimize handler/transfer threads per DataNode (1 each)
- Reduce heartbeat frequency (30 s)
- Single replica (`dfs.replication=1`)
- Single storage dir per DataNode
- G1GC with minimal parallelism
- Effectively disable `du` shell process spawning

---

## Plotting & Visualization

All plot scripts require Python 3 with `matplotlib` and `numpy`:

```bash
pip install matplotlib numpy
```

| Script | Input | Output |
|--------|-------|--------|
| `plot-blocksize-results.py` | Single-node blocksize CSV | Runtime vs block size (bar + line) |
| `plot-multinode-results.py` | Multi-node CSV (with averages) | Combined lines, per-node bars, heatmap, speedup — all with error bars |
| `plot_memory.py` | MiniDFS memory CSV | Memory vs DataNodes (linear/log) |
| `plot_fixed_blocks.py` | Fixed-blocks CSV | Memory vs DataNodes + Blocks/DN dual-axis; Memory vs Blocks/DN |
| `plot-storage-virtualization.py` | Storage virtualization CSV | Various storage experiment charts |

---

## Known Issues & Notes

- **Local storage path**: `/home/mostufa.j/` — Hadoop installation and HDFS data directories
- **Shared storage path**: `/csl/mostufa.j/` — Configuration files and SSH keys
- **ulimit constraints**: MiniDFS experiments are limited by `ulimit -u` (max processes) and `ulimit -n` (max open files). Ask admin to raise these for large-scale experiments.
- **Memory dips in MiniDFS**: See [Experiment 3](#experiment-3-minidfscluster-memory-scaling) for explanation of non-monotonic memory readings.
- **Block sizes as exponents**: The multi-node benchmark expresses block sizes as `2^N` bytes (e.g., `2^27 = 128 MB`).
- Always verify the cluster configuration file (`/csl/mostufa.j/cluster`) before running setup scripts.

---

## Results Directory Layout

```
results/
├── blocksize-benchmark/
│   ├── run_2026-01-26_10-30-00/
│   │   ├── results.csv
│   │   ├── metadata.json
│   │   └── benchmark.log
│   └── latest -> run_...
│
├── multinode-benchmark/
│   ├── run_2026-03-11_14-00-00/
│   │   ├── all_results.csv            # avg_runtime_seconds, stddev, individual runs
│   │   ├── results_2nodes.csv
│   │   ├── results_3nodes.csv
│   │   ├── results_4nodes.csv
│   │   ├── results_5nodes.csv
│   │   ├── metadata.json              # Includes repetitions_k
│   │   ├── benchmark.log
│   │   ├── combined_results.png
│   │   ├── heatmap.png
│   │   └── speedup.png
│   └── latest -> run_...
│
├── mini_dfs_cluster/
│   ├── run_2026-03-11_15-00-00/
│   │   ├── memory_usage.csv
│   │   ├── memory_scaling.png
│   │   └── memory_scaling_log.png
│   └── latest -> run_...
│
└── fixed_blocks/
    ├── run_2026-03-11_16-00-00/
    │   ├── fixed_blocks_memory.csv
    │   ├── fixed_blocks_memory_vs_dns.png
    │   └── fixed_blocks_memory_vs_blocks_per_dn.png
    └── latest -> run_...
```
