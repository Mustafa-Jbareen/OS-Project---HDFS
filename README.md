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
│   ├── storage_virtualization_loopback/   # Storage virtualization (loopback FS) experiment — see Experiment 5
│   │   ├── run-experiment-loopback-fs.sh  # Main orchestrator
│   │   ├── start-single-dn-cluster.sh     # Start cluster with k loopback dirs per DN
│   │   ├── stop-single-dn-cluster.sh      # Stop cluster and tear down loopbacks
│   │   ├── generate-single-dn-configs.sh  # Generate hdfs-site.xml with k data dirs
│   │   ├── setup-loopback-fs.sh           # Create / format / mount loopback images
│   │   ├── teardown-loopback-fs.sh        # Unmount and remove loopback images
│   │   ├── count-input-blocks-per-fs.sh   # Count input blocks per loopback FS on disk
│   │   └── plot-results.py                # Generate all plots from a results directory
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

### Experiment 5: Storage Virtualization (Loopback Filesystems)

**Purpose**: Measure how WordCount performance, disk I/O, and NameNode memory scale as the number of loopback-backed storage directories per DataNode grows from 1 to 1024. Tests whether splitting one DataNode's storage across many virtual disks helps or hurts performance, and at what k the overhead becomes the bottleneck.

**Design**: Cluster topology stays fixed — one DataNode process per physical node. Only `k` (the number of ext4 loopback filesystems listed in `dfs.datanode.data.dir`) varies. Each image is sized as `floor(220 GB × 1024 / k)` MB, keeping total disk use per node within 220 GB.

#### Parameters

| Parameter | Value |
|-----------|-------|
| k values | 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 |
| Input | 22 GB, 16 MB blocks, replication = 3 |
| Repetitions | 5 runs per k (averaged) |
| DataNode heap | 5 500 MB / node |
| Loopback budget | 220 GB / node |

#### Cluster topology

```
tapuz14  NameNode + ResourceManager  (no DataNode by default)
tapuz10  DataNode + NodeManager
tapuz11  DataNode + NodeManager
tapuz12  DataNode + NodeManager
tapuz13  DataNode + NodeManager
```

Set `MASTER_HAS_DN=1` to also run a DataNode on tapuz14 (5 total instead of 4).

#### Storage layout (example k=4)

```
dfs.datanode.data.dir =
  /scratch/hdfs_loop/dn1/hdfs_data,
  /scratch/hdfs_loop/dn2/hdfs_data,
  /scratch/hdfs_loop/dn3/hdfs_data,
  /scratch/hdfs_loop/dn4/hdfs_data

Each mount: /scratch/hdfs_loop/dnX <- loop device <- /scratch/loop_images/hdfs_dnX.img (ext4)
```

#### Scripts

| Script | Purpose |
|--------|---------|
| `run-experiment-loopback-fs.sh` | Main orchestrator — iterates k values, runs WordCount, collects all metrics |
| `start-single-dn-cluster.sh <k> <img_mb> <heap_mb> <repl>` | Creates k loopbacks, generates config, starts cluster |
| `stop-single-dn-cluster.sh <k>` | Stops Hadoop, tears down loopback filesystems |
| `generate-single-dn-configs.sh` | Generates `hdfs-site.xml` with k comma-separated data dirs |
| `setup-loopback-fs.sh` | Creates, formats (ext4), and mounts k image files per node |
| `teardown-loopback-fs.sh` | Unmounts and removes loopback images |
| `count-input-blocks-per-fs.sh` | Counts input-file blocks on disk per loopback FS (parallelised) |
| `plot-results.py <run_dir>` | Generates all plots from a results directory |

#### Running

```bash
cd experiments/storage_virtualization_loopback

# Full experiment: k=1..1024, 5 repetitions each (~5-8 hours)
bash run-experiment-loopback-fs.sh

# Custom repetition count
bash run-experiment-loopback-fs.sh 3

# Plot a completed run
python3 plot-results.py results/storage_virtualization_loopback/latest
```

#### Metrics collected and why

| Metric | Why it matters |
|--------|---------------|
| WordCount runtime | Primary: does more virtual dirs help or hurt MapReduce? |
| NameNode heap (before/peak/avg) | Does metadata overhead grow with k? |
| iostat r_await / w_await | Does disk latency rise as one physical disk serves k virtual FSes? |
| iostat %util | Is the underlying disk saturated at high k? |
| iostat wkB/s (cluster sum) | Does aggregate write throughput improve with more dirs? |
| Block distribution per FS | Are blocks spread evenly across virtual dirs? |
| Used capacity per FS | Is data volume balanced, not just block count? |

#### Output CSV columns

```
k_storage_dirs, total_storage_dirs, datanodes,
avg_runtime_seconds, stddev_runtime, individual_runtimes,
nn_heap_before_mb, nn_heap_peak_mb, nn_heap_avg_mb, nn_block_count,
block_counts_per_fs, input_block_counts_per_fs, fs_used_mb_per_fs
```

#### Output plots (~32 PNGs per run)

**Runtime**: `runtime_vs_k.png`, `runtime_vs_total_dirs.png`, `runtime_vs_k_logscale.png`, `individual_runs.png`, `speedup_vs_k.png`

**NameNode memory**: `nn_memory_vs_k.png`, `runtime_and_memory_vs_k.png`, `nn_memory_timeseries.png`

**Block distribution**: `per_fs_block_distribution.png`, `input_blocks_per_fs.png`, `input_blocks_boxplot.png`, `input_blocks_balance.png`, `fs_capacity_per_node.png`, `fs_capacity_balance.png`

**iostat (one file per metric)**: `iostat_latency_vs_k.png`, `iostat_util_vs_k.png`, 8× `iostat_ts_<metric>.png` (time series), 10× `iostat_node_<metric>.png` (per node/device)

#### iostat measurement design

- `iostat -dxyt 5` runs on all DataNode nodes throughout all repetitions for a given k.
- Per-run wall-clock start/end is recorded in `iostat/wc_windows_k<k>.txt`.
- Parser only includes samples whose 5 s window overlaps a WordCount run — idle time between runs is excluded.
- Latency (`r_await`, `w_await`) uses an IOPS-weighted mean to exclude zero-traffic intervals.
- Throughput (`wkB/s`, `r/s`, etc.) is **summed** across all devices/nodes at each timestamp (cluster total).

#### Comparison with related experiments

| Aspect | `loopback_datanodes` | `storage_virtualization_loopback` |
|--------|---------------------|----------------------------------|
| DataNodes per node | k (multiple processes) | 1 (single process) |
| Storage dirs per DN | 1 | k (multiple loopback FSes) |
| What it tests | DataNode process scaling | Storage virtualization scaling |

#### Troubleshooting

```bash
df -h | grep hdfs_loop        # Check mounted loopback filesystems
losetup -a | grep hdfs_dn     # Check loop device attachments
sudo umount -l /scratch/hdfs_loop/dn*   # Force-clean stuck mounts
sudo losetup -D
ls /tmp/hadoop_dn_logs/hadoop-*-datanode-*.log   # DataNode logs
```

#### Known limitations

- No cold-cache reset between the K repetitions within a k — later runs benefit from OS page cache; average reflects warm-cache performance.
- If an ssh call fails during block counting, that node's counts are missing and per-FS node-color assignment in plots shifts for subsequent nodes.
- No automatic abort if fewer DataNodes than expected register with the NameNode.

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
| `storage_virtualization_loopback/plot-results.py` | Storage virtualization run dir | ~32 plots: runtime, NN memory, block distribution, iostat I/O (one file per metric) |

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
├── fixed_blocks/
│   ├── run_2026-03-11_16-00-00/
│   │   ├── fixed_blocks_memory.csv
│   │   ├── fixed_blocks_memory_vs_dns.png
│   │   └── fixed_blocks_memory_vs_blocks_per_dn.png
│   └── latest -> run_...
│
└── storage_virtualization_loopback/
    ├── run_<timestamp>/
    │   ├── results.csv                 # One row per k: runtime + NN memory + block stats
    │   ├── metadata.json
    │   ├── experiment.log
    │   ├── namenode_memory/            # Per-k NN heap time series CSVs
    │   ├── iostat/                     # Raw logs + parsed summaries + WC windows
    │   ├── runtime_vs_k.png            # (and ~30 more plots)
    │   └── hdfs_fsck_k*.txt            # HDFS block info per k (debugging)
    └── latest -> run_...
```
