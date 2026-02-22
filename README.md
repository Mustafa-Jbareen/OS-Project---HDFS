# Hadoop Cluster Management Guide

This guide provides instructions for setting up and managing a multi-node Hadoop cluster using the provided scripts.

## Directory Structure

```
my_scripts/
├── README.md                          # This file
├── pom.xml                            # Maven project for MiniDFSCluster experiments
│
├── # === Root-Level HDFS Management Scripts ===
├── start-hdfs.sh                      # Start all Hadoop services (HDFS + YARN)
├── stop-hdfs.sh                       # Stop all Hadoop services (HDFS + YARN)
├── restart-hdfs.sh                    # Full HDFS restart (stop, clean, reformat, start)
├── clear-hdfs.sh                      # Clear HDFS data directories (requires restart)
├── reformat-and-start.sh              # Format NameNode and start cluster
├── reset-hdfs-contents.sh             # Remove all HDFS files (keeps services running)
│
├── # === Cluster Setup Scripts ===
├── install-all-nodes.sh               # Install Java, SSH, Hadoop on all nodes
├── setup-cluster-automated.sh         # Full automated cluster setup
├── setup-ssh.sh                       # Configure passwordless SSH
│
├── scripts/                           # Organized script modules
│   ├── hdfs/                          # HDFS management scripts (mirrors root scripts)
│   │   ├── start-hdfs.sh
│   │   ├── stop-hdfs.sh
│   │   ├── restart-hdfs.sh
│   │   ├── clear-hdfs.sh
│   │   ├── reformat-and-start.sh
│   │   └── reset-hdfs-contents.sh
│   └── cluster/                       # Cluster setup scripts
│       └── config/                    # Configuration generators
│           ├── generate-core-site-xml.sh
│           ├── generate-hdfs-site-xml.sh
│           ├── generate-mapred-site-xml.sh
│           ├── generate-yarn-site-xml.sh
│           └── generate-workers-file.sh
│
├── experiments/                       # Experiment scripts and utilities
│   ├── common/                        # Shared configuration and utilities
│   │   ├── cluster.conf               # Cluster configuration
│   │   └── utils.sh                   # Utility functions
│   ├── wordcount/                     # WordCount benchmark experiment
│   │   ├── benchmark-blocksize.sh     # Block size benchmark (with timestamped runs)
│   │   ├── run.sh                     # Run WordCount job
│   │   ├── generate-input.sh          # Generate input data
│   │   ├── collect-results.sh         # Collect results from HDFS
│   │   ├── analyze.py                 # Analyze multiple runs
│   │   └── plot-blocksize-results.py  # Plot benchmark results
│   ├── storage_virtualization/        # Storage virtualization experiments
│   │   ├── README.md                  # Detailed experiment documentation
│   │   ├── benchmark-block-scaling.sh # Block count scaling benchmark
│   │   ├── benchmark-storage-dirs.sh  # Storage directory scaling benchmark
│   │   ├── monitor-namenode-memory.sh # NameNode memory monitoring
│   │   ├── run-full-experiment.sh     # Run all experiments
│   │   ├── simulate-virtual-storage-failure.sh  # Failure simulation
│   │   └── plot-storage-virtualization.py       # Plot results
│   ├── mini_dfs_cluster/              # MiniDFSCluster memory experiment
│   │   ├── plot_memory.py
│   │   └── requirements.txt
│   └── results/                       # Experiment results (gitignored)
│
├── backup_unused_files/               # Archived unused scripts
│   └── README.md
│
└── src/                               # Java source code
    └── main/java/com/example/
        └── MiniDFSClusterExperiment.java
```

## HDFS Management Scripts

### restart-hdfs.sh (Full Restart)
Performs a complete HDFS reset equivalent to a fresh setup:
1. Stops all Hadoop services (HDFS + YARN)
2. Cleans NameNode state
3. Cleans DataNode state
4. Reformats NameNode
5. Starts fresh cluster

```bash
bash restart-hdfs.sh
```

### reset-hdfs-contents.sh (Soft Reset)
Removes all files from HDFS while keeping services running:
- Deletes all user files and directories
- Empties trash
- Does NOT restart services

```bash
bash reset-hdfs-contents.sh
```

### start-hdfs.sh (Start Services)
Starts all Hadoop services (HDFS and YARN):

```bash
bash start-hdfs.sh
```

### Other HDFS Scripts
- **stop-hdfs.sh**: Stop all Hadoop services
- **clear-hdfs.sh**: Clear data directories (requires manual restart)
- **reformat-and-start.sh**: Format NameNode and start cluster

---

## Quick Start Guide

### 1. Prepare the Cluster Configuration File

On the **master node**, create the cluster configuration file at `/csl/mostufa.j/cluster` with the list of all nodes:

```bash
cat > /csl/mostufa.j/cluster << EOF
master-hostname
worker1-hostname
worker2-hostname
EOF
```

**Example:**
```bash
cat > /csl/mostufa.j/cluster << EOF
tapuz14
tapuz12
tapuz13
EOF
```

### 2. Run the Automated Setup

1. Run `install-all-nodes.sh` to install Java, SSH, and Hadoop on all nodes.
2. Run `setup-cluster-automated.sh` to configure and start the cluster.

### 3. Manage the Cluster

Use the following scripts for cluster management:

- `clear-hdfs.sh`: Clears all HDFS data.
- `reformat-and-start.sh`: Reformats the NameNode and starts the cluster.
- `restart-hdfs.sh`: Restarts HDFS services.
- `stop-hdfs.sh`: Stops all Hadoop services.

---

## Notes

- The local storage path `/home/mostufa.j/` is used for Hadoop installation and HDFS data directories.
- The shared storage path `/csl/mostufa.j/` is used for configuration files and SSH keys.
- Always verify the cluster configuration file (`/csl/mostufa.j/cluster`) before running any scripts.

## MiniDFSCluster memory scaling experiment

Use the Maven project under `src/` to spin up a `MiniDFSCluster` and collect memory usage samples while the number of DataNodes doubles every iteration. The Java class now accepts two optional arguments:

1. `maxDataNodes` (default `256`) – upper bound for the number of DataNodes; the loop still starts from 2 and doubles each time.
2. `outputCsv` (default `memory_usage.csv`) – where the measurements are written.

From the workspace root you can build and run the experiment with:

```bash
mvn -q -DskipTests package
java -jar target/minidfscluster-experiment-1.0-SNAPSHOT-shaded.jar 256 results/memory_usage.csv
```

If you only want to execute it directly via Maven (without producing a shaded jar), use the exec plugin:

```bash
mvn exec:java -Dexec.mainClass="com.example.MiniDFSClusterExperiment" -Dexec.args="256 results/memory_usage.csv"
```

The program automatically tears down each MiniDFSCluster, pauses briefly between iterations, and stops early if the JVM runs out of resources; you do not need to run up to `2^20` DataNodes on a single machine. After the run, copy `memory_usage.csv` from the remote machine to your local workstation (for example with `scp`) before plotting.

Generate the figure with the Python helper:

```bash
pip install -r experiments/mini_dfs_cluster/requirements.txt
python experiments/mini_dfs_cluster/plot_memory.py results/memory_usage.csv -o results/mini_dfs_memory.png
```

The helper sorts the samples, converts the memory usage to MiB, and draws a logarithmic x-axis with an optional log y-axis via the `--log-y` flag.

## Real Cluster WordCount Benchmark (Block-Size Scaling)

Once the cluster is configured, you can run the WordCount job on actual nodes and measure how the runtime behaves as you change the HDFS block size from **128KB to 256MB**.

### Block Size Notation

Block sizes are expressed as: `KB × 2^10` bytes

This makes the relationship between KB and bytes explicit:
- 128 KB = 128 × 2^10 = 128 × 1024 = 131,072 bytes
- 1 MB = 1024 × 2^10 = 1024 × 1024 = 1,048,576 bytes

### Prerequisites

1. **Lower the minimum block size** to 128KB so small block sizes are allowed:
   ```bash
   bash scripts/cluster/config/generate-hdfs-site-xml.sh
   bash restart-hdfs.sh  # Full restart with new config
   ```

2. Ensure `matplotlib` and `pandas` are installed for plotting:
   ```bash
   pip install matplotlib pandas
   ```

### Automated Benchmark Script

Run the full sweep from 128KB to 256MB with a single command:

```bash
bash experiments/wordcount/benchmark-blocksize.sh 512
```

This will:
- Generate 512MB of input data for each block size configuration
- Test 12 block sizes: 128KB, 256KB, 512KB, 1MB, 2MB, 4MB, 8MB, 16MB, 32MB, 64MB, 128MB, 256MB
- Record runtime and number of splits for each configuration
- Save results to a **timestamped directory** (never overwrites previous runs)

### Result Preservation

Each benchmark run creates a unique timestamped directory:
```
results/blocksize-benchmark/
├── run_2026-01-26_10-30-00/
│   ├── results.csv       # Main results file
│   ├── metadata.json     # Run configuration
│   └── benchmark.log     # Detailed execution log
├── run_2026-01-26_14-45-00/
│   └── ...
└── latest -> run_2026-01-26_14-45-00/  # Symlink to most recent
```

### Generate the Figure

After the benchmark completes, create the visualization:

```bash
# Plot the latest run
python3 experiments/wordcount/plot-blocksize-results.py

# Or specify a specific run
python3 experiments/wordcount/plot-blocksize-results.py results/blocksize-benchmark/run_2026-01-26_10-30-00/results.csv
```

This produces:
- PNG and PDF plots saved in the run directory
- Console summary with optimal block size recommendation

### Understanding the Results

- **Smaller block sizes** (128KB-1MB): More blocks mean more map tasks, higher overhead, but better parallelism for small files
- **Larger block sizes** (64MB-256MB): Fewer blocks mean fewer map tasks, less overhead, but may underutilize cluster resources
- **Optimal block size**: Depends on your cluster size, input data characteristics, and job type

The plot shows runtime on the primary y-axis and number of blocks/splits on the secondary y-axis, helping you visualize the trade-off between parallelism and overhead.
