# Hadoop Cluster Management Guide

This guide provides instructions for setting up and managing a multi-node Hadoop cluster using the provided scripts.

## Directory Overview

### Key Scripts
- **install-all-nodes.sh**: Installs Java, SSH, and Hadoop on all nodes in parallel.
- **setup-cluster-automated.sh**: Automates the entire cluster configuration, deployment, and startup process.
- **setup-ssh.sh**: Configures passwordless SSH between all nodes in the cluster.
- **clear-hdfs.sh**: Clears all HDFS data from NameNode and DataNode directories.
- **reformat-and-start.sh**: Reformats the NameNode and starts the Hadoop cluster.
- **restart-hdfs.sh**: Restarts HDFS services while preserving data.
- **stop-hdfs.sh**: Stops all Hadoop services (HDFS and YARN).

### Experiments Directory
- **common/**: Contains shared configuration and utility scripts for experiments.
  - `cluster.conf`: Cluster configuration file.
  - `utils.sh`: Utility functions for experiments.
- **wordcount/**: Contains scripts for running the WordCount experiment.
  - `analyze.py`: Analyzes WordCount experiment results.
  - `collect-results.sh`: Collects results from WordCount experiments.
  - `generate-input.sh`: Generates input data for WordCount experiments.
  - `run.sh`: Executes the WordCount experiment.

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

## Real cluster WordCount benchmark (block-size scaling)

Once the cluster is configured, you can run the WordCount job on actual nodes and measure how the runtime behaves as you change the HDFS block size from **128KB to 1GB**.

### Prerequisites

1. **Lower the minimum block size** to 128KB so small block sizes are allowed:
   ```bash
   bash generate-hdfs-site-xml.sh
   # Then restart HDFS on all nodes
   bash stop-hdfs.sh && bash restart-hdfs.sh
   ```

2. Ensure `matplotlib` and `pandas` are installed for plotting:
   ```bash
   pip install matplotlib pandas
   ```

### Automated Benchmark Script

Run the full sweep from 128KB to 1GB with a single command:

```bash
bash experiments/wordcount/benchmark-blocksize.sh 512
```

This will:
- Generate 512MB of input data for each block size configuration
- Test 14 block sizes: 128KB, 256KB, 512KB, 1MB, 2MB, 4MB, 8MB, 16MB, 32MB, 64MB, 128MB, 256MB, 512MB, 1GB
- Record runtime and number of splits for each configuration
- Output results to `results/wordcount-blocksize.csv`

### Generate the Figure

After the benchmark completes, create the visualization:

```bash
python3 experiments/wordcount/plot-blocksize-results.py
```

This produces:
- `results/wordcount-blocksize.png` - PNG image of the plot
- `results/wordcount-blocksize.pdf` - High-quality PDF version
- Console summary with optimal block size recommendation

### Manual Benchmark Loop

If you prefer to run manually or customize the block sizes:

```bash
# Block sizes from 128KB to 1GB
for block_size in 131072 262144 524288 1048576 2097152 4194304 8388608 16777216 33554432 67108864 134217728 268435456 536870912 1073741824; do
  hdfs dfs -rm -r -f /user/$USER/wordcount/input
  bash experiments/wordcount/generate-input.sh 512 "$block_size"
  bash experiments/wordcount/run.sh
  runtime=$(cat results/wordcount/*/runtime_seconds.txt | tail -1)
  echo "$block_size,$runtime" >> results/wordcount-blocksize-manual.csv
done
```

### Understanding the Results

- **Smaller block sizes** (128KB-1MB): More blocks mean more map tasks, higher overhead, but better parallelism for small files
- **Larger block sizes** (64MB-1GB): Fewer blocks mean fewer map tasks, less overhead, but may underutilize cluster resources
- **Optimal block size**: Depends on your cluster size, input data characteristics, and job type

The plot shows runtime on the primary y-axis and number of blocks/splits on the secondary y-axis, helping you visualize the trade-off between parallelism and overhead.
