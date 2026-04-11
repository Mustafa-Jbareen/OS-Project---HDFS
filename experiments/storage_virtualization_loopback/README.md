# Storage Virtualization Loopback Filesystem Experiment

## Overview

This experiment measures how Hadoop WordCount performance scales as the number of **storage directories per DataNode** increases from 2 to 512. Unlike the `loopback_datanodes` experiment which creates multiple DataNode processes, this experiment keeps **one DataNode per node** but distributes its storage across multiple loopback-mounted filesystems.

This tests **storage virtualization** — the ability of a single DataNode to manage data across many virtual disks (loopback filesystems).

## Experiment Design

### Key Characteristics

- **Fixed cluster size**: 1 DataNode per physical node (5 DataNodes total with standard config)
- **Variable storage directories (k)**: Each DataNode stores data across k loopback filesystems
- **k values tested**: 2, 4, 8, 16, 32, 64, 128, 256, 512 (doubling progression)
- **Storage configuration**: `dfs.datanode.data.dir` is set to a comma-separated list of k mount points
- **Workload**: Fixed 20GB WordCount job with 128MB blocks and replication factor 3
- **Repetitions**: 5 runs per k value, results averaged

### Research Questions

1. **Scalability**: How does WordCount runtime change as storage directory count increases?
2. **Overhead**: Is there a penalty for splitting storage across many virtual disks?
3. **Optimal configuration**: What is the best number of storage directories for performance?
4. **Limits**: At what point does storage virtualization become a bottleneck?

## Architecture

### Storage Layout

For k=4, each DataNode uses:
```
dfs.datanode.data.dir = /mnt/hdfs_loop/dn1/hdfs_data,
                        /mnt/hdfs_loop/dn2/hdfs_data,
                        /mnt/hdfs_loop/dn3/hdfs_data,
                        /mnt/hdfs_loop/dn4/hdfs_data
```

Each mount point is a separate loopback filesystem:
```
/mnt/hdfs_loop/dn1 -> loopback device -> /data/loop_images/hdfs_dn1.img (ext4)
/mnt/hdfs_loop/dn2 -> loopback device -> /data/loop_images/hdfs_dn2.img (ext4)
...
```

### Comparison with `loopback_datanodes` Experiment

| Aspect | loopback_datanodes | storage_virtualization_loopback |
|--------|-------------------|--------------------------------|
| **DataNodes per node** | k (multiple processes) | 1 (single process) |
| **Storage dirs per DN** | 1 | k (multiple loopback FSes) |
| **Port allocation** | Unique ports per DN | Standard ports (9866, 9864, 9867) |
| **Tests** | DataNode process scaling | Storage virtualization scaling |

## Scripts

### Core Scripts

1. **`run-experiment-loopback-fs.sh`** — Main orchestration script
   - Iterates over k = 2, 4, 8, 16, 32, 64, 128, 256, 512
   - Runs 5 WordCount jobs per k value
   - Collects runtime statistics
   - Generates results CSV

2. **`start-single-dn-cluster.sh`** — Cluster startup
   - Creates k loopback filesystems per node
   - Generates config with k storage directories
   - Starts 1 DataNode per node
   - Waits for all DataNodes to register

3. **`stop-single-dn-cluster.sh`** — Cluster shutdown
   - Stops all Hadoop processes
   - Tears down loopback filesystems
   - Cleans up temporary files

4. **`generate-single-dn-configs.sh`** — Config generation
   - Creates Hadoop config for 1 DataNode
   - Sets `dfs.datanode.data.dir` to k comma-separated paths
   - Configures heap size and replication

5. **`setup-loopback-fs.sh`** — Loopback filesystem creation
   - Creates k disk image files (`.img` files)
   - Formats each as ext4
   - Mounts via loop driver
   - Sets proper permissions

6. **`teardown-loopback-fs.sh`** — Loopback cleanup
   - Unmounts all loopback filesystems (up to MAX_K=512)
   - Removes disk images
   - Detaches loop devices

7. **`plot-results.py`** — Visualization
   - Generates 5 plots from experiment results
   - Shows runtime vs k in various formats
   - Includes speedup and log-scale plots

## Usage

### Quick Start

```bash
cd experiments/storage_virtualization_loopback

# Run full experiment (k=2 to k=512, 5 runs each)
bash run-experiment-loopback-fs.sh

# Run with different number of repetitions
bash run-experiment-loopback-fs.sh 3  # 3 runs per k
```

### View Results

```bash
# Results are in results/storage_virtualization_loopback/run_<timestamp>/

# View CSV results
cat results/storage_virtualization_loopback/latest/results.csv

# Generate plots
python3 plot-results.py results/storage_virtualization_loopback/latest
```

### Manual Cluster Control

```bash
# Start cluster with k=8 storage dirs per DataNode
bash start-single-dn-cluster.sh 8 30 2048 3
# Args: k, image_size_gb, dn_heap_mb, replication

# Stop cluster and cleanup (cleanup up to k=512)
bash stop-single-dn-cluster.sh 512
```

## Resource Requirements

### Per k value

| k | Image Size | Disk per Node | Total Disk (5 nodes) |
|---|------------|---------------|---------------------|
| 2 | 20 GB | 40 GB | 200 GB |
| 4 | 10 GB | 40 GB | 200 GB |
| 8 | 5 GB | 40 GB | 200 GB |
| 16 | 2.5 GB | 40 GB | 200 GB |
| 32 | 2 GB | 64 GB | 320 GB |
| 64 | 2 GB | 128 GB | 640 GB |
| 128 | 2 GB | 256 GB | 1.28 TB |
| 256 | 2 GB | 512 GB | 2.56 TB |
| 512 | 2 GB | 1024 GB | 5.12 TB |

**Note**: Disk requirements increase for high k values. The loopback budget per node is 40GB by default, but this may not be sufficient for k > 16. Adjust `LOOPBACK_BUDGET_PER_NODE_GB` in the script if needed.

### Memory

- **NameNode**: 1-2 GB (unchanged, same metadata regardless of k)
- **DataNode**: 2048 MB (fixed per node, 1 DN per node)
- **YARN**: Standard requirements

## Output

### Results Directory Structure

```
results/storage_virtualization_loopback/run_<timestamp>/
├── results.csv              # Main results (runtime per k)
├── metadata.json            # Experiment configuration
├── experiment.log           # Detailed execution log
├── runtime_vs_k.png         # Bar chart: runtime vs k
├── runtime_vs_total_dirs.png # Line plot: runtime vs total storage dirs
├── speedup_vs_k.png         # Speedup relative to k=2 baseline
├── individual_runs.png      # Scatter plot of individual runs
└── runtime_vs_k_logscale.png # Log-scale plot for k values
```

### CSV Format

```csv
k_storage_dirs,total_storage_dirs,datanodes,avg_runtime_seconds,stddev_runtime,individual_runtimes
2,10,5,45.23,1.2,44.1;45.8;45.0;46.2;44.0
4,20,5,43.67,0.9,43.2;44.1;43.5;44.3;43.2
...
```

## Expected Behavior

### Hypotheses

1. **Initial improvement**: Moving from k=2 to k=4 or k=8 may improve performance due to better I/O parallelism
2. **Plateau**: After optimal k, performance should stabilize
3. **Degradation**: At very high k (e.g., k=256, k=512), overhead may increase runtime

### Factors Affecting Performance

- **I/O parallelism**: More storage dirs can enable parallel block writes
- **Filesystem overhead**: Each loopback FS has its own journal, inode table, etc.
- **DataNode management**: Single DN must coordinate writes across k directories
- **Block distribution**: HDFS distributes blocks across available storage dirs

## Troubleshooting

### Loopback Filesystem Issues

```bash
# Check mounted loopback filesystems
df -h | grep hdfs_loop

# Check loop devices
losetup -a | grep hdfs_dn

# Manually clean up stuck mounts
sudo umount -l /mnt/hdfs_loop/dn*
sudo losetup -D
```

### DataNode Startup Failures

- **Check logs**: `/tmp/hadoop_dn_logs/hadoop-*-datanode-*.log`
- **Verify mounts**: Ensure all k loopback FSes are mounted before starting DN
- **Disk space**: Ensure each loopback FS has sufficient free space
- **Permissions**: Check that mount points have 777 permissions

### Disk Space Issues

If you run out of disk space for high k values:

1. Reduce `LOOPBACK_BUDGET_PER_NODE_GB` in `run-experiment-loopback-fs.sh`
2. Reduce `INPUT_SIZE_GB` (default 20GB) for smaller test
3. Skip very high k values by modifying `K_VALUES` array

## Comparison with Related Experiments

### vs. `loopback_datanodes`
- **That experiment**: Tests scaling of DataNode process count
- **This experiment**: Tests scaling of storage directory count per DataNode
- **Key difference**: Process architecture vs. storage architecture

### vs. `storage_virtualization`
- **That experiment**: Tests native `dfs.datanode.data.dir` with regular directories
- **This experiment**: Uses loopback filesystems for true storage isolation

## References

- Hadoop Configuration: `dfs.datanode.data.dir` — https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml
- Linux loopback devices: `man losetup`, `man mount`
- HDFS Architecture: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html

## Notes

- Each run of the full experiment takes ~3-4 hours (9 k values × 5 runs × ~3 min per run)
- Results are deterministic assuming stable cluster resources
- Loopback filesystems provide true storage isolation with independent journals and inodes
- This experiment is complementary to `loopback_datanodes` for comprehensive scaling analysis
