# Loopback DataNodes Experiment

## Goal

Measure how WordCount (20GB, 128MB block size) performance changes when each
physical node runs **k** DataNode processes (k = 1, 2, 4, 8, …), where each
DataNode has its own **loopback filesystem** (a virtual block-device backed by
a regular file, formatted as ext4 and mounted independently).

## What is a Loopback Filesystem?

A loopback (or "loop") filesystem lets you treat a **regular file** as if it
were a block device (like a real disk). The Linux kernel's loop driver
(`/dev/loop0`, `/dev/loop1`, …) maps file I/O to block-device I/O so you can
`mkfs` and `mount` a plain file:

```bash
# 1. Create a 30GB disk image file
fallocate -l 30G /data/loop_images/disk1.img

# 2. Format it with ext4
mkfs.ext4 -F /data/loop_images/disk1.img

# 3. Mount it via the loop driver
mkdir -p /mnt/hdfs_loop/dn1
mount -o loop /data/loop_images/disk1.img /mnt/hdfs_loop/dn1
```

Once mounted, `/mnt/hdfs_loop/dn1` is a **fully independent** filesystem with
its own superblock, journal, inode table, and free-space bitmap — exactly like
a physical drive.  HDFS DataNodes that store blocks on different loopback
mounts therefore experience realistic, isolated I/O paths (at least at the
filesystem layer).

## Experiment Design

| Parameter          | Value                                      |
|--------------------|--------------------------------------------|
| Input size         | 20 GB                                      |
| Block size         | 128 MB (default)                           |
| Replication factor | 3 (standard Hadoop default)                |
| Physical nodes     | 5 (tapuz14 master + tapuz10-13 workers)    |
| k values           | 1, 2, 4, 8, 16                             |
| Repetitions (K)    | configurable (default 3)                   |

### Resource Budget (per node: ~8GB RAM, 4 cores)

The runner now performs a preflight on all nodes using hard open-files limits
(`ulimit -Hn`) and may automatically skip large `k` values if limits are too
low for stable operation.

**Memory:**

| k  | DN Heap | Total DN RAM/node | Fits on master? | Fits on workers? |
|----|---------|-------------------|-----------------|------------------|
| 1  | 1024MB  | ~1.1GB            | Yes             | Yes              |
| 2  | 512MB   | ~1.2GB            | Yes             | Yes              |
| 4  | 384MB   | ~1.9GB            | Yes             | Yes              |
| 8  | 256MB   | ~2.8GB            | Yes             | Yes              |
| 16 | 200MB   | ~4.8GB            | Tight (~300MB spare) | Yes         |

Master budget: 8GB - 1.5GB(OS) - 1.0GB(NN) - 0.5GB(YARN RM) - 0.5GB(NM) = 4.5GB for DNs
Worker budget: 8GB - 1.5GB(OS) - 0.5GB(NM) = 6.0GB for DNs

**Disk (20GB × 3 replicas = 60GB total HDFS data):**

| k  | Data/DN | Image size | Images × k = Disk/node |
|----|---------|------------|------------------------|
| 1  | 12GB    | 20GB       | 20GB                   |
| 2  | 6GB     | 10GB       | 20GB                   |
| 4  | 3GB     | 5GB        | 20GB                   |
| 8  | 1.5GB   | 3GB        | 24GB                   |
| 16 | 0.75GB  | 2GB        | 32GB                   |

Image sizes are auto-calculated by the script (1.5× safety margin over expected data).

### Data Distribution

HDFS distributes blocks approximately equally across DataNodes by default (round-robin
with space-aware balancing). Since all virtual DataNodes have similar capacity, the 20GB
input (with 3× replication = 60GB) will be spread evenly. No special configuration needed.

For each value of k:
1. Create k loopback images per node (auto-sized based on data share)
2. Mount them at `/mnt/hdfs_loop/dn{1..k}` on every node
3. Start 1 NameNode + (5 x k) DataNodes  (each DN has unique ports, heap, & data dir)
4. Generate and upload the 20GB input (replication=3)
5. Snapshot NameNode memory via JMX
6. Run WordCount K times, record wall-clock time + NN memory during each run
7. Tear down: stop all DNs, unmount, delete images

Total DataNodes with k=4: 5 nodes x 4 = 20 DataNodes.

## Cluster Layout (k = 4 example)

```
tapuz14 (NameNode + 4 DataNodes)
├── /mnt/hdfs_loop/dn1 → loop image 1 → DN on ports 9866/9864/9867 (1024MB heap)
├── /mnt/hdfs_loop/dn2 → loop image 2 → DN on ports 9876/9874/9877
├── /mnt/hdfs_loop/dn3 → loop image 3 → DN on ports 9886/9884/9887
└── /mnt/hdfs_loop/dn4 → loop image 4 → DN on ports 9896/9894/9897

tapuz10-13 (4 DataNodes each, same layout)
```

Total DataNodes with k=4: 5 nodes x 4 = 20 DataNodes.
Replication=3 means each 128MB block is stored on 3 different virtual DataNodes.

## Scripts

| Script                          | Purpose                                          |
|---------------------------------|--------------------------------------------------|
| `setup-loopback-fs.sh`          | Create + mount k loopback images on one node     |
| `teardown-loopback-fs.sh`       | Unmount + delete all loopback images             |
| `generate-multi-dn-configs.sh`  | Generate per-DN configs (unique ports + heap)    |
| `start-multi-dn-cluster.sh`     | Start NameNode + k×5 DataNodes                   |
| `stop-multi-dn-cluster.sh`      | Stop all DataNode and NameNode processes          |
| `run-experiment.sh`             | Main runner (loops k, monitors NN memory)         |
| `run-experiment-nn-only.sh`     | Same runner, but master runs no DataNode          |
| `plot-results.py`               | Generate 8 plots (runtime + NN memory)            |

## Output

The experiment produces:

```
results/loopback-datanodes/run_<timestamp>/
├── results.csv                      # k, total_dns, avg_runtime, stddev, nn_heap_before/peak/avg, block_count
├── metadata.json                    # Full experiment config
├── experiment.log                   # Detailed log
└── namenode_memory/
    ├── nn_memory_k1.csv             # NN heap sampled every 5s during k=1 runs
    ├── nn_memory_k2.csv
    ├── nn_memory_k4.csv
    ├── nn_memory_k8.csv
    └── nn_memory_k16.csv
```

Plots generated (8 total):
1. `runtime_vs_k.png` - Bar chart of WordCount runtime per k
2. `runtime_vs_total_datanodes.png` - Line plot of runtime vs total DN count
3. `speedup_vs_k.png` - Speedup relative to k=1 baseline
4. `individual_runs.png` - Scatter of individual run times
5. `nn_memory_vs_k.png` - NameNode heap (before/peak/avg) per k
6. `runtime_and_memory_vs_k.png` - Dual-axis: runtime + NN memory
7. `nn_memory_timeseries.png` - NN heap over time for all k values overlaid
8. `block_count_vs_k.png` - Total HDFS blocks per k

## Usage

```bash
# Run with default 3 repetitions per k value
bash experiments/loopback_datanodes/run-experiment.sh

# Run variant where tapuz14 is NameNode-only (no DataNode on master)
bash experiments/loopback_datanodes/run-experiment-nn-only.sh

# Or specify 5 repetitions
bash experiments/loopback_datanodes/run-experiment.sh 5

# Same variant with 5 repetitions
bash experiments/loopback_datanodes/run-experiment-nn-only.sh 5

# Plot after completion
python3 experiments/loopback_datanodes/plot-results.py results/loopback-datanodes/latest

# Plot NN-only-master run
python3 experiments/loopback_datanodes/plot-results.py results/loopback-datanodes-nn-only/latest
```

## Prerequisites

- **`sudo` access** on all 5 nodes (see below)
- Enough disk space for loopback images (max ~32GB per node at k=16)
- SSH key-based access to all nodes (already configured)

## What needs `sudo` and why

Three operations in the loopback workflow require root privileges:

| Command | Why it needs `sudo` | When it runs |
|---------|--------------------|--------------|
| `fallocate` + `mkfs.ext4` | Creating + formatting a disk image as ext4 requires writing a filesystem superblock. `mkfs` on non-block-devices needs root. | Once per image, at setup |
| `mount -o loop` | Mounting a filesystem (attaching a loop device to a file and mounting it) is a privileged kernel operation. | Once per image, at setup |
| `umount` | Unmounting a filesystem is privileged. | Once per image, at teardown |

### How to get `sudo` access

**Option A: You already have sudo** (check with `sudo -v`)

**Option B: Ask your sysadmin** to either:
1. Grant your user full sudo: add `mostufa.j ALL=(ALL) NOPASSWD: ALL` to `/etc/sudoers`
2. Grant limited sudo for just what's needed (more secure):
   ```
   mostufa.j ALL=(ALL) NOPASSWD: /sbin/mkfs.ext4, /bin/mount, /bin/umount, /usr/bin/fallocate, /sbin/losetup, /bin/mkdir, /bin/chmod, /bin/rmdir, /bin/rm
   ```

**Option C: Use user namespaces (no sudo needed)** — if your kernel supports
`unshare` and FUSE:
```bash
# Instead of sudo mount, use udisksctl or fuse2fs:
fuse2fs /data/loop_images/hdfs_dn1.img /mnt/hdfs_loop/dn1
```
This requires `fuse2fs` (from e2fsprogs) and FUSE support. Not all clusters have this.

**Option D: Skip loopback, use regular directories** — you lose filesystem isolation
but the experiment still works. Set `IMAGE_DIR` to a tmpfs or regular directory and
the scripts will fall back to plain directories (you'd need to modify `setup-loopback-fs.sh`).
