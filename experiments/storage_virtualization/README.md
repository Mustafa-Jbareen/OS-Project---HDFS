# Storage Virtualization Experiments for HDFS

## Research Goal

Investigate the impact of **virtualizing storage** in HDFS DataNodes to enable:
- **Finer-grained fault tolerance** - retire small virtual units instead of entire disks
- **Extended flash lifespan** - wear-leveling across virtual units
- **Software-defined failure domains** - more flexible data placement

### The Core Idea

```
Traditional HDFS:
┌─────────────────┐
│   DataNode      │
│  ┌───────────┐  │
│  │ 1 Storage │  │  ← Single point of failure
│  │  (10 TB)  │  │    Must retire entire disk
│  └───────────┘  │
└─────────────────┘

Proposed Virtualized Storage:
┌─────────────────────────────────────────────┐
│   DataNode                                  │
│  ┌────┐ ┌────┐ ┌────┐     ┌────┐           │
│  │vol1│ │vol2│ │vol3│ ... │volN│           │
│  │10GB│ │10GB│ │10GB│     │10GB│           │
│  └────┘ └────┘ └────┘     └────┘           │
│     ↓      ↓      ↓          ↓             │
│  Can retire individual 10GB units          │
│  Finer-grained wear tracking               │
│  Smaller blast radius on failure           │
└─────────────────────────────────────────────┘
```

### Key Research Questions

1. **NameNode Scalability**: How does NameNode memory scale with:
   - More blocks (smaller block sizes)?
   - More storage units to track?
   - More block reports from DataNodes?

2. **DataNode Overhead**: What's the CPU/IO overhead of:
   - Managing many storage directories?
   - Sending larger block reports?
   - More frequent health checks?

3. **Optimal Configuration**: What's the sweet spot for:
   - Block size (128KB → 1GB)?
   - Virtual storage count per node (1 → 1000)?
   - Replication factor?

4. **Failure Characteristics**: How does failure behavior change:
   - Detection time for failed virtual storage?
   - Re-replication overhead?
   - Recovery time?

## Experiments

### Quick Start: Run All Experiments
**Script**: `run-full-experiment.sh`

Master script that runs all storage virtualization experiments in sequence:

```bash
# Run all experiments
bash run-full-experiment.sh all

# Run specific experiment
bash run-full-experiment.sh block_scaling    # Only block scaling
bash run-full-experiment.sh storage_dirs     # Only storage dirs
bash run-full-experiment.sh memory           # Only memory monitoring
bash run-full-experiment.sh failure          # Only failure simulation
```

Output is saved to `results/full_experiment_<timestamp>/` with:
- Combined results from all experiments
- `SUMMARY.md` with key findings
- `experiment.log` with detailed execution log

### Experiment 1: Block Size Scaling
**Script**: `../wordcount/benchmark-blocksize.sh`

Tests how block size (128KB → 256MB) affects:
- NameNode metadata overhead
- MapReduce job performance
- Number of blocks per file

```bash
bash ../wordcount/benchmark-blocksize.sh 1024  # 1GB input
```

**Why this matters**: Smaller blocks = more blocks = more NameNode memory = similar to having more storage units.

### Experiment 2: Block Count Scaling
**Script**: `benchmark-block-scaling.sh`

Directly measures NameNode heap growth as block count increases:
- Creates thousands of small files
- Measures heap, latency, block report time

```bash
bash benchmark-block-scaling.sh 100000  # Up to 100K blocks
```

**Expected finding**: ~150 bytes NameNode heap per block.

### Experiment 3: Storage Directory Scaling
**Script**: `benchmark-storage-dirs.sh`

Tests DataNode performance with multiple storage directories:
- Simulates virtual storage units
- Measures throughput, block report time, overhead

```bash
bash benchmark-storage-dirs.sh 64  # Test 1, 2, 4, 8, ... 64 dirs
```

**This is the core experiment** for your research.

### Experiment 4: NameNode Memory Monitoring
**Script**: `monitor-namenode-memory.sh`

Real-time monitoring of NameNode heap during experiments:

```bash
# Run in background while doing other experiments
bash monitor-namenode-memory.sh 5 60 &  # Every 5s for 60 min
```

### Experiment 5: Virtual Storage Failure Simulation
**Script**: `simulate-virtual-storage-failure.sh`

Tests what happens when individual virtual storages fail:
- Simulates failure by corrupting/removing storage dir
- Measures detection time, re-replication overhead

```bash
bash simulate-virtual-storage-failure.sh 16 2  # 16 dirs, fail 2
```

## Configuration for Virtual Storage

To enable multiple storage directories per DataNode:

```xml
<!-- hdfs-site.xml on each DataNode -->
<property>
  <name>dfs.datanode.data.dir</name>
  <value>
    /data/dn/vol1,
    /data/dn/vol2,
    /data/dn/vol3,
    ...
    /data/dn/volN
  </value>
</property>

<!-- Tolerate some failures without marking DataNode dead -->
<property>
  <name>dfs.datanode.failed.volumes.tolerated</name>
  <value>10</value>  <!-- Allow up to 10 volumes to fail -->
</property>
```

## Expected Results & Trade-offs

| Virtual Storages | NameNode Overhead | Failure Blast Radius | Management Complexity |
|-----------------|-------------------|---------------------|----------------------|
| 1 (traditional) | Low | High (entire disk) | Low |
| 10 | Low-Medium | Medium (10% capacity) | Medium |
| 100 | Medium | Low (1% capacity) | High |
| 1000 | High | Very Low (0.1%) | Very High |

### NameNode Memory Estimates

Assuming ~150 bytes per block in NameNode heap:

| Scenario | Blocks | NameNode Heap Needed |
|----------|--------|---------------------|
| 1TB data, 128MB blocks | 8,192 | ~1.2 MB |
| 1TB data, 1MB blocks | 1,048,576 | ~150 MB |
| 1TB data, 128KB blocks | 8,388,608 | ~1.2 GB |
| 100TB cluster, 128KB blocks | 838,860,800 | ~120 GB ❌ |

**Conclusion**: Block size has huge impact on NameNode memory!

## Recommendations for Your Research

1. **Start with block size experiments** - they're simpler and give quick insights
2. **Monitor NameNode heap** during all experiments
3. **Test with realistic data sizes** - 50GB+ to see real effects
4. **Document everything** - your findings could be valuable research

## Plotting Results

```bash
# Plot any experiment results
python3 plot-storage-virtualization.py results/<experiment>/results.csv
```
