# Storage virtualization: k loopback disks per DataNode

Each worker runs one DataNode whose blocks are spread over **k loopback ext4
filesystems** carved out of the node's single physical disk ("k virtual
disks"). The experiment measures what that costs the server as k grows:
WordCount runtime, map-task times, NameNode memory, disk I/O and DataNode CPU.

What the data shows so far, and why the scripts changed in September 2026:
[FINDINGS.md](FINDINGS.md).

Everything runs on the cluster master (tapuz14 or CloudLab node0); the laptop
only edits code and looks at results.

## Quick start (Tapuz)

On the laptop, in Git Bash, from `my_scripts/`:

```bash
git add -A && git commit -m "..."     # push sends committed/tracked files
bash sync-cluster.sh push             # code -> tapuz14:~/my_scripts
```

On tapuz14:

```bash
cd ~/my_scripts/experiments/storage_virtualization_loopback
screen -S exp                         # survives disconnects: Ctrl+A D, later screen -r exp
bash run-2x2.sh                       # the 2x2 test (about 3 hours)
# or a k sweep at full load:
K_VALUES="1 16 128 512 1024" bash run-experiment-loopback-fs.sh 5
```

Back on the laptop:

```bash
bash sync-cluster.sh pull             # -> hadoop/storage_virtualization_loopback_tapuz/run_.../
python experiments/storage_virtualization_loopback/summarize-runs.py ../storage_virtualization_loopback_tapuz/run_<id>
python experiments/storage_virtualization_loopback/plot-results.py   ../storage_virtualization_loopback_tapuz/run_<id>
```

`summary.txt` in the run folder already holds the comparison table.

## CloudLab (c6620)

1. On a new reservation, update the node names in `clusters/c6620.conf` if they
   differ, then run `bash bootstrap-c6620.sh` once on node0 (symlinks
   `/scratch -> /mydata`, installs sysstat for iostat/pidstat/mpstat).
2. Push from the laptop: `bash sync-cluster.sh push Mostufa@<node0 public name>`.
3. On node0 the cluster is detected from the hostname (`CLUSTER=c6620`); run
   the same commands as on Tapuz.
4. Pull: `bash sync-cluster.sh pull Mostufa@<node0 public name>`.

Use the internal names (node0..node4) inside the cluster, never the public
er###.utah.cloudlab.us names (CloudLab rate-limits the control network).

## Settings

Set as environment variables in front of the command. Cluster defaults are in
`clusters/tapuz.conf` and `clusters/c6620.conf`.

| Variable | Default (tapuz / c6620) | Meaning |
|---|---|---|
| `K_VALUES` | `1 256 1024` (run-2x2: `1 1024`) | k values to test |
| `K_ORDER` | `given` | `random` shuffles the k order (seeded) |
| `INPUT_SIZE_GB` | 8 (run-2x2: 2 / 8) | WordCount input size |
| `BLOCK_SIZE_MB` | 32 | HDFS block size of the input |
| `CONDITIONS` | `maps=all,cache=cold` | see below |
| `SLOTS_PER_NODE`, `CONTAINER_MB` | 8 x 2048 / 52 x 1024 | YARN pool per node; the loads of the runs that showed the slowdown |
| `DN_HEAP_MB` | 5500 | DataNode heap (MB or `auto`) |
| `LOOPBACK_BUDGET_PER_NODE_GB` | 200 | disk space for all k images of one node |
| `SEED` | current time | seed for all random orders (stored in metadata.json) |
| `REUPLOAD_EACH_REP` | 0 | 1 = re-upload the input before every job (slow on tapuz: ~2 min/GB) |
| `WORDCOUNT_MODE` | `real` | `trivial` = mapper without tokenizing (build with `../wordcount/trivial/build.sh`) |
| `MASTER_HAS_DN` | 0 | 1 = the master also runs a DataNode |

The first argument is the number of repetitions per k and condition (default 5).

### Conditions and the 2x2 test

A condition is `maps=N,cache=cold|warm`:

- **maps** = map tasks per node running at once (1 .. `SLOTS_PER_NODE`, or `all`).
  Implemented per job by enlarging the map container so exactly N fit in a
  NodeManager; the task heap stays the same. With `maps=1` the nodes that host
  the job's ApplicationMaster or reducer run no map.
- **cache** = `cold`: the page cache is emptied before the job, so the input
  is read from disk. Uses `drop_caches` where passwordless sudo allows it
  (CloudLab); on Tapuz, where it does not, the HDFS block files and loopback
  images are evicted with `posix_fadvise(DONTNEED)`. The log says which method
  ran. `warm`: every block file is read once before the job, so the job reads
  from RAM.

With several conditions, all of them run in every repetition, in a random
(seeded) order, on the same cluster and uploaded data. `run-2x2.sh` runs
`maps=1` vs `maps=all` crossed with `cold` vs `warm`, at k=1 and k=1024.

On Tapuz (7.7 GB RAM) a warm run can only keep a small input in RAM, and
loopback caches data twice (block file + image). Check `diskMB/job` in the
summary: warm jobs should read close to 0 MB from disk.

## Output

`results/storage_virtualization_loopback_<cluster>/run_<timestamp>/` on the cluster:

| File | Content |
|---|---|
| `summary.txt` | per condition and k: runtime mean/sd, change vs the smallest k with 95% CI, maps running at once, seconds per map, data-local %, disk MB read per job |
| `runs.csv` | one row per job: k, repetition, condition, runtime, disk MB read/written, map counts, locality, map/reduce/CPU/GC time |
| `results.csv` | per-k summary read by `plot-results.py` (first condition; `results_<condition>.csv` per condition) |
| `metadata.json` | all settings, seed, code version, node hardware |
| `configs/k<k>/` | the generated Hadoop configs of the master and one DataNode host |
| `jobs/` | full output of every WordCount job |
| `sysstat/` | per node: DataNode CPU/memory/I/O (`pidstat`) and node CPU (`mpstat`), every 5 s |
| `iostat/`, `namenode_memory/`, `fragmentation/`, `hdfs_fsck_k*.txt`, `input_block_*` | as before |

## Built-in checks

- Before anything starts: required tools, passwordless ssh to the workers,
  free space on `/scratch` for the loopback images.
- After the cluster starts: every DataNode host must run a NodeManager, and
  the YARN pool must be exactly `SLOTS_PER_NODE x CONTAINER_MB` per node,
  otherwise the run stops. (In May 2026 runs silently went ahead with one
  usable container per node.)
- A failed job is logged, marked `failed` in runs.csv, and left out of the
  averages; the run continues.

## Analysing old runs

```bash
python analyze-counters.py <run_dir> [<run_dir> ...]
```

prints, per k, how many maps ran at once, seconds and CPU per map task,
container size and data-local share -- from the job counters in
`experiment.log`. Works for runs from before September 2026 too.

## Scripts

| Script | Purpose |
|---|---|
| `run-experiment-loopback-fs.sh` | main runner: per k, start cluster, upload, run all conditions x repetitions, collect metrics |
| `run-2x2.sh` | the load x cache test (wrapper around the runner) |
| `start-single-dn-cluster.sh <k> <img_mb> <heap_mb> <repl>` | loopback setup, config generation on every node, start HDFS + YARN, verify |
| `stop-single-dn-cluster.sh <max_k>` | stop Hadoop, tear down loopback filesystems (also after an aborted run) |
| `generate-single-dn-configs.sh` | Hadoop configs with k data dirs and the fixed YARN pool (runs on every node) |
| `setup-loopback-fs.sh`, `teardown-loopback-fs.sh` | create/format/mount and remove the k images on one node |
| `measure-fragmentation.sh`, `count-input-blocks-per-fs.sh` | filefrag and per-FS block counts |
| `summarize-runs.py`, `analyze-counters.py`, `plot-results.py` | analysis |
| `cluster.conf`, `clusters/*.conf` | cluster selection, topology, paths, defaults |
| `bootstrap-tapuz.sh`, `bootstrap-c6620.sh` | one-time checks / setup per cluster |
