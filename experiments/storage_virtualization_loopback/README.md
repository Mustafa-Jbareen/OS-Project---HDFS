# Storage virtualization: k loopback disks per DataNode

Each worker runs one DataNode whose blocks are spread over **k loopback ext4
filesystems** carved out of the node's single physical disk ("k virtual
disks"). The experiment measures what that costs the server as k grows:
WordCount runtime, map-task times, NameNode memory, disk I/O, DataNode CPU
and memory.

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

On tapuz14, first the smoke test (~25 min), then the full run:

```bash
cd ~/my_scripts/experiments/storage_virtualization_loopback
screen -S exp                         # survives disconnects: Ctrl+A D, later screen -r exp
bash run-2x2.sh smoke                 # tiny 2x2; ends with READY / NOT READY
bash run-2x2.sh                       # full 2x2: k = 1 64 256 512 1024, ~7-8 hours
```

Back on the laptop:

```bash
bash sync-cluster.sh pull             # -> hadoop/storage_virtualization_loopback_tapuz[_smoke]/run_.../
python experiments/storage_virtualization_loopback/plot-results.py ../storage_virtualization_loopback_tapuz/run_<id>
```

`summary.txt` in the run folder holds the comparison table (`checks.txt` for
the smoke test).

## CloudLab (c6620)

The experiment settings are identical on both clusters (`experiment.conf`),
so a CloudLab run differs from a Tapuz run only in hardware.

1. On a new reservation, update the node names in `clusters/c6620.conf` if they
   differ, then run `bash bootstrap-c6620.sh` once on node0 (symlinks
   `/scratch -> /mydata`, installs sysstat for iostat/pidstat/mpstat).
2. Push from the laptop: `bash sync-cluster.sh push Mostufa@<node0 public name>`.
3. On node0 the cluster is detected from the hostname (`CLUSTER=c6620`); run
   the same commands as on Tapuz (smoke test first).
4. Pull: `bash sync-cluster.sh pull Mostufa@<node0 public name>`.

Use the internal names (node0..node4) inside the cluster, never the public
er###.utah.cloudlab.us names (CloudLab rate-limits the control network).

## Settings

`experiment.conf` holds every measured setting, the same for all clusters;
`clusters/<name>.conf` only holds node names and paths. Override any setting
with an environment variable in front of the command.

| Variable | Default | Meaning |
|---|---|---|
| `K_VALUES` | `1 256 1024` (run-2x2: `1 64 256 512 1024`, smoke: `1 4`) | k values to test |
| `K_ORDER` | `given` (run-2x2: `random`) | `random` shuffles the k order (seeded) |
| `INPUT_SIZE_GB` / `INPUT_SIZE_MB` | 8 GB (run-2x2: 2 GB, smoke: 1024 MB) | WordCount input size |
| `BLOCK_SIZE_MB` | 32 (smoke: 16) | HDFS block size of the input |
| `CONDITIONS` | `maps=all,cache=cold` (run-2x2: the 4 combinations) | see below |
| `SLOTS_PER_NODE`, `CONTAINER_MB` | 8 x 2048 | YARN pool per node (the load of the April runs that showed the slowdown) |
| `DN_HEAP_MB` | 5500 | DataNode heap (MB or `auto`) |
| `LOOPBACK_BUDGET_PER_NODE_GB` | 200 (smoke: 20) | disk space for all k images of one node |
| `SETTLE_SECONDS` | 10 | pause before every measured job |
| `WARMUP_JOBS` | 1 | untimed jobs after each cluster start |
| `SEED` | current time | seed for all random orders (stored in metadata.json) |
| `REUPLOAD_EACH_REP` | 0 | 1 = re-upload the input before every job (slow on Tapuz: ~2 min/GB) |
| `WORDCOUNT_MODE` | `real` | `trivial` = mapper without tokenizing (build with `../wordcount/trivial/build.sh`) |
| `MASTER_HAS_DN` | 0 | 1 = the master also runs a DataNode |

The first argument of `run-experiment-loopback-fs.sh` / `run-2x2.sh` is the
number of repetitions per k and condition (default 5).

To reproduce the load of c6620 run 2026-05-08_06-37-06 (~52 maps at once per
node): `SLOTS_PER_NODE=52 CONTAINER_MB=1024`.

### Conditions and the 2x2 test

A condition is `maps=N,cache=cold|warm`:

- **maps** = map tasks per node running at once (1 .. `SLOTS_PER_NODE`, or `all`).
  Implemented per job by enlarging the map container so exactly N fit in a
  NodeManager; the task heap stays the same. With `maps=1` the nodes that host
  the job's ApplicationMaster or reducer run no map.
- **cache** = `cold`: the input is evicted from the page cache before the job,
  so it is read from disk. `warm`: every block file is read once before the
  job, so the job reads from RAM. See the protocol below.

With several conditions, all of them run in every repetition, in a random
(seeded) order, on the same cluster and the same uploaded data. `run-2x2.sh`
runs `maps=1` vs `maps=all` crossed with `cold` vs `warm`.

On Tapuz (7.7 GB RAM) a warm run can only keep a small input in RAM, and
loopback caches data twice (block file + image). The smoke test checks it:
warm jobs must read close to 0 MB from disk.

### Per-job protocol

Every job -- measured or warm-up, any condition, any k, any cluster -- goes
through exactly the same steps; only step 3's cache action and the map
container size depend on the condition:

1. Wait until YARN is idle (no application running, the whole pool free).
2. Delete the previous job's output.
3. On every DataNode host: `sync` (flush the previous job's writes), then
   *cold*: evict the input's HDFS block files and loopback images from the
   page cache (`posix_fadvise(DONTNEED)`), or *warm*: read every block file.
   Same method on every cluster; needs no root, and in both modes the OS,
   the Hadoop jars and filesystem metadata stay cached, so the modes differ
   only in the input data.
4. Pause `SETTLE_SECONDS`.
5. Record how much of the input is in the page cache (`fincore`) and the disk
   counters, run and time the job, record the counters again.

Around it: after every cluster start (one per k) the input is uploaded once,
then `WARMUP_JOBS` untimed jobs run, so no condition pays for being the first
job on a fresh cluster. Speculative task attempts are disabled (how many run
depends on the load).

## Output

`results/storage_virtualization_loopback_<cluster>/run_<timestamp>/` on the
cluster (`..._<cluster>_smoke/` for smoke tests):

| File | Content |
|---|---|
| `summary.txt` | per condition and k: runtime mean/sd, change vs the smallest k with 95% CI, maps running at once, seconds per map, data-local %, share of the input cached at job start, disk MB read per job |
| `checks.txt` | smoke test: PASS / WARN / FAIL per check and READY / NOT READY |
| `runs.csv` | one row per job (warm-up jobs have status `warmup`): k, repetition, position in the shuffled order, condition, runtime, input MB cached at start, disk MB read/written, map counts, locality, map/reduce/CPU/GC time |
| `results.csv` | per-k summary read by `plot-results.py` (first condition; `results_<condition>.csv` per condition) |
| `metadata.json` | all settings, seed, protocol, code version, node hardware |
| `configs/k<k>/` | the generated Hadoop configs of the master and one DataNode host |
| `jobs/` | full output of every WordCount job |
| `sysstat/` | per node every 5 s: DataNode CPU/memory/I/O (`pidstat`), CPU (`mpstat`), memory/page cache/swap (`vmstat`) |
| `iostat/`, `namenode_memory/`, `fragmentation/`, `hdfs_fsck_k*.txt`, `input_block_*` | as before |

`python summarize-runs.py <run_dir> --check` runs the smoke-test checks on any run.

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
| `run-experiment-loopback-fs.sh` | main runner: per k, start cluster, upload, warm-up, all conditions x repetitions, collect metrics |
| `run-2x2.sh` | the load x cache test (`smoke` = tiny version with checks) |
| `start-single-dn-cluster.sh <k> <img_mb> <heap_mb> <repl>` | loopback setup, config generation on every node, start HDFS + YARN, verify |
| `stop-single-dn-cluster.sh <max_k>` | stop Hadoop, tear down loopback filesystems (also after an aborted run) |
| `generate-single-dn-configs.sh` | Hadoop configs with k data dirs and the fixed YARN pool (runs on every node) |
| `setup-loopback-fs.sh`, `teardown-loopback-fs.sh` | create/format/mount and remove the k images on one node |
| `measure-fragmentation.sh`, `count-input-blocks-per-fs.sh` | filefrag and per-FS block counts |
| `summarize-runs.py`, `analyze-counters.py`, `plot-results.py` | analysis |
| `experiment.conf` | the measured settings, shared by all clusters |
| `cluster.conf`, `clusters/*.conf` | cluster selection, node names, paths |
| `bootstrap-tapuz.sh`, `bootstrap-c6620.sh` | one-time checks / setup per cluster |
