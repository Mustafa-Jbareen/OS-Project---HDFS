# Project notes: k virtual disks per DataNode

Summary of the September 2026 work session: the question, what the old data
shows, how the experiment works now, its settings, and what we learned about
the machines along the way.

Commands are in [COMMANDS.md](COMMANDS.md). The detailed analysis of the old
runs is [FINDINGS.md](experiments/storage_virtualization_loopback/FINDINGS.md),
and the full experiment docs are in the
[experiment README](experiments/storage_virtualization_loopback/README.md).

## The question

What happens to a server when its one physical disk is split into k virtual
disks? Here each worker runs one HDFS DataNode whose blocks are spread over
k loopback ext4 filesystems, all carved out of the same physical disk, with k
up to 1024.

What is measured:

- WordCount runtime and map-task time
- DataNode time per block served, and CPU per task
- DataNode threads, memory and file descriptors
- NameNode heap
- block-report time, DataNode start-up time and upload speed

## Status (2026-09-27)

- **Code:** the scripts were rewritten, merged into
  `experiments/storage_virtualization_loopback/` and pushed to GitHub (`main`).
  Every commit is authored by Mustafa-Jbareen only.
- **Tapuz is ready:**
  - the old copy was moved to `/home/mostufa.j/my_scripts_before_sep2026`, and
    the new code was pushed to `/home/mostufa.j/my_scripts`;
  - `/scratch` was cleaned, freeing 26 GB on tapuz14;
  - `bootstrap-tapuz.sh` passed on all 5 nodes, including the new page-cache
    test.
- **Next:**
  - start `run-all.sh`: a smoke test that gates the rest, then about 13 hours;
  - then `.\sync-cluster.ps1 pull`, and write the conclusion from
    `FINAL_REPORT.md`.
- **CloudLab:** there is no reservation right now, so everything runs on Tapuz.

## What the old runs showed

From FINDINGS.md:

- **The slowdown appears under load.** With about 30 map tasks running at
  once on Tapuz, or about 208 on c6620, k=1024 made WordCount **9-16% slower**
  than k=1. This held on an HDD (Tapuz), a SATA SSD (m400) and NVMe (c6620).
  The shape: flat up to k≈128, then about +3-4% at 256, +6-10% at 512 and
  +12-16% at 1024.
- **The May runs without a slowdown ran at a much lower load:** 2.6 to 13 map
  tasks at once. Two script changes caused this:
  - YARN was sized from each node's RAM, which left one container per Tapuz
    node;
  - on c6620 the NodeManager and DataNode host names didn't match, so 0% of
    the map tasks read local data.
- So **load, not the page cache**, is the likely explanation. Even at low
  load, map tasks are 2-5% slower at k=1024.
- **Measurement problems found and fixed:**
  - `drop_caches` probably never worked on Tapuz, since there is no sudo for
    it. Cold and warm now use `posix_fadvise`, the same on every cluster.
  - `mkfs.ext4` picks a different layout (1 KB blocks) for images under 512 MB.
    With a 200 GB budget that means exactly k ≥ 512, where the slowdown grew.
    Images now always get 4 KB blocks, and stage 5 measures the old default.
  - YARN capacity is now fixed and checked at start-up. On CloudLab the host
    names are pinned.

## How the experiment works now

### Factors

- **k:** 1, 64, 256, 512, 1024, in a random order.
- **Load:** 1, 4 or 8 map tasks per node at once. It is set per job by the
  map container size, as shown in the table below. The task heap stays at
  1638 MB, so only the number of parallel tasks changes.

  | Map tasks per node | Container size |
  |---|---|
  | 1 | 16384 MB |
  | 4 | 4096 MB |
  | 8 | 2048 MB |

  The YARN pool is 8 × 2048 MB per node.
- **Cache:** cold means the input is evicted from RAM before the job; warm
  means the input is read into RAM before the job.
- **Conditions:** 5 in total: 1 cold, 1 warm, 4 cold, 8 cold, 8 warm.

### Per-job protocol (identical for every job)

1. Wait until YARN is idle, then delete the previous output.
2. Run `sync` on every DataNode, then the cache step:
   - **cold:** evict the block files and the loop images from RAM;
   - **warm:** read the block files.
3. Wait 10 s.
4. Record how much of the input is in RAM (`cache-step.py measure`, mincore),
   the disk counters, other users' CPU and the DataNode metrics.
5. Run and time the job, then record the same numbers again.

Around it:

- one untimed warm-up job runs after each cluster start (once per k);
- speculative execution is off;
- the conditions run in a random, seeded order within each repetition.

### The pipeline (`run-all.sh`)

| Stage | What | On failure |
|---|---|---|
| 0 clean | `clean-scratch.sh`: stop Hadoop, remove loopback disks, input copies, YARN caches, old logs | -- |
| 1 smoke | the main run in small, plus checks: READY / NOT READY | **stops the pipeline** |
| 2 main | the 5 conditions at k = 1 64 256 512 1024 | stops |
| 3 bench | the storage stack alone, no Hadoop | noted, goes on |
| 4 directio | control: loop devices with direct I/O | noted, goes on |
| 5 mkfs | control: mkfs's default layout | noted, goes on |
| 6 report | `FINAL_REPORT.md` with 5 figures; restart the normal cluster | -- |

The smoke test checks that:

- all jobs finished and all cells are present;
- the load levels really differ;
- cold jobs read the input from disk and warm jobs don't;
- the share of the input in RAM at job start is ~0% for cold and ~100% for
  warm;
- at least 70% of the map tasks read local data;
- all measurements are present, and nothing swapped.

## Experiment parameters

### Shared settings (`experiment.conf`, the same on Tapuz and CloudLab)

| Setting | Value |
|---|---|
| YARN pool per node | 8 containers × 2048 MB = 16384 MB, 8 vcores |
| Map task heap | 1638 MB in every condition (80% of 2048) |
| DataNode heap | 5500 MB |
| Input | 2 GB of text in 32 MB blocks = 64 map tasks; 1 reducer |
| Replication | 3, so 1.5 GB of block files per worker |
| Loopback budget | 200 GB per worker, split into k images |
| Filesystem | ext4 with `-b 4096 -i 16384 -m0` (fixed 4 KB blocks) |
| Loop devices | buffered (no direct I/O) |
| Settle pause | 10 s before each job |
| Warm-up | 1 untimed job per k |
| Speculative execution | off |

### Smoke test vs main run

| | Smoke test (stage 1) | Main run (stage 2) |
|---|---|---|
| k | 1, 4 | 1, 64, 256, 512, 1024 (random order) |
| Repetitions | 1 | 5 (`--reps 3` for 3) |
| Conditions | the same 5 | the same 5 |
| Input | 2 GB, 32 MB blocks (64 maps) | 2 GB, 32 MB blocks (64 maps) |
| Loopback budget | 20 GB per worker | 200 GB per worker |
| Timed jobs | 2 × 5 × 1 = 10, plus 2 warm-ups | 5 × 5 × 5 = 125, plus 5 warm-ups |
| Time on Tapuz | ~35 min | ~9 h (~6 h with 3 reps) |

The other stages:

| Stage | k | What runs |
|---|---|---|
| 3 bench | 1, 256, 1024 | 1 and 8 parallel readers × cold / warm × 3 repetitions. Each worker reads 1536 MB as 48 files of 32 MB, spread over its k filesystems. Same images as the main run. |
| 4 directio | 1, 1024 | 8 maps cold + warm, loop devices with direct I/O, 5 repetitions |
| 5 mkfs | 1, 1024 | 8 maps cold, mkfs default layout, 5 repetitions |

The whole pipeline takes about 13 h, or about 9 h with `--reps 3`.

### Loopback image sizes

Each image is the budget divided by k (minimum 100 MB). Images exist only on
the workers (tapuz10-13), and space is reserved up front with `fallocate`. The
total stays the same at every k, so only the number of filesystems changes.

| k | Image size (main run, controls, bench) | Image size (smoke test) |
|---|---|---|
| 1 | 204800 MB (200 GB) | 20480 MB (20 GB) |
| 4 | -- | 5120 MB (5 GB) |
| 64 | 3200 MB | -- |
| 256 | 800 MB | -- |
| 512 | 400 MB | -- |
| 1024 | 200 MB | -- |

With mkfs's default layout (stage 5), images under 512 MB get 1 KB blocks.
That is the layout every run before September 2026 used at k ≥ 512.

## Facts about the machines

### Laptop (this one): code only, never runs experiments

- **OS:** Windows 11. Commands are PowerShell 5.1, which has no `&&`.
- **Tools:** Windows' built-in ssh (OpenSSH 9.5) and tar (bsdtar) are what
  `sync-cluster.ps1` uses.
- **Python:** 3.13 in `C:\Python313`, with matplotlib. Use `python`;
  `python3` is only the Microsoft Store placeholder.
- **SSH key:** `C:\Users\mostufa.j\.ssh\id_ed25519`.
- **Git identity:** Mustafa-Jbareen <Mostufa-Mokhtar@hotmail.com>. No
  Claude co-author lines in commits.

### Tapuz

- **Nodes:**
  - tapuz14 is the master (NameNode and ResourceManager, no DataNode);
  - tapuz10-13 are the workers (one DataNode and one NodeManager each);
  - each node has 4 cores, 7.7 GB RAM, 31 GB swap and one HDD;
  - the login shell is bash.
- **Home folders:**
  - `~` is `/csl/mostufa.j`, the shared CSL network home. Don't use it.
  - Use `/home/mostufa.j` for everything:
    - the code, in `/home/mostufa.j/my_scripts`;
    - Hadoop 3.3.1, in `/home/mostufa.j/hadoop`;
    - the ssh keys, in `/home/mostufa.j/.ssh`;
    - the normal cluster's HDFS data, in `/home/mostufa.j/hadoop_data`.
- **`/scratch`** is the local HDD partition: 248 GB, with 236 GB free after
  cleaning.
  - `hadoop_data`, `tmp`, `yarn-local` and `yarn-logs` belong to the
    experiment's cluster.
  - `loop_images` and `hdfs_loop` exist only during a run.
  - Never touch `lost+found`.
- **sudo:** passwordless for the commands the experiment needs: `mkfs.ext4`,
  `mount`, `umount`, `fallocate`, `losetup`, `mkdir`, `chmod`, `rmdir` and
  `rm`. It doesn't cover `sh` or `sync`, so there is no `drop_caches`, and no
  package installs.
- **No `fincore`:** `cache-step.py measure` does the same job (mmap +
  mincore). It was verified on all 5 nodes on 2026-09-27.
- **Code on workers:** the workers get their helper scripts by scp into
  `/tmp`, so the code only needs to be on tapuz14.
- **Normal cluster:** `run-all.sh` stops your normal Hadoop cluster and
  restarts it at the end with a reformatted, empty HDFS.

### CloudLab (c6620, when reserved)

- **Nodes:** node0 to node4, with NVMe disks. Inside the cluster, always use
  these internal names, never the public `er###.utah.cloudlab.us` ones.
- **Home:** the normal home works there, so push with
  `-RemoteDir '~/my_scripts'`.
- **Storage:** `bootstrap-c6620.sh` links `/scratch` to `/mydata`.

## Tools and files

| File | What it does |
|---|---|
| `sync-cluster.ps1` | laptop: push the code, pull the results (and write the report) |
| `run-all.sh` | the whole pipeline, stages 0-6 |
| `clean-scratch.sh` | stage 0: stop Hadoop, remove loopback disks and your leftovers in `/scratch` |
| `bootstrap-tapuz.sh`, `bootstrap-c6620.sh` | pre-flight checks of the nodes |
| `run-2x2.sh` | the smoke test (`smoke`) or the full 2x2 on its own |
| `run-experiment-loopback-fs.sh` | one run: every k, condition and repetition |
| `storage-bench.sh` | the storage stack without Hadoop |
| `start-/stop-single-dn-cluster.sh`, `setup-/teardown-loopback-fs.sh`, `generate-single-dn-configs.sh` | cluster and virtual disks for one k |
| `cache-step.py` | cold / warm / measure, on each DataNode host |
| `summarize-runs.py` | comparison table; `--check` gives the smoke checks |
| `analyze-counters.py` | job counters: maps at once, container size |
| `final-report.py`, `report_figures.py` | `FINAL_REPORT.md` and the 5 figures (PNG + PDF) |
| `restore-base-cluster.sh` | restarts the normal cluster at the end |
| `experiment.conf`, `cluster.conf`, `clusters/*.conf` | shared settings; names and paths per cluster |

The five figures, and the question each one answers:

1. `fig1_runtime_vs_k`: how big is the slowdown, from which k, and is it load
   or cache?
2. `fig2_where_the_time_goes`: time per map task, DataNode time per block,
   CPU per task.
3. `fig3_storage_stack_alone`: is the cost in Linux or in HDFS?
4. `fig4_controls`: is it an artifact of how the disks were built?
5. `fig5_server_cost`: DataNode threads and memory, NameNode heap, block
   reports, start-up time, upload speed.

A pipeline writes `results/pipeline_<date>/`, with these contents:

- one folder per stage;
- `pipeline.log` and `stages.env`;
- `FINAL_REPORT.md` and `figures/`.

Each run folder inside it contains:

- `runs.csv` (one row per job), `dn_metrics.csv`, `server_metrics.csv` and
  `results.csv`;
- `summary.txt`, `checks.txt` and `metadata.json`;
- the job logs and sysstat samples.

## Decisions, and why

- **Same measured settings on Tapuz and CloudLab**, so a comparison shows only
  the hardware (your choice). Caveat: 8 tasks per node fill Tapuz's 4 cores
  but are a light load for c6620.
- **8 containers of 2 GB per node:** this matches the April runs that showed
  the slowdown (about 8 map tasks per Tapuz node). The 16 GB pool is YARN's
  bookkeeping and exceeds the 7.7 GB of RAM, which is why the smoke test
  checks that nothing swaps.
- **Load set by container size, with a fixed heap:** only the number of
  parallel tasks changes. A middle level (4) shows the shape.
- **fadvise instead of `drop_caches`:** it needs no root, is identical on both
  clusters, and affects only the input data.
- **Cells can't affect each other:** each job waits for an idle YARN, the
  order is random, a warm-up job runs per k, and speculative execution is off.
- **The smoke test gates the pipeline:** no 13-hour run starts with broken
  settings.
- **Three controls against artifacts:** a fixed mkfs layout plus a
  default-layout control, a direct-I/O control, and a storage-only benchmark.

## Open points

- If the smoke test says NOT READY, fix the problem and restart `run-all.sh`.
- After the pipeline, write the conclusion from `FINAL_REPORT.md`:
  - the size of the slowdown per load level and cache state;
  - where the time goes;
  - whether the cost is in Linux or in HDFS;
  - what the controls show;
  - what the server pays.
- On CloudLab c6620, run the same pipeline plus a heavier load, since 8 tasks
  don't load its cores. For example: `maps=48` with `SLOTS_PER_NODE=52
  CONTAINER_MB=1024`.
- **Where the old material is:**
  - old code and results on Tapuz: `/home/mostufa.j/my_scripts_before_sep2026`;
  - old results on the laptop: `C:\Users\mostufa.j\Desktop\Dan\hadoop\`;
  - the notes from before this session there are marked "Superseded".
