# Findings so far (as of September 2026)

Question: what happens to a server when its one physical disk is split into
k virtual disks (here: k loopback ext4 filesystems under one DataNode)?

## 0. The controlled run of September 2026 (corrects section 2)

Tapuz, `pipeline_2026-09-27_20-42-03`: 2 GB in 32 MB blocks, 5 repetitions.
k is in a random order, every condition runs at every k, and every job
follows the same protocol.

| Condition | k=1 -> k=1024 runtime [95% CI] | Time per map task |
|---|---|---|
| 1 map/node, cold | +12.1% [+10.2, +14.0] | +6.9% |
| 1 map/node, warm | +11.6% [+9.5, +13.8] | +7.6% |
| 4 maps/node, cold | +12.8% [+10.7, +14.9] | +10.8% |
| 8 maps/node, cold | +14.9% [+10.9, +18.9] | +15.1% |
| 8 maps/node, warm | +14.8% [+11.1, +18.6] | +17.0% |

- **k=1024 is 12-15% slower at every load and cache state.** The cost
  starts between k=256 and k=512. The page cache makes no difference. Load
  adds only ~3 points, and that difference is not significant.
- **Section 2's conclusion, that light load shows no slowdown, does not
  hold.** The May 9 Tapuz run hid the slowdown by chance:
  - its map tasks were 4% slower at k=1024 (section 3);
  - but YARN kept 3.5% more maps running at once in both k=1024 jobs (2.65
    vs 2.56), which cancelled the slower tasks in the runtime;
  - with 2 repetitions this was invisible.

  With one container per node, the runtime depends on where YARN places the
  ApplicationMaster and the reducer. Time per map task does not, so it is
  the better measure at light load.
- **Scheduling outliers:** 3 of the 50 one-map-per-node jobs ran 1.8 maps at
  once instead of 2.4. `final-report.py` now leaves such jobs out (below 85%
  of their condition's median) and lists them.
- **Not the kernel's read path.** Reading the same data through 1024
  loopback filesystems without Hadoop is not slower (storage-only
  benchmark). Direct I/O on the loop devices does not change the slowdown.
  The cost comes with the DataNode managing 1024 volumes:
  - threads: 73 -> 2137;
  - RSS: 0.6 -> 2.3 GB;
  - DataNode start to first block report: 2 s -> 10 min.
- **The mkfs control could not test the old 1 KB layout:** this OS's mkfs
  gives small images 4 KB blocks too.
- **Swapping** happened only in the 8-maps warm jobs at k >= 512 (Tapuz has
  7.7 GB of RAM). The cold 8-maps jobs did not swap and slowed down by the
  same amount.

## 1. Under load, k=1024 makes WordCount 9-16% slower

Runtime change vs k=1. "Maps at once" = average number of map tasks running
at the same time (total map-task time / job runtime), from the job counters.

| Run | Hardware | Input / block | Maps at once | Data-local | k=512 | k=1024 |
|---|---|---|---|---|---|---|
| 2026-04-14_00-17-53 | Tapuz, HDD | 40 GB / 16 MB | 29 | 98% | +6.4% | +13.8% |
| 2026-04-18_19-21-56 | Tapuz, HDD | 50 GB / 32 MB | 30 | 94% | +10.3% | +13.6% |
| 2026-05-08_18-45-32 | Tapuz, HDD | 50 GB / 32 MB | 29 | 96% | +10.6% | – |
| 2026-05-05_15-50-38 | CloudLab m400 (ARM), SATA SSD | 50 GB / 64 MB | 28 | 100% | +8.4% | – |
| 2026-05-08_06-37-06 | CloudLab c6620, NVMe | 50 GB / 32 MB | 208 | 100% | +4.2% | +8.9% |

Other April Tapuz runs (8-128 MB blocks, 1-160 GB inputs) show the same
shape: flat up to k≈128, about +3-4% at k=256, +6-10% at k=512, +12-16% at
k=1024, with 5 repetitions and standard deviations of 1-3%.

This happens on HDD and on both SSD types. On Tapuz the input (40-50 GB, ~37
GB per node) could not fit in 7.7 GB of RAM, and iostat shows the jobs
reading it from disk. So the slowdown is **not** a page-cache artifact and
not an HDD-seek effect.

## 2. The runs without slowdown ran at a much lower load

> Corrected by section 0: light load also slows down, by 12% at k=1024 in
> the controlled run. The low load explains why the runtimes below are
> noisy, not why the slowdown is missing.

| Run | Hardware | Maps at once | Data-local | k=1024 |
|---|---|---|---|---|
| 2026-05-08_11-36-32 | c6620, NVMe | 13 | 0% | +0.1% |
| 2026-05-09_21-45-10 | Tapuz, HDD | 2.6 | 82% | +0.5% |
| 2026-05-10_02-31-53 | c6620, NVMe (reads from RAM) | 4 | 0% | +0.3% |

These runs used scripts changed on May 8-10. Several things changed at once:

- **YARN pool sized from each node's RAM.** On Tapuz (7.7 GB) that left a
  3.7 GB pool with 2 GB containers: one task per node. On c6620 the
  containers became 8 GB. Before, YARN ran ~8 tasks per Tapuz node (from the
  base install's config) and ~52 per c6620 node.
- **0% data-local maps on c6620.** The NodeManagers were pinned to the
  internal names (node1..node4) but the DataNodes still registered under
  their public names, so YARN could not place maps next to their data.
- **Cache order.** Upload-then-drop-caches instead of drop-then-upload.
- **DataNode heap** changed from 5500 MB to "auto".
- Input size, block size and k values also differed.

The May 10 run read its input from RAM on purpose and still showed nothing,
so the cache change alone cannot explain the missing slowdown. All three runs
had very low load, which fits the data in section 3.

## 3. The per-task overhead is always there; it grows with load

Average duration and CPU time of one map task, k=1 -> k=1024:

| Run | Maps at once | Time per map task | CPU per map task |
|---|---|---|---|
| Tapuz 2026-04-18 | 30 | 26.3 -> 30.1 s (+14.7%) | +9.1% |
| Tapuz 2026-04-14 | 29 | 20.8 -> 23.2 s (+11.8%) | +10.2% |
| c6620 2026-05-08 06:37 | 208 | 17.8 -> 19.5 s (+9.5%) | +8.8% |
| c6620 2026-05-10 | 4 | 3.43 -> 3.60 s (+5.0%) | +7.3% |
| Tapuz 2026-05-09 | 2.6 | 4.97 -> 5.17 s (+4.0%) | +3.9% |
| c6620 2026-05-08 11:36 | 13 | 11.4 -> 11.6 s (+1.6%) | +2.5% |

Map tasks get slower at k=1024 in every run, but by 10-15% under heavy load
and only 2-5% under light load. At light load the job runtime barely moves.
The NameNode heap stays small (under 400 MB) throughout, and the extra time is
spent inside map tasks, which read data from the DataNodes. So the evidence
points at the DataNode / kernel read path under concurrency, not at the
NameNode.

## 4. Measurement problems found in the old scripts (fixed)

- On Tapuz the cache drop (`sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'`)
  most likely never ran: passwordless sudo there covers only mkfs, mount,
  losetup and a few others, and the failure was hidden by `|| true`. Harmless
  for the big April inputs (they could not fit in RAM anyway), but small
  inputs would have been read from RAM.
- The input was re-uploaded before every job (about 90 minutes for 50 GB on
  Tapuz), and on RAM-rich machines the fresh upload stayed in cache.
- YARN pool and data locality as in section 2; nothing checked them.
- Scripts had Windows line endings in the repository working copy.
- The loopback images were formatted with mkfs.ext4's defaults, which depend
  on the image size: below 512 MB (k >= 512 with a 200-220 GB budget) mkfs
  may switch to 1 KB blocks and denser inodes. So at k=512 and k=1024 the
  filesystem layout may have changed together with k -- exactly where the
  April slowdown jumped. The main run now uses one fixed layout (4 KB blocks)
  for every k and records the block size; a control stage repeats k=1 vs
  1024 with the old default layout.
- Earlier notes (NEXT_STEPS.md) said "HDD is 2x faster than SSD". That
  compared ARM m400 nodes with x86 Tapuz nodes on a CPU-heavy job, so it says
  little about the disks.

## 5. Next steps

1. **`run-all.sh` on Tapuz** (one command, ~13 h): smoke test (gate), then
   load (1 / 4 / 8 maps per node) x cache (cold / warm) at k = 1, 64, 256,
   512, 1024, conditions interleaved on one cluster through an identical
   per-job protocol; then the storage stack without Hadoop, and two controls
   (loop direct I/O; the old default mkfs layout); then `FINAL_REPORT.md`.
   If load is the cause, the 8-maps cells show roughly +10% at k=1024 and the
   1-map cells show little, cold or warm.
2. Repeat it on c6620 when a reservation is available, with the same settings
   (they are shared by all clusters now), so only the hardware differs. Warm
   runs are clean there (plenty of RAM).
3. Where the time goes: `sysstat/` now records DataNode CPU and node
   %usr/%sys/%iowait per k. Candidates: per-volume DataNode threads and locks,
   one loop device + ext4 journal per virtual disk (kernel threads), double
   page caching through the loop devices.
4. A k sweep at full load (e.g. 1, 16, 128, 256, 512, 1024) with `K_ORDER=random`
   to pin down where the slowdown starts.
5. An I/O-bound workload (e.g. TestDFSIO read) to see whether the cost is in
   the read path itself.
