# Commands: the k-virtual-disks experiment

Every command, in the order you use them.

- **LOCAL** = this laptop (Windows), in **PowerShell** (the VS Code terminal).
- **TAPUZ** = tapuz14 (Linux). Log in from PowerShell with
  `ssh mostufa.j@tapuz14.cslcs.technion.ac.il`.

On Tapuz `~` is the shared `/csl` home, so every Tapuz path here is spelled
out under `/home/mostufa.j`. What each stage runs, and with which settings, is
in [PROJECT_NOTES.md](PROJECT_NOTES.md#experiment-parameters).

## 1. Once per laptop: ssh key

LOCAL (asks the Tapuz password one last time):

```powershell
Get-Content $env:USERPROFILE\.ssh\id_ed25519.pub | ssh mostufa.j@tapuz14.cslcs.technion.ac.il "mkdir -p /home/mostufa.j/.ssh && chmod 700 /home/mostufa.j/.ssh && tr -d '\r' >> /home/mostufa.j/.ssh/authorized_keys && chmod 600 /home/mostufa.j/.ssh/authorized_keys"
ssh mostufa.j@tapuz14.cslcs.technion.ac.il hostname
```

The second line should print `tapuz14` without asking for a password. Without
the key everything still works; ssh just asks for the password each time.

## 2. Send the code (after every change)

LOCAL:

```powershell
cd C:\Users\mostufa.j\Desktop\Dan\hadoop\my_scripts
.\sync-cluster.ps1 push
```

Afterwards `/home/mostufa.j/my_scripts` on tapuz14 matches this folder, with
Linux line endings. Files deleted here are deleted there, and `results/` is
never touched. Push refuses while an experiment is running.

## 3. Before a run: clean and check the nodes

TAPUZ:

```bash
cd /home/mostufa.j/my_scripts/experiments/storage_virtualization_loopback
bash clean-scratch.sh
bash bootstrap-tapuz.sh
```

- `clean-scratch.sh` stops Hadoop, removes the loopback disks, and deletes
  your own leftovers in `/scratch` on all 5 nodes. It prints the free space
  before and after.
- In the `bootstrap-tapuz.sh` output, no line may say MISSING or FAIL. Every
  node should show `page cache: ok (64 MB test file: 64 MB in RAM after
  reading, 0 MB after evicting)`. Each worker needs at least 205 GB free on
  `/scratch` and 0 loopback mounts.

## 4. Run everything

TAPUZ:

```bash
screen -S exp
cd /home/mostufa.j/my_scripts/experiments/storage_virtualization_loopback
bash run-all.sh                  # ~13 h; "bash run-all.sh --reps 3" takes ~9 h
```

Press **Ctrl+A, then D** to detach. The run keeps going, and you can log out.

The first ~35 minutes are the smoke test, which ends with **READY** or **NOT
READY**. NOT READY stops the pipeline and prints the failed checks.

## 5. Reconnect to the run

LOCAL:

```powershell
ssh mostufa.j@tapuz14.cslcs.technion.ac.il
```

TAPUZ:

```bash
screen -r exp                    # back into the running experiment
screen -ls                       # list the sessions, if "screen -r" finds none
screen -d -r exp                 # if it says "Attached" (an old connection still holds it)
```

To leave again: **Ctrl+A, then D**. Don't press Ctrl+C in there unless you
want to stop the run.

To watch without entering screen:

```bash
tail -f /home/mostufa.j/my_scripts/results/pipeline_latest/pipeline.log      # Ctrl+C stops only tail
cat /home/mostufa.j/my_scripts/results/pipeline_latest/stages.env             # stages done so far
cat /home/mostufa.j/my_scripts/results/pipeline_latest/1_smoke/run_*/checks.txt   # smoke test result
```

## 6. Stop early, continue later

TAPUZ:

```bash
screen -r exp                    # then press Ctrl+C
cd /home/mostufa.j/my_scripts/experiments/storage_virtualization_loopback
bash stop-single-dn-cluster.sh 1024
```

Later, still in that folder (inside `screen -S exp`):

```bash
bash run-all.sh --from 2         # continue the latest pipeline at a stage (here 2)
bash run-all.sh --only 3         # or run a single stage
```

## 7. Get the results

LOCAL:

```powershell
cd C:\Users\mostufa.j\Desktop\Dan\hadoop\my_scripts
.\sync-cluster.ps1 pull
```

This copies the results to `hadoop\pipeline_<date>\` and writes
`FINAL_REPORT.md` there, with the figures in `figures\`. The report goes into
the newest pipeline or single-run folder that was copied. Pull also works
during a run, as a snapshot.

For any other single run folder:

```powershell
python experiments\storage_virtualization_loopback\final-report.py ..\storage_virtualization_loopback_tapuz\run_<date>
```

## 8. After the pipeline: the large-input run (100 GB, 16 MB blocks)

1. TAPUZ: check that the pipeline has finished. The last lines should say
   `Finished after ... min` and list each stage's status.

   ```bash
   tail -n 15 /home/mostufa.j/my_scripts/results/pipeline_latest/pipeline.log
   ```

2. LOCAL: fetch it. This writes `hadoop\pipeline_<date>\FINAL_REPORT.md`
   with the figures.

   ```powershell
   cd C:\Users\mostufa.j\Desktop\Dan\hadoop\my_scripts
   .\sync-cluster.ps1 pull
   ```

3. LOCAL: send the new code. `run-all.sh` now takes `--input-gb` and
   `--block-mb`. Push only works once nothing is running.

   ```powershell
   .\sync-cluster.ps1 push
   ```

4. TAPUZ: start the large pipeline. `screen -r exp` returns to the finished
   session and gives you a prompt; if that session is gone, use `screen -S exp`.

   ```bash
   screen -r exp
   cd /home/mostufa.j/my_scripts/experiments/storage_virtualization_loopback
   bash run-all.sh --input-gb 100 --block-mb 16 --reps 3
   ```

   Detach with **Ctrl+A, then D**. It is the same pipeline, with this input:

   - 75 GB of block files per worker can't stay in RAM, so every condition is
     cold: 1, 4 and 8 maps per node, plus the benchmark and both controls;
   - the smoke test keeps a 2 GB input (with 16 MB blocks), so the gate still
     comes after ~30 minutes;
   - it takes about 5.5 days with `--reps 3` and about 8 days with
     `--reps 5`. 1 map per node is the slow part, at about 3.4 h per job.

   To shorten it, start it as
   `MAIN_K_VALUES="1 256 1024" bash run-all.sh --input-gb 100 --block-mb 16 --reps 3`
   (about 4 days). To watch it:

   ```bash
   tail -f /home/mostufa.j/my_scripts/results/pipeline_latest/pipeline.log
   ```

   If you stop it, `bash run-all.sh --from <stage>` continues it with the
   same 100 GB input; there's no need to repeat `--input-gb`.

5. LOCAL, when it is done: `.\sync-cluster.ps1 pull`. The report and figures
   go into `hadoop\pipeline_<date>_100GB_16MB\`.

## 9. Add missing k values to a finished pipeline

Example: the 90 GB pipeline, whose k=1024 jobs failed before the block-size
fix. This measures k=1 and k=1024 again for the main run and both controls,
and adds them to that pipeline's report.

1. LOCAL: send the fixed code (only once nothing is running on Tapuz).

   ```powershell
   cd C:\Users\mostufa.j\Desktop\Dan\hadoop\my_scripts
   .\sync-cluster.ps1 push
   ```

2. TAPUZ: run the main run (2) and the two controls (4, 5) again, then the
   report (6). `--pipeline` names the folder in `results/`, and `--add`
   keeps the earlier runs.

   ```bash
   screen -r exp
   cd /home/mostufa.j/my_scripts/experiments/storage_virtualization_loopback
   MAIN_K_VALUES="1 1024" bash run-all.sh --pipeline pipeline_2026-09-28_08-37-09_90GB_32MB --only 2,4,5,6 --add
   ```

   - It uses the pipeline's own input (90 GB, 32 MB blocks) and 5
     repetitions, like the first run. It takes about 3 days; add `--reps 3`
     for about 2.
   - The benchmark (stage 3) is not repeated: its k=1024 was fine.
   - k=1 runs again on purpose. Each k is compared with k=1 from its own
     session, and the report's "Runs combined" section shows whether the
     cluster drifted since the first run.

3. LOCAL, when it is done: `.\sync-cluster.ps1 pull`. The report and figures
   of `hadoop\pipeline_2026-09-28_08-37-09_90GB_32MB\` then cover k = 1 to
   1024.

## 10. Single runs instead of the pipeline

TAPUZ, in the experiment folder, inside screen:

```bash
bash run-2x2.sh smoke                    # small 2x2, ~35 min, ends with READY / NOT READY
bash run-2x2.sh                          # full 2x2: k = 1 64 256 512 1024, 5 repetitions
bash storage-bench.sh                    # the storage stack alone, no Hadoop
K_VALUES="1 1024" bash run-2x2.sh 3      # any setting can be overridden like this
```

## CloudLab (c6620, when reserved)

On CloudLab `~` is the normal home. **NODE0** = node0 of the experiment, after
`ssh Mostufa@<node0 public name>` from PowerShell. The public names
(`er###.utah.cloudlab.us`) are in the experiment's List View.

1. **Website, once per laptop:** CloudLab only accepts registered ssh keys.
   Show this laptop's public key with the LOCAL command below, then paste it
   under *Manage SSH Keys* in the portal. Do this before starting the
   experiment, so that its nodes get the key.

   ```powershell
   Get-Content $env:USERPROFILE\.ssh\id_ed25519.pub
   ```

2. **Website: start the experiment** with the small-lan profile, or your own
   profile with the same settings:
   - number of nodes and hardware type exactly as in the reservation
     (`c6620` -- watch out, `c6220` is a different machine);
   - image `UBUNTU24-64-STD`;
   - Advanced: *Temp Filesystem Max Space* checked, mount point `/mydata`;
   - on the last page, the reservation's cluster (Cloudlab Utah) and project;
   - a duration that ends before the reservation does.

3. **LOCAL:** send the code to node0.

   ```powershell
   cd C:\Users\mostufa.j\Desktop\Dan\hadoop\my_scripts
   .\sync-cluster.ps1 push Mostufa@<node0 public name> -RemoteDir '~/my_scripts'
   ```

4. **NODE0:** set up all nodes. This installs Java 11, Hadoop 3.3.6 (the same
   as on Tapuz) and the tools, and links `/scratch` to `/mydata`. It takes
   about 5-10 minutes and is safe to run again.

   ```bash
   cd ~/my_scripts/experiments/storage_virtualization_loopback
   bash bootstrap-c6620.sh
   ```

   If its step 1 says FAIL (node0 cannot ssh to the other nodes), run this
   on LOCAL with the 5 public names, node0 first, then run the script again:

   ```powershell
   $nodes = 'er101', 'er099', 'er127', 'er113', 'er080'
   $pub = ssh "Mostufa@$($nodes[0]).utah.cloudlab.us" "test -f ~/.ssh/id_ed25519 || ssh-keygen -q -t ed25519 -N '' -f ~/.ssh/id_ed25519; cat ~/.ssh/id_ed25519.pub"
   foreach ($n in $nodes) { $pub | ssh "Mostufa@$n.utah.cloudlab.us" "tr -d '\r' >> ~/.ssh/authorized_keys" }
   ```

5. **NODE0:** run the pipeline, with the same settings as on Tapuz.

   ```bash
   screen -S exp
   cd ~/my_scripts/experiments/storage_virtualization_loopback
   bash run-all.sh
   ```

   Detach with Ctrl+A, then D. Watch it with
   `tail -f ~/my_scripts/results/pipeline_latest/pipeline.log`.

6. **LOCAL**, when it is done:

   ```powershell
   .\sync-cluster.ps1 pull Mostufa@<node0 public name> -RemoteDir '~/my_scripts'
   ```

## If something goes wrong

| What you see | What to do |
|---|---|
| push says "An experiment is running" | wait for it to finish, or stop it (section 6) |
| ssh asks for a password | install the key (section 1), or type it |
| the smoke test says NOT READY | send the FAIL lines from `1_smoke/run_*/checks.txt` |
| `page cache: FAIL` in bootstrap | send that line; don't start the run |
| your normal Hadoop cluster is down | expected during a run; `run-all.sh` restarts it at the end, with an empty (reformatted) HDFS |
