# CloudLab m400 Experiment Checklist

**READ THIS BEFORE RUNNING THE EXPERIMENT**

## ⚠️ CRITICAL: Storage Constraint

CloudLab m400 nodes have **120 GB total disk** (~50 GB available after OS boot).  
The experiment's default loopback budget is **220 GB**, which is **impossible on m400**.

### ✋ STOP: You MUST do this FIRST

**Edit `experiments/storage_virtualization_loopback/run-experiment-loopback-fs.sh`:**

Find line ~50:
```bash
LOOPBACK_BUDGET_PER_NODE_GB=220
```

Change it to:
```bash
LOOPBACK_BUDGET_PER_NODE_GB=30
```

Also, find the line with K_VALUES (around line ~60) and reduce it for faster testing on constrained m400 nodes:

Instead of:
```bash
K_VALUES=(1 2 4 8 16 32 64 128 256 512 1024)
```

Use:
```bash
K_VALUES=(1 2 4 8 16 32 64)
```

**Without these changes, the experiment will fail with "No space left on device" errors.**

---

## Checklist

- [ ] **Edit `run-experiment-loopback-fs.sh` to reduce `LOOPBACK_BUDGET_PER_NODE_GB` to 30**
- [ ] Reserve CloudLab m400 nodes (at least 4 nodes: 1 master + 3 workers recommended)
- [ ] SSH to master and clone repo: `git clone <repo> ~/my_scripts`
- [ ] Verify experiment script changes took effect: `grep LOOPBACK_BUDGET_PER_NODE_GB ~/my_scripts/experiments/storage_virtualization_loopback/run-experiment-loopback-fs.sh`
- [ ] Create `cloudlab/nodes.txt` with all node hostnames (first = master)
- [ ] Run bootstrap from your workstation: `bash cloudlab/bootstrap.sh cloudlab/nodes.txt`
- [ ] Verify setup on master:
  ```bash
  df -h /scratch                                    # Should show ~40-50 GB available
  /scratch/hadoop/hadoop-3.3.1/bin/hadoop version # Hadoop should run
  ```
- [ ] Check passwordless SSH works:
  ```bash
  while read n; do ssh -o BatchMode=yes $n hostname; done < ~/my_scripts/cloudlab/nodes.txt
  ```
- [ ] Run experiment: `bash ~/my_scripts/cloudlab/run-experiment-wrapper.sh 3`
- [ ] Monitor: `tail -f /scratch/results/storage_virtualization_loopback/latest/experiment.log`

---

## Expected Behavior

On m400 with 30 GB loopback budget and K_VALUES=(1 2 4 8 16 32 64):
- k=1: Single loopback filesystem per DataNode, ~30 GB used
- k=2: Two loopback filesystems per DataNode, ~15 GB each
- k=4, 8, ..., 64: Progressively smaller images to split the budget
- Storage directories scale as: k * num_datanodes (e.g., k=64, 4 nodes = 256 total directories)

Experiment should run without "No space left on device" errors.

---

## Troubleshooting

**Error: `No space left on device` during cluster setup or WordCount**
- Loopback budget is still too high for available space, OR
- You didn't edit `run-experiment-loopback-fs.sh` before running
- Check: `df -h /scratch` on a DataNode; ensure > 5 GB free
- **Solution**: Reduce `LOOPBACK_BUDGET_PER_NODE_GB` to 20 or lower, restart experiment

**Error: `HADOOP_HOME not found` or `hdfs` command not found**
- Check: `/scratch/hadoop/hadoop-3.3.1/bin/hadoop version` works
- If not, verify `setup-node.sh` completed successfully on all nodes
- Check: `echo $HADOOP_HOME` on master (should be `/scratch/hadoop/hadoop-3.3.1`)
- **Solution**: Re-run bootstrap or manually verify Hadoop install on each node

**Error: `SSH permission denied` or `Cannot connect to worker nodes`**
- Cluster SSH key may not be in authorized_keys
- Check: `ssh -o BatchMode=yes <worker-node> hostname` from master
- **Solution**: Re-run bootstrap with the correct nodes.txt, or manually copy `~/.ssh/cloudlab_cluster_id_rsa.pub` to each node's `~/.ssh/authorized_keys`

**Experiment takes very long or nodes are slow**
- m400 is a slower ARM platform; this is expected
- Consider reducing K_REPS (repetitions) from default 5 to 2-3
- Reduce K_VALUES further if needed for quick testing

---

## Files Modified

- `experiments/storage_virtualization_loopback/run-experiment-loopback-fs.sh`: Reduced LOOPBACK_BUDGET_PER_NODE_GB and K_VALUES
- `cloudlab/setup-node.sh`: Uses mkextrafs.pl and /mydata (not /local)
- `cloudlab/bootstrap.sh`: Unchanged (works as-is)
- `cloudlab/run-experiment-wrapper.sh`: Uses /scratch paths
- `cloudlab/README.md`: Updated with CloudLab-specific instructions

---

## More Info

See `cloudlab/README.md` for full setup and usage instructions.
