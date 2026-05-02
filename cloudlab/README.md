CloudLab setup notes for running the storage-virtualization experiment
============================================================

CRITICAL: CloudLab m400 nodes have 120 GB total disk (~50 GB available after OS).
This is MUCH less than what the original experiment assumes (220 GB loopback budget).

You MUST edit the experiment script to reduce loopback budget significantly BEFORE running.
See "Updating experiment parameters for m400" below.

Overview
- This folder contains helper scripts to provision CloudLab m400 (X-Gene) nodes
  and run the existing experiment repository on CloudLab. The scripts create
  `/mydata` with available disk space and bind it to `/scratch` (for compatibility),
  then install a local Hadoop build under `/mydata/hadoop`.

Files
- `setup-node.sh` : runs on each CloudLab node (installs packages, binds /local -> /scratch, installs Hadoop)
- `bootstrap.sh`   : run from your laptop (or a management host) to copy `setup-node.sh`, install cluster key, and run setup on all nodes
- `run-experiment-wrapper.sh`: wrapper to set CloudLab-friendly paths and invoke the existing `run-experiment-loopback-fs.sh`

**CRITICAL: Storage Constraint for m400**
Before running ANY experiment on CloudLab m400 nodes, you MUST update the loopback budget
in `experiments/storage_virtualization_loopback/run-experiment-loopback-fs.sh` from 220 GB to ~30 GB.

Edit the line (around line 50):
  OLD: `LOOPBACK_BUDGET_PER_NODE_GB=220`
  NEW: `LOOPBACK_BUDGET_PER_NODE_GB=30` (or lower, depending on OS footprint)

Also reduce K_VALUES to test fewer k values (e.g., `K_VALUES=(1 2 4 8 16 32 64)` instead of all the way to 1024)
to stay within the 30 GB budget.

High-level steps

1. Reserve a CloudLab profile with m400 nodes
   - Go to https://www.cloudlab.us/ → Experiments → Start Experiment
   - Search for "m400" or "ARM"; available at OneLab (45 nodes) or CloudLab Utah (45 nodes)
   - Choose number of nodes: 1 master + N workers (e.g., 1 master + 3 workers = 4 nodes total)
   - Create experiment and wait for nodes to boot (~2-5 min)

2. Get node hostnames from CloudLab portal
   - Once experiment is ready, click "List View" to see node names (e.g., node0, node1, etc.)

3. SSH to master and clone repo
   ```bash
   ssh <username>@<master-hostname>
   # Replace <username> with your CloudLab username (shown in Account → Settings)
   git clone <your-repo-url> ~/my_scripts
   cd ~/my_scripts
   ```

4. **CRITICAL: Update experiment script for m400 disk constraint**
   ```bash
   # Edit this file and change LOOPBACK_BUDGET_PER_NODE_GB from 220 to 30
   nano experiments/storage_virtualization_loopback/run-experiment-loopback-fs.sh
   # Also reduce K_VALUES if desired for faster testing
   ```

5. Create CloudLab nodes.txt listing all node hostnames (first line = master)
   ```bash
   cat > cloudlab/nodes.txt <<'EOF'
   node0
   node1
   node2
   node3
   EOF
   # Match the order and names exactly as shown in CloudLab List View
   ```

6. From your workstation, run bootstrap to prepare all nodes
   ```bash
   # Assumes you can SSH into CloudLab nodes from your workstation
   bash cloudlab/bootstrap.sh cloudlab/nodes.txt
   # Enter password if needed; bootstrap will set up cluster SSH keys
   ```

7. Verify node setup on master
   ```bash
   ssh <username>@<master-hostname>
   # Check /mydata mount:
   df -h /scratch
   # Check Hadoop:
   /scratch/hadoop/hadoop-3.3.1/bin/hadoop version
   # Check cluster SSH (passwordless access):
   while read n; do ssh -o BatchMode=yes $n hostname; done < ~/my_scripts/cloudlab/nodes.txt
   ```

8. Run the experiment on master
   ```bash
   ssh <username>@<master-hostname>
   cd ~/my_scripts
   # Example: 3 repetitions per k (keep low for quick testing on small disks)
   bash cloudlab/run-experiment-wrapper.sh 3
   ```

9. Monitor progress (on master in another terminal)
   ```bash
   tail -f /scratch/results/storage_virtualization_loopback/latest/experiment.log
   ```

Important notes
- The scripts create `/mydata` using `mkextrafs.pl` with available disk space (~50 GB on m400).
- `/mydata` is bound to `/scratch` (via `mount --bind`) so existing experiment scripts that use `/scratch` continue to work.
- **Disk Constraint**: m400 has only 120 GB total. After OS (~64 GB), ~50 GB remains. 
  Loopback filesystem budget MUST be reduced from default 220 GB to ~30 GB or less.
- The bootstrap creates a cluster SSH key (`~/.ssh/cloudlab_cluster_id_rsa`) for inter-node SSH.

SSH / Key Management
- CloudLab accounts use public key authentication set during account creation (not password-based).
- The cluster key is created for convenience (inter-node SSH); keep it private and remove after experiments if desired.
- Bootstrap will use your CloudLab public key for initial access, then add the cluster key for inter-node communication.

Filesystem Layout on CloudLab m400
- Total disk: 120 GB (Micron M500 SSD)
- OS partition: 64 GB (newer images)
- Available after OS + system: ~50 GB in `/mydata` (via mkextrafs.pl)
- Experiment data (all under `/scratch` via mount bind):
  - Hadoop: `/scratch/hadoop/hadoop-3.3.1`
  - HDFS data: `/scratch/hadoop_data`
  - Loopback FSes: `/scratch/hdfs_loop`
  - Results: `/scratch/results`
