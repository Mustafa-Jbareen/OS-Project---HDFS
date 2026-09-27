#!/bin/bash
################################################################################
# SCRIPT: bootstrap-tapuz.sh
# DESCRIPTION: One-time pre-flight for the tapuz HDD cluster.
#              - Verifies /scratch exists and we can sudo mkdir/chmod inside it
#              - Verifies passwordless SSH between nodes
#              - Reports presence of iostat, filefrag, bc, java, hadoop
#              - Verifies the specific NOPASSWD sudo commands the experiment
#                uses (mkfs.ext4, mount, umount, fallocate, losetup, mkdir,
#                chmod, rmdir, rm)
#
# RUN ON: master node (tapuz14). Will SSH to all peers.
#
# USAGE: bash bootstrap-tapuz.sh
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CLUSTER=tapuz
source "$SCRIPT_DIR/cluster.conf"

echo "============================================================"
echo "Bootstrapping tapuz cluster"
echo "  Master:  $MASTER_NODE"
echo "  Nodes:   ${ALL_NODES[*]}"
echo "  Storage: $STORAGE_BASE  (Hadoop: $HADOOP_HOME)"
echo "============================================================"

# ---- Step 1: verify $STORAGE_BASE exists and sudo mkdir/chmod work ---------
# /scratch is root-owned on tapuzes. The experiment uses `sudo mkdir`/`sudo chmod`
# (both NOPASSWD on tapuz) to create $IMAGE_DIR / $MOUNT_BASE / $HADOOP_DATA_DIR
# at runtime, so we don't need /scratch itself to be user-writable. We only
# verify the same sudo path the experiment will take actually works.
echo ""
echo "=== STEP 1: Checking $STORAGE_BASE + sudo mkdir/chmod on each node ==="
ALL_OK=1
for node in "${ALL_NODES[@]}"; do
    echo "--- $node ---"
    ssh -o StrictHostKeyChecking=accept-new "$node" "STORAGE_BASE='$STORAGE_BASE' bash -s" <<'REMOTE' || ALL_OK=0
set -e
mp="$STORAGE_BASE"
if [ ! -d "$mp" ]; then
    echo "  ERROR: $mp does not exist"
    exit 1
fi
testdir="$mp/.bootstrap_test_$$"
if ! sudo -n /bin/mkdir -p "$testdir" 2>/dev/null; then
    echo "  ERROR: sudo -n mkdir $testdir failed (passwordless sudo for /bin/mkdir not granted?)"
    exit 1
fi
if ! sudo -n /bin/chmod 777 "$testdir" 2>/dev/null; then
    echo "  ERROR: sudo -n chmod 777 $testdir failed"
    sudo -n /bin/rmdir "$testdir" 2>/dev/null || true
    exit 1
fi
sudo -n /bin/rmdir "$testdir" 2>/dev/null || true
df -h "$mp" | tail -1 | awk '{printf "  %s available on %s (mount %s, used %s)\n", $4, $1, $6, $5}'
REMOTE
done

# ---- Step 2: verify passwordless SSH ---------------------------------------
echo ""
echo "=== STEP 2: Verifying passwordless SSH from $(hostname) to all nodes ==="
SSH_OK=1
for node in "${ALL_NODES[@]}"; do
    if ssh -o BatchMode=yes -o ConnectTimeout=5 "$node" "echo ok" >/dev/null 2>&1; then
        echo "  $node: ok"
    else
        echo "  $node: FAIL (passwordless SSH not working)"
        SSH_OK=0
    fi
done
if [[ "$SSH_OK" != "1" ]]; then
    echo "  Fix SSH first. NFS-shared \$HOME means adding pubkey to"
    echo "  ~/.ssh/authorized_keys on tapuz14 propagates to every node."
fi

# ---- Step 3: report tooling presence ---------------------------------------
echo ""
echo "=== STEP 3: Tooling check (monitors, filefrag, fincore, python3, bc, java, hadoop) ==="
for node in "${ALL_NODES[@]}"; do
    echo "--- $node ---"
    ssh "$node" "HADOOP_HOME='$HADOOP_HOME' bash -s" <<'REMOTE'
check() { command -v "$1" >/dev/null 2>&1 && echo "  $1: $(command -v "$1")" || echo "  $1: MISSING"; }
check iostat
check pidstat
check mpstat
check vmstat
check filefrag
check fincore
check losetup
check python3
check curl
check bc
check java
check sudo
if [ -d "$HADOOP_HOME" ]; then
    echo "  hadoop: present at $HADOOP_HOME"
else
    echo "  hadoop: MISSING at $HADOOP_HOME"
fi
REMOTE
done

# ---- Step 4: passwordless-sudo coverage for the commands we actually use ---
echo ""
echo "=== STEP 4: Verifying NOPASSWD sudo for required commands ==="
REQUIRED_CMDS=(/sbin/mkfs.ext4 /bin/mount /bin/umount /usr/bin/fallocate /sbin/losetup /bin/mkdir /bin/chmod /bin/rmdir /bin/rm)
SUDO_OK=1
for node in "${ALL_NODES[@]}"; do
    echo "--- $node ---"
    # Flatten multi-line NOPASSWD section into one comma-separated list,
    # then split on commas so each command is its own token. Exact-match
    # against REQUIRED_CMDS avoids false positives like /bin/rm matching /bin/rmdir.
    rules=$(ssh "$node" "sudo -n -l 2>/dev/null" || echo "")
    nopasswd_line=$(echo "$rules" | tr '\n' ' ' | grep -oE 'NOPASSWD:[^()]*' | head -1 || echo "")
    declare -a granted=()
    IFS=',' read -ra parts <<<"${nopasswd_line#NOPASSWD:}"
    for part in "${parts[@]}"; do
        # Trim whitespace; take the executable path only (drop any args)
        trimmed=$(echo "$part" | awk '{print $1}')
        [[ -n "$trimmed" ]] && granted+=("$trimmed")
    done
    for cmd in "${REQUIRED_CMDS[@]}"; do
        found=0
        for g in "${granted[@]}"; do
            [[ "$g" == "$cmd" ]] && { found=1; break; }
        done
        if (( found )); then
            echo "  $cmd: ok"
        else
            echo "  $cmd: MISSING (not in NOPASSWD list)"
            SUDO_OK=0
        fi
    done
    unset granted
done
if [[ "$SUDO_OK" != "1" ]]; then
    echo "  Ask the lab admin for: NOPASSWD: ${REQUIRED_CMDS[*]}"
fi

echo ""
echo "============================================================"
echo "Bootstrap done. Resolve any 'MISSING' / 'FAIL' lines above"
echo "before running the experiment."
echo "============================================================"
