#!/bin/bash
################################################################################
# SCRIPT: bootstrap-tapuz.sh
# DESCRIPTION: One-time setup for the tapuz HDD cluster.
#              - Verifies /scratch is writable on every node
#              - Verifies passwordless SSH between nodes
#              - Reports presence of iostat, filefrag, bc, java, hadoop
#              - Does NOT auto-install packages (tapuzes are managed; ask the
#                lab admin if anything is missing).
#
# RUN ON: master node (tapuz14). Will SSH to all peers.
#
# USAGE: bash bootstrap-tapuz.sh
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster.conf"

echo "============================================================"
echo "Bootstrapping tapuz cluster"
echo "  Master:  $MASTER_NODE"
echo "  Nodes:   ${ALL_NODES[*]}"
echo "  Storage: $STORAGE_BASE  (Hadoop: $HADOOP_HOME)"
echo "============================================================"

# ---- Step 1: verify /scratch is usable on every node ------------------------
echo ""
echo "=== STEP 1: Checking $STORAGE_BASE is writable on each node ==="
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
testfile="$mp/.bootstrap_test_$$"
if ! touch "$testfile" 2>/dev/null; then
    echo "  ERROR: $mp not writable by $USER"
    exit 1
fi
rm -f "$testfile"
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
    echo "  Fix SSH first. On tapuzes /home is NFS-shared, so adding"
    echo "  ~/.ssh/id_*.pub to ~/.ssh/authorized_keys on tapuz14 should"
    echo "  propagate to every node automatically."
fi

# ---- Step 3: report tooling presence ---------------------------------------
echo ""
echo "=== STEP 3: Tooling check (iostat, filefrag, bc, java, hadoop) ==="
for node in "${ALL_NODES[@]}"; do
    echo "--- $node ---"
    ssh "$node" "HADOOP_HOME='$HADOOP_HOME' bash -s" <<'REMOTE'
check() { command -v "$1" >/dev/null 2>&1 && echo "  $1: $(command -v "$1")" || echo "  $1: MISSING"; }
check iostat
check filefrag
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

# ---- Step 4: sudo-without-password check (needed for loopback mounts) -------
echo ""
echo "=== STEP 4: Passwordless sudo check (needed for losetup/mount) ==="
SUDO_OK=1
for node in "${ALL_NODES[@]}"; do
    if ssh "$node" "sudo -n true" >/dev/null 2>&1; then
        echo "  $node: passwordless sudo ok"
    else
        echo "  $node: FAIL — sudo requires a password"
        SUDO_OK=0
    fi
done
if [[ "$SUDO_OK" != "1" ]]; then
    echo "  Loopback FS setup uses sudo losetup/mount/mkfs. Without"
    echo "  passwordless sudo the start-/stop-cluster scripts will hang."
fi

echo ""
echo "============================================================"
echo "Bootstrap done. Resolve any 'MISSING' / 'FAIL' lines above"
echo "before running the experiment."
echo "============================================================"
