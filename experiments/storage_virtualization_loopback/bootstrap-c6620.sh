#!/bin/bash
################################################################################
# SCRIPT: bootstrap-c6620.sh
# DESCRIPTION: One-time setup for a fresh c6620 reservation.
#              - Symlinks /scratch -> /mydata on every node
#              - Installs sysstat (iostat) and e2fsprogs (filefrag)
#              - Verifies passwordless SSH between nodes
#              - Reports what is still missing (Hadoop, Java) so you can
#                install those once and rsync to all workers.
#
# RUN ON: master node (er101). Will SSH to all peers.
#
# USAGE: bash bootstrap-c6620.sh
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster.conf"

echo "============================================================"
echo "Bootstrapping c6620 cluster"
echo "  Master:  $MASTER_NODE"
echo "  Nodes:   ${ALL_NODES[*]}"
echo "  /scratch -> /mydata symlink will be created on each node"
echo "============================================================"

# ---- Step 1: symlink /scratch -> /mydata on every node ----------------------
echo ""
echo "=== STEP 1: /scratch -> /mydata symlink on all nodes ==="
for node in "${ALL_NODES[@]}"; do
    echo "--- $node ---"
    ssh -o StrictHostKeyChecking=accept-new "$node" "bash -s" <<'REMOTE'
set -e
if [ ! -d /mydata ]; then
    echo "  ERROR: /mydata does not exist on this node"
    exit 1
fi
sudo chmod 777 /mydata
if [ -L /scratch ]; then
    target=$(readlink /scratch)
    if [ "$target" = "/mydata" ]; then
        echo "  /scratch -> /mydata already in place"
        exit 0
    fi
    echo "  /scratch is a symlink to $target; replacing with /mydata"
    sudo rm /scratch
elif [ -e /scratch ]; then
    echo "  ERROR: /scratch exists and is not a symlink. Refusing to clobber."
    exit 1
fi
sudo ln -s /mydata /scratch
echo "  /scratch -> $(readlink /scratch)"
REMOTE
done

# ---- Step 2: install required packages --------------------------------------
echo ""
echo "=== STEP 2: Installing sysstat (iostat) + e2fsprogs (filefrag) ==="
for node in "${ALL_NODES[@]}"; do
    echo "--- $node ---"
    ssh "$node" "bash -s" <<'REMOTE'
set -e
need=()
command -v iostat >/dev/null 2>&1 || need+=(sysstat)
command -v filefrag >/dev/null 2>&1 || need+=(e2fsprogs)
command -v bc >/dev/null 2>&1 || need+=(bc)
if (( ${#need[@]} > 0 )); then
    echo "  Installing: ${need[*]}"
    sudo apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${need[@]}"
else
    echo "  iostat, filefrag, bc already present"
fi
REMOTE
done

# ---- Step 3: verify passwordless SSH ---------------------------------------
echo ""
echo "=== STEP 3: Verifying SSH between master and workers ==="
ALL_OK=1
for node in "${ALL_NODES[@]}"; do
    if ssh -o BatchMode=yes -o ConnectTimeout=5 "$node" "echo ok" >/dev/null 2>&1; then
        echo "  $node: ok"
    else
        echo "  $node: FAIL (passwordless SSH not working)"
        ALL_OK=0
    fi
done
[[ "$ALL_OK" != "1" ]] && {
    echo "Fix SSH first; on cloudlab generate a key and copy to ~/.ssh/authorized_keys."
}

# ---- Step 4: report what still needs to be installed ------------------------
echo ""
echo "=== STEP 4: Hadoop + Java check ==="
for node in "${ALL_NODES[@]}"; do
    echo "--- $node ---"
    ssh "$node" "bash -s" <<REMOTE
java_ver=\$(java -version 2>&1 | head -1 || echo 'NOT INSTALLED')
echo "  java: \$java_ver"
if [ -d "$HADOOP_HOME" ]; then
    echo "  hadoop: present at $HADOOP_HOME"
else
    echo "  hadoop: MISSING at $HADOOP_HOME"
fi
REMOTE
done

echo ""
echo "============================================================"
echo "Bootstrap done. If java or hadoop is missing, install them"
echo "on master and rsync to /scratch/hadoop on every node."
echo "============================================================"
