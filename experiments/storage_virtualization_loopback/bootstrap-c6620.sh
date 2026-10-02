#!/bin/bash
################################################################################
# SCRIPT: bootstrap-c6620.sh
# DESCRIPTION: One-time setup of a fresh CloudLab c6620 experiment, so it runs
#              the same software as tapuz (Java 11, Hadoop 3.3.6):
#              1. checks passwordless SSH from the master to every node
#              2. /scratch -> /mydata symlink on every node (/mydata = the
#                 profile's temporary filesystem on the local disk)
#              3. installs Java 11, sysstat, e2fsprogs, bc, python3, curl
#              4. installs Hadoop (the version in clusters/c6620.conf) under
#                 /scratch/hadoop on every node -- downloaded once on the
#                 master -- and sets JAVA_HOME in its hadoop-env.sh
#              Steps 2-4 run on all nodes in parallel. Safe to run again:
#              every step skips what is already done.
#
# RUN ON: master node (node0). Will SSH to all peers.
#
# USAGE: bash bootstrap-c6620.sh
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CLUSTER=c6620
source "$SCRIPT_DIR/cluster.conf"
HADOOP_VERSION=${HADOOP_HOME##*/hadoop-}
TGZ_NAME="hadoop-$HADOOP_VERSION.tar.gz"

echo "============================================================"
echo "Bootstrapping c6620 cluster"
echo "  Master:  $MASTER_NODE"
echo "  Nodes:   ${ALL_NODES[*]}"
echo "  Hadoop:  $HADOOP_VERSION at $HADOOP_HOME (Java 11)"
echo "============================================================"

# Run a bash script on every node in parallel; print the last line of each
# node's output; return 1 if any node failed.
run_on_all() {
    local script=$1 node rc=0
    local -A pids=()
    for node in "${ALL_NODES[@]}"; do
        ssh "$node" "bash -s" <<< "$script" > "/tmp/bootstrap-c6620_$node.log" 2>&1 &
        pids[$node]=$!
    done
    for node in "${ALL_NODES[@]}"; do
        if wait "${pids[$node]}"; then
            echo "  $node: $(tail -n 1 "/tmp/bootstrap-c6620_$node.log")"
        else
            echo "  $node: FAILED -- last lines of /tmp/bootstrap-c6620_$node.log:"
            tail -n 5 "/tmp/bootstrap-c6620_$node.log" | sed 's/^/      /'
            rc=1
        fi
    done
    return $rc
}

# ---- Step 1: passwordless SSH ----------------------------------------------
echo ""
echo "=== STEP 1: passwordless SSH from $(hostname -s) to every node ==="
SSH_OK=1
for node in "${ALL_NODES[@]}"; do
    if ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$node" true 2>/dev/null; then
        echo "  $node: ok"
    else
        echo "  $node: FAIL"
        SSH_OK=0
    fi
done
if (( ! SSH_OK )); then
    echo "  The master must reach every node without a password: add its public"
    echo "  key to every node (COMMANDS.md, CloudLab section), then run this again."
    exit 1
fi

# ---- Step 2: /scratch -> /mydata -------------------------------------------
echo ""
echo "=== STEP 2: /scratch -> /mydata on every node ==="
read -r -d '' SCRATCH_SCRIPT <<'REMOTE' || true
set -e
if [ ! -d /mydata ]; then
    echo "ERROR: /mydata does not exist (the profile needs a temporary filesystem at /mydata)"
    exit 1
fi
sudo chmod 777 /mydata
if [ -L /scratch ]; then
    if [ "$(readlink /scratch)" = "/mydata" ]; then
        echo "/scratch -> /mydata already ($(df -BG --output=avail /mydata | tail -1 | tr -d ' ') free)"
        exit 0
    fi
    sudo rm /scratch
elif [ -e /scratch ]; then
    echo "ERROR: /scratch exists and is not a symlink; refusing to replace it"
    exit 1
fi
sudo ln -s /mydata /scratch
echo "/scratch -> /mydata ($(df -BG --output=avail /mydata | tail -1 | tr -d ' ') free)"
REMOTE
run_on_all "$SCRATCH_SCRIPT"

# ---- Step 3: Java 11 and tools ---------------------------------------------
echo ""
echo "=== STEP 3: Java 11, sysstat, e2fsprogs, bc, python3, curl on every node ==="
read -r -d '' PKG_SCRIPT <<'REMOTE' || true
set -e
need=()
ls -d /usr/lib/jvm/java-11-openjdk-* >/dev/null 2>&1 || need+=(openjdk-11-jdk-headless)
command -v iostat >/dev/null 2>&1 || need+=(sysstat)
command -v filefrag >/dev/null 2>&1 || need+=(e2fsprogs)
command -v bc >/dev/null 2>&1 || need+=(bc)
command -v python3 >/dev/null 2>&1 || need+=(python3)
command -v curl >/dev/null 2>&1 || need+=(curl)
if (( ${#need[@]} > 0 )); then
    # A fresh node's automatic updates may hold the apt lock for a while: wait for it.
    sudo apt-get -o DPkg::Lock::Timeout=900 update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=900 install -y -qq "${need[@]}" > /dev/null
    echo "installed: ${need[*]}"
else
    echo "everything already installed"
fi
REMOTE
run_on_all "$PKG_SCRIPT"

# ---- Step 4: Hadoop ---------------------------------------------------------
echo ""
echo "=== STEP 4: Hadoop $HADOOP_VERSION on every node ==="
mkdir -p /scratch/hadoop
TGZ="/scratch/hadoop/$TGZ_NAME"
if [[ ! -d "$HADOOP_HOME" && ! -s "$TGZ" ]]; then
    for url in "https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/$TGZ_NAME" \
               "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/$TGZ_NAME"; do
        echo "  downloading $url"
        if curl -fsSL --retry 3 -o "$TGZ.part" "$url"; then
            mv "$TGZ.part" "$TGZ"
            break
        fi
    done
    [[ -s "$TGZ" ]] || { echo "  ERROR: could not download $TGZ_NAME"; exit 1; }
fi
# Copy the tarball to every other node that does not have Hadoop yet.
declare -A CP_PIDS=()
for node in "${ALL_NODES[@]}"; do
    [[ "$node" == "$MASTER_NODE" ]] && continue
    if ! ssh "$node" "test -d '$HADOOP_HOME' || test -s '$TGZ'"; then
        { ssh "$node" "mkdir -p /scratch/hadoop" && scp -q "$TGZ" "$node:$TGZ"; } &
        CP_PIDS[$node]=$!
    fi
done
for node in "${!CP_PIDS[@]}"; do
    wait "${CP_PIDS[$node]}" || { echo "  ERROR: copying $TGZ_NAME to $node failed"; exit 1; }
done
read -r -d '' HADOOP_SCRIPT <<'REMOTE' || true
set -e
mkdir -p /scratch/hadoop
if [ ! -d "$HADOOP_HOME" ]; then
    tar -xzf "/scratch/hadoop/$TGZ_NAME" -C /scratch/hadoop
fi
jh=$(ls -d /usr/lib/jvm/java-11-openjdk-* 2>/dev/null | head -1)
[ -n "$jh" ] || { echo "ERROR: Java 11 not installed"; exit 1; }
envf="$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
grep -qx "export JAVA_HOME=$jh" "$envf" || echo "export JAVA_HOME=$jh" >> "$envf"
hv=$("$HADOOP_HOME/bin/hadoop" version 2>/dev/null | head -1)
jv=$("$jh/bin/java" -version 2>&1 | head -1)
[ -n "$hv" ] || { echo "ERROR: $HADOOP_HOME/bin/hadoop does not run"; exit 1; }
echo "$hv; $jv"
REMOTE
run_on_all "HADOOP_HOME='$HADOOP_HOME'
TGZ_NAME='$TGZ_NAME'
$HADOOP_SCRIPT"

echo ""
echo "============================================================"
echo "Bootstrap done. Next: bash run-all.sh (inside screen)."
echo "============================================================"
