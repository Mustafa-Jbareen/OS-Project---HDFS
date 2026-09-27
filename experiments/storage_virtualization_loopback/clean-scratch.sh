#!/bin/bash
################################################################################
# SCRIPT: clean-scratch.sh
# DESCRIPTION: Stops the experiment cluster, removes its loopback disks, and
#              deletes what earlier runs left in /scratch on every node:
#                $TMP_BASE/wordcount_*          input copies
#                $TMP_BASE/hadoop               Hadoop temp files
#                $TMP_BASE/hadoop_dn_logs|pids  DataNode logs and pid files
#                /scratch/yarn-local            YARN's cached jars, old map outputs
#                /scratch/yarn-logs             container logs of old jobs
#                $HADOOP_DATA_DIR               the experiment NameNode's metadata
#              Everything there is re-created by the next cluster start. Only
#              your own files are deleted: the folders themselves, lost+found
#              and other users' files stay. The normal cluster keeps its data
#              in /home/mostufa.j/hadoop_data and is not touched.
#              run-all.sh runs this as stage 0.
#
# RUN ON: master node (tapuz14). Will SSH to all peers.
#
# USAGE: bash clean-scratch.sh
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster.conf"

echo "=== Stopping Hadoop and removing loopback disks (stop-single-dn-cluster.sh) ==="
bash "$SCRIPT_DIR/stop-single-dn-cluster.sh" 1024 || true

echo ""
echo "=== Removing leftovers of earlier runs in $STORAGE_BASE ==="
for node in "${ALL_NODES[@]}"; do
    ssh "$node" "bash -s" -- "$STORAGE_BASE" "$TMP_BASE" "$HADOOP_DATA_DIR" <<'REMOTE' || echo "  $node: FAILED"
set -u
storage=$1 tmp=$2 data=$3
me=$(id -un)
for p in "$storage" "$tmp" "$data"; do
    case "$p" in /?*) ;; *) echo "  $(hostname): refusing to clean '$p'"; exit 1 ;; esac
done
free_gb() { df -BG --output=avail "$storage" | tail -1 | tr -dc '0-9'; }
# Delete the entries I own directly inside a folder; the folder itself stays.
clean() {
    [ -d "$1" ] || return 0
    find "$1" -mindepth 1 -maxdepth 1 -user "$me" -exec rm -rf {} + 2>/dev/null || true
}
before=$(free_gb)
find "$tmp" -maxdepth 1 -name 'wordcount_*' -user "$me" -delete 2>/dev/null || true
clean "$tmp/hadoop"
clean "$tmp/hadoop_dn_logs"
clean "$tmp/hadoop_dn_pids"
clean "$data"
note=""
if pgrep -u "$me" -f 'yarn.server.nodemanager.NodeManager' >/dev/null 2>&1; then
    note=" (a NodeManager is still running, so the YARN folders were left alone)"
else
    clean "$storage/yarn-local"
    clean "$storage/yarn-logs"
fi
echo "  $(hostname): ${before} GB -> $(free_gb) GB free on ${storage}${note}"
# Anything big that is still there did not come from this experiment, or is not yours.
left=$(du -sm "$tmp" 2>/dev/null | cut -f1)
if [ "${left:-0}" -gt 100 ]; then
    echo "    still in $tmp: ${left} MB (not from this experiment, or not yours):"
    du -sm "$tmp"/* 2>/dev/null | sort -rn | head -5 | awk '$1 >= 10 {printf "      %8d MB  %s\n", $1, $2}'
fi
REMOTE
done
echo "Done."
