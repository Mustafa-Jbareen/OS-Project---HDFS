#!/bin/bash
set -euo pipefail

# Wrapper to run the existing experiment on CloudLab nodes.
# Assumes repository is cloned to ~/my_scripts on the master node.
#
# Uses /scratch paths (which are bound to /mydata on CloudLab m400 nodes).

REPO_DIR="${REPO_DIR:-$HOME/my_scripts}"
EXP_DIR="$REPO_DIR/experiments/storage_virtualization_loopback"

export RESULTS_BASE="${RESULTS_BASE:-/scratch/results/storage_virtualization_loopback}"
export HADOOP_HOME="${HADOOP_HOME:-/scratch/hadoop/hadoop-3.3.1}"
export CONFIG_DIR="${CONFIG_DIR:-/scratch/tmp/hadoop_single_dn_k_dirs}"

echo "Using REPO_DIR=$REPO_DIR"
echo "RESULTS_BASE=$RESULTS_BASE"
echo "HADOOP_HOME=$HADOOP_HOME"
echo "CONFIG_DIR=$CONFIG_DIR"

if [ ! -d "$EXP_DIR" ]; then
    echo "Experiment directory not found: $EXP_DIR"
    echo "Please clone your repo into ~/my_scripts and retry."
    exit 1
fi

cd "$EXP_DIR"

echo "Invoking run-experiment-loopback-fs.sh with args: $*"

# If a nodes file exists at cloudlab/nodes.txt, patch the experiment script's
# MASTER_NODE and ALL_NODES definitions to match the CloudLab hostnames.
NODES_FILE="$REPO_DIR/cloudlab/nodes.txt"
if [ -f "$NODES_FILE" ]; then
    echo "Found nodes file: $NODES_FILE — patching experiment script hostnames"
    mapfile -t NODES < "$NODES_FILE"
    if [ ${#NODES[@]} -ge 1 ]; then
        MASTER_HOST=${NODES[0]}
        # Build replacement ALL_NODES line: ALL_NODES=("host1" "host2" ...)
        ALL_NODES_LINE="ALL_NODES=($(printf '"%s" ' "${NODES[@]}"))"

        # Replace MASTER_NODE line if present
        if grep -q "^MASTER_NODE=" run-experiment-loopback-fs.sh; then
            sed -i "s/^MASTER_NODE=.*/MASTER_NODE=\"$MASTER_HOST\"/" run-experiment-loopback-fs.sh
        fi

        # Replace ALL_NODES line if present
        if grep -q "^ALL_NODES=(" run-experiment-loopback-fs.sh; then
            # escape slashes & ampersands for sed
            esc=$(printf '%s' "$ALL_NODES_LINE" | sed -e 's/[\/&]/\\&/g')
            sed -i "s/^ALL_NODES=.*/$esc/" run-experiment-loopback-fs.sh
        fi
        echo "Patched MASTER_NODE=$MASTER_HOST and ALL_NODES to: ${NODES[*]}"
    else
        echo "nodes.txt is empty — skipping hostname patch"
    fi
fi

bash run-experiment-loopback-fs.sh "$@"
