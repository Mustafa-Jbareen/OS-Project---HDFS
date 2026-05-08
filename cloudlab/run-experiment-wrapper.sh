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

NODES_FILE="${NODES_FILE:-$REPO_DIR/cloudlab/nodes.txt}"
export NODES_FILE
if [ -f "$NODES_FILE" ]; then
    echo "Using nodes file: $NODES_FILE"
else
    echo "nodes.txt not found; using script defaults"
fi

bash run-experiment-loopback-fs.sh "$@"
