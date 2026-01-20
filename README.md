# Hadoop Cluster Management Guide

This guide provides instructions for setting up and managing a multi-node Hadoop cluster using the provided scripts.

## Directory Overview

### Key Scripts
- **install-all-nodes.sh**: Installs Java, SSH, and Hadoop on all nodes in parallel.
- **setup-cluster-automated.sh**: Automates the entire cluster configuration, deployment, and startup process.
- **setup-ssh.sh**: Configures passwordless SSH between all nodes in the cluster.
- **clear-hdfs.sh**: Clears all HDFS data from NameNode and DataNode directories.
- **reformat-and-start.sh**: Reformats the NameNode and starts the Hadoop cluster.
- **restart-hdfs.sh**: Restarts HDFS services while preserving data.
- **stop-hdfs.sh**: Stops all Hadoop services (HDFS and YARN).

### Experiments Directory
- **common/**: Contains shared configuration and utility scripts for experiments.
  - `cluster.conf`: Cluster configuration file.
  - `utils.sh`: Utility functions for experiments.
- **wordcount/**: Contains scripts for running the WordCount experiment.
  - `analyze.py`: Analyzes WordCount experiment results.
  - `collect-results.sh`: Collects results from WordCount experiments.
  - `generate-input.sh`: Generates input data for WordCount experiments.
  - `run.sh`: Executes the WordCount experiment.

---

## Quick Start Guide

### 1. Prepare the Cluster Configuration File

On the **master node**, create the cluster configuration file at `/csl/mostufa.j/cluster` with the list of all nodes:

```bash
cat > /csl/mostufa.j/cluster << EOF
master-hostname
worker1-hostname
worker2-hostname
EOF
```

**Example:**
```bash
cat > /csl/mostufa.j/cluster << EOF
tapuz14
tapuz13
EOF
```

### 2. Run the Automated Setup

1. Run `install-all-nodes.sh` to install Java, SSH, and Hadoop on all nodes.
2. Run `setup-cluster-automated.sh` to configure and start the cluster.

### 3. Manage the Cluster

Use the following scripts for cluster management:

- `clear-hdfs.sh`: Clears all HDFS data.
- `reformat-and-start.sh`: Reformats the NameNode and starts the cluster.
- `restart-hdfs.sh`: Restarts HDFS services.
- `stop-hdfs.sh`: Stops all Hadoop services.

---

## Notes

- The local storage path `/home/mostufa.j/` is used for Hadoop installation and HDFS data directories.
- The shared storage path `/csl/mostufa.j/` is used for configuration files and SSH keys.
- Always verify the cluster configuration file (`/csl/mostufa.j/cluster`) before running any scripts.
