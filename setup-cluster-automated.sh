#!/bin/bash
################################################################################
# SCRIPT: setup-cluster-automated.sh
# DESCRIPTION: Fully automated Hadoop cluster setup with master running DataNode
# USAGE: bash setup-cluster-automated.sh
# PREREQUISITES:
#   - Run on master node only
#   - SSH passwordless access configured to all worker nodes
#   - Hadoop installed on all nodes
################################################################################

set -euo pipefail

# ============================================================================ 
# CONFIGURATION
# ============================================================================ 
MASTER_NODE="tapuz14"
WORKER_NODES=("tapuz13")   # master included automatically
REPLICATION_FACTOR=2
NAMENODE_PORT=9000
HADOOP_HOME="/home/mostufa.j/hadoop"
HADOOP_DATA_DIR="/home/mostufa.j/hadoop_data"

# Export environment variables for subshells
export HADOOP_HOME
export HADOOP_DATA_DIR

# ============================================================================ 
# COLORS AND LOGGING
# ============================================================================ 
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

# ============================================================================ 
# FUNCTIONS
# ============================================================================ 
validate_prerequisites() {
    log_info "Validating prerequisites..."
    if [[ $(hostname) != "$MASTER_NODE" ]]; then
        log_error "Run this script on master node only ($MASTER_NODE)"
        exit 1
    fi

    if [ ! -d "$HADOOP_HOME" ]; then
        log_error "Hadoop not found at $HADOOP_HOME"
        exit 1
    fi
    log_success "Hadoop found at $HADOOP_HOME"

    log_info "Checking SSH connectivity and hostname resolution to workers..."
    for node in "${WORKER_NODES[@]}"; do
        if ! ssh -o ConnectTimeout=5 "$node" "echo OK" &>/dev/null; then
            log_error "Cannot SSH to $node"
            exit 1
        fi
        if ! ping -c1 -W1 "$node" &>/dev/null; then
            log_warning "Node $node not reachable by hostname, consider adding to /etc/hosts"
        fi
    done
    log_success "SSH connectivity verified"
}

call_config_generators() {
    log_info "Generating Hadoop configuration files..."
    bash "$(dirname "$0")/generate-core-site-xml.sh" "$HADOOP_HOME/etc/hadoop"
    bash "$(dirname "$0")/generate-hdfs-site-xml.sh" "$HADOOP_HOME/etc/hadoop"
    bash "$(dirname "$0")/generate-yarn-site-xml.sh" "$HADOOP_HOME/etc/hadoop"
    bash "$(dirname "$0")/generate-mapred-site-xml.sh" "$HADOOP_HOME/etc/hadoop"
    bash "$(dirname "$0")/generate-workers-file.sh" "$HADOOP_HOME/etc/hadoop"
    log_success "Hadoop configuration files generated"
}

distribute_configs() {
    log_info "Distributing Hadoop configs and creating data directories on workers..."

    for node in "${WORKER_NODES[@]}"; do
        (
          scp "$HADOOP_HOME/etc/hadoop/"{core-site.xml,hdfs-site.xml,yarn-site.xml,mapred-site.xml,workers} "$node:$HADOOP_HOME/etc/hadoop/"
          ssh "$node" "mkdir -p $HADOOP_DATA_DIR/{namenode,datanode}"
        ) &
    done
    wait

    mkdir -p $HADOOP_DATA_DIR/{namenode,datanode}

    log_success "Configs distributed and data directories created"
}

format_namenode_and_datanodes() {
    log_info "Stopping HDFS (if running) and cleaning DataNodes..."
    stop-dfs.sh > /dev/null 2>&1 || true

    rm -rf "$HADOOP_DATA_DIR/datanode/"*
    for node in "${WORKER_NODES[@]}"; do
        ssh "$node" "rm -rf $HADOOP_DATA_DIR/datanode/*"
    done

    rm -rf "$HADOOP_DATA_DIR/namenode/"*

    log_info "Formatting NameNode..."
    hdfs namenode -format -force -nonInteractive > /dev/null 2>&1
    log_success "NameNode formatted and DataNodes cleared"
}

start_hadoop_services() {
    log_info "Starting HDFS..."
    start-dfs.sh > /dev/null 2>&1
    sleep 1
    log_info "Starting YARN..."
    start-yarn.sh > /dev/null 2>&1
    sleep 1
    log_success "Hadoop services started"
}

verify_cluster() {
    log_info "Verifying cluster status..."
    for service in NameNode ResourceManager; do
        if jps | grep -q "$service"; then
            log_success "$service running"
        else
            log_error "$service NOT running"
            exit 1
        fi
    done

    LIVE_NODES=$(hdfs dfsadmin -report | grep -i "Live datanodes" | sed -E 's/.*\(([0-9]+)\).*/\1/')
    EXPECTED_NODES=$((${#WORKER_NODES[@]} + 1))
    if [[ "$LIVE_NODES" -ne "$EXPECTED_NODES" ]]; then
        log_error "Expected $EXPECTED_NODES DataNodes, got $LIVE_NODES"
        exit 1
    fi
    log_success "All DataNodes reporting"
}

generate_setup_report() {
    REPORT_FILE="$HOME/cluster-setup-report-$(date +%Y%m%d-%H%M%S).txt"
    {
    echo "================================================================================"
    echo "HADOOP CLUSTER SETUP REPORT"
    echo "================================================================================"
    echo "Setup Date: $(date)"
    echo "Master Node: $MASTER_NODE"
    echo "Worker Nodes: ${WORKER_NODES[*]}"
    echo "Total Nodes: $EXPECTED_NODES"
    echo "================================================================================"
    echo "Master Node Services:"
    jps
    echo "Worker Nodes Services:"
    for node in "${WORKER_NODES[@]}"; do
        echo "  $node:"
        ssh "$node" jps | sed 's/^/    /'
    done
    echo "================================================================================"
    echo "Web UI:"
    echo "NameNode: http://$MASTER_NODE:9870"
    echo "ResourceManager: http://$MASTER_NODE:8088"
    for node in "${WORKER_NODES[@]}"; do
        IP=$(ssh "$node" "hostname -I | awk '{print \$1}'")
        echo "NodeManager ($node): http://$IP:8042"
    done
    echo "================================================================================"
    } > "$REPORT_FILE"

    log_success "Setup report saved: $REPORT_FILE"
    cat "$REPORT_FILE"
}

# ============================================================================ 
# MAIN EXECUTION
# ============================================================================ 
validate_prerequisites
call_config_generators
distribute_configs
format_namenode_and_datanodes
start_hadoop_services
verify_cluster
generate_setup_report

echo -e "${GREEN}Hadoop cluster setup complete! Master is also running a DataNode.${NC}"
