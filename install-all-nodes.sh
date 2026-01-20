#!/bin/bash
################################################################################
# SCRIPT: install-all-nodes.sh
# DESCRIPTION: Installs Java, SSH, and Hadoop on all nodes in parallel.
# USAGE: bash install-all-nodes.sh
# PREREQUISITES:
#   - SSH passwordless access configured to all nodes.
#   - List of nodes available in the cluster configuration file.
# OUTPUT: Java, SSH, and Hadoop installed on all nodes.
################################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Export logging functions to make them available in subshells
export -f log_info
export -f log_success
export -f log_error

# ============================================================================
# CONFIGURATION
# ============================================================================

CLUSTER_FILE="$HOME/cluster"
HADOOP_VERSION="3.3.6"

# Prompt for sudo password once at the start
read -sp "Enter your sudo password (for apt installations): " SUDO_PASSWORD
echo ""
export SUDO_PASSWORD

# ============================================================================
# VALIDATION
# ============================================================================

log_info "Validating prerequisites..."

if [ ! -f "$CLUSTER_FILE" ]; then
    log_error "Cluster file not found: $CLUSTER_FILE"
    log_error "Create it with: cat > ~/cluster << EOF"
    log_error "node1-hostname"
    log_error "node2-hostname"
    log_error "EOF"
    exit 1
fi

log_success "Cluster file found: $CLUSTER_FILE"
log_info "Nodes in cluster:"
cat "$CLUSTER_FILE" | while read node; do
    [[ -z "$node" || "$node" =~ ^# ]] && continue
    echo "  - $node"
done

# ============================================================================
# MAIN EXECUTION
# ============================================================================

log_info "Starting parallel installation on all nodes..."

while read node; do
    # Skip empty lines or comments
    [[ -z "$node" || "$node" =~ ^# ]] && continue

    log_info "Installing on $node..."
    {
        log_info "[$node] Installing prerequisites..."
        ssh "$node" SUDO_PASSWORD="$SUDO_PASSWORD" bash <<'SCRIPT'
set -e
echo "$SUDO_PASSWORD" | sudo -S apt-get update -qq
if ! java -version >/dev/null 2>&1; then
    echo "$SUDO_PASSWORD" | sudo -S apt-get install -y openjdk-11-jdk > /dev/null 2>&1
fi
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa > /dev/null 2>&1
fi
SCRIPT
        log_success "[$node] Prerequisites installed"

        log_info "[$node] Installing Hadoop..."
        ssh "$node" SUDO_PASSWORD="$SUDO_PASSWORD" bash <<'SCRIPT'
set -e
HADOOP_VERSION="3.3.6"
HADOOP_HOME="/home/mostufa.j/hadoop"
if [ ! -d "$HADOOP_HOME" ]; then
    cd /tmp
    wget -q https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz
    echo "$SUDO_PASSWORD" | sudo -S mv hadoop-$HADOOP_VERSION /home/mostufa.j/hadoop
    rm hadoop-$HADOOP_VERSION.tar.gz
fi
grep -q HADOOP_HOME ~/.bashrc || cat >> ~/.bashrc <<EOF
export HADOOP_HOME=/home/mostufa.j/hadoop
export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH
EOF
JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")
[ -d "$HADOOP_HOME/etc/hadoop" ] && grep -q JAVA_HOME "$HADOOP_HOME/etc/hadoop/hadoop-env.sh" 2>/dev/null || echo "export JAVA_HOME=$JAVA_HOME" >> "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
SCRIPT
        log_success "[$node] Hadoop installed"
    } &

done < "$CLUSTER_FILE"

wait
log_success "Installation complete on all nodes"

# ============================================================================
# SETUP PUBLIC KEY DISTRIBUTION
# ============================================================================

log_info "Setting up passwordless SSH authentication..."

# Get this node's public key
if [ ! -f ~/.ssh/id_rsa.pub ]; then
    log_error "SSH public key not found on master"
    exit 1
fi

# Update paths to handle local and shared directories
LOCAL_HOME="/home/mostufa.j"
SHARED_HOME="$HOME"  # $HOME points to /csl/mostufa.j

# Ensure temporary file for public keys is created successfully
PUBKEY_FILE=$(mktemp) || { log_error "Failed to create temporary file for public keys"; exit 1; }

# Add the master node's public key to the temporary file
if [ -f "$LOCAL_HOME/.ssh/id_rsa.pub" ]; then
    cat "$LOCAL_HOME/.ssh/id_rsa.pub" >> "$PUBKEY_FILE"
else
    log_error "SSH public key not found on master node in $LOCAL_HOME/.ssh/id_rsa.pub"
    exit 1
fi

# Collect public keys from all nodes
while read node; do
    [[ -z "$node" || "$node" =~ ^# ]] && continue
    ssh "$node" cat "$LOCAL_HOME/.ssh/id_rsa.pub" >> "$PUBKEY_FILE" || { log_error "Failed to collect public key from $node"; exit 1; }
done < "$CLUSTER_FILE"

# Ensure the temporary file contains unique keys
sort -u "$PUBKEY_FILE" -o "$PUBKEY_FILE" || { log_error "Failed to sort and remove duplicates from public keys"; exit 1; }

# Distribute the unique public keys to all nodes
log_info "Distributing public keys to all nodes..."
while read node; do
    [[ -z "$node" || "$node" =~ ^# ]] && continue
    {
        log_info "[$node] Distributing SSH keys..."
        scp "$PUBKEY_FILE" "$node:/tmp/all_keys.pub" > /dev/null 2>&1 || { log_error "Failed to copy public keys to $node"; exit 1; }
        ssh "$node" bash <<SCRIPT
# Logging functions
log_info() {
    echo -e "\033[0;34m[INFO]\033[0m $1"
}

log_success() {
    echo -e "\033[0;32m[SUCCESS]\033[0m $1"
}

log_error() {
    echo -e "\033[0;31m[ERROR]\033[0m $1"
}

# Add new keys only if they don't already exist
while IFS= read -r key; do
    if ! grep -F "$key" $SHARED_HOME/.ssh/authorized_keys > /dev/null 2>&1; then
        echo "$key" >> $SHARED_HOME/.ssh/authorized_keys
    fi
    # Ensure the key was added successfully
    if ! grep -F "$key" $SHARED_HOME/.ssh/authorized_keys > /dev/null 2>&1; then
        log_error "Failed to add key: $key"
        exit 1
    fi
done < /tmp/all_keys.pub

chmod 600 $SHARED_HOME/.ssh/authorized_keys
rm /tmp/all_keys.pub

# Remove any duplicate lines and keep unique keys
if [ -f $SHARED_HOME/.ssh/authorized_keys ]; then
    sort -u $SHARED_HOME/.ssh/authorized_keys -o $SHARED_HOME/.ssh/authorized_keys || { log_error "Failed to sort and remove duplicates from authorized_keys"; exit 1; }
else
    log_error "$SHARED_HOME/.ssh/authorized_keys not found. Aborting."
    exit 1
fi
SCRIPT
        log_success "[$node] SSH keys distributed"
    } &
done < "$CLUSTER_FILE"

wait
rm "$PUBKEY_FILE" || { log_error "Failed to remove temporary public key file"; exit 1; }
log_success "Passwordless SSH configured on all nodes"

# ============================================================================
# VERIFICATION
# ============================================================================

log_info "Verifying installation on all nodes..."

while read node; do
    [[ -z "$node" || "$node" =~ ^# ]] && continue
    
    {
        if ssh -o ConnectTimeout=5 "$node" "java -version 2>&1 | grep -q OpenJDK" 2>/dev/null; then
            log_success "[$node] Java verified"
        else
            log_error "[$node] Java not found"
            exit 1
        fi
        
        if ssh "$node" "[ -d /home/mostufa.j/hadoop ]" 2>/dev/null; then
            log_success "[$node] Hadoop verified"
        else
            log_error "[$node] Hadoop not installed at /home/mostufa.j/hadoop"
            exit 1
        fi
        
        if ssh "$node" "[ -f ~/.ssh/id_rsa ]" 2>/dev/null; then
            log_success "[$node] SSH keys verified"
        else
            log_error "[$node] SSH keys not found"
            exit 1
        fi
    } &
done < "$CLUSTER_FILE"

wait

# ============================================================================
# SUMMARY
# ============================================================================

log_success "All nodes prepared successfully!"
log_info "Next steps:"
log_info "1. Edit configuration variables in setup-cluster-automated.sh"
log_info "2. Run: bash setup-cluster-automated.sh"
