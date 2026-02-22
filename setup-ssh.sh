#!/bin/bash
################################################################################
# SCRIPT: setup-ssh.sh
# DESCRIPTION: Distribute SSH keys from local storage to all cluster nodes
# PURPOSE: Set up passwordless SSH using keys in /home/mostufa.j/.ssh/
# USAGE: bash setup-ssh.sh
# PREREQUISITES:
#   - SSH keys exist in /home/mostufa.j/.ssh/ on all nodes
#   - /csl/mostufa.j/cluster file with list of all nodes
# OUTPUT: Passwordless SSH configured between all nodes
################################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

CLUSTER_FILE="$HOME/cluster"

log_info "Validating prerequisites..."

if [ ! -f "$CLUSTER_FILE" ]; then
    log_error "Cluster file not found: $CLUSTER_FILE"
    exit 1
fi

log_success "Cluster file found"

# ============================================================================
# STEP 1: COLLECT SSH KEYS FROM ALL NODES
# ============================================================================

log_info ""
log_info "Step 1: Collecting SSH public keys from all nodes..."

KEYS_FILE=$(mktemp)
> "$KEYS_FILE"

while read node; do
    [[ -z "$node" || "$node" =~ ^# ]] && continue
    
    log_info "[$node] Collecting key..."
    
    # Try to get the key - this should always work since we have SSH access
    if ssh "$node" "cat /home/mostufa.j/.ssh/id_rsa.pub" >> "$KEYS_FILE" 2>/dev/null; then
        KEY_FINGERPRINT=$(ssh "$node" "ssh-keygen -l -f /home/mostufa.j/.ssh/id_rsa.pub 2>/dev/null | awk '{print \$2}'" 2>/dev/null || echo "unknown")
        log_success "[$node] Key collected: $KEY_FINGERPRINT"
    else
        log_error "[$node] Failed to collect key"
    fi
done < "$CLUSTER_FILE"

TOTAL_KEYS=$(wc -l < "$KEYS_FILE")
log_success "Total keys collected: $TOTAL_KEYS"

if [ $TOTAL_KEYS -eq 0 ]; then
    log_error "No keys were collected!"
    exit 1
fi

# ============================================================================
# STEP 2: DISTRIBUTE KEYS TO ALL NODES
# ============================================================================

log_info ""
log_info "Step 2: Distributing collected keys to all nodes..."

while read node; do
    [[ -z "$node" || "$node" =~ ^# ]] && continue

    {
        log_info "[$node] Distributing keys..."
        
        # Copy keys file to node
        scp "$KEYS_FILE" "$node:/tmp/cluster_keys.pub" > /dev/null 2>&1
        
        # Add keys to authorized_keys
        ssh "$node" bash <<'SCRIPT'
set -e

mkdir -p /home/mostufa.j/.ssh
chmod 700 /home/mostufa.j/.ssh

# Append all keys from collected keys file
cat /tmp/cluster_keys.pub >> /home/mostufa.j/.ssh/authorized_keys 2>/dev/null || true

# Remove duplicates and keep only unique keys
sort -u /home/mostufa.j/.ssh/authorized_keys > /tmp/authorized_keys.tmp
mv /tmp/authorized_keys.tmp /home/mostufa.j/.ssh/authorized_keys

chmod 600 /home/mostufa.j/.ssh/authorized_keys
rm /tmp/cluster_keys.pub
SCRIPT
        
        log_success "[$node] Keys distributed"
    } &

done < "$CLUSTER_FILE"

wait
rm "$KEYS_FILE"

# ============================================================================
# STEP 3: VERIFY SSH CONNECTIVITY
# ============================================================================

log_info ""
log_info "Step 3: Verifying SSH connectivity and key count..."

while read node; do
    [[ -z "$node" || "$node" =~ ^# ]] && continue
    
    KEY_COUNT=$(ssh "$node" "wc -l < /home/mostufa.j/.ssh/authorized_keys" 2>/dev/null || echo "0")
    
    if [ "$KEY_COUNT" -gt 0 ]; then
        log_success "[$node] Has $KEY_COUNT key(s) in authorized_keys"
    else
        log_error "[$node] No keys in authorized_keys!"
    fi
done < "$CLUSTER_FILE"

log_success "SSH setup complete!"
