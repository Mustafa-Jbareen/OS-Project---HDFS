#!/bin/bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 nodes.txt"
    echo "nodes.txt: one hostname or IP per line (include master and workers)."
    exit 1
fi

NODES_FILE="$1"
USER_NAME=${USER}

if [ ! -f "$NODES_FILE" ]; then
    echo "Nodes file not found: $NODES_FILE"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_SCRIPT="$SCRIPT_DIR/setup-node.sh"

if [ ! -f "$SETUP_SCRIPT" ]; then
    echo "Cannot find setup-node.sh in $SCRIPT_DIR"
    exit 1
fi

# Generate a cluster SSH key (private + public) for node-to-node auth
CLUSTER_KEY="$HOME/.ssh/cloudlab_cluster_id_rsa"
if [ ! -f "$CLUSTER_KEY" ]; then
    echo "Generating cluster SSH key: $CLUSTER_KEY"
    ssh-keygen -t rsa -b 4096 -f "$CLUSTER_KEY" -N "" -C "cloudlab_cluster_key"
fi

echo "Copying setup script and cluster key to nodes and running setup..."
while read -r node; do
    node=$(echo "$node" | tr -d '\r' | awk '{print $1}')
    [ -z "$node" ] && continue

    echo "==> Preparing node: $node"

    # copy setup script
    scp -q "$SETUP_SCRIPT" "$USER_NAME@${node}:/tmp/" || { echo "scp failed to $node"; exit 1; }

    # copy cluster key pair to node's ~/.ssh
    scp -q "$CLUSTER_KEY" "$CLUSTER_KEY.pub" "$USER_NAME@${node}:~/.ssh/" || { echo "scp key failed to $node"; exit 1; }

    ssh -o BatchMode=yes "$USER_NAME@${node}" "mkdir -p ~/.ssh && chmod 700 ~/.ssh && chmod 600 ~/.ssh/$(basename $CLUSTER_KEY) || true && chmod 644 ~/.ssh/$(basename ${CLUSTER_KEY}.pub) || true"

    # Ensure authorized_keys contains the cluster pub key and (optionally) your local public key
    PUBKEY_CONTENT=$(cat "$CLUSTER_KEY.pub")
    ssh "$USER_NAME@${node}" "grep -qxF '$PUBKEY_CONTENT' ~/.ssh/authorized_keys 2>/dev/null || echo '$PUBKEY_CONTENT' >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"

    # Run setup script (setup-node.sh will escalate to sudo if needed)
    ssh "$USER_NAME@${node}" "bash /tmp/$(basename $SETUP_SCRIPT)" || { echo "setup failed on $node"; exit 1; }

    echo "Node $node prepared."
done < "$NODES_FILE"

echo "All nodes prepared. Next steps:
- Clone your repository to ~/my_scripts on the master node (if not already):
    ssh $USER_NAME@<master> 'git clone <your-repo-url> ~/my_scripts'
- On the master node run the experiment wrapper:
    ~/my_scripts/cloudlab/run-experiment-wrapper.sh [K_REPS]
" 
