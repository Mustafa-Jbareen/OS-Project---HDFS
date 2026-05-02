#!/bin/bash
set -euo pipefail

# This script prepares a CloudLab node for the experiment.
# It should be copied to the node and executed as a user with sudo privileges.
#
# IMPORTANT: CloudLab m400 nodes have 120 GB total disk (~50 GB available after OS).
# We use mkextrafs.pl to create /mydata with available space, then bind /mydata → /scratch.

if [ "$(id -u)" -ne 0 ]; then
    echo "Re-running as root via sudo..."
    exec sudo bash "$0" "$@"
fi

SUDO_USER_NAME=${SUDO_USER:-$(logname 2>/dev/null || echo "$USER")}

HADOOP_VERSION=${HADOOP_VERSION:-3.3.1}
HADOOP_BASE=/mydata/hadoop
HADOOP_TARGET="$HADOOP_BASE/hadoop-$HADOOP_VERSION"

echo "Setting up CloudLab node for user: $SUDO_USER_NAME"
echo "Note: m400 has 120 GB total disk (~50 GB available after OS)"

apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openjdk-11-jdk python3 python3-pip sysstat bc curl ssh rsync wget net-tools procps

# Create /mydata with available local disk space
echo "Creating /mydata with available disk space..."
if [ ! -d /mydata ]; then
    sudo /usr/local/etc/emulab/mkextrafs.pl /mydata 2>&1 || echo "mkextrafs.pl not available; /mydata not created"
fi

if [ -d /mydata ]; then
    # Create Hadoop and experiment directories
    mkdir -p /mydata/hadoop /mydata/hadoop_data /mydata/hdfs_loop /mydata/results /mydata/tmp
    chown -R "$SUDO_USER_NAME":"$SUDO_USER_NAME" /mydata || true

    # Bind /mydata to /scratch so existing scripts using /scratch continue to work
    mkdir -p /scratch
    if ! mountpoint -q /scratch; then
        mount --bind /mydata /scratch || true
    fi
    if ! grep -qE "^/mydata\s+/scratch\s+none\s+bind" /etc/fstab 2>/dev/null; then
        echo "/mydata /scratch none bind 0 0" >> /etc/fstab
    fi
else
    echo "WARNING: /mydata not created. Falling back to /tmp for experiment data."
    mkdir -p /tmp/hadoop /tmp/hadoop_data /tmp/hdfs_loop /tmp/results /tmp/tmp
    chown -R "$SUDO_USER_NAME":"$SUDO_USER_NAME" /tmp/hadoop /tmp/hadoop_data /tmp/hdfs_loop /tmp/results /tmp/tmp || true
fi

# Install Hadoop under /mydata (or /tmp if /mydata unavailable)
if [ ! -d "$HADOOP_TARGET" ]; then
    echo "Installing Hadoop $HADOOP_VERSION to $HADOOP_TARGET"
    mkdir -p "$HADOOP_BASE"
    cd /tmp
    HADOOP_TGZ="hadoop-$HADOOP_VERSION.tar.gz"
    if [ ! -f "$HADOOP_TGZ" ]; then
        wget -q https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/$HADOOP_TGZ
    fi
    tar -xzf "$HADOOP_TGZ" -C "$HADOOP_BASE"
    chown -R "$SUDO_USER_NAME":"$SUDO_USER_NAME" "$HADOOP_BASE"
fi

# Fallback: also store Hadoop path in /tmp in case /mydata bind fails
if [ -d /mydata/hadoop ]; then
    HADOOP_PATH=/mydata/hadoop/hadoop-$HADOOP_VERSION
else
    HADOOP_PATH=/tmp/hadoop/hadoop-$HADOOP_VERSION
fi

# Set HADOOP_HOME in /etc/profile.d so experiment scripts find it
cat > /etc/profile.d/hadoop.sh <<'ENV'
#!/bin/sh
export HADOOP_HOME=HADOOP_PATH_PLACEHOLDER
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV
sed -i "s|HADOOP_PATH_PLACEHOLDER|$HADOOP_PATH|g" /etc/profile.d/hadoop.sh
chmod +x /etc/profile.d/hadoop.sh

# Enable sysstat data collection
if [ -f /etc/default/sysstat ]; then
    sed -i 's/ENABLED="false"/ENABLED="true"/' /etc/default/sysstat || true
    systemctl restart sysstat.service 2>/dev/null || true
fi

echo "Node setup complete."
echo "  Hadoop at: $HADOOP_PATH"
echo "  /mydata (or /tmp) is bound to /scratch for compatibility."
echo "  Results placed under $HADOOP_BASE/../results or /tmp/results."
