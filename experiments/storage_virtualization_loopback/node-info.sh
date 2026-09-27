#!/bin/bash
################################################################################
# SCRIPT: node-info.sh
# DESCRIPTION: One line describing a node, for the run metadata:
#   node|cores|mem_mb|kernel|device|rotational|model|java|hadoop
#   device = the physical disk behind STORAGE_BASE (sda5 -> sda, dm-0 -> nvme0n1)
#
# USAGE: ssh <node> "bash -s" -- <node> <storage_base> <hadoop_home> < node-info.sh
################################################################################

set +e
node=$1
mp=$2
hadoop_home=$3

# -T <path> finds the mount containing <path>; works when $mp is a symlink
# (c6620 has /scratch -> /mydata). lsblk -s walks from a partition / LVM
# volume down to the physical disk.
src=$(findmnt -T "$mp" -no SOURCE 2>/dev/null)
[ -z "$src" ] && src=$(df "$mp" 2>/dev/null | awk 'NR==2{print $1}')
phys=""
if [ -n "$src" ]; then
    phys=$(lsblk -snro NAME,TYPE "$src" 2>/dev/null | awk '$2=="disk"{print $1}' | head -1)
fi
if [ -z "$phys" ]; then
    for c in nvme0n1 sda vda; do
        if [ -b "/dev/$c" ]; then phys=$c; break; fi
    done
fi
[ -z "$phys" ] && phys=sda

rota=$(cat "/sys/block/$phys/queue/rotational" 2>/dev/null || echo "?")
model=$(cat "/sys/block/$phys/device/model" 2>/dev/null | xargs)
mem=$(awk '/^MemTotal:/ {printf "%d", $2/1024}' /proc/meminfo)
java_ver=""
if command -v java >/dev/null 2>&1; then
    java_ver=$(java -version 2>&1 | head -1 | tr -d '"|')
fi
hadoop_ver=""
if [ -x "$hadoop_home/bin/hadoop" ]; then
    hadoop_ver=$("$hadoop_home/bin/hadoop" version 2>/dev/null | head -1 | tr -d '|')
fi

echo "$node|$(nproc)|$mem|$(uname -r)|$phys|$rota|${model:-?}|${java_ver:-?}|${hadoop_ver:-?}"
