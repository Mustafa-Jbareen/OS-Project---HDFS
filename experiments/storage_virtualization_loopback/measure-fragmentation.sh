#!/bin/bash
################################################################################
# SCRIPT: measure-fragmentation.sh
# DESCRIPTION: Records filefrag stats for the loopback-image hypothesis.
#              Run AFTER the input data is uploaded to HDFS, BEFORE WordCount.
#
#              Captures, for each DataNode:
#                A. extent count of every loopback image on the host FS
#                   (e.g. /scratch/loop_images/hdfs_dn*.img)
#                B. extent count of HDFS block files inside each loopback FS
#                   (sampled - up to MAX_BLOCKS_PER_FS per FS to keep cost
#                   bounded at high k)
#
# USAGE: bash measure-fragmentation.sh <k> <out_dir> <node1> [node2 ...]
#
# OUTPUT: <out_dir>/frag_k${k}_images.csv
#         <out_dir>/frag_k${k}_blocks.csv
#         <out_dir>/frag_k${k}_summary.txt   (medians, p95, totals)
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster.conf"

K=${1:?Usage: measure-fragmentation.sh <k> <out_dir> <node1> [node2 ...]}
OUT_DIR=${2:?Missing out_dir}
shift 2
NODES=("$@")
if (( ${#NODES[@]} == 0 )); then
    echo "ERROR: no nodes given" >&2
    exit 1
fi

MAX_BLOCKS_PER_FS=${MAX_BLOCKS_PER_FS:-50}

mkdir -p "$OUT_DIR"
IMG_CSV="$OUT_DIR/frag_k${K}_images.csv"
BLK_CSV="$OUT_DIR/frag_k${K}_blocks.csv"
SUM_TXT="$OUT_DIR/frag_k${K}_summary.txt"

echo "node,fs_index,image_path,size_bytes,extents" > "$IMG_CSV"
echo "node,fs_index,block_path,size_bytes,extents" > "$BLK_CSV"

# ---- collect from every node in parallel -----------------------------------
echo "[frag] k=$K  collecting from ${#NODES[@]} nodes..."
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

for node in "${NODES[@]}"; do
    (
        ssh "$node" "bash -s" -- "$K" "$IMAGE_DIR" "$MOUNT_BASE" "$MAX_BLOCKS_PER_FS" <<'REMOTE' > "$TMPDIR/${node}.out" 2>"$TMPDIR/${node}.err"
set -u
k=$1
img_dir=$2
mount_base=$3
max_blocks=$4

# A. loopback image extent counts
for ((i=1; i<=k; i++)); do
    img="$img_dir/hdfs_dn${i}.img"
    [ -f "$img" ] || continue
    size=$(stat -c%s "$img" 2>/dev/null || echo 0)
    # filefrag prints "<path>: N extents found"
    ext=$(sudo filefrag "$img" 2>/dev/null | grep -oP '\d+(?= extents found)' || echo "")
    [ -z "$ext" ] && ext=0
    echo "IMG;$i;$img;$size;$ext"
done

# B. sample HDFS block files inside each loopback FS
for ((i=1; i<=k; i++)); do
    fs_root="$mount_base/dn${i}/hdfs_data"
    [ -d "$fs_root" ] || continue
    # find block data files (not .meta), sample up to max_blocks per FS
    while IFS= read -r blk; do
        [ -z "$blk" ] && continue
        size=$(stat -c%s "$blk" 2>/dev/null || echo 0)
        ext=$(sudo filefrag "$blk" 2>/dev/null | grep -oP '\d+(?= extents found)' || echo "")
        [ -z "$ext" ] && ext=0
        echo "BLK;$i;$blk;$size;$ext"
    done < <(find "$fs_root" -type f -name 'blk_*' ! -name '*.meta' 2>/dev/null | head -n "$max_blocks")
done
REMOTE
        rc=$?
        if [ $rc -ne 0 ]; then
            echo "[frag] WARNING: $node returned rc=$rc" >&2
        fi
    ) &
done
wait

# ---- merge into the two CSVs -----------------------------------------------
for node in "${NODES[@]}"; do
    out="$TMPDIR/${node}.out"
    [ -f "$out" ] || continue
    while IFS=';' read -r kind idx path size ext; do
        case "$kind" in
            IMG) echo "$node,$idx,$path,$size,$ext" >> "$IMG_CSV" ;;
            BLK) echo "$node,$idx,$path,$size,$ext" >> "$BLK_CSV" ;;
        esac
    done < "$out"
done

# ---- summary stats ---------------------------------------------------------
python3 - "$IMG_CSV" "$BLK_CSV" "$SUM_TXT" "$K" <<'PY'
import csv, statistics, sys

img_csv, blk_csv, out_txt, k = sys.argv[1:5]

def stats(rows, field):
    vals = []
    for r in rows:
        try:
            vals.append(int(r[field]))
        except (ValueError, KeyError):
            pass
    if not vals:
        return None
    vals.sort()
    n = len(vals)
    p95 = vals[min(n-1, int(0.95*n))]
    return dict(n=n, total=sum(vals), mean=statistics.mean(vals),
                median=statistics.median(vals), p95=p95, max=max(vals))

img_rows = list(csv.DictReader(open(img_csv)))
blk_rows = list(csv.DictReader(open(blk_csv)))

with open(out_txt, 'w') as f:
    f.write(f"Fragmentation summary for k={k}\n")
    f.write("="*60 + "\n\n")
    s = stats(img_rows, 'extents')
    f.write(f"Loopback IMAGE files (host filesystem extents)\n")
    if s:
        f.write(f"  count={s['n']}  total_extents={s['total']}\n")
        f.write(f"  mean={s['mean']:.2f}  median={s['median']}  p95={s['p95']}  max={s['max']}\n")
    else:
        f.write("  (no data)\n")
    f.write("\n")
    s = stats(blk_rows, 'extents')
    f.write(f"HDFS BLOCK files (extents inside the loopback ext4)\n")
    if s:
        f.write(f"  count={s['n']}  total_extents={s['total']}\n")
        f.write(f"  mean={s['mean']:.2f}  median={s['median']}  p95={s['p95']}  max={s['max']}\n")
    else:
        f.write("  (no data)\n")
print(open(out_txt).read())
PY

echo "[frag] wrote $IMG_CSV  $BLK_CSV  $SUM_TXT"
