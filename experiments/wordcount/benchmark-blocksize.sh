#!/bin/bash
################################################################################
# SCRIPT: benchmark-blocksize.sh
# DESCRIPTION: Runs WordCount benchmark across multiple block sizes (128KB - 1GB)
#              and records runtime for each configuration.
# USAGE: bash benchmark-blocksize.sh [input_size_mb]
# PREREQUISITES:
#   - Hadoop cluster running with min-block-size set to 131072 (128KB)
#   - Run: bash generate-hdfs-site-xml.sh && restart HDFS
# OUTPUT: results/wordcount-blocksize.csv with block_size,runtime_seconds
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/../../results"

# Input size in MB (default: 128 MB to keep split count manageable)
# With 128MB input: 128KB blocks = 1024 splits, 1GB blocks = 1 split
INPUT_SIZE_MB=${1:-128}

# Maximum number of splits to allow (to avoid OOM with tiny blocks)
MAX_SPLITS=${2:-512}

# Block sizes from 128KB to 1GB (doubling each time)
BLOCK_SIZES=(
  131072      # 128 KB
  262144      # 256 KB
  524288      # 512 KB
  1048576     # 1 MB
  2097152     # 2 MB
  4194304     # 4 MB
  8388608     # 8 MB
  16777216    # 16 MB
  33554432    # 32 MB
  67108864    # 64 MB
  134217728   # 128 MB
  268435456   # 256 MB
)

CSV_FILE="$RESULTS_DIR/wordcount-blocksize.csv"

echo "=============================================="
echo "WordCount Block Size Benchmark"
echo "=============================================="
echo "Input size: ${INPUT_SIZE_MB} MB"
echo "Max splits: ${MAX_SPLITS} (to avoid OOM)"
# Initialize CSV with header
mkdir -p "$RESULTS_DIR"
echo "block_size_bytes,block_size_human,runtime_seconds,num_splits" > "$CSV_FILE"

# Helper function to convert bytes to human-readable
bytes_to_human() {
  local bytes=$1
  if (( bytes >= 1073741824 )); then
    echo "$((bytes / 1073741824))GB"
  elif (( bytes >= 1048576 )); then
    echo "$((bytes / 1048576))MB"
  elif (( bytes >= 1024 )); then
    echo "$((bytes / 1024))KB"
  else
    echo "${bytes}B"
  fi
}

for block_size in "${BLOCK_SIZES[@]}"; do
  human_size=$(bytes_to_human "$block_size")
  
  # Calculate expected number of splits
  input_bytes=$((INPUT_SIZE_MB * 1024 * 1024))
  expected_splits=$(( (input_bytes + block_size - 1) / block_size ))
  
  echo ""
  echo "----------------------------------------------"
  echo "Running with block size: $human_size ($block_size bytes)"
  echo "Expected splits: $expected_splits"
  echo "----------------------------------------------"

  # Clean up previous input
  hdfs dfs -rm -r -f /user/$USER/wordcount/input 2>/dev/null || true
  hdfs dfs -rm -r -f /user/$USER/wordcount/output 2>/dev/null || true

  # Generate and upload input with specified block size
  echo "Generating ${INPUT_SIZE_MB}MB input with block size $human_size..."
  bash "$SCRIPT_DIR/generate-input.sh" "$INPUT_SIZE_MB" "$block_size"

  # Count number of blocks/splits
  num_splits=$(hdfs fsck /user/$USER/wordcount/input -files -blocks 2>/dev/null | grep -c "blk_" || echo "0")
  echo "Number of blocks: $num_splits"

  # Run WordCount
  echo "Running WordCount..."
  bash "$SCRIPT_DIR/run.sh"

  # Get runtime from the most recent run
  latest_run=$(ls -td "$RESULTS_DIR/wordcount/"*/ 2>/dev/null | head -1)
  if [[ -f "$latest_run/runtime_seconds.txt" ]]; then
    runtime=$(cat "$latest_run/runtime_seconds.txt")
    echo "Runtime: ${runtime} seconds"

    # Append to CSV
    echo "$block_size,$human_size,$runtime,$num_splits" >> "$CSV_FILE"
  else
    echo "WARNING: Could not find runtime for this run"
    echo "$block_size,$human_size,ERROR,0" >> "$CSV_FILE"
  fi

  echo "----------------------------------------------"
done

echo ""
echo "=============================================="
echo "Benchmark Complete!"
echo "=============================================="
echo "Results saved to: $CSV_FILE"
echo ""
echo "Run the plotting script to visualize:"
echo "  python3 $SCRIPT_DIR/plot-blocksize-results.py"
echo "=============================================="

# Display summary
echo ""
echo "Summary:"
cat "$CSV_FILE"
