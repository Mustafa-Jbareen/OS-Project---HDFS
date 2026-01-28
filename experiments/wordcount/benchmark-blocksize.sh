#!/bin/bash
################################################################################
# SCRIPT: benchmark-blocksize.sh
# DESCRIPTION: Runs WordCount benchmark across multiple block sizes (128KB - 256MB)
#              and records runtime for each configuration.
#              Results are saved with timestamps to prevent overwriting.
# USAGE: bash benchmark-blocksize.sh [input_size_mb] [max_splits]
# PREREQUISITES:
#   - Hadoop cluster running with min-block-size set to 131072 (128KB)
#   - Run: bash generate-hdfs-site-xml.sh && restart HDFS
# OUTPUT: results/blocksize-benchmark/run_<timestamp>/results.csv
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_BASE_DIR="$SCRIPT_DIR/../../results/blocksize-benchmark"

# Input size in MB (default: 128 MB to keep split count manageable)
# With 128MB input: 128KB blocks = 1024 splits, 1GB blocks = 1 split
INPUT_SIZE_MB=${1:-128}

# Maximum number of splits to allow (to avoid OOM with tiny blocks)
MAX_SPLITS=${2:-2048}

# Generate unique run ID with timestamp
RUN_TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_DIR="$RESULTS_BASE_DIR/run_$RUN_TIMESTAMP"
mkdir -p "$RUN_DIR"

CSV_FILE="$RUN_DIR/results.csv"
LOG_FILE="$RUN_DIR/benchmark.log"

# ============================================================================
# Block Size Definitions
# ============================================================================
# Block sizes expressed as powers of 2: 2^N bytes
# This makes the binary nature of block sizes explicit and clean.
# 
# Formula: block_size_bytes = 2^EXPONENT
#
# Examples:
#   2^17 = 131072 bytes   = 128 KB
#   2^20 = 1048576 bytes  = 1 MB
#   2^27 = 134217728 bytes = 128 MB
# ============================================================================

# Block size exponents (actual size = 2^N bytes)
BLOCK_SIZE_EXPONENTS=(
  # 17    # 2^17 = 128 KB
  # 18    # 2^18 = 256 KB
  # 19    # 2^19 = 512 KB
  # 20    # 2^20 = 1 MB
  # 21    # 2^21 = 2 MB
  22    # 2^22 = 4 MB
  23    # 2^23 = 8 MB
  24    # 2^24 = 16 MB
  25    # 2^25 = 32 MB
  26    # 2^26 = 64 MB
  27    # 2^27 = 128 MB
  28    # 2^28 = 256 MB
  29    # 2^29 = 512 MB 
  30    # 2^30 = 1 GB 
  # 31    # 2^31 = 2 GB 
  # 32  # 2^32 = 4 GB 
  # 33  # 2^33 = 8 GB 
  # 34  # 2^34 = 16 GB
)

# ============================================================================
# Helper Functions
# ============================================================================

# Convert exponent to bytes: 2^N
exp_to_bytes() {
  local exp=$1
  echo $((2**exp))
}

# Convert bytes to human-readable format
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

# Log to both console and file
log() {
  echo "$1" | tee -a "$LOG_FILE"
}

echo "=============================================="
echo "WordCount Block Size Benchmark"
echo "=============================================="
echo "Run ID: $RUN_TIMESTAMP"
echo "Input size: ${INPUT_SIZE_MB} MB"
echo "Max splits: ${MAX_SPLITS} (to avoid OOM)"
echo "Results directory: $RUN_DIR"
echo "=============================================="

# Save run metadata
cat > "$RUN_DIR/metadata.json" <<EOF
{
  "run_id": "$RUN_TIMESTAMP",
  "input_size_mb": $INPUT_SIZE_MB,
  "max_splits": $MAX_SPLITS,
  "start_time": "$(date -Iseconds)",
  "block_size_exponents": [$(IFS=,; echo "${BLOCK_SIZE_EXPONENTS[*]}")],
  "notes": "Block size = 2^exponent bytes"
}
EOF

# Initialize CSV with header
echo "block_size_exp,block_size_bytes,block_size_formula,block_size_human,runtime_seconds,num_splits" > "$CSV_FILE"

for exp in "${BLOCK_SIZE_EXPONENTS[@]}"; do
  block_size_bytes=$(exp_to_bytes "$exp")
  human_size=$(bytes_to_human "$block_size_bytes")
  formula="2^${exp}"
  
  # Calculate expected number of splits
  input_bytes=$((INPUT_SIZE_MB * 1024 * 1024))
  expected_splits=$(( (input_bytes + block_size_bytes - 1) / block_size_bytes ))
  
  log ""
  log "----------------------------------------------"
  log "Block Size: $human_size ($formula = $block_size_bytes bytes)"
  log "  Expected splits: $expected_splits"
  log "----------------------------------------------"

  # Skip if too many splits (would cause OOM)
  if (( expected_splits > MAX_SPLITS )); then
    log "SKIPPING: Too many splits ($expected_splits > $MAX_SPLITS limit)"
    echo "$exp,$block_size_bytes,$formula,$human_size,SKIPPED,$expected_splits" >> "$CSV_FILE"
    continue
  fi

  # Clean up previous input
  hdfs dfs -rm -r -f /user/$USER/wordcount/input 2>/dev/null || true
  hdfs dfs -rm -r -f /user/$USER/wordcount/output 2>/dev/null || true

  # Generate and upload input with specified block size
  log "Generating ${INPUT_SIZE_MB}MB input with block size $human_size..."
  bash "$SCRIPT_DIR/generate-input.sh" "$INPUT_SIZE_MB" "$block_size_bytes"

  # Count number of blocks/splits
  num_splits=$(hdfs fsck /user/$USER/wordcount/input -files -blocks 2>/dev/null | grep -c "blk_" || echo "0")
  log "Actual number of blocks: $num_splits"

  # Run WordCount
  log "Running WordCount..."
  bash "$SCRIPT_DIR/run.sh"

  # Get runtime from the most recent run
  latest_run=$(ls -td "$SCRIPT_DIR/../../results/wordcount/"*/ 2>/dev/null | head -1)
  if [[ -f "$latest_run/runtime_seconds.txt" ]]; then
    runtime=$(cat "$latest_run/runtime_seconds.txt")
    log "Runtime: ${runtime} seconds"

    # Append to CSV
    echo "$exp,$block_size_bytes,$formula,$human_size,$runtime,$num_splits" >> "$CSV_FILE"
  else
    log "WARNING: Could not find runtime for this run"
    echo "$exp,$block_size_bytes,$formula,$human_size,ERROR,$num_splits" >> "$CSV_FILE"
  fi

  log "----------------------------------------------"
done

# Update metadata with end time
END_TIME=$(date -Iseconds)
sed -i "s/\"start_time\"/\"end_time\": \"$END_TIME\",\n  \"start_time\"/" "$RUN_DIR/metadata.json" 2>/dev/null || true

echo ""
echo "=============================================="
echo "Benchmark Complete!"
echo "=============================================="
echo "Run ID: $RUN_TIMESTAMP"
echo "Results saved to: $RUN_DIR"
echo "  - results.csv: Main results file"
echo "  - metadata.json: Run configuration"
echo "  - benchmark.log: Detailed log"
echo ""
echo "Previous runs are preserved in:"
echo "  $RESULTS_BASE_DIR/"
echo ""
echo "Run the plotting script to visualize:"
echo "  python3 $SCRIPT_DIR/plot-blocksize-results.py $CSV_FILE"
echo "=============================================="

# Display summary
echo ""
echo "Summary:"
cat "$CSV_FILE"

# Create a symlink to the latest run for convenience
ln -sfn "$RUN_DIR" "$RESULTS_BASE_DIR/latest"
echo ""
echo "Latest run symlink: $RESULTS_BASE_DIR/latest"
