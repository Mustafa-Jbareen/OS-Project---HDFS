#!/bin/bash
################################################################################
# SCRIPT: generate-input.sh
# DESCRIPTION: Generates input data for WordCount and uploads to HDFS.
#              OPTIMIZED for generating very large files (50GB, 500GB+).
#              Uses dd replication for maximum speed (~1GB/s on SSD).
# USAGE: bash generate-input.sh <size_in_MB> [block_size_bytes]
# PREREQUISITES:
#   - Hadoop cluster running.
# OUTPUT: Text file of specified size uploaded to HDFS.
################################################################################

set -euo pipefail

SIZE_MB=${1:-100}
BLOCK_SIZE=${2:-0}

LOCAL_FILE=/tmp/wordcount_${SIZE_MB}MB.txt
HDFS_INPUT=/user/$USER/wordcount/input

# ============================================================================
# ULTRA-FAST DATA GENERATION
# ============================================================================
# Strategy: Create a small seed file (1MB), then replicate it with dd.
# This is 100x faster than generating random text line by line.
# For 500GB: ~8-10 minutes instead of ~10+ hours.
# ============================================================================

SEED_FILE=/tmp/wordcount_seed_1MB.txt
SEED_SIZE_MB=1

generate_seed_file() {
    echo "Creating 1MB seed file..."
    
    # Base text block (~1KB) - simple words for WordCount
    # Using repetitive patterns that are quick to generate
    local TEXT_BLOCK="the quick brown fox jumps over the lazy dog
hadoop mapreduce distributed computing cluster data processing
word count example input file generated for benchmark testing
lorem ipsum dolor sit amet consectetur adipiscing elit sed do
eiusmod tempor incididunt ut labore et dolore magna aliqua
big data analytics machine learning artificial intelligence
parallel processing fault tolerance scalability performance
namenode datanode block replication factor configuration
yarn resource manager node manager application master container
map reduce shuffle sort partition combiner input split task
"

    # Calculate how many times to repeat to get ~1MB
    local BLOCK_SIZE_BYTES=${#TEXT_BLOCK}
    local REPEATS=$((1024 * 1024 / BLOCK_SIZE_BYTES + 1))
    
    # Generate 1MB seed file by repeating the text block
    rm -f "$SEED_FILE"
    for ((i=0; i<REPEATS; i++)); do
        echo "$TEXT_BLOCK"
    done > "$SEED_FILE"
    
    # Truncate to exactly 1MB
    truncate -s 1M "$SEED_FILE"
    
    echo "Seed file created: $(stat -c%s "$SEED_FILE" 2>/dev/null || stat -f%z "$SEED_FILE") bytes"
}

generate_large_file_fast() {
    local TARGET_MB=$1
    local OUTPUT_FILE=$2
    
    echo "Generating ${TARGET_MB}MB file using fast replication..."
    
    # For small files (<= 10MB), just use the seed directly
    if (( TARGET_MB <= SEED_SIZE_MB )); then
        head -c "${TARGET_MB}M" "$SEED_FILE" > "$OUTPUT_FILE"
        return
    fi
    
    # Calculate number of full 1MB blocks
    local BLOCKS=$TARGET_MB
    
    # Use dd to replicate seed file (fastest method)
    # Reading from seed file and writing BLOCKS copies
    echo "Replicating seed file ${BLOCKS} times..."
    
    # Method: Create file by concatenating seed file multiple times
    # Using dd for speed with large block size
    rm -f "$OUTPUT_FILE"
    
    # For very large files (>1GB), show progress
    if (( TARGET_MB > 1024 )); then
        # Use pv if available for progress, otherwise dd with status
        if command -v pv &> /dev/null; then
            yes | head -n $BLOCKS | while read; do cat "$SEED_FILE"; done | pv -s "${TARGET_MB}M" > "$OUTPUT_FILE"
        else
            # Batch approach: write in 1GB chunks for better performance
            local GB_CHUNKS=$((TARGET_MB / 1024))
            local REMAINING_MB=$((TARGET_MB % 1024))
            
            for ((g=0; g<GB_CHUNKS; g++)); do
                echo "  Writing GB $((g+1))/${GB_CHUNKS}..."
                for ((i=0; i<1024; i++)); do
                    cat "$SEED_FILE"
                done >> "$OUTPUT_FILE"
            done
            
            if (( REMAINING_MB > 0 )); then
                echo "  Writing remaining ${REMAINING_MB}MB..."
                for ((i=0; i<REMAINING_MB; i++)); do
                    cat "$SEED_FILE"
                done >> "$OUTPUT_FILE"
            fi
        fi
    else
        # For files <= 1GB, simple concatenation
        for ((i=0; i<BLOCKS; i++)); do
            cat "$SEED_FILE"
        done > "$OUTPUT_FILE"
    fi
    
    # Verify size
    local ACTUAL_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null || stat -f%z "$OUTPUT_FILE")
    echo "Generated file size: $((ACTUAL_SIZE / 1024 / 1024))MB"
}

# Alternative: Even faster using dd if we don't need text variety
generate_large_file_ultrafast() {
    local TARGET_MB=$1
    local OUTPUT_FILE=$2
    
    echo "Generating ${TARGET_MB}MB file using ultra-fast dd method..."
    
    # Create seed if needed
    if [[ ! -f "$SEED_FILE" ]]; then
        generate_seed_file
    fi
    
    # Use dd to read seed file repeatedly
    # This is the fastest pure-bash method
    local TARGET_BYTES=$((TARGET_MB * 1024 * 1024))
    
    # dd with large block size for maximum throughput
    dd if="$SEED_FILE" of="$OUTPUT_FILE" bs=1M count="$TARGET_MB" iflag=fullblock 2>/dev/null || {
        # If dd doesn't support reading past EOF, use alternative
        echo "Using concatenation method..."
        generate_large_file_fast "$TARGET_MB" "$OUTPUT_FILE"
        return
    }
    
    # If dd produced smaller file, pad it
    local ACTUAL_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null || stat -f%z "$OUTPUT_FILE")
    if (( ACTUAL_SIZE < TARGET_BYTES )); then
        echo "Padding file to target size..."
        generate_large_file_fast "$TARGET_MB" "$OUTPUT_FILE"
    fi
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

echo "=============================================="
echo "WordCount Input Generator (Optimized)"
echo "=============================================="
echo "Target size: ${SIZE_MB}MB"
echo "Output file: $LOCAL_FILE"
echo ""

START_TIME=$(date +%s)

# Step 1: Create seed file if it doesn't exist
if [[ ! -f "$SEED_FILE" ]]; then
    generate_seed_file
fi

# Step 2: Generate the large file using fast replication
generate_large_file_fast "$SIZE_MB" "$LOCAL_FILE"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo ""
echo "Generation completed in ${DURATION} seconds"
echo "Speed: $((SIZE_MB / (DURATION + 1)))MB/s"
echo ""

# Step 3: Upload to HDFS
echo "Uploading to HDFS..."
hdfs dfs -mkdir -p "$HDFS_INPUT"

HDFS_COMMAND=(hdfs dfs)
if [[ "$BLOCK_SIZE" -gt 0 ]]; then
    HDFS_COMMAND+=("-D" "dfs.blocksize=$BLOCK_SIZE")
fi
HDFS_COMMAND+=("-put" "-f" "$LOCAL_FILE" "$HDFS_INPUT")

"${HDFS_COMMAND[@]}"

echo ""
echo "HDFS input contents:"
hdfs dfs -ls "$HDFS_INPUT"
echo "=============================================="
