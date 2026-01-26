#!/bin/bash
################################################################################
# SCRIPT: generate-input.sh
# DESCRIPTION: Generates input data for the WordCount experiment and uploads it to HDFS.
# USAGE: bash generate-input.sh <size_in_MB>
# PREREQUISITES:
#   - Hadoop cluster running.
# OUTPUT: Sentence-based text file of specified size uploaded to HDFS.
################################################################################

set -euo pipefail

SIZE_MB=${1:-100}
BLOCK_SIZE=${2:-0}

LOCAL_FILE=/tmp/wordcount_${SIZE_MB}MB.txt
HDFS_INPUT=/user/$USER/wordcount/input

generate_sentences() {
  python3 - "$SIZE_MB" "$LOCAL_FILE" <<'PY'
import random
import sys

sentences = [
  "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
  "Vestibulum ac diam sit amet quam vehicula elementum.",
  "Praesent sapien massa, convallis a pellentesque nec, egestas non nisi.",
  "Cras ultricies ligula sed magna dictum porta.",
  "Curabitur aliquet quam id dui posuere blandit.",
  "Nulla porttitor accumsan tincidunt.",
  "Quisque velit nisi, pretium ut lacinia in, elementum id enim.",
  "Donec rutrum congue leo eget malesuada.",
  "Sed porttitor lectus nibh.",
  "Vivamus suscipit tortor eget felis porttitor volutpat.",
  "Mauris blandit aliquet elit, eget tincidunt nibh pulvinar a.",
]

target_bytes = int(sys.argv[1]) * 1024 * 1024
path = sys.argv[2]
chunk_target = 4 * 1024 * 1024
written = 0

with open(path, "w", encoding="utf-8", buffering=2*1024*1024) as fp:
  while written < target_bytes:
    chunk = []
    chunk_bytes = 0

    while chunk_bytes < chunk_target and written + chunk_bytes < target_bytes:
      sentence = random.choice(sentences)
      chunk.append(sentence)
      chunk_bytes += len(sentence.encode("utf-8"))

    chunk_text = "".join(chunk)
    chunk_data = chunk_text.encode("utf-8")

    if written + len(chunk_data) > target_bytes:
      remainder = target_bytes - written
      chunk_data = chunk_data[:remainder]
      chunk_text = chunk_data.decode("utf-8", "ignore")
      fp.write(chunk_text)
      written = target_bytes
      break

    fp.write(chunk_text)
    written += len(chunk_data)

  print(f"Generated {written} bytes of sentence data")
PY
}

# Generate input file using sentence generator
if ! generate_sentences "$SIZE_MB" "$LOCAL_FILE"; then
  echo "Error: Failed to generate input file."
  exit 1
fi

echo "Uploading input to HDFS..."
hdfs dfs -mkdir -p "$HDFS_INPUT"
HDFS_COMMAND=(hdfs dfs)
if [[ "$BLOCK_SIZE" -gt 0 ]]; then
  HDFS_COMMAND+=("-D" "dfs.blocksize=$BLOCK_SIZE")
fi
HDFS_COMMAND+=("-put" "-f" "$LOCAL_FILE" "$HDFS_INPUT")

"${HDFS_COMMAND[@]}"

echo "HDFS input contents:"
hdfs dfs -ls "$HDFS_INPUT"
