#!/bin/bash
################################################################################
# SCRIPT: collect-results.sh
# DESCRIPTION: Collects and processes the results of the WordCount experiment.
# USAGE: bash collect-results.sh
# PREREQUISITES:
#   - WordCount job completed successfully.
#   - Output data available in HDFS.
# OUTPUT: Top 10 words and total unique words displayed.
################################################################################

set -e

OUTPUT=/user/$USER/wordcount/output
LOCAL_OUT=/tmp/wordcount_output

# Ensure output directory is clean
rm -rf "$LOCAL_OUT"

# Fetch results from HDFS
hdfs dfs -get "$OUTPUT" "$LOCAL_OUT"

# Display top 10 words
if [ -d "$LOCAL_OUT" ]; then
  echo "Top 10 words:"
  sort -nr "$LOCAL_OUT"/part-* | head -10

  echo ""
  echo "Total unique words:"
  wc -l "$LOCAL_OUT"/part-* | tail -1
else
  echo "Error: Output directory not found in HDFS."
  exit 1
fi
