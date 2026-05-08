#!/bin/bash
################################################################################
# build.sh - compile trivial-wordcount.jar against the local Hadoop install.
# Run this on the c6620 master AFTER Hadoop is installed and `hadoop classpath`
# works on the command line.
################################################################################

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

if ! command -v hadoop >/dev/null 2>&1; then
    echo "ERROR: 'hadoop' not on PATH. Make sure HADOOP_HOME/bin is on PATH first." >&2
    exit 1
fi
if ! command -v javac >/dev/null 2>&1; then
    echo "ERROR: 'javac' not on PATH. Install a JDK (sudo apt-get install -y default-jdk)." >&2
    exit 1
fi

CP="$(hadoop classpath)"
rm -rf build trivial-wordcount.jar
mkdir -p build

echo "Compiling TrivialWordCount.java..."
javac -cp "$CP" -d build TrivialWordCount.java

echo "Packaging trivial-wordcount.jar..."
jar cf trivial-wordcount.jar -C build .

echo ""
echo "Built: $(pwd)/trivial-wordcount.jar"
echo "Sanity check:"
jar tf trivial-wordcount.jar
echo ""
echo "Smoke test (no execution, just main class lookup):"
hadoop jar trivial-wordcount.jar TrivialWordCount 2>&1 | head -3 || true
