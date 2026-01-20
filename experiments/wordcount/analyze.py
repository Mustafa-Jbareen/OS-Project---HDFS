"""
SCRIPT: analyze.py
DESCRIPTION: Analyzes the results of multiple WordCount runs and provides a summary.
USAGE: python analyze.py
PREREQUISITES:
    - WordCount experiment results available in the results directory.
OUTPUT:
    - Total runs, mean runtime, and standard deviation of runtimes.
"""

import glob
from statistics import mean, stdev

BASE = "/home/mostufa.j/my_scripts/results/wordcount"

runs = sorted(glob.glob(f"{BASE}/*"))
times = []

for run in runs:
    try:
        with open(f"{run}/runtime_seconds.txt") as f:
            times.append(float(f.read().strip()))
    except FileNotFoundError:
        print(f"Warning: runtime_seconds.txt not found in {run}")

print("========================================")
print("WordCount Experiment Summary")
print("========================================")
print(f"Total runs: {len(times)}")

if times:
    print(f"Mean runtime: {mean(times):.2f} sec")
    if len(times) > 1:
        print(f"Std deviation: {stdev(times):.2f} sec")

print("========================================")
