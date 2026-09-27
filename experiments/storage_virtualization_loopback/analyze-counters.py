#!/usr/bin/env python3
"""Hadoop job counters of loopback-experiment runs, per k (and per condition).

The runtime alone does not show *how* a run was loaded. The job counters in
experiment.log do: how many map tasks ran at once, how long each took, the
container size, and how many read their block from the local DataNode.
This is what showed that the May 2026 "no slowdown" runs ran only 1-13 maps
at a time instead of ~30 (tapuz) / ~200 (c6620).

Usage:
    analyze-counters.py RUN_DIR [RUN_DIR ...]
        Table per k for each run (works for old and new experiment.log files).
    analyze-counters.py --job-log FILE
        Prints "maps data_local map_ms reduce_ms cpu_ms gc_ms" for one job
        (used by run-experiment-loopback-fs.sh).
    analyze-counters.py --dn-delta BEFORE.json AFTER.json
        DataNode work between two DataNodeActivity snapshots (dn_snapshot in
        the runner), summed over all DataNodes, as one CSV line:
        read_ops,blocks_read,mb_read,read_block_avg_ms,packet_transfer_avg_us,
        packet_blocked_on_network_avg_us,total_read_ms  (-1 = not available)

Columns:
    conc        average number of map tasks running at once
                (total map task time / job runtime)
    map_s       average duration of one map task (s)
    cpu_s       CPU time per map task (s)
    cont_MB     map container size (MB)
    local%      map tasks that read their block from the local DataNode
    vs_k0       runtime change vs the first (smallest) k of the same condition
"""
import os
import re
import statistics
import sys
from collections import OrderedDict

COUNTERS = {
    "Launched map tasks": "maps",
    "Data-local map tasks": "local",
    "Total time spent by all map tasks (ms)": "map_ms",
    "Total time spent by all reduce tasks (ms)": "red_ms",
    "Total megabyte-milliseconds taken by all map tasks": "map_mbms",
    "CPU time spent (ms)": "cpu_ms",
    "GC time elapsed (ms)": "gc_ms",
}

# "  Run 2/3 (k=512)..."                     (runs up to May 2026)
OLD_JOB = re.compile(r"Run \d+/\d+ \(k=(\d+)\)")
# "  Rep 2/5  k=512  condition=maps8_cold"   (current runner)
NEW_JOB = re.compile(r"Rep \d+/\d+\s+k=(\d+)\s+condition=(\S+)")
# "  Warm-up job 1/1  k=512  (not measured; ...)" -- skipped
WARMUP = re.compile(r"Warm-up job \d+/\d+")
RUNTIME = re.compile(r"Runtime: ([\d.]+)s")


def parse_counter_line(line, into):
    name, sep, value = line.strip().rpartition("=")
    if sep and name in COUNTERS:
        try:
            into[COUNTERS[name]] = int(value)
        except ValueError:
            pass


def job_log_counters(path):
    values = dict.fromkeys(COUNTERS.values(), 0)
    with open(path, errors="ignore") as f:
        for line in f:
            parse_counter_line(line, values)
    return values


def parse_experiment_log(path):
    """Yield (condition, k, counters+runtime) for every finished job."""
    k = None
    condition = "-"
    current = {}
    with open(path, errors="ignore") as f:
        for line in f:
            m = NEW_JOB.search(line)
            if m:
                k, condition, current = int(m.group(1)), m.group(2), {}
                continue
            m = OLD_JOB.search(line)
            if m:
                k, condition, current = int(m.group(1)), "-", {}
                continue
            if WARMUP.search(line):
                k, current = None, {}
                continue
            parse_counter_line(line, current)
            m = RUNTIME.search(line)
            if m and k is not None and current.get("map_ms"):
                current["rt"] = float(m.group(1))
                yield condition, k, current
                current = {}


def mean(values):
    return statistics.mean(values) if values else 0.0


def summarize_run(run_dir):
    log = os.path.join(run_dir, "experiment.log")
    if not os.path.exists(log):
        print(f"### {run_dir}: no experiment.log")
        return
    jobs = OrderedDict()
    for condition, k, c in parse_experiment_log(log):
        jobs.setdefault(condition, OrderedDict()).setdefault(k, []).append(c)
    print(f"### {run_dir}")
    if not jobs:
        print("    (no finished jobs with counters)")
        return
    header = (f"  {'condition':<16}{'k':>6}{'jobs':>6}{'runtime':>9}{'vs_k0':>8}{'conc':>7}"
              f"{'map_s':>8}{'cpu_s':>8}{'cont_MB':>9}{'local%':>8}{'reduce_s':>10}{'gc_s':>7}")
    print(header)
    for condition, per_k in jobs.items():
        base = None
        for k in sorted(per_k):
            J = per_k[k]
            rt = mean([j["rt"] for j in J])
            if base is None:
                base = rt
            vs = f"{(rt / base - 1) * 100:+.1f}%" if base else ""
            conc = mean([j["map_ms"] / 1000 / j["rt"] for j in J])
            map_s = mean([j["map_ms"] / 1000 / max(j.get("maps", 0), 1) for j in J])
            cpu_s = mean([j.get("cpu_ms", 0) / 1000 / max(j.get("maps", 0), 1) for j in J])
            cont = mean([j.get("map_mbms", 0) / j["map_ms"] for j in J])
            local = mean([100 * j.get("local", 0) / max(j.get("maps", 0), 1) for j in J])
            red = mean([j.get("red_ms", 0) / 1000 for j in J])
            gc = mean([j.get("gc_ms", 0) / 1000 for j in J])
            print(f"  {condition:<16}{k:>6}{len(J):>6}{rt:>9.0f}{vs:>8}{conc:>7.1f}"
                  f"{map_s:>8.2f}{cpu_s:>8.2f}{cont:>9.0f}{local:>7.0f}%{red:>10.0f}{gc:>7.0f}")


def dn_delta(before_path, after_path):
    """Sum of DataNode work between two snapshots, over all DataNodes.

    Counters (NumOps, BlocksRead, BytesRead, TotalReadTime) are cumulative:
    their difference is the work in between. The *AvgTime values are
    per-interval averages that the DataNode resets whenever its metrics are
    read, so the value in the AFTER snapshot is the average over exactly the
    operations since BEFORE; they are weighted by each DataNode's op count.
    """
    import json

    with open(before_path) as f:
        before = json.load(f)
    with open(after_path) as f:
        after = json.load(f)
    ops = blocks = nbytes = 0
    total_read_ms = 0
    have_total_read = False
    weighted = {"ReadBlockOp": [0.0, 0], "SendDataPacketTransferNanos": [0.0, 0],
                "SendDataPacketBlockedOnNetworkNanos": [0.0, 0]}
    for node, doc in after.items():
        old_beans = {b.get("name"): b for b in before.get(node, {}).get("beans", [])}
        for bean in doc.get("beans", []):
            old = old_beans.get(bean.get("name"), {})

            def diff(key):
                return max(0, bean.get(key, 0) - old.get(key, 0))

            ops += diff("ReadBlockOpNumOps")
            blocks += diff("BlocksRead")
            nbytes += diff("BytesRead")
            if "TotalReadTime" in bean:
                have_total_read = True
                total_read_ms += diff("TotalReadTime")
            for name, acc in weighted.items():
                n = diff(name + "NumOps")
                if n > 0:
                    acc[0] += bean.get(name + "AvgTime", 0) * n
                    acc[1] += n

    def avg(name, scale=1.0):
        s, n = weighted[name]
        return f"{s / n * scale:.3f}" if n else "-1"

    return ",".join([
        str(ops), str(blocks), f"{nbytes / 1048576:.1f}",
        avg("ReadBlockOp"),
        avg("SendDataPacketTransferNanos", 1e-3),
        avg("SendDataPacketBlockedOnNetworkNanos", 1e-3),
        str(total_read_ms) if have_total_read else "-1",
    ])


def main(argv):
    if len(argv) >= 3 and argv[0] == "--dn-delta":
        print(dn_delta(argv[1], argv[2]))
        return 0
    if len(argv) >= 2 and argv[0] == "--job-log":
        c = job_log_counters(argv[1])
        print(c["maps"], c["local"], c["map_ms"], c["red_ms"], c["cpu_ms"], c["gc_ms"])
        return 0
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0 if argv else 1
    for run_dir in argv:
        summarize_run(run_dir)
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
