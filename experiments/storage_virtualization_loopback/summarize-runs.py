#!/usr/bin/env python3
"""Comparison table for a loopback-experiment run (reads RUN_DIR/runs.csv).

Usage: summarize-runs.py RUN_DIR [--check]

For every condition and k: number of jobs, mean and standard deviation of the
runtime, and the change vs the smallest k of the same condition with a 95%
confidence interval (Welch t-interval on the difference of means, divided by
the baseline mean). Also shows how the jobs actually ran -- maps running at
once, seconds per map task, data-local share, how much of the input was in
the page cache when the job started, MB read from disk during the job -- so a
condition that did not get the intended load or cache state is visible.

--check  verifies that the run did what it was meant to (every job finished,
         the load levels differ, cold jobs read from disk and warm jobs do
         not, locality, no swapping) and prints PASS / WARN / FAIL per item.
         Exit code 1 if anything FAILs. Used by `run-2x2.sh smoke`.

Standard library only (runs on the cluster nodes as well as on a laptop).
"""
import csv
import glob
import json
import math
import os
import statistics
import sys
from collections import OrderedDict

# Two-sided 95% Student t quantiles for 1..30 degrees of freedom.
T975 = [12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
        2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
        2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042]


def t975(df):
    if df < 1:
        return float("nan")
    return T975[int(df) - 1] if df < 31 else 1.96


def diff_ci(base, other):
    """Percent change of mean(other) vs mean(base), with a 95% CI."""
    m0, m1 = statistics.mean(base), statistics.mean(other)
    pct = (m1 / m0 - 1) * 100
    if len(base) < 2 or len(other) < 2:
        return pct, None
    v0, v1 = statistics.variance(base) / len(base), statistics.variance(other) / len(other)
    se = math.sqrt(v0 + v1)
    if se == 0:
        return pct, (pct, pct)
    df = (v0 + v1) ** 2 / (v0 ** 2 / (len(base) - 1) + v1 ** 2 / (len(other) - 1))
    half = t975(df) * se / m0 * 100
    return pct, (pct - half, pct + half)


def fnum(row, key, default=0.0):
    try:
        return float(row[key])
    except (KeyError, ValueError, TypeError):
        return default


def load(run_dir):
    rows = []
    with open(os.path.join(run_dir, "runs.csv")) as f:
        rows = list(csv.DictReader(f))
    meta = {}
    meta_path = os.path.join(run_dir, "metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)
    return rows, meta


def replica_mb(meta):
    """MB of block files on all DataNodes when the whole input is stored."""
    return meta.get("input_size_mb", 0) * meta.get("replication", 3)


def cached_pct(row, meta):
    cached = fnum(row, "cached_input_mb", -1)
    total = replica_mb(meta)
    if cached < 0 or total <= 0:
        return None
    return min(100.0, 100.0 * cached / total)


def print_table(rows, meta, run_dir):
    groups = OrderedDict()
    failed = sum(1 for r in rows if r.get("status") in ("failed", "warmup-failed"))
    for r in rows:
        if r.get("status") == "ok":
            groups.setdefault(r["condition"], OrderedDict()).setdefault(int(r["k"]), []).append(r)
    if not groups:
        print("no successful measured jobs in runs.csv")
        return None

    print(f"Run: {os.path.basename(os.path.normpath(run_dir))}   "
          f"({failed} failed jobs excluded; warm-up jobs not counted)")
    print()
    print(f"{'condition':<14}{'k':>6}{'n':>4}{'mean_s':>9}{'sd_s':>7}   {'vs smallest k [95% CI]':<27}"
          f"{'maps@once':>10}{'map_s':>7}{'local%':>8}{'cached@start':>13}{'diskMB/job':>11}")
    changes = OrderedDict()
    for condition, per_k in groups.items():
        ks = sorted(per_k)
        base = [fnum(r, "runtime_s") for r in per_k[ks[0]]]
        for k in ks:
            sel = per_k[k]
            rts = [fnum(r, "runtime_s") for r in sel]
            sd = statistics.stdev(rts) if len(rts) > 1 else 0.0
            if k == ks[0]:
                change = "(baseline)"
            else:
                pct, ci = diff_ci(base, rts)
                changes[(condition, k)] = (pct, ci)
                change = f"{pct:+6.1f}%" + (f" [{ci[0]:+.1f}, {ci[1]:+.1f}]" if ci else "")
            conc = statistics.mean(fnum(r, "avg_concurrent_maps") for r in sel)
            map_s = statistics.mean(fnum(r, "avg_map_s") for r in sel)
            local = statistics.mean(
                100 * fnum(r, "data_local_maps") / max(fnum(r, "launched_maps"), 1) for r in sel)
            cps = [c for c in (cached_pct(r, meta) for r in sel) if c is not None]
            cached = f"{statistics.mean(cps):.0f}%" if cps else "n/a"
            disk = statistics.mean(fnum(r, "disk_read_mb") for r in sel)
            print(f"{condition:<14}{k:>6}{len(rts):>4}{statistics.mean(rts):>9.1f}{sd:>7.1f}   {change:<27}"
                  f"{conc:>10.1f}{map_s:>7.2f}{local:>7.0f}%{cached:>13}{disk:>11.0f}")
        print()

    if len(groups) > 1 and changes:
        print("Change in runtime at the largest k vs the smallest k, per condition:")
        for condition, per_k in groups.items():
            ks = sorted(per_k)
            if len(ks) < 2 or (condition, ks[-1]) not in changes:
                continue
            pct, ci = changes[(condition, ks[-1])]
            verdict = ""
            if ci:
                verdict = "  slower" if ci[0] > 0 else ("  faster" if ci[1] < 0 else "  no clear difference")
            ci_text = f" [{ci[0]:+.1f}, {ci[1]:+.1f}]" if ci else ""
            print(f"  {condition:<14} k={ks[0]} -> k={ks[-1]}: {pct:+.1f}%{ci_text}{verdict}")
        print()
    print("How to read it: 'maps@once', 'cached@start' and 'diskMB/job' show whether each")
    print("condition got the intended load and cache state (cold: ~0% cached, reads ~ the")
    print("input size from disk; warm: ~100% cached, ~0 MB from disk).")
    return groups


def swap_activity(run_dir):
    """Max si/so (swap in/out, KB/s) per node from the vmstat logs."""
    result = {}
    for path in glob.glob(os.path.join(run_dir, "sysstat", "vmstat_k*_*.log")):
        node = os.path.basename(path)[:-4].split("_", 2)[-1]
        idx = None
        with open(path, errors="ignore") as f:
            for line in f:
                parts = line.split()
                if "si" in parts and "so" in parts:
                    idx = (parts.index("si"), parts.index("so"))
                    continue
                if idx and parts and parts[0].isdigit():
                    try:
                        si, so = int(parts[idx[0]]), int(parts[idx[1]])
                    except (ValueError, IndexError):
                        continue
                    old = result.get(node, (0, 0))
                    result[node] = (max(old[0], si), max(old[1], so))
    return result


def check(rows, meta, run_dir):
    results = []

    def add(level, text):
        results.append((level, text))

    measured = [r for r in rows if r.get("status") in ("ok", "failed")]
    warmups = [r for r in rows if r.get("status", "").startswith("warmup")]
    ok = [r for r in measured if r["status"] == "ok"]
    failed = [r for r in rows if r.get("status") in ("failed", "warmup-failed")]
    input_mb = meta.get("input_size_mb", 0)
    nodes = meta.get("datanode_hosts", 4)
    slots = meta.get("yarn_slots_per_node", 0)

    # 1. every job finished
    if not measured:
        add("FAIL", "no measured jobs in runs.csv")
    elif failed:
        add("FAIL", f"{len(failed)} job(s) failed -- see the jobs/ folder")
    else:
        add("PASS", f"all {len(measured)} measured and {len(warmups)} warm-up jobs finished")

    # 2. every condition x k measured, with counters
    conds = OrderedDict()
    for r in ok:
        conds.setdefault(r["condition"], {}).setdefault(int(r["k"]), []).append(r)
    ks = sorted({int(r["k"]) for r in measured})
    missing = [f"{c}@k={k}" for c in conds for k in ks if k not in conds[c]]
    if missing:
        add("FAIL", "no successful job for " + ", ".join(missing))
    elif conds:
        add("PASS", f"{len(conds)} conditions x {len(ks)} k values all measured")
    if ok and any(fnum(r, "launched_maps") <= 0 for r in ok):
        add("FAIL", "job counters missing in some jobs (analyze-counters.py could not read them)")

    # 3. load levels
    conc = {c: statistics.mean(fnum(r, "avg_concurrent_maps") for rs in per_k.values() for r in rs)
            for c, per_k in conds.items()}
    maps_of = {c: int(fnum(next(iter(per_k.values()))[0], "maps_per_node")) for c, per_k in conds.items()}
    for c, m in maps_of.items():
        if m == 1:
            if conc[c] <= nodes + 0.5:
                add("PASS", f"{c}: {conc[c]:.1f} maps at once (light load: at most 1 per node)")
            else:
                add("FAIL", f"{c}: {conc[c]:.1f} maps at once, expected at most {nodes} (1 per node)")
        elif slots:
            target = m * nodes
            if conc[c] >= 0.4 * target:
                add("PASS", f"{c}: {conc[c]:.1f} maps at once (target up to {target})")
            else:
                add("WARN", f"{c}: only {conc[c]:.1f} maps at once (target up to {target}); "
                            "the job may be too small to fill the slots")
    light = [conc[c] for c, m in maps_of.items() if m == 1]
    heavy = [conc[c] for c, m in maps_of.items() if m > 1]
    if light and heavy:
        if min(heavy) > 2 * max(light):
            add("PASS", f"load levels differ clearly ({max(light):.1f} vs {min(heavy):.1f} maps at once)")
        else:
            add("FAIL", f"load levels too close ({max(light):.1f} vs {min(heavy):.1f} maps at once)")

    # 4. cache state
    for c, per_k in conds.items():
        rs = [r for v in per_k.values() for r in v]
        mode = rs[0]["cache"]
        disk = statistics.mean(fnum(r, "disk_read_mb") for r in rs)
        cps = [p for p in (cached_pct(r, meta) for r in rs) if p is not None]
        cached = statistics.mean(cps) if cps else None
        cached_txt = f"{cached:.0f}% of the input cached at start" if cached is not None else "cached share n/a (no fincore)"
        if input_mb <= 0:
            add("WARN", f"{c}: input size unknown (metadata.json), cache not checked")
            continue
        if mode == "cold":
            if disk >= 0.5 * input_mb:
                add("PASS", f"{c}: read {disk:.0f} MB from disk per job (input {input_mb} MB); {cached_txt}")
            else:
                add("FAIL", f"{c}: read only {disk:.0f} MB from disk per job (input {input_mb} MB) -- "
                            f"eviction did not work or disk counters are wrong; {cached_txt}")
        else:
            if disk <= 0.2 * input_mb:
                add("PASS", f"{c}: read {disk:.0f} MB from disk per job (input {input_mb} MB); {cached_txt}")
            else:
                add("FAIL", f"{c}: read {disk:.0f} MB from disk per job (input {input_mb} MB) -- the input "
                            f"does not stay in RAM; use a smaller input for warm runs; {cached_txt}")

    # 5. locality
    for c, per_k in conds.items():
        rs = [r for v in per_k.values() for r in v]
        local = statistics.mean(100 * fnum(r, "data_local_maps") / max(fnum(r, "launched_maps"), 1) for r in rs)
        if maps_of.get(c, 1) > 1:
            level = "PASS" if local >= 70 else "WARN"
            add(level, f"{c}: {local:.0f}% of map tasks read their block locally")

    # 6. measurements beyond runtime
    dn_path = os.path.join(run_dir, "dn_metrics.csv")
    dn_rows = []
    if os.path.exists(dn_path):
        with open(dn_path) as f:
            dn_rows = [r for r in csv.DictReader(f) if r.get("status") == "ok"]
    timed = [r for r in dn_rows if fnum(r, "dn_read_block_avg_ms", -1) > 0]
    if not dn_rows:
        add("WARN", "no DataNode metrics (dn_metrics.csv) -- DataNode JMX not reachable on port 9864?")
    elif len(dn_rows) < len(ok):
        add("WARN", f"DataNode metrics for only {len(dn_rows)} of {len(ok)} jobs")
    else:
        add("PASS", f"DataNode metrics for all {len(dn_rows)} jobs"
                    + (" (time per block served available)" if timed else " (no per-block timing)"))

    srv_path = os.path.join(run_dir, "server_metrics.csv")
    srv = []
    if os.path.exists(srv_path):
        with open(srv_path) as f:
            srv = list(csv.DictReader(f))
    srv_ks = sorted({int(r["k"]) for r in srv})
    if not srv:
        add("WARN", "no server metrics (server_metrics.csv)")
    else:
        add("PASS" if srv_ks == ks else "WARN", f"server metrics for k = {', '.join(map(str, srv_ks))}")
        sizes = {r.get("fs_block_size") for r in srv}
        if meta.get("mkfs_mode") == "fixed" and len(sizes) > 1:
            add("FAIL", f"loopback filesystems have different block sizes {sorted(sizes)} despite mkfs_mode=fixed")
        else:
            add("PASS", f"loopback filesystem block size: {', '.join(sorted(sizes))} bytes")

    for tool in ("pidstat_datanode", "vmstat"):
        logs = glob.glob(os.path.join(run_dir, "sysstat", f"{tool}_k*_*.log"))
        samples = 0
        for path in logs:
            with open(path, errors="ignore") as f:
                samples += sum(1 for line in f if line.split()[:1] and line.split()[0].isdigit())
        if samples:
            add("PASS", f"{tool} monitor: {samples} samples in {len(logs)} logs")
        else:
            add("WARN", f"{tool} monitor recorded nothing (is sysstat/procps installed on the workers?)")

    others = [fnum(r, "other_users_cpu_pct", -1) for r in measured]
    busy = [v for v in others if v > 50]
    if others and max(others) < 0:
        add("WARN", "other users' CPU not measured (pidstat missing)")
    elif busy:
        add("WARN", f"{len(busy)} job(s) started while other users' processes used >50% CPU on a DataNode host")
    else:
        add("PASS", f"no other users' load before the jobs (max {max(others, default=0):.0f}% CPU)")

    # 7. swapping
    swaps = swap_activity(run_dir)
    if not swaps:
        add("WARN", "no vmstat logs found, swapping not checked")
    else:
        bad = {n: v for n, v in swaps.items() if v[0] > 0 or v[1] > 0}
        if bad:
            add("WARN", "swapping seen (max si/so KB/s): " +
                ", ".join(f"{n} {si}/{so}" for n, (si, so) in sorted(bad.items())))
        else:
            add("PASS", f"no swapping on {len(swaps)} DataNode host(s)")

    print()
    print("Checks:")
    for level, text in results:
        print(f"  {level:<5} {text}")
    fails = sum(1 for level, _ in results if level == "FAIL")
    warns = sum(1 for level, _ in results if level == "WARN")
    print()
    if fails:
        print(f"Result: NOT READY -- {fails} check(s) failed. Fix those before the full run.")
    elif warns:
        print(f"Result: READY, with {warns} warning(s) worth a look.")
    else:
        print("Result: READY for the full run.")
    return fails == 0


def main(argv):
    args = [a for a in argv if not a.startswith("--")]
    if len(args) != 1:
        print(__doc__)
        return 1
    run_dir = args[0]
    if not os.path.exists(os.path.join(run_dir, "runs.csv")):
        print(f"no runs.csv in {run_dir}")
        return 1
    rows, meta = load(run_dir)
    print_table(rows, meta, run_dir)
    if "--check" in argv:
        return 0 if check(rows, meta, run_dir) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
