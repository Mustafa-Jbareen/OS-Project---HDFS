#!/usr/bin/env python3
"""Comparison table for a loopback-experiment run (reads RUN_DIR/runs.csv).

Usage: summarize-runs.py RUN_DIR

For every condition and k: number of jobs, mean and standard deviation of the
runtime, and the change vs the smallest k of the same condition with a 95%
confidence interval (Welch t-interval on the difference of means, divided by
the baseline mean). Also shows how the jobs actually ran -- maps running at
once, seconds per map task, data-local share, MB read from disk per job -- so
a condition that did not get the intended load or cache state is visible.

Standard library only (runs on the cluster nodes as well as on a laptop).
"""
import csv
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


def fnum(row, key):
    try:
        return float(row[key])
    except (KeyError, ValueError):
        return 0.0


def main(argv):
    if len(argv) != 1:
        print(__doc__)
        return 1
    path = os.path.join(argv[0], "runs.csv")
    if not os.path.exists(path):
        print(f"no runs.csv in {argv[0]}")
        return 1

    groups = OrderedDict()
    failed = 0
    with open(path) as f:
        for row in csv.DictReader(f):
            if row.get("status") != "ok":
                failed += 1
                continue
            groups.setdefault(row["condition"], OrderedDict()).setdefault(int(row["k"]), []).append(row)
    if not groups:
        print("no successful jobs in runs.csv")
        return 1

    print(f"Run: {os.path.basename(os.path.normpath(argv[0]))}   ({failed} failed jobs excluded)")
    print()
    print(f"{'condition':<14}{'k':>6}{'n':>4}{'mean_s':>9}{'sd_s':>7}   {'vs smallest k [95% CI]':<27}"
          f"{'maps@once':>10}{'map_s':>7}{'local%':>8}{'diskMB/job':>11}")
    changes = OrderedDict()
    for condition, per_k in groups.items():
        ks = sorted(per_k)
        base = [fnum(r, "runtime_s") for r in per_k[ks[0]]]
        for k in ks:
            rows = per_k[k]
            rts = [fnum(r, "runtime_s") for r in rows]
            sd = statistics.stdev(rts) if len(rts) > 1 else 0.0
            if k == ks[0]:
                change = "(baseline)"
            else:
                pct, ci = diff_ci(base, rts)
                changes[(condition, k)] = (pct, ci)
                change = f"{pct:+6.1f}%" + (f" [{ci[0]:+.1f}, {ci[1]:+.1f}]" if ci else "")
            conc = statistics.mean(fnum(r, "avg_concurrent_maps") for r in rows)
            map_s = statistics.mean(fnum(r, "avg_map_s") for r in rows)
            local = statistics.mean(
                100 * fnum(r, "data_local_maps") / max(fnum(r, "launched_maps"), 1) for r in rows)
            disk = statistics.mean(fnum(r, "disk_read_mb") for r in rows)
            print(f"{condition:<14}{k:>6}{len(rts):>4}{statistics.mean(rts):>9.1f}{sd:>7.1f}   {change:<27}"
                  f"{conc:>10.1f}{map_s:>7.2f}{local:>7.0f}%{disk:>11.0f}")
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
        print("How to read it: 'maps@once' and 'diskMB/job' show whether each condition got the")
        print("intended load and cache state (warm jobs should read ~0 MB from disk).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
