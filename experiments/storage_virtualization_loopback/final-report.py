#!/usr/bin/env python3
"""Final report of an experiment pipeline (run-all.sh).

Usage:
  final-report.py PIPELINE_DIR | RUN_DIR
      Writes FINAL_REPORT.md into the folder, plus figures/fig1..fig5 as PNG
      and PDF when matplotlib is installed (e.g. run it again on the laptop
      after pulling). A single run folder gets the sections that apply to it.
  final-report.py --bench-summary RUN_DIR
      Table of one storage-bench.sh run (used by storage-bench.sh).

PIPELINE_DIR/stages.env names the run directory of each stage (MAIN_RUN,
BENCH_RUN, DIO_RUN, MKFS_RUN, SMOKE_RUN). Every comparison comes with a 95%
confidence interval; "clearly" means the interval excludes zero.
Standard library only, apart from the optional figures.
"""
import calendar
import contextlib
import csv
import glob
import importlib.util
import io
import json
import math
import os
import statistics
import sys
import time
from collections import OrderedDict, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("summarize_runs", os.path.join(HERE, "summarize-runs.py"))
sr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sr)


# ---------------------------------------------------------------- helpers
def read_csv(path):
    if not path or not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def read_json(path):
    if not path or not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def fnum(v, default=None):
    try:
        x = float(v)
        return x if not math.isnan(x) else default
    except (TypeError, ValueError):
        return default


def mean(xs):
    xs = [x for x in xs if x is not None]
    return statistics.mean(xs) if xs else None


def change(base, other):
    """(% change of mean(other) vs mean(base), (lo, hi) or None)."""
    base = [x for x in base if x is not None]
    other = [x for x in other if x is not None]
    if not base or not other:
        return None, None
    return sr.diff_ci(base, other)


def fmt_change(pct, ci):
    if pct is None:
        return "n/a"
    return f"{pct:+.1f}%" + (f" [{ci[0]:+.1f}, {ci[1]:+.1f}]" if ci else "")


def diff_changes(a, b):
    """Difference of two % changes (a - b), CI from the two half-widths."""
    (pa, ca), (pb, cb) = a, b
    if pa is None or pb is None:
        return None, None
    if not ca or not cb:
        return pa - pb, None
    half = math.sqrt(((ca[1] - ca[0]) / 2) ** 2 + ((cb[1] - cb[0]) / 2) ** 2)
    d = pa - pb
    return d, (d - half, d + half)


def fmt_points(d, ci):
    if d is None:
        return "n/a"
    return f"{d:+.1f} points" + (f" [{ci[0]:+.1f}, {ci[1]:+.1f}]" if ci else "")


def fmt_mb(mb):
    """102400 -> '100 GB', 1536 -> '1.5 GB', 512 -> '512 MB'."""
    try:
        mb = float(mb)
    except (TypeError, ValueError):
        return "?"
    return f"{mb / 1024:g} GB" if mb >= 1024 else f"{mb:g} MB"


def verdict(ci, pos="larger", neg="smaller"):
    if not ci:
        return "(too few repetitions for an interval)"
    if ci[0] > 0:
        return f"clearly {pos}"
    if ci[1] < 0:
        return f"clearly {neg}"
    return "no clear difference"


def fmt(v, digits=1):
    return "n/a" if v is None else f"{v:.{digits}f}"


def md_table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return out


def resolve_run(pipe_dir, path):
    """Stage run folders are stored relative to the pipeline folder, so the
    pipeline can be copied to another machine; absolute paths from an older
    pipeline fall back to their last two components under pipe_dir."""
    if not path:
        return path
    if not os.path.isabs(path) and not path.startswith("/"):
        return os.path.join(pipe_dir, path)
    if os.path.exists(path):
        return path
    parts = path.replace("\\", "/").rstrip("/").split("/")
    candidate = os.path.join(pipe_dir, *parts[-2:])
    return candidate if os.path.exists(candidate) else path


def load_stages(pipe_dir):
    stages = {}
    path = os.path.join(pipe_dir, "stages.env")
    if os.path.exists(path):
        for line in open(path):
            if "=" in line:
                k, v = line.strip().split("=", 1)
                stages[k] = resolve_run(pipe_dir, v) if k.endswith("_RUN") else v
    return stages


# ---------------------------------------------------------------- run data
class Run:
    """One run of run-experiment-loopback-fs.sh."""

    def __init__(self, run_dir):
        self.dir = run_dir
        self.meta = read_json(os.path.join(run_dir, "metadata.json"))
        self.rows = [r for r in read_csv(os.path.join(run_dir, "runs.csv")) if r.get("status") == "ok"]
        self.all_rows = read_csv(os.path.join(run_dir, "runs.csv"))
        self.dn = {(r["k"], r["rep"], r["order_pos"], r["condition"]): r
                   for r in read_csv(os.path.join(run_dir, "dn_metrics.csv"))}
        self.server = read_csv(os.path.join(run_dir, "server_metrics.csv"))
        self.ks = sorted({int(r["k"]) for r in self.rows})
        self.conditions = list(OrderedDict.fromkeys(r["condition"] for r in self.rows))
        self._samples = None

    def cond_info(self, c):
        r = next(r for r in self.rows if r["condition"] == c)
        return int(r["maps_per_node"]), r["cache"]

    def jobs(self, c, k):
        return [r for r in self.rows if r["condition"] == c and int(r["k"]) == k]

    def runtimes(self, c, k):
        return [fnum(r["runtime_s"]) for r in self.jobs(c, k)]

    def per_job(self, c, k, field):
        return [fnum(r.get(field)) for r in self.jobs(c, k)]

    def cpu_per_map(self, c, k):
        return [fnum(r["cpu_ms"]) / 1000 / max(fnum(r["launched_maps"], 1), 1) for r in self.jobs(c, k)]

    def dn_field(self, c, k, field):
        vals = []
        for r in self.jobs(c, k):
            d = self.dn.get((r["k"], r["rep"], r["order_pos"], r["condition"]))
            v = fnum(d.get(field)) if d else None
            vals.append(v if v is not None and v >= 0 else None)
        return vals

    def change_k(self, c, values_fn):
        if len(self.ks) < 2:
            return None, None
        return change(values_fn(c, self.ks[0]), values_fn(c, self.ks[-1]))

    # monitor samples (pidstat: epoch; vmstat: UTC) assigned to jobs
    def samples(self):
        if self._samples is not None:
            return self._samples
        pid, vm = defaultdict(list), defaultdict(list)
        for path in glob.glob(os.path.join(self.dir, "sysstat", "pidstat_datanode_k*_*.log")):
            k = int(os.path.basename(path).split("_k")[1].split("_")[0])
            cols = None
            for line in open(path, errors="ignore"):
                t = line.split()
                if t and t[0] == "#":
                    cols = t[1:]
                    continue
                if cols and t and t[0].isdigit() and len(t) >= len(cols):
                    rec = dict(zip(cols, t))
                    pid[k].append((int(t[0]), fnum(rec.get("%CPU")), fnum(rec.get("RSS"))))
        for path in glob.glob(os.path.join(self.dir, "sysstat", "vmstat_k*_*.log")):
            k = int(os.path.basename(path).split("_k")[1].split("_")[0])
            cols = None
            for line in open(path, errors="ignore"):
                t = line.split()
                if "us" in t and "sy" in t:
                    cols = t
                    continue
                if cols and t and t[0].isdigit() and len(t) >= 2:
                    try:
                        ts = calendar.timegm(time.strptime(t[-2] + " " + t[-1], "%Y-%m-%d %H:%M:%S"))
                    except ValueError:
                        continue
                    rec = dict(zip(cols, t))
                    vm[k].append((ts, fnum(rec.get("us")), fnum(rec.get("sy")), fnum(rec.get("wa"))))
        self._samples = (pid, vm)
        return self._samples

    def monitor(self, c, k, which):
        pid, vm = self.samples()
        out = []
        for r in self.jobs(c, k):
            t0, t1 = int(r["start_epoch"]), int(r["end_epoch"])
            if which == "dn_cpu":
                v = [s[1] for s in pid.get(k, []) if t0 <= s[0] <= t1 and s[1] is not None]
            elif which == "dn_rss":
                v = [s[2] / 1024 for s in pid.get(k, []) if t0 <= s[0] <= t1 and s[2] is not None]
            else:
                idx = {"sys": 2, "usr": 1, "iowait": 3}[which]
                v = [s[idx] for s in vm.get(k, []) if t0 <= s[0] <= t1 and s[idx] is not None]
            out.append(mean(v))
        return out


# ---------------------------------------------------------------- sections
def section_setup(run, lines):
    m = run.meta
    lines += ["## Setup", ""]
    hw = m.get("hardware", [])
    if hw:
        lines += md_table(["node", "cores", "RAM MB", "disk", "rotational", "model", "kernel", "java", "hadoop"],
                          [[h.get("node"), h.get("cores"), h.get("mem_mb"), h.get("scratch_device"),
                            h.get("rotational"), h.get("model"), h.get("kernel"), h.get("java", "?"),
                            h.get("hadoop", "?")] for h in hw])
        lines.append("")
    lines += [
        f"- Cluster **{m.get('cluster')}**, DataNode hosts: {', '.join(m.get('datanode_host_names', []))}; code {m.get('code_version')}",
        f"- YARN pool per node: {m.get('yarn_slots_per_node')} x {m.get('yarn_container_mb')} MB; task heap {m.get('task_heap_mb')} MB; DataNode heap {m.get('datanode_heap_mb')} MB",
        f"- Input {fmt_mb(m.get('input_size_mb'))} in {m.get('block_size_human')} blocks, replication {m.get('replication')}, uploaded {m.get('input_uploaded')}",
        f"- Loopback budget {m.get('loopback_budget_per_node_gb')} GB per host; mkfs {m.get('mkfs_mode')}; direct I/O {m.get('loop_direct_io')}",
        f"- k values {m.get('k_values')} (order {m.get('k_order')}), {m.get('repetitions')} repetitions per k and condition, seed {m.get('seed')}",
        f"- Per-job protocol: {m.get('per_job_protocol')}; {m.get('warmup_jobs_per_k')} warm-up job(s) per k; speculative execution {m.get('speculative_execution')}",
        "",
    ]


def section_main(run, lines, figs):
    lines += ["## 1. Runtime vs k", ""]
    header = ["condition"] + [f"k={k}" for k in run.ks]
    rows = []
    for c in run.conditions:
        cells = []
        for k in run.ks:
            rt = [x for x in run.runtimes(c, k) if x is not None]
            cells.append(f"{statistics.mean(rt):.1f} ± {statistics.stdev(rt):.1f} s (n={len(rt)})"
                         if len(rt) > 1 else (f"{rt[0]:.1f} s" if rt else "n/a"))
        rows.append([c] + cells)
    lines += md_table(header, rows) + [""]
    lines += ["Change vs k=%d (95%% CI):" % run.ks[0], ""]
    rows = []
    for c in run.conditions:
        base = run.runtimes(c, run.ks[0])
        rows.append([c] + [fmt_change(*change(base, run.runtimes(c, k))) for k in run.ks[1:]])
    lines += md_table(["condition"] + [f"k={k}" for k in run.ks[1:]], rows) + [""]
    if figs.enabled and len(run.ks) >= 2:
        figure_runtime(run, figs, lines)


def section_factors(run, lines):
    lines += ["## 2. Which factor makes the slowdown appear", ""]
    if len(run.ks) < 2:
        lines += ["(needs at least two k values)", ""]
        return
    kmax = run.ks[-1]
    info = {c: run.cond_info(c) for c in run.conditions}
    by = {(m, cache): c for c, (m, cache) in info.items()}
    mmax = max(m for m, _ in info.values())
    ch = {c: run.change_k(c, run.runtimes) for c in run.conditions}
    lines.append(f"Runtime change from k={run.ks[0]} to k={kmax}:")
    lines.append("")
    for c in run.conditions:
        lines.append(f"- {c}: {fmt_change(*ch[c])} -- {verdict(ch[c][1], 'slower', 'faster')}")
    lines.append("")
    for cache in ("cold", "warm"):
        if (1, cache) in by and (mmax, cache) in by:
            d, ci = diff_changes(ch[by[(mmax, cache)]], ch[by[(1, cache)]])
            lines.append(f"- **Load ({cache} cache):** slowdown at k={kmax} with {load_label(mmax)} minus "
                         f"slowdown with 1 map/node: {fmt_points(d, ci)} -- {verdict(ci)}.")
    middle = sorted(m for m, cache in info.values() if 1 < m < mmax and cache == "cold")
    if middle and (1, "cold") in by and (mmax, "cold") in by:
        steps = [(1, ch[by[(1, 'cold')]])] + [(m, ch[by[(m, 'cold')]]) for m in middle] + [(mmax, ch[by[(mmax, 'cold')]])]
        lines.append("- **Load steps (cold):** " + ", ".join(f"{load_label(m)} {fmt_change(*v)}" for m, v in steps))
    for m in sorted({m for m, _ in info.values()}):
        if (m, "cold") in by and (m, "warm") in by:
            d, ci = diff_changes(ch[by[(m, "warm")]], ch[by[(m, "cold")]])
            lines.append(f"- **Page cache ({load_label(m)}):** slowdown warm minus slowdown cold: "
                         f"{fmt_points(d, ci)} -- {verdict(ci)}.")
    lines.append("")


def section_where(run, lines, figs):
    lines += ["## 3. Where the time goes", "",
              f"Per condition, k={run.ks[0]} -> k={run.ks[-1]} (mean per job; change with 95% CI):", ""]
    if len(run.ks) < 2:
        return
    k0, k1 = run.ks[0], run.ks[-1]
    rows = []
    for c in run.conditions:
        def pair(values_fn, digits=2):
            a, b = mean(values_fn(c, k0)), mean(values_fn(c, k1))
            pct, ci = change(values_fn(c, k0), values_fn(c, k1))
            return f"{fmt(a, digits)} -> {fmt(b, digits)} ({fmt_change(pct, ci)})"
        rows.append([
            c,
            pair(lambda c_, k: run.per_job(c_, k, "avg_map_s")),
            pair(run.cpu_per_map),
            pair(lambda c_, k: run.dn_field(c_, k, "dn_read_block_avg_ms"), 1),
            pair(lambda c_, k: run.monitor(c_, k, "dn_cpu"), 1),
            pair(lambda c_, k: run.monitor(c_, k, "sys"), 1),
            pair(lambda c_, k: run.monitor(c_, k, "iowait"), 1),
        ])
    lines += md_table(["condition", "map task (s)", "CPU per map task (s)", "DataNode ms per block served",
                       "DataNode %CPU", "node %sys", "node %iowait"], rows)
    lines += ["", "How to read it: if the time per map task and the DataNode's time per block rise "
              "together while %sys rises, the extra cost sits in the storage path (DataNode + kernel); "
              "if only the map tasks' CPU time rises, it is contention on the node's CPU.", ""]
    if figs.enabled:
        figure_where(run, figs, lines)


def section_server(run, lines, figs):
    lines += ["## 4. What k virtual disks cost the server (per DataNode host, mean over hosts)", ""]
    if not run.server:
        lines += ["(no server_metrics.csv)", ""]
        return
    by_k = defaultdict(list)
    for r in run.server:
        by_k[int(r["k"])].append(r)
    cols = [("cluster setup s", "cluster_setup_s"), ("DataNode start -> 1st block report s", "dn_start_to_block_report_s"),
            ("block report: storages", "block_report_storages"), ("generate ms", "block_report_generate_ms"),
            ("RPC+NameNode ms", "block_report_rpc_ms"), ("DataNode threads", "dn_threads"),
            ("DataNode RSS MB", "dn_rss_mb"), ("heap used MB", "dn_heap_used_mb"), ("open files", "dn_fds"),
            ("kernel loop/jbd2 threads", "kernel_loop_threads"), ("upload MB/s", "upload_mb_per_s"),
            ("fs block size", "fs_block_size")]
    rows = []
    for k in sorted(by_k):
        rows.append([k] + [fmt(mean([fnum(r.get(f)) for r in by_k[k]]), 0 if f != "upload_mb_per_s" else 1)
                           for _, f in cols])
    lines += md_table(["k"] + [c for c, _ in cols], rows) + [""]
    if figs.enabled:
        figure_server(run, figs, lines)


def bench_table(run_dir):
    rows = read_csv(os.path.join(run_dir, "bench.csv"))
    meta = read_json(os.path.join(run_dir, "metadata.json"))
    if not rows:
        return None, None, None
    cells = OrderedDict()
    for r in rows:
        cells.setdefault((int(r["readers"]), r["cache"]), defaultdict(list))[int(r["k"])].append(r)
    ks = sorted({int(r["k"]) for r in rows})
    table = []
    changes = {}
    for (readers, cache), per_k in sorted(cells.items()):
        base = [fnum(r["seconds"]) for r in per_k[ks[0]]]
        for k in ks:
            rs = per_k.get(k, [])
            secs = [fnum(r["seconds"]) for r in rs]
            pct, ci = change(base, secs) if k != ks[0] else (None, None)
            if k == ks[-1]:
                changes[(readers, cache)] = (pct, ci)
            gb = [fnum(r["mb"], 0) / 1024 for r in rs]
            sys_s = [fnum(r["sys_ticks"], 0) / max(fnum(r["clk_tck"], 100), 1) for r in rs]
            sys_per_gb = mean([s / g for s, g in zip(sys_s, gb) if g])
            table.append([readers, cache, k, len(rs), fmt(mean(secs), 2),
                          "(baseline)" if k == ks[0] else fmt_change(pct, ci),
                          fmt(mean([fnum(r["mb_per_s"]) for r in rs])), fmt(sys_per_gb, 2),
                          fmt(mean([fnum(r["disk_read_mb"]) for r in rs]), 0)])
    header = ["readers", "cache", "k", "n", "seconds", f"vs k={ks[0]} [95% CI]", "MB/s per host",
              "kernel CPU s per GB", "disk MB read"]
    return (header, table), changes, meta


def section_bench(bench_dir, lines, figs):
    lines += ["## 5. The storage stack alone (no Hadoop)", ""]
    header_table, changes, meta = bench_table(bench_dir) if bench_dir else (None, None, None)
    if not header_table:
        lines += ["(storage benchmark not run or no data)", ""]
        return
    lines += [f"Same loopback disks and data per host as the HDFS runs ({fmt_mb(meta.get('data_mb_per_host'))} in "
              f"{meta.get('file_mb')} MB files), read by plain parallel readers.", ""]
    lines += md_table(*header_table) + [""]
    for (readers, cache), (pct, ci) in sorted(changes.items()):
        lines.append(f"- {readers} reader(s), {cache}: {fmt_change(pct, ci)} -- {verdict(ci, 'slower', 'faster')}")
    lines += ["", "If the storage stack alone slows down like the HDFS runs, the cost is in the kernel / "
              "loopback layer; if it does not, it is in HDFS (the DataNode).", ""]
    if figs.enabled:
        figure_bench(bench_dir, figs, lines)


def section_control(main, other, title, what, lines):
    lines += [f"### {title}", ""]
    if not other or not other.rows:
        lines += ["(not run or no data)", ""]
        return
    lines.append(what)
    lines.append("")
    for c in other.conditions:
        a = other.change_k(c, other.runtimes)
        b = main.change_k(c, main.runtimes) if c in main.conditions else (None, None)
        d, ci = diff_changes(a, b)
        lines.append(f"- {c}: slowdown k={other.ks[0]}->k={other.ks[-1]} {fmt_change(*a)} here vs "
                     f"{fmt_change(*b)} in the main run; here minus main: {fmt_points(d, ci)} -- {verdict(ci)}.")
    sizes = sorted({r.get("fs_block_size") for r in other.server if r.get("k") == str(other.ks[-1])})
    if sizes:
        lines.append(f"- filesystem block size at k={other.ks[-1]}: {', '.join(sizes)} bytes")
    lines.append("")


def section_quality(run, lines):
    lines += ["## 7. Data quality (main run)", ""]
    buf = io.StringIO()
    rows_all, meta = sr.load(run.dir)
    with contextlib.redirect_stdout(buf):
        sr.check(rows_all, meta, run.dir)
    lines += ["```"] + [l for l in buf.getvalue().splitlines() if l.strip()] + ["```", ""]


# ---------------------------------------------------------------- figures
# Five figures, one per part of the conclusion (drawn by report_figures.py):
#   fig1 runtime vs k, fig2 where the time goes, fig3 storage stack alone,
#   fig4 controls, fig5 server cost vs k.
CACHE_TITLES = {"cold": "Cold cache: input read from disk", "warm": "Warm cache: input already in RAM"}
CACHE_SHORT = {"cold": "Cold cache", "warm": "Warm cache"}


class FigureMaker:
    def __init__(self, out_dir, meta):
        self.out_dir = out_dir
        try:
            import report_figures
            self.rf = report_figures
            os.makedirs(out_dir, exist_ok=True)
            self.enabled = True
        except Exception as e:  # matplotlib missing (e.g. on the cluster)
            self.rf = None
            self.enabled = False
            self.why = str(e)
        hosts = meta.get("datanode_hosts") or len(meta.get("datanode_host_names", []))
        self.subtitle = (f"{meta.get('cluster', '?')}, {hosts} DataNode hosts; input {fmt_mb(meta.get('input_size_mb'))} "
                         f"in {meta.get('block_size_human', '?')} blocks; {meta.get('repetitions', '?')} jobs per point; "
                         f"shaded band = 95% confidence interval")

    def path(self, name):
        return os.path.join(self.out_dir, name)


def load_label(m):
    return f"{m} map{'s' if m != 1 else ''}/node"


def change_series(run, cond, values_fn, level=None, label=None):
    """% change vs the smallest k, with 95% CI, as a report_figures series."""
    maps, _ = run.cond_info(cond)
    base = values_fn(cond, run.ks[0])
    ys, los, his = [], [], []
    for k in run.ks:
        if k == run.ks[0]:
            ys.append(0.0 if any(v is not None for v in base) else None)
            los.append(0.0)
            his.append(0.0)
            continue
        pct, ci = change(base, values_fn(cond, k))
        ys.append(pct)
        los.append(ci[0] if ci else None)
        his.append(ci[1] if ci else None)
    return {"level": level if level is not None else maps, "label": label or load_label(maps),
            "x": list(run.ks), "y": ys, "lo": los, "hi": his}


def conds_by_cache(run):
    out = OrderedDict()
    for cache in ("cold", "warm"):
        conds = [c for c in run.conditions if run.cond_info(c)[1] == cache]
        if conds:
            out[cache] = conds
    return out


def figure_runtime(run, figs, lines):
    groups = conds_by_cache(run)
    cells = [[[change_series(run, c, run.runtimes) for c in conds] for conds in groups.values()]]
    figs.rf.change_grid(figs.path("fig1_runtime_vs_k"), "WordCount runtime vs k", figs.subtitle, cells,
                        run.ks, f"runtime change vs k={run.ks[0]}",
                        col_titles=[CACHE_TITLES[c] for c in groups])
    lines += ["![Figure 1: runtime vs k](figures/fig1_runtime_vs_k.png)", ""]


def figure_where(run, figs, lines):
    measures = [("Time per map task", lambda c, k: run.per_job(c, k, "avg_map_s")),
                ("DataNode time per block served", lambda c, k: run.dn_field(c, k, "dn_read_block_avg_ms")),
                ("CPU time per map task", run.cpu_per_map)]
    groups = conds_by_cache(run)
    cells = [[[change_series(run, c, fn) for c in conds] for _, fn in measures] for conds in groups.values()]
    figs.rf.change_grid(figs.path("fig2_where_the_time_goes"), "Where the extra time goes", figs.subtitle,
                        cells, run.ks, f"change vs k={run.ks[0]}",
                        row_titles=[CACHE_SHORT[c] for c in groups], col_titles=[t for t, _ in measures],
                        direct_labels=False, sharey="col")
    lines += ["![Figure 2: where the time goes](figures/fig2_where_the_time_goes.png)", ""]


def figure_bench(bench_dir, figs, lines):
    rows = read_csv(os.path.join(bench_dir, "bench.csv"))
    meta = read_json(os.path.join(bench_dir, "metadata.json"))
    ks = sorted({int(r["k"]) for r in rows})
    if len(ks) < 2:
        return
    secs = defaultdict(list)
    for r in rows:
        secs[(r["cache"], int(r["readers"]), int(r["k"]))].append(fnum(r["seconds"]))
    cells, titles = [], []
    for cache in ("cold", "warm"):
        readers = sorted({rd for (c, rd, _) in secs if c == cache})
        if not readers:
            continue
        series = []
        for rd in readers:
            base = secs[(cache, rd, ks[0])]
            ys, los, his = [], [], []
            for k in ks:
                if k == ks[0]:
                    ys.append(0.0)
                    los.append(0.0)
                    his.append(0.0)
                    continue
                pct, ci = change(base, secs[(cache, rd, k)])
                ys.append(pct)
                los.append(ci[0] if ci else None)
                his.append(ci[1] if ci else None)
            series.append({"level": rd, "label": f"{rd} reader{'s' if rd != 1 else ''}",
                           "x": ks, "y": ys, "lo": los, "hi": his})
        cells.append(series)
        titles.append(CACHE_TITLES[cache])
    subtitle = (f"{meta.get('cluster', '?')}; {fmt_mb(meta.get('data_mb_per_host'))} per host in "
                f"{meta.get('file_mb', '?')} MB files, read with plain parallel readers (no Hadoop); "
                f"{meta.get('repetitions', '?')} reads x hosts per point; band = 95% CI")
    figs.rf.change_grid(figs.path("fig3_storage_stack_alone"), "The storage stack alone: read time vs k",
                        subtitle, [cells], ks, f"read time change vs k={ks[0]}", col_titles=titles)
    lines += ["![Figure 3: storage stack alone](figures/fig3_storage_stack_alone.png)", ""]


def figure_controls(main, dio, mkfs, figs, lines):
    panels = []
    for cache in ("cold", "warm"):
        conds = [c for c in main.conditions if main.cond_info(c)[1] == cache]
        if not conds:
            continue
        heavy = max(conds, key=lambda c: main.cond_info(c)[0])
        rows = []
        for label, run in (("Main run", main), ("Loop devices with direct I/O", dio),
                           ("Default mkfs layout (pre-Sept 2026)", mkfs)):
            if run and heavy in run.conditions and len(run.ks) >= 2:
                pct, ci = run.change_k(heavy, run.runtimes)
                rows.append((label, pct, ci[0] if ci else None, ci[1] if ci else None))
        if len(rows) >= 2:
            panels.append((f"{CACHE_TITLES[cache]} ({load_label(main.cond_info(heavy)[0])})", rows))
    if not panels:
        return
    kmax = main.ks[-1]
    figs.rf.controls(figs.path("fig4_controls"), f"Slowdown at k={kmax}: does it depend on how the disks were built?",
                     f"Runtime change from k={main.ks[0]} to k={kmax} at the highest load; line = 95% CI",
                     panels)
    lines += ["![Figure 4: controls](figures/fig4_controls.png)", ""]


def figure_server(run, figs, lines):
    by_k = defaultdict(list)
    for r in run.server:
        by_k[int(r["k"])].append(r)
    ks = sorted(by_k)
    if len(ks) < 2:
        return

    def per_k(field):
        return [mean([fnum(r.get(field)) for r in by_k[k]]) for k in ks]

    nn_peak = []
    for k in ks:
        rows = read_csv(os.path.join(run.dir, "namenode_memory", f"nn_memory_k{k}.csv"))
        vals = [fnum(r.get("heap_used_mb")) for r in rows]
        vals = [v for v in vals if v]
        nn_peak.append(max(vals) if vals else None)
    panels = [("DataNode threads", per_k("dn_threads")),
              ("DataNode memory, RSS (MB)", per_k("dn_rss_mb")),
              ("NameNode heap, peak (MB)", nn_peak),
              ("Block report: RPC + NameNode (ms)", per_k("block_report_rpc_ms")),
              ("DataNode start to first block report (s)", per_k("dn_start_to_block_report_s")),
              ("Input upload into HDFS (MB/s)", per_k("upload_mb_per_s"))]
    figs.rf.small_multiples(figs.path("fig5_server_cost"), "What k virtual disks cost the server",
                            f"{run.meta.get('cluster', '?')}; mean over the DataNode hosts, measured once per k "
                            f"with the cluster idle", panels, ks)
    lines += ["![Figure 5: server cost](figures/fig5_server_cost.png)", ""]


# ---------------------------------------------------------------- main
def report(pipe_dir):
    """pipe_dir: a pipeline folder (stages.env) or a single run folder (runs.csv)."""
    if os.path.exists(os.path.join(pipe_dir, "stages.env")):
        stages = load_stages(pipe_dir)
    elif os.path.exists(os.path.join(pipe_dir, "runs.csv")):
        stages = {"MAIN_RUN": pipe_dir}
    else:
        print(f"{pipe_dir}: neither a pipeline folder (stages.env) nor a run folder (runs.csv)")
        return 1
    main_dir = stages.get("MAIN_RUN")
    if not main_dir or not os.path.exists(os.path.join(main_dir, "runs.csv")):
        print(f"no main run in {pipe_dir}/stages.env")
        return 1
    main = Run(main_dir)
    figs = FigureMaker(os.path.join(pipe_dir, "figures"), main.meta)
    lines = [
        "# Final report: k virtual disks per DataNode",
        "",
        f"Generated {time.strftime('%Y-%m-%d %H:%M')} from `{pipe_dir}`.",
        "Numbers in brackets are 95% confidence intervals; \"clearly\" means the interval excludes zero.",
        "",
    ]
    section_setup(main, lines)
    section_main(main, lines, figs)
    section_factors(main, lines)
    section_where(main, lines, figs)
    section_server(main, lines, figs)
    section_bench(stages.get("BENCH_RUN"), lines, figs)
    lines += ["## 6. Controls: is it an artifact of how the virtual disks were built?", ""]
    dio = Run(stages["DIO_RUN"]) if stages.get("DIO_RUN") else None
    mkfs = Run(stages["MKFS_RUN"]) if stages.get("MKFS_RUN") else None
    section_control(main, dio, "Loop devices with direct I/O (no second cached copy of the data)",
                    "Same conditions, but the loop devices bypass the page cache for the image files.", lines)
    section_control(main, mkfs, "Default mkfs layout (as in all runs before September 2026)",
                    "Same conditions, but mkfs.ext4 picks the layout from the image size "
                    "(small images may get 1 KB blocks); the main run uses 4 KB blocks for every k.", lines)
    if figs.enabled and (dio or mkfs):
        figure_controls(main, dio, mkfs, figs, lines)
    section_quality(main, lines)
    failed = [s for s in ("SMOKE", "MAIN", "BENCH", "DIO", "MKFS") if stages.get(s + "_STATUS") == "failed"]
    if failed:
        lines += ["## Stages that failed", "", ", ".join(failed) + " -- see pipeline.log.", ""]
    out = os.path.join(pipe_dir, "FINAL_REPORT.md")
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Wrote {out}" + (" (with figures in figures/, PNG + PDF)" if figs.enabled
                            else " (no figures: matplotlib not available here; run it again on the laptop)"))
    return 0


def main(argv):
    if len(argv) == 2 and argv[0] == "--bench-summary":
        header_table, changes, _ = bench_table(argv[1])
        if not header_table:
            print("no bench.csv data")
            return 1
        header, rows = header_table
        widths = [max(len(str(x)) for x in [h] + [r[i] for r in rows]) for i, h in enumerate(header)]
        print("  ".join(str(h).ljust(w) for h, w in zip(header, widths)))
        for r in rows:
            print("  ".join(str(c).ljust(w) for c, w in zip(r, widths)))
        return 0
    if len(argv) == 1:
        return report(argv[0])
    print(__doc__)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
