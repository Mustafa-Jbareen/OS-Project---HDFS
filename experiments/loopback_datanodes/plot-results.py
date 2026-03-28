#!/usr/bin/env python3
"""
Plot Loopback DataNodes Experiment Results

Generates visualizations showing how WordCount (20GB, 128MB blocks, replication=3)
performance AND NameNode memory usage change as the number of virtual DataNodes
per physical node (k) scales.

CSV format (produced by run-experiment.sh):
  k_per_node,total_datanodes,avg_runtime_seconds,stddev_runtime,individual_runtimes,
  live_datanodes,nn_heap_before_mb,nn_heap_peak_mb,nn_heap_avg_mb,nn_block_count

Usage:
    python3 plot-results.py <results_directory>
    python3 plot-results.py results/loopback-datanodes/latest
"""

import argparse
import csv
import json
import glob
import os
import re
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import numpy as np
except ImportError as exc:
    raise SystemExit(
        "matplotlib and numpy are required. Install with:\n"
        "  pip install matplotlib numpy"
    ) from exc


def read_results(csv_path: Path):
    """Read the results CSV."""
    results = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append({
                "k": int(row["k_per_node"]),
                "total_dns": int(row["total_datanodes"]),
                "avg_runtime": float(row["avg_runtime_seconds"]),
                "stddev": float(row["stddev_runtime"]),
                "individual": row["individual_runtimes"],
                "live_dns": int(row["live_datanodes"]),
                "nn_heap_before": int(row.get("nn_heap_before_mb", 0)),
                "nn_heap_peak": int(row.get("nn_heap_peak_mb", 0)),
                "nn_heap_avg": int(row.get("nn_heap_avg_mb", 0)),
                "nn_block_count": int(row.get("nn_block_count", 0)),
            })
    return results


def read_metadata(meta_path: Path):
    """Read experiment metadata."""
    if meta_path.exists():
        try:
            with meta_path.open() as f:
                return json.load(f)
        except json.JSONDecodeError as exc:
            print(f"WARNING: Invalid metadata JSON in {meta_path}: {exc}")
            print("         Attempting compatibility repair parse...")
            try:
                raw_text = meta_path.read_text(encoding="utf-8", errors="replace")
                repaired = repair_and_parse_metadata(raw_text)
                if repaired:
                    print("         Metadata repaired successfully.")
                    return repaired
            except Exception:
                pass
            print("         Repair failed; continuing with CSV-only defaults.")
            return {}
    return {}


def repair_and_parse_metadata(raw_text: str):
    """Best-effort parser for known malformed metadata.json variants.

    The main broken format observed is an invalid node_names array such as:
      "node_names": ["tapuz14 tapuz10 ..."],
    or quoting/comma issues produced by shell interpolation.
    """
    # First, try lightweight structural fix for malformed node_names line.
    text = raw_text

    def _fix_node_names(match):
        inner = match.group(1).strip()
        if not inner:
            return '"node_names": []'

        # Strip one level of wrapping quotes if present
        if (inner.startswith('"') and inner.endswith('"')) or (
            inner.startswith("'") and inner.endswith("'")
        ):
            inner = inner[1:-1]

        # If already comma-delimited with quotes, keep as-is by trying JSON load later.
        # Otherwise split on whitespace/commas and rebuild a valid JSON array.
        parts = [p for p in re.split(r"[\s,]+", inner) if p]
        fixed = ", ".join(json.dumps(p) for p in parts)
        return f'"node_names": [{fixed}]'

    text = re.sub(
        r'"node_names"\s*:\s*\[(.*?)\]',
        _fix_node_names,
        text,
        flags=re.DOTALL,
    )

    # Clean common trailing-comma issue before closing braces/brackets.
    text = re.sub(r",\s*([}\]])", r"\1", text)

    # Try normal JSON decode after repairs.
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Last resort: extract key fields with regex so subtitles still work.
    meta = {}

    def _extract_int(key):
        m = re.search(rf'"{re.escape(key)}"\s*:\s*([0-9]+)', raw_text)
        return int(m.group(1)) if m else None

    def _extract_str(key):
        m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]*)"', raw_text)
        return m.group(1) if m else None

    for int_key in [
        "input_size_gb",
        "block_size_bytes",
        "replication",
        "physical_nodes",
        "repetitions",
        "loopback_budget_per_node_gb",
        "min_image_size_gb",
        "fd_reserved_per_node",
        "fd_per_datanode_assumption",
    ]:
        value = _extract_int(int_key)
        if value is not None:
            meta[int_key] = value

    for str_key in ["run_id", "block_size_human", "start_time", "end_time"]:
        value = _extract_str(str_key)
        if value is not None:
            meta[str_key] = value

    # Extract integer arrays k_values / skipped_k_values
    for arr_key in ["k_values", "skipped_k_values"]:
        m = re.search(rf'"{re.escape(arr_key)}"\s*:\s*\[([^\]]*)\]', raw_text, re.DOTALL)
        if m:
            nums = [int(x) for x in re.findall(r"\d+", m.group(1))]
            meta[arr_key] = nums

    # Extract node_names robustly even if space-delimited in one quoted token.
    m = re.search(r'"node_names"\s*:\s*\[([^\]]*)\]', raw_text, re.DOTALL)
    if m:
        inner = m.group(1).strip()
        quoted = re.findall(r'"([^"]+)"', inner)
        if quoted:
            if len(quoted) == 1 and re.search(r"\s", quoted[0]):
                meta["node_names"] = [p for p in quoted[0].split() if p]
            else:
                meta["node_names"] = quoted
        else:
            parts = [p for p in re.split(r"[\s,]+", inner) if p and p not in ["'", '"']]
            if parts:
                meta["node_names"] = parts

    return meta


def read_nn_memory_timeseries(nn_dir: Path):
    """Read per-k NameNode memory time series CSVs."""
    timeseries = {}
    for csv_file in sorted(nn_dir.glob("nn_memory_k*.csv")):
        # Extract k from filename: nn_memory_k4.csv -> 4
        k_str = csv_file.stem.replace("nn_memory_k", "")
        try:
            k = int(k_str)
        except ValueError:
            continue
        rows = []
        with csv_file.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    rows.append({
                        "timestamp": row["timestamp"],
                        "heap_used_mb": int(row["heap_used_mb"]),
                        "heap_max_mb": int(row["heap_max_mb"]),
                        "block_count": int(row["block_count"]),
                    })
                except (ValueError, KeyError):
                    pass
        if rows:
            timeseries[k] = rows
    return timeseries


def validate_results_structure(results_dir: Path, results, metadata, timeseries):
    """Validate that expected result artifacts exist and are internally consistent."""
    warnings = []

    required = ["results.csv", "experiment.log", "metadata.json", "namenode_memory"]
    for name in required:
        if not (results_dir / name).exists():
            warnings.append(f"Missing expected artifact: {name}")

    k_from_results = sorted({r["k"] for r in results})
    k_from_timeseries = sorted(timeseries.keys()) if timeseries else []

    if k_from_timeseries:
        missing_ts = [k for k in k_from_results if k not in k_from_timeseries]
        extra_ts = [k for k in k_from_timeseries if k not in k_from_results]
        if missing_ts:
            warnings.append(f"Missing nn_memory_k*.csv for k values: {missing_ts}")
        if extra_ts:
            warnings.append(f"Found nn_memory_k*.csv without matching CSV rows: {extra_ts}")

    k_meta = metadata.get("k_values") if isinstance(metadata, dict) else None
    if isinstance(k_meta, list) and k_meta:
        if sorted(k_meta) != k_from_results:
            warnings.append(
                f"metadata k_values ({sorted(k_meta)}) differs from results.csv k values ({k_from_results})"
            )

    if warnings:
        print("Validation warnings:")
        for item in warnings:
            print(f"  - {item}")
    else:
        print("Validation: all expected result artifacts look consistent.")


def _subtitle(metadata):
    """Build a standard subtitle string from metadata."""
    input_gb = metadata.get("input_size_gb", 20)
    block_human = metadata.get("block_size_human", "128MB")
    num_nodes = metadata.get("physical_nodes", "?")
    replication = metadata.get("replication", 3)
    reps = metadata.get("repetitions", "?")
    return (
        f"{input_gb}GB input, {block_human} blocks, replication={replication}, "
        f"{num_nodes} physical nodes, {reps} runs averaged"
    )


# ============================================================================
# PLOT 1: Runtime vs k (bar chart)
# ============================================================================
def plot_runtime_vs_k(results, metadata, output_dir: Path):
    fig, ax = plt.subplots(figsize=(11, 7))

    k_vals = [r["k"] for r in results]
    runtimes = [r["avg_runtime"] for r in results]
    stddevs = [r["stddev"] for r in results]
    total_dns = [r["total_dns"] for r in results]

    colors = cm.viridis(np.linspace(0.2, 0.8, len(k_vals)))

    bars = ax.bar(
        range(len(k_vals)), runtimes,
        yerr=stddevs,
        color=colors, alpha=0.85,
        error_kw={"capsize": 6},
        edgecolor="black", linewidth=0.5,
    )

    for bar, rt, sd, dns in zip(bars, runtimes, stddevs, total_dns):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + sd + max(runtimes) * 0.03,
            f"{rt:.1f}s\n({dns} DNs)",
            ha="center", va="bottom", fontsize=10, fontweight="bold",
        )

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("DataNodes per Physical Node (k)", fontsize=13)
    ax.set_ylabel("Average WordCount Runtime (seconds)", fontsize=13)
    ax.set_title(
        f"WordCount Runtime vs Virtual DataNodes per Node\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "runtime_vs_k.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 2: Runtime vs total DataNodes (line plot)
# ============================================================================
def plot_runtime_vs_total_dns(results, metadata, output_dir: Path):
    fig, ax = plt.subplots(figsize=(10, 7))

    total_dns = [r["total_dns"] for r in results]
    runtimes = [r["avg_runtime"] for r in results]
    stddevs = [r["stddev"] for r in results]
    k_vals = [r["k"] for r in results]

    ax.errorbar(
        total_dns, runtimes,
        yerr=stddevs,
        marker="o", markersize=10, linewidth=2,
        capsize=5, color="steelblue",
    )

    for dns, rt, sd, k in zip(total_dns, runtimes, stddevs, k_vals):
        ax.annotate(
            f"k={k}",
            (dns, rt),
            textcoords="offset points", xytext=(10, 10),
            fontsize=11, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color="gray"),
        )

    ax.set_xlabel("Total DataNodes in Cluster", fontsize=13)
    ax.set_ylabel("Average WordCount Runtime (seconds)", fontsize=13)
    ax.set_title(
        f"WordCount Runtime vs Total DataNodes\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    out = output_dir / "runtime_vs_total_datanodes.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 3: Speedup vs k=1 baseline
# ============================================================================
def plot_speedup(results, metadata, output_dir: Path):
    if not results or results[0]["k"] != 1:
        print("WARNING: k=1 not found as first result, skipping speedup plot.")
        return

    fig, ax = plt.subplots(figsize=(10, 7))

    baseline = results[0]["avg_runtime"]
    k_vals = [r["k"] for r in results]
    speedups = [baseline / r["avg_runtime"] if r["avg_runtime"] > 0 else 0 for r in results]

    bars = ax.bar(
        range(len(k_vals)), speedups,
        color=["green" if s >= 1.0 else "salmon" for s in speedups],
        alpha=0.8, edgecolor="black", linewidth=0.5,
    )
    ax.axhline(y=1.0, color="black", linestyle="--", linewidth=1, alpha=0.5)

    for bar, s, k in zip(bars, speedups, k_vals):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.05,
            f"{s:.2f}x",
            ha="center", va="bottom", fontsize=11, fontweight="bold",
        )

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("DataNodes per Physical Node (k)", fontsize=13)
    ax.set_ylabel("Speedup vs k=1", fontsize=13)
    ax.set_title(
        "Speedup from Virtual DataNodes\n(>1.0 = faster than baseline k=1)",
        fontsize=13,
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "speedup_vs_k.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 4: Individual run scatter
# ============================================================================
def plot_individual_runs(results, metadata, output_dir: Path):
    fig, ax = plt.subplots(figsize=(10, 7))

    k_vals = [r["k"] for r in results]

    for i, r in enumerate(results):
        individual = [float(x) for x in r["individual"].split(";") if x]
        x_jitter = np.random.normal(i, 0.05, len(individual))
        ax.scatter(x_jitter, individual, alpha=0.6, s=60, zorder=5)
        ax.hlines(
            r["avg_runtime"], i - 0.25, i + 0.25,
            color="red", linewidth=2, zorder=10,
            label="Mean" if i == 0 else None,
        )

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("DataNodes per Physical Node (k)", fontsize=13)
    ax.set_ylabel("WordCount Runtime (seconds)", fontsize=13)
    ax.set_title("Individual Run Times per Configuration", fontsize=13)
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "individual_runs.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 5: NameNode Memory vs k (bar chart)
# ============================================================================
def plot_nn_memory_vs_k(results, metadata, output_dir: Path):
    """Bar chart of NameNode heap memory (before, peak, avg) for each k."""
    fig, ax = plt.subplots(figsize=(11, 7))

    k_vals = [r["k"] for r in results]
    heap_before = [r["nn_heap_before"] for r in results]
    heap_peak = [r["nn_heap_peak"] for r in results]
    heap_avg = [r["nn_heap_avg"] for r in results]

    x = np.arange(len(k_vals))
    width = 0.25

    bars1 = ax.bar(x - width, heap_before, width, label="Before WordCount",
                   color="lightblue", edgecolor="black", linewidth=0.5)
    bars2 = ax.bar(x,         heap_avg,    width, label="Avg During WordCount",
                   color="steelblue", edgecolor="black", linewidth=0.5)
    bars3 = ax.bar(x + width, heap_peak,   width, label="Peak During WordCount",
                   color="darkred", alpha=0.8, edgecolor="black", linewidth=0.5)

    # Annotate peak bars with values
    for bar, val in zip(bars3, heap_peak):
        if val > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(heap_peak) * 0.02,
                f"{val}MB",
                ha="center", va="bottom", fontsize=9, fontweight="bold",
            )

    ax.set_xticks(x)
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("DataNodes per Physical Node (k)", fontsize=13)
    ax.set_ylabel("NameNode Heap Memory (MB)", fontsize=13)
    ax.set_title(
        f"NameNode Memory Usage vs Virtual DataNodes\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "nn_memory_vs_k.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 6: Dual-axis — Runtime + NN Peak Memory vs k
# ============================================================================
def plot_runtime_and_memory(results, metadata, output_dir: Path):
    """Combined dual-axis plot: runtime on left y-axis, NN memory on right."""
    fig, ax1 = plt.subplots(figsize=(11, 7))

    k_vals = [r["k"] for r in results]
    runtimes = [r["avg_runtime"] for r in results]
    stddevs = [r["stddev"] for r in results]
    heap_peak = [r["nn_heap_peak"] for r in results]
    total_dns = [r["total_dns"] for r in results]

    x = np.arange(len(k_vals))

    # Left axis: Runtime (bars)
    color1 = "steelblue"
    bars = ax1.bar(
        x - 0.15, runtimes, 0.35,
        yerr=stddevs,
        color=color1, alpha=0.7,
        error_kw={"capsize": 4},
        edgecolor="black", linewidth=0.5,
        label="WordCount Runtime",
    )
    ax1.set_xlabel("DataNodes per Physical Node (k)", fontsize=13)
    ax1.set_ylabel("Average Runtime (seconds)", fontsize=13, color=color1)
    ax1.tick_params(axis="y", labelcolor=color1)

    # Right axis: NN Memory (bars)
    ax2 = ax1.twinx()
    color2 = "darkred"
    bars2 = ax2.bar(
        x + 0.15, heap_peak, 0.35,
        color=color2, alpha=0.7,
        edgecolor="black", linewidth=0.5,
        label="NN Peak Heap (MB)",
    )
    ax2.set_ylabel("NameNode Peak Heap (MB)", fontsize=13, color=color2)
    ax2.tick_params(axis="y", labelcolor=color2)

    # Annotate with total DN count
    for i, (rt, mem, dns) in enumerate(zip(runtimes, heap_peak, total_dns)):
        ax1.text(
            i, -max(runtimes) * 0.08,
            f"{dns} DNs",
            ha="center", fontsize=9, color="gray",
        )

    ax1.set_xticks(x)
    ax1.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=10)

    ax1.set_title(
        f"WordCount Runtime & NameNode Memory vs Virtual DataNodes\n"
        f"({_subtitle(metadata)})",
        fontsize=12,
    )
    ax1.grid(True, axis="y", alpha=0.2)
    fig.tight_layout()

    out = output_dir / "runtime_and_memory_vs_k.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 7: NameNode Memory Time Series (per k)
# ============================================================================
def plot_nn_memory_timeseries(timeseries, metadata, output_dir: Path):
    """Line plots of NameNode heap over time for each k value, overlaid."""
    if not timeseries:
        print("No NameNode memory time series data found, skipping.")
        return

    fig, ax = plt.subplots(figsize=(12, 7))

    colors = cm.tab10(np.linspace(0, 0.8, len(timeseries)))
    markers = ["o", "s", "^", "D", "v"]

    for idx, (k, rows) in enumerate(sorted(timeseries.items())):
        # Use relative seconds from first sample as x-axis
        heap_values = [r["heap_used_mb"] for r in rows]
        time_offsets = list(range(0, len(heap_values) * 5, 5))  # 5s intervals

        ax.plot(
            time_offsets[:len(heap_values)], heap_values,
            marker=markers[idx % len(markers)],
            color=colors[idx],
            linewidth=1.5, markersize=4, alpha=0.8,
            label=f"k={k} ({k * metadata.get('physical_nodes', 5)} DNs)",
        )

    ax.set_xlabel("Time Since Monitor Start (seconds)", fontsize=13)
    ax.set_ylabel("NameNode Heap Used (MB)", fontsize=13)
    ax.set_title(
        f"NameNode Heap Memory Over Time (During WordCount)\n"
        f"({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.legend(fontsize=10, loc="best")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    out = output_dir / "nn_memory_timeseries.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 8: Block Count vs k
# ============================================================================
def plot_block_count_vs_k(results, metadata, output_dir: Path):
    """Show how total block count (data blocks × replication) scales with k."""
    fig, ax = plt.subplots(figsize=(10, 6))

    k_vals = [r["k"] for r in results]
    block_counts = [r["nn_block_count"] for r in results]

    if all(b == 0 for b in block_counts):
        print("No block count data available, skipping block count plot.")
        return

    ax.bar(
        range(len(k_vals)), block_counts,
        color="teal", alpha=0.8,
        edgecolor="black", linewidth=0.5,
    )

    for i, (k, bc) in enumerate(zip(k_vals, block_counts)):
        ax.text(i, bc + max(block_counts) * 0.02, str(bc),
                ha="center", fontsize=10, fontweight="bold")

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("DataNodes per Physical Node (k)", fontsize=13)
    ax.set_ylabel("Total HDFS Blocks", fontsize=13)

    replication = metadata.get("replication", 3)
    ax.set_title(
        f"Total HDFS Block Count vs Virtual DataNodes\n"
        f"(replication={replication}, should be constant across k values)",
        fontsize=12,
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "block_count_vs_k.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# MAIN
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Plot loopback DataNodes experiment results."
    )
    parser.add_argument(
        "results_dir",
        type=Path,
        help="Path to the results directory (e.g., results/loopback-datanodes/latest)",
    )
    args = parser.parse_args()

    results_dir = args.results_dir
    csv_path = results_dir / "results.csv"
    meta_path = results_dir / "metadata.json"
    nn_dir = results_dir / "namenode_memory"

    if not csv_path.exists():
        raise SystemExit(f"ERROR: {csv_path} not found")

    results = read_results(csv_path)
    metadata = read_metadata(meta_path)
    timeseries = read_nn_memory_timeseries(nn_dir) if nn_dir.exists() else {}

    validate_results_structure(results_dir, results, metadata, timeseries)

    print(f"Loaded {len(results)} configurations from {csv_path}")
    print(f"k values: {[r['k'] for r in results]}")
    print(f"NN memory time series: {sorted(timeseries.keys()) if timeseries else 'none'}")
    print()

    # Performance plots
    plot_runtime_vs_k(results, metadata, results_dir)
    plot_runtime_vs_total_dns(results, metadata, results_dir)
    plot_speedup(results, metadata, results_dir)
    plot_individual_runs(results, metadata, results_dir)

    # NameNode memory plots
    plot_nn_memory_vs_k(results, metadata, results_dir)
    plot_runtime_and_memory(results, metadata, results_dir)
    plot_nn_memory_timeseries(timeseries, metadata, results_dir)
    plot_block_count_vs_k(results, metadata, results_dir)

    print()
    print(f"All plots generated! ({8} total)")
    print(f"Output directory: {results_dir}")


if __name__ == "__main__":
    main()
