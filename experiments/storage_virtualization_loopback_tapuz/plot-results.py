#!/usr/bin/env python3
"""
Plot Storage Virtualization Loopback Experiment Results

Generates visualizations showing how WordCount performance, NameNode memory,
and disk I/O change as the number of loopback-backed storage directories per
DataNode scales from 1 to 1024.

CSV format (produced by run-experiment-loopback-fs.sh):
    k_storage_dirs,total_storage_dirs,datanodes,avg_runtime_seconds,stddev_runtime,
    individual_runtimes,nn_heap_before_mb,nn_heap_peak_mb,nn_heap_avg_mb,nn_block_count,
    block_counts_per_fs,input_block_counts_per_fs,fs_used_mb_per_fs

Usage:
    python3 plot-results.py <results_directory>
    python3 plot-results.py results/storage_virtualization_loopback/latest
"""

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import numpy as np
except ImportError as exc:
    raise SystemExit(
        "matplotlib and numpy are required. Install with:\n"
        "  pip install matplotlib numpy"
    ) from exc

DPI = 200  # output resolution for all saved figures
# Threshold: if total bars across all k groups would exceed this, use a heatmap
# instead of a grouped bar chart (avoids thousands of unreadable slivers).
HEATMAP_THRESHOLD = 80
MARKERS = ["o", "s", "^", "D", "v", "P", "X", "h", "*", "d", ">", "<", "p", "H"]


def _k_colors(n):
    """Return n visually distinct colours. Uses tab20 for >10, tab10 otherwise."""
    cmap = plt.colormaps["tab20"] if n > 10 else plt.colormaps["tab10"]
    return cmap(np.linspace(0, 0.9, n))


def read_results(csv_path: Path):
    """Read the results CSV."""
    results = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append({
                "k": int(row["k_storage_dirs"]),
                "total_dirs": int(row["total_storage_dirs"]),
                "datanodes": int(row["datanodes"]),
                "avg_runtime": float(row["avg_runtime_seconds"]),
                "stddev": float(row["stddev_runtime"]),
                "individual": row["individual_runtimes"],
                "nn_heap_before": int(row.get("nn_heap_before_mb", 0) or 0),
                "nn_heap_peak": int(row.get("nn_heap_peak_mb", 0) or 0),
                "nn_heap_avg": int(row.get("nn_heap_avg_mb", 0) or 0),
                "nn_block_count": int(row.get("nn_block_count", 0) or 0),
                "block_counts_per_fs": [int(x) for x in row.get("block_counts_per_fs", "").split(";") if x],
                "input_block_counts_per_fs": [int(x) for x in row.get("input_block_counts_per_fs", "").split(";") if x],
                "fs_used_mb_per_fs": [int(x) for x in row.get("fs_used_mb_per_fs", "").split(";") if x],
            })
    return results


# ============================================================================
# PLOT X: Per-Filesystem Block Distribution
# ============================================================================
def plot_per_fs_block_distribution(results, metadata, output_dir: Path):
    """Block counts per loopback FS for every k.

    Uses a heatmap (k × FS-index) when there are too many bars to read,
    otherwise a grouped bar chart coloured by k value.
    """
    valid = [r for r in results if r.get("block_counts_per_fs")]
    if not valid:
        return

    total_bars = sum(len(r["block_counts_per_fs"]) for r in valid)

    if total_bars > HEATMAP_THRESHOLD:
        # --- Heatmap: rows = k values, columns = FS index ---
        max_fs = max(len(r["block_counts_per_fs"]) for r in valid)
        k_labels = [f"k={r['k']}" for r in valid]
        matrix = np.zeros((len(valid), max_fs))
        for i, r in enumerate(valid):
            for j, c in enumerate(r["block_counts_per_fs"]):
                matrix[i, j] = c

        fig, ax = plt.subplots(figsize=(min(max_fs * 0.3 + 4, 22), max(5, len(valid) * 0.6 + 2)))
        im = ax.imshow(matrix, aspect="auto", cmap="viridis")
        ax.set_yticks(range(len(k_labels)))
        ax.set_yticklabels(k_labels, fontsize=10)
        ax.set_xlabel("Filesystem index (across all DataNodes)", fontsize=12)
        ax.set_ylabel("k (storage dirs per DataNode)", fontsize=12)
        ax.set_title(
            f"Block Distribution per Loopback FS (All Files)\n({_subtitle(metadata)})",
            fontsize=13,
        )
        plt.colorbar(im, ax=ax, label="Blocks")
        fig.tight_layout()
    else:
        colors = _k_colors(len(valid))
        fig, ax = plt.subplots(figsize=(max(10, total_bars * 0.4), 7))
        x = 0
        for idx, r in enumerate(valid):
            counts = r["block_counts_per_fs"]
            xs = list(range(x, x + len(counts)))
            ax.bar(xs, counts, color=colors[idx], edgecolor="black", linewidth=0.4,
                   label=f"k={r['k']}")
            ax.text(np.mean(xs), max(counts) * 1.04, f"k={r['k']}",
                    ha="center", fontsize=9, fontweight="bold")
            x += len(counts) + 1.5
        ax.set_xlabel("Filesystem index", fontsize=13)
        ax.set_ylabel("Blocks per Filesystem", fontsize=13)
        ax.set_title(
            f"Block Distribution per Local Filesystem (All Files)\n({_subtitle(metadata)})",
            fontsize=13,
        )
        ax.legend(fontsize=9, loc="best")
        ax.grid(True, axis="y", alpha=0.3)
        fig.tight_layout()

    out = output_dir / "per_fs_block_distribution.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


def plot_input_blocks_per_fs(results, metadata, output_dir: Path):
    """
    Improved visualization of input file block distribution per filesystem.
    Creates multiple views:
    1. Grouped bar chart showing blocks per FS, grouped by k value
    2. Box plot showing distribution statistics
    3. Heatmap showing balance across filesystems
    """
    # Filter results that have input block data
    valid_results = [r for r in results if r.get("input_block_counts_per_fs")]
    if not valid_results:
        print("No input block data available, skipping input block plots.")
        return

    num_datanode_hosts = metadata.get("datanode_hosts", 4)

    total_bars = sum(len(r["input_block_counts_per_fs"]) for r in valid_results)

    # =========================================================================
    # PLOT A: Per-FS counts — heatmap for large k, grouped bars otherwise
    # =========================================================================
    if total_bars > HEATMAP_THRESHOLD:
        max_fs = max(len(r["input_block_counts_per_fs"]) for r in valid_results)
        k_labels = [f"k={r['k']}" for r in valid_results]
        matrix = np.zeros((len(valid_results), max_fs))
        for i, r in enumerate(valid_results):
            for j, c in enumerate(r["input_block_counts_per_fs"]):
                matrix[i, j] = c

        fig, ax = plt.subplots(figsize=(min(max_fs * 0.3 + 4, 22), max(5, len(valid_results) * 0.6 + 2)))
        im = ax.imshow(matrix, aspect="auto", cmap="viridis")
        ax.set_yticks(range(len(k_labels)))
        ax.set_yticklabels(k_labels, fontsize=10)
        ax.set_xlabel("Filesystem index (across all DataNodes)", fontsize=12)
        ax.set_ylabel("k (storage dirs per DataNode)", fontsize=12)
        ax.set_title(
            "Input File Block Distribution per Filesystem\n"
            f"({_subtitle(metadata)})",
            fontsize=13,
        )
        plt.colorbar(im, ax=ax, label="Input blocks (with replicas)")
        fig.tight_layout()
    else:
        color_palette = plt.colormaps["tab10"](np.linspace(0, 1, num_datanode_hosts))
        fig, ax = plt.subplots(figsize=(14, 8))
        x_positions, x_labels, all_counts, node_colors, k_boundaries = [], [], [], [], []
        current_x = 0

        for r in valid_results:
            k = r["k"]
            counts = r["input_block_counts_per_fs"]
            k_boundaries.append(current_x)
            for i, cnt in enumerate(counts):
                node_idx = i // k if k > 0 else 0
                fs_in_node = i % k + 1 if k > 0 else i + 1
                x_positions.append(current_x)
                x_labels.append(f"N{node_idx+1}\nFS{fs_in_node}")
                all_counts.append(cnt)
                node_colors.append(color_palette[node_idx % num_datanode_hosts])
                current_x += 1
            current_x += 1.5

        ax.bar(x_positions, all_counts, color=node_colors, edgecolor="black",
               linewidth=0.5, alpha=0.85)

        global_max = max(all_counts) if all_counts else 1
        for i, r in enumerate(valid_results):
            k = r["k"]
            counts = r["input_block_counts_per_fs"]
            if not counts:
                continue
            mid_x = (k_boundaries[i] + k_boundaries[i] + len(counts) - 1) / 2
            ax.text(mid_x, max(counts) + global_max * 0.04, f"k={k}",
                    ha="center", va="bottom", fontsize=11, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="lightgray", alpha=0.7))
            mean_val = np.mean(counts)
            cv = np.std(counts) / mean_val * 100 if mean_val > 0 else 0
            ax.text(mid_x, global_max * 1.14,
                    f"CV={cv:.1f}%", ha="center", fontsize=8, color="gray")

        legend_handles = [plt.Rectangle((0, 0), 1, 1, color=color_palette[i], alpha=0.85)
                          for i in range(num_datanode_hosts)]
        ax.legend(legend_handles, [f"Node {i+1}" for i in range(num_datanode_hosts)],
                  loc="upper right", fontsize=10, title="DataNodes")
        ax.set_ylabel("Input File Blocks (with replicas)", fontsize=13)
        ax.set_xlabel("Filesystem (Node / FS index)", fontsize=13)
        ax.set_title(
            "Input File Block Distribution per Filesystem\n"
            f"(Grouped by k, coloured by DataNode — {_subtitle(metadata)})",
            fontsize=13,
        )
        for boundary in k_boundaries[1:]:
            ax.axvline(boundary - 0.75, color="gray", linewidth=0.8, alpha=0.4)
        ax.set_xticks(x_positions)
        ax.set_xticklabels(x_labels, fontsize=7, rotation=90)
        ax.set_ylim(top=global_max * 1.22)
        ax.grid(True, axis="y", alpha=0.3)
        fig.tight_layout()

    out = output_dir / "input_blocks_per_fs.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)

    # =========================================================================
    # PLOT B: Box plot showing distribution per k value
    # =========================================================================
    fig, ax = plt.subplots(figsize=(max(8, len(valid_results) * 0.9), 7))

    box_data, box_labels = [], []
    for r in valid_results:
        counts = r["input_block_counts_per_fs"]
        if counts:
            box_data.append(counts)
            box_labels.append(f"k={r['k']}\n({len(counts)} FSes)")

    if box_data:
        bp = ax.boxplot(box_data, tick_labels=box_labels, patch_artist=True)
        colors = plt.colormaps["viridis"](np.linspace(0.2, 0.8, len(box_data)))
        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        means = [np.mean(d) for d in box_data]
        ax.scatter(range(1, len(means) + 1), means, marker="D", color="red",
                   s=50, zorder=5, label="Mean")
        ax.set_ylabel("Blocks per Filesystem", fontsize=13)
        ax.set_xlabel("Configuration", fontsize=13)
        ax.set_title(
            "Input Block Distribution per k\n(Box = quartiles, diamond = mean)",
            fontsize=13,
        )
        ax.legend(fontsize=10)
        ax.grid(True, axis="y", alpha=0.3)
        out = output_dir / "input_blocks_boxplot.png"
        fig.savefig(out, dpi=DPI)
        print(f"Saved: {out}")
    plt.close(fig)

    # =========================================================================
    # PLOT C: Coefficient of Variation (balance metric) across k values
    # =========================================================================
    fig, ax = plt.subplots(figsize=(10, 6))

    k_vals = []
    cv_vals = []
    for r in valid_results:
        k = r["k"]
        counts = r["input_block_counts_per_fs"]
        if counts and len(counts) > 1:
            mean_val = np.mean(counts)
            std_val = np.std(counts)
            cv = (std_val / mean_val * 100) if mean_val > 0 else 0
            k_vals.append(k)
            cv_vals.append(cv)

    if cv_vals:
        bars = ax.bar(range(len(k_vals)), cv_vals,
                      color=['green' if cv < 10 else 'orange' if cv < 20 else 'red' for cv in cv_vals],
                      edgecolor='black', linewidth=0.5, alpha=0.8)

        for bar, cv in zip(bars, cv_vals):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f"{cv:.1f}%", ha='center', va='bottom', fontsize=11, fontweight='bold')

        ax.axhline(y=10, color='green', linestyle='--', alpha=0.5, label='Good balance (<10%)')
        ax.axhline(y=20, color='orange', linestyle='--', alpha=0.5, label='Moderate imbalance (<20%)')

        ax.set_xticks(range(len(k_vals)))
        ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
        ax.set_ylabel("Coefficient of Variation (%)", fontsize=13)
        ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
        ax.set_title("Block Distribution Balance Across Filesystems\n(Lower CV = more balanced)", fontsize=13)
        ax.legend(fontsize=10, loc='upper right')
        ax.grid(True, axis='y', alpha=0.3)

        out = output_dir / "input_blocks_balance.png"
        fig.savefig(out, dpi=DPI)
        print(f"Saved: {out}")
    plt.close(fig)


def plot_fs_capacity_per_node(results, metadata, output_dir: Path):
    """
    Histogram showing used filesystem capacity per loopback FS.
    - X-axis: Filesystem 'number' (grouped by k value)
    - Y-axis: Used filesystem capacity (MB)
    - Different coloring per DataNode
    Uses a heatmap when total bars would exceed HEATMAP_THRESHOLD.
    """
    valid_results = [r for r in results if r.get("fs_used_mb_per_fs")]
    if not valid_results:
        print("No filesystem capacity data available, skipping capacity plot.")
        return

    num_datanode_hosts = metadata.get("datanode_hosts", 4)
    total_bars = sum(len(r["fs_used_mb_per_fs"]) for r in valid_results)

    # =========================================================================
    # PLOT A: Heatmap for large k, grouped bars otherwise
    # =========================================================================
    if total_bars > HEATMAP_THRESHOLD:
        max_fs = max(len(r["fs_used_mb_per_fs"]) for r in valid_results)
        k_labels = [f"k={r['k']}" for r in valid_results]
        matrix = np.zeros((len(valid_results), max_fs))
        for i, r in enumerate(valid_results):
            for j, c in enumerate(r["fs_used_mb_per_fs"]):
                matrix[i, j] = c

        fig, ax = plt.subplots(figsize=(min(max_fs * 0.3 + 4, 22), max(5, len(valid_results) * 0.6 + 2)))
        im = ax.imshow(matrix, aspect="auto", cmap="viridis")
        ax.set_yticks(range(len(k_labels)))
        ax.set_yticklabels(k_labels, fontsize=10)
        ax.set_xlabel("Filesystem index (across all DataNodes)", fontsize=12)
        ax.set_ylabel("k (storage dirs per DataNode)", fontsize=12)
        ax.set_title(
            "Filesystem Used Capacity per Loopback FS\n"
            f"({_subtitle(metadata)})",
            fontsize=13,
        )
        plt.colorbar(im, ax=ax, label="Used capacity (MB)")
        fig.tight_layout()
    else:
        color_palette = plt.colormaps["tab10"](np.linspace(0, 1, num_datanode_hosts))
        fig, ax = plt.subplots(figsize=(14, 8))
        x_positions, x_labels, all_capacities, node_colors, k_boundaries = [], [], [], [], []
        current_x = 0

        for r in valid_results:
            k = r["k"]
            capacities = r["fs_used_mb_per_fs"]
            k_boundaries.append(current_x)
            for i, cap in enumerate(capacities):
                node_idx = i // k if k > 0 else 0
                fs_in_node = i % k + 1 if k > 0 else i + 1
                x_positions.append(current_x)
                x_labels.append(f"N{node_idx+1}\nFS{fs_in_node}")
                all_capacities.append(cap)
                node_colors.append(color_palette[node_idx % num_datanode_hosts])
                current_x += 1
            current_x += 1.5

        ax.bar(x_positions, all_capacities, color=node_colors,
               edgecolor="black", linewidth=0.5, alpha=0.85)

        global_max = max(all_capacities) if all_capacities else 1
        for i, r in enumerate(valid_results):
            k = r["k"]
            capacities = r["fs_used_mb_per_fs"]
            if not capacities:
                continue
            mid_x = (k_boundaries[i] + k_boundaries[i] + len(capacities) - 1) / 2
            ax.text(mid_x, max(capacities) + global_max * 0.04, f"k={k}",
                    ha="center", va="bottom", fontsize=12, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray", alpha=0.7))
            mean_val = np.mean(capacities)
            cv = np.std(capacities) / mean_val * 100 if mean_val > 0 else 0
            ax.text(mid_x, global_max * 1.14,
                    f"CV={cv:.1f}%", ha="center", fontsize=8, color="gray")

        legend_handles = [plt.Rectangle((0, 0), 1, 1, color=color_palette[i], alpha=0.85)
                          for i in range(min(num_datanode_hosts, len(color_palette)))]
        ax.legend(legend_handles, [f"Node {i+1}" for i in range(num_datanode_hosts)],
                  loc="upper right", fontsize=10, title="DataNodes")
        ax.set_ylabel("Used Filesystem Capacity (MB)", fontsize=13)
        ax.set_xlabel("Filesystem (Node / FS index)", fontsize=13)
        ax.set_title("Filesystem Used Capacity per Loopback FS\n(Grouped by k, colored by DataNode)", fontsize=14)
        for boundary in k_boundaries[1:]:
            ax.axvline(boundary - 0.75, color="gray", linewidth=0.8, alpha=0.4)
        ax.set_xticks(x_positions)
        ax.set_xticklabels(x_labels, fontsize=7, rotation=90)
        ax.set_ylim(top=global_max * 1.22)
        ax.grid(True, axis="y", alpha=0.3)
        fig.tight_layout()

    out = output_dir / "fs_capacity_per_node.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)

    # =========================================================================
    # PLOT B: Coefficient of Variation (balance metric) for capacity
    # =========================================================================
    fig, ax = plt.subplots(figsize=(10, 6))

    k_vals = []
    cv_vals = []
    for r in valid_results:
        k = r["k"]
        capacities = r["fs_used_mb_per_fs"]
        if capacities and len(capacities) > 1:
            mean_val = np.mean(capacities)
            std_val = np.std(capacities)
            cv = (std_val / mean_val * 100) if mean_val > 0 else 0
            k_vals.append(k)
            cv_vals.append(cv)

    if cv_vals:
        bars = ax.bar(range(len(k_vals)), cv_vals,
                      color=['green' if cv < 10 else 'orange' if cv < 20 else 'red' for cv in cv_vals],
                      edgecolor='black', linewidth=0.5, alpha=0.8)

        for bar, cv in zip(bars, cv_vals):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f"{cv:.1f}%", ha='center', va='bottom', fontsize=11, fontweight='bold')

        ax.axhline(y=10, color='green', linestyle='--', alpha=0.5, label='Good balance (<10%)')
        ax.axhline(y=20, color='orange', linestyle='--', alpha=0.5, label='Moderate imbalance (<20%)')

        ax.set_xticks(range(len(k_vals)))
        ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
        ax.set_ylabel("Coefficient of Variation (%)", fontsize=13)
        ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
        ax.set_title("Capacity Distribution Balance Across Filesystems\n(Lower CV = more even data spread)", fontsize=13)
        ax.legend(fontsize=10, loc='upper right')
        ax.grid(True, axis='y', alpha=0.3)

        out = output_dir / "fs_capacity_balance.png"
        fig.savefig(out, dpi=DPI)
        print(f"Saved: {out}")
    plt.close(fig)


def read_nn_memory_timeseries(nn_dir: Path):
    """Read per-k NameNode memory time series CSVs."""
    timeseries = {}
    for csv_file in sorted(nn_dir.glob("nn_memory_k*.csv")):
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


def has_nn_memory_data(results):
    """Whether CSV rows contain NameNode memory summary data."""
    return any(
        r["nn_heap_before"] > 0 or r["nn_heap_peak"] > 0 or r["nn_heap_avg"] > 0
        for r in results
    )


def read_metadata(meta_path: Path):
    """Read experiment metadata."""
    if meta_path.exists():
        try:
            with meta_path.open() as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}


def _subtitle(metadata):
    """Build a standard subtitle string from metadata."""
    input_gb = metadata.get("input_size_gb", 22)
    block_human = metadata.get("block_size_human", "16MB")
    num_dns = metadata.get("datanode_hosts", "?")
    replication = metadata.get("replication", 3)
    reps = metadata.get("repetitions", "?")
    return (
        f"{input_gb}GB input, {block_human} blocks, rep={replication}, "
        f"{num_dns} DataNodes, {reps} runs avg"
    )


# ============================================================================
# PLOT 1: Runtime vs k (storage dirs per DN) - bar chart
# ============================================================================
def plot_runtime_vs_k(results, metadata, output_dir: Path):
    fig, ax = plt.subplots(figsize=(12, 7))

    k_vals = [r["k"] for r in results]
    runtimes = [r["avg_runtime"] for r in results]
    stddevs = [r["stddev"] for r in results]
    total_dirs = [r["total_dirs"] for r in results]

    colors = plt.colormaps["viridis"](np.linspace(0.2, 0.8, len(k_vals)))

    bars = ax.bar(
        range(len(k_vals)), runtimes,
        yerr=stddevs,
        color=colors, alpha=0.85,
        error_kw={"capsize": 6},
        edgecolor="black", linewidth=0.5,
    )

    for bar, rt, sd, dirs in zip(bars, runtimes, stddevs, total_dirs):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + sd + max(runtimes) * 0.03,
            f"{rt:.1f}s\n({dirs} dirs)",
            ha="center", va="bottom", fontsize=10, fontweight="bold",
        )

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel("Average WordCount Runtime (seconds)", fontsize=13)
    ax.set_title(
        f"WordCount Runtime vs Storage Directory Count\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "runtime_vs_k.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 2: Runtime vs total storage dirs (line plot)
# ============================================================================
def plot_runtime_vs_total_dirs(results, metadata, output_dir: Path):
    fig, ax = plt.subplots(figsize=(10, 7))

    total_dirs = [r["total_dirs"] for r in results]
    runtimes = [r["avg_runtime"] for r in results]
    stddevs = [r["stddev"] for r in results]
    k_vals = [r["k"] for r in results]

    ax.errorbar(
        total_dirs, runtimes,
        yerr=stddevs,
        marker="o", markersize=10, linewidth=2,
        capsize=5, color="steelblue",
    )

    for dirs, rt, sd, k in zip(total_dirs, runtimes, stddevs, k_vals):
        ax.annotate(
            f"k={k}",
            (dirs, rt),
            textcoords="offset points", xytext=(10, 10),
            fontsize=11, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color="gray"),
        )

    ax.set_xlabel("Total Storage Directories in Cluster", fontsize=13)
    ax.set_ylabel("Average WordCount Runtime (seconds)", fontsize=13)
    ax.set_title(
        f"WordCount Runtime vs Total Storage Directories\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    out = output_dir / "runtime_vs_total_dirs.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 3: Speedup vs k=2 baseline
# ============================================================================
def plot_speedup(results, metadata, output_dir: Path):
    if not results or len(results) < 2:
        print("WARNING: Need at least 2 results for speedup plot, skipping.")
        return

    baseline_k = results[0]["k"]
    baseline = results[0]["avg_runtime"]
    if baseline <= 0:
        print("WARNING: Baseline runtime is zero, skipping speedup plot.")
        return

    fig, ax = plt.subplots(figsize=(10, 7))

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
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel(f"Speedup vs k={baseline_k} baseline", fontsize=13)
    ax.set_title(
        f"Speedup from Storage Virtualization\n(>1.0 = faster than baseline k={baseline_k})",
        fontsize=13,
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "speedup_vs_k.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 4: Individual run scatter
# ============================================================================
def plot_individual_runs(results, metadata, output_dir: Path):
    fig, ax = plt.subplots(figsize=(12, 7))

    k_vals = [r["k"] for r in results]
    colors = _k_colors(len(results))

    for i, r in enumerate(results):
        individual = [float(x) for x in r["individual"].split(";") if x]
        x_jitter = np.random.normal(i, 0.05, len(individual))
        ax.scatter(x_jitter, individual, alpha=0.7, s=60, zorder=5, color=colors[i])
        ax.hlines(
            r["avg_runtime"], i - 0.25, i + 0.25,
            color="black", linewidth=2, zorder=10,
            label="Mean" if i == 0 else None,
        )

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel("WordCount Runtime (seconds)", fontsize=13)
    ax.set_title("Individual Run Times per Configuration", fontsize=13)
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "individual_runs.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 5: Log-scale runtime vs k
# ============================================================================
def plot_runtime_vs_k_logscale(results, metadata, output_dir: Path):
    """Line plot with log scale on x-axis for k values."""
    fig, ax = plt.subplots(figsize=(10, 7))

    k_vals = [r["k"] for r in results]
    runtimes = [r["avg_runtime"] for r in results]
    stddevs = [r["stddev"] for r in results]

    ax.errorbar(
        k_vals, runtimes,
        yerr=stddevs,
        marker="o", markersize=10, linewidth=2,
        capsize=5, color="darkred",
    )

    for k, rt in zip(k_vals, runtimes):
        ax.annotate(
            f"{rt:.1f}s",
            (k, rt),
            textcoords="offset points", xytext=(0, 10),
            ha="center", fontsize=9,
        )

    ax.set_xscale("log", base=2)
    ax.set_xticks(k_vals)
    ax.set_xticklabels([str(k) for k in k_vals], fontsize=11)
    ax.set_xlabel("Storage Directories per DataNode (k) [log scale]", fontsize=13)
    ax.set_ylabel("Average WordCount Runtime (seconds)", fontsize=13)
    ax.set_title(
        f"WordCount Runtime vs Storage Directory Count (Log Scale)\n"
        f"({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.grid(True, alpha=0.3, which="both")
    fig.tight_layout()

    out = output_dir / "runtime_vs_k_logscale.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 6: NameNode Memory vs k (bar chart)
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

    ax.bar(
        x - width,
        heap_before,
        width,
        label="Before WordCount",
        color="lightblue",
        edgecolor="black",
        linewidth=0.5,
    )
    ax.bar(
        x,
        heap_avg,
        width,
        label="Avg During WordCount",
        color="steelblue",
        edgecolor="black",
        linewidth=0.5,
    )
    peak_bars = ax.bar(
        x + width,
        heap_peak,
        width,
        label="Peak During WordCount",
        color="darkred",
        alpha=0.8,
        edgecolor="black",
        linewidth=0.5,
    )

    peak_max = max(heap_peak) if heap_peak else 0
    for bar, val in zip(peak_bars, heap_peak):
        if val > 0 and peak_max > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + peak_max * 0.02,
                f"{val}MB",
                ha="center",
                va="bottom",
                fontsize=9,
                fontweight="bold",
            )

    ax.set_xticks(x)
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel("NameNode Heap Memory (MB)", fontsize=13)
    ax.set_title(
        f"NameNode Memory Usage vs Storage Virtualization\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "nn_memory_vs_k.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 7: Dual-axis — Runtime + NN Peak Memory vs k
# ============================================================================
def plot_runtime_and_memory(results, metadata, output_dir: Path):
    """Combined dual-axis plot: runtime on left y-axis, NN memory on right."""
    fig, ax1 = plt.subplots(figsize=(11, 7))

    k_vals = [r["k"] for r in results]
    runtimes = [r["avg_runtime"] for r in results]
    stddevs = [r["stddev"] for r in results]
    heap_peak = [r["nn_heap_peak"] for r in results]
    total_dirs = [r["total_dirs"] for r in results]

    x = np.arange(len(k_vals))

    color1 = "steelblue"
    ax1.bar(
        x - 0.15,
        runtimes,
        0.35,
        yerr=stddevs,
        color=color1,
        alpha=0.7,
        error_kw={"capsize": 4},
        edgecolor="black",
        linewidth=0.5,
        label="WordCount Runtime",
    )
    ax1.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax1.set_ylabel("Average Runtime (seconds)", fontsize=13, color=color1)
    ax1.tick_params(axis="y", labelcolor=color1)

    ax2 = ax1.twinx()
    color2 = "darkred"
    ax2.bar(
        x + 0.15,
        heap_peak,
        0.35,
        color=color2,
        alpha=0.7,
        edgecolor="black",
        linewidth=0.5,
        label="NN Peak Heap (MB)",
    )
    ax2.set_ylabel("NameNode Peak Heap (MB)", fontsize=13, color=color2)
    ax2.tick_params(axis="y", labelcolor=color2)

    runtime_max = max(runtimes) if runtimes else 0
    for i, dirs in enumerate(total_dirs):
        if runtime_max > 0:
            ax1.text(
                i,
                -runtime_max * 0.08,
                f"{dirs} dirs",
                ha="center",
                fontsize=9,
                color="gray",
            )

    ax1.set_xticks(x)
    ax1.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=10)

    ax1.set_title(
        f"WordCount Runtime & NameNode Memory vs Storage Virtualization\n"
        f"({_subtitle(metadata)})",
        fontsize=12,
    )
    ax1.grid(True, axis="y", alpha=0.2)
    fig.tight_layout()

    out = output_dir / "runtime_and_memory_vs_k.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 8: NameNode Memory Time Series (per k)
# ============================================================================
def plot_nn_memory_timeseries(timeseries, metadata, output_dir: Path):
    """Line plots of NameNode heap over time for each k value, overlaid."""
    if not timeseries:
        print("No NameNode memory time series data found, skipping.")
        return

    sorted_items = sorted(timeseries.items())
    colors = _k_colors(len(sorted_items))
    datanode_hosts = metadata.get("datanode_hosts", metadata.get("physical_nodes", 5))

    many = len(sorted_items) > 8
    fig, ax = plt.subplots(figsize=(14 if many else 12, 7))

    for idx, (k, rows) in enumerate(sorted_items):
        heap_values = [r["heap_used_mb"] for r in rows]
        time_offsets = list(range(0, len(heap_values) * 5, 5))

        ax.plot(
            time_offsets[:len(heap_values)],
            heap_values,
            marker=MARKERS[idx % len(MARKERS)],
            color=colors[idx],
            linewidth=1.5,
            markersize=4,
            alpha=0.8,
            label=f"k={k} ({k * datanode_hosts} dirs)",
        )

    ax.set_xlabel("Time Since Monitor Start (seconds)", fontsize=13)
    ax.set_ylabel("NameNode Heap Used (MB)", fontsize=13)
    ax.set_title(
        f"NameNode Heap Memory Over Time (During WordCount)\n"
        f"({_subtitle(metadata)})",
        fontsize=12,
    )
    if many:
        ax.legend(fontsize=9, bbox_to_anchor=(1.01, 1), loc="upper left",
                  borderaxespad=0, title="k value")
    else:
        ax.legend(fontsize=10, loc="best")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    out = output_dir / "nn_memory_timeseries.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 9: Mean blocks per FS (block dilution as k grows)
# ============================================================================
def plot_block_distribution(results, metadata, output_dir: Path):
    """Line+bar chart showing mean blocks per filesystem as k grows.

    The total block count is constant (same input every run), so plotting the
    raw total would be a flat line.  Instead we show nn_block_count / (k * num_dns)
    — the expected blocks each FS holds if the NameNode distributes evenly.
    This illustrates block *dilution*: each FS gets fewer blocks as k increases,
    which means smaller per-FS metadata working sets for the NameNode.
    """
    valid = [r for r in results if r["nn_block_count"] > 0]
    if not valid:
        return

    num_dns = metadata.get("datanode_hosts", 4)
    k_vals = [r["k"] for r in valid]
    total_blocks = [r["nn_block_count"] for r in valid]
    # total storage dirs in cluster = k * num_dns
    total_fses = [k * num_dns for k in k_vals]
    mean_per_fs = [t / f for t, f in zip(total_blocks, total_fses)]

    colors = _k_colors(len(valid))
    fig, ax = plt.subplots(figsize=(max(10, len(valid) * 1.1), 7))

    bars = ax.bar(range(len(k_vals)), mean_per_fs, color=colors,
                  alpha=0.85, edgecolor="black", linewidth=0.5)

    max_val = max(mean_per_fs) if mean_per_fs else 1
    for bar, mpf, total in zip(bars, mean_per_fs, total_blocks):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_val * 0.03,
            f"{mpf:.0f}\n({total} total)",
            ha="center", va="bottom", fontsize=9, fontweight="bold",
        )

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}\n({k * num_dns} FSes)" for k in k_vals], fontsize=11)
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel("Mean Blocks per Filesystem", fontsize=13)
    ax.set_title(
        f"Block Dilution: Mean Blocks per FS vs k\n"
        f"(total block count is constant; each FS holds fewer as k grows)\n"
        f"({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "block_distribution.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# IOSTAT PLOTS
# ============================================================================

def read_iostat_data(iostat_dir: Path):
    """Read parsed iostat summary CSVs for each k value.

    Returns dict: {k: [row_dicts]} where each row has keys:
        timestamp, node, device, r_per_s, w_per_s, rkB_per_s, wkB_per_s,
        r_await, w_await, rareq_sz, wareq_sz, aqu_sz, util
    """
    data = {}
    for csv_file in sorted(iostat_dir.glob("iostat_summary_k*.csv")):
        k_str = csv_file.stem.replace("iostat_summary_k", "")
        try:
            k = int(k_str)
        except ValueError:
            continue

        rows = []
        with csv_file.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # Validate device name: should be alphanumeric (not "Linux", timestamp, etc)
                    device = row["device"].strip()
                    if not device or device in ("Linux", "Device"):
                        continue
                    # Skip malformed device values (dates sometimes leak here)
                    if "/" in device:
                        continue
                    if not any(c.isalnum() for c in device):
                        continue

                    # Validate timestamp format (date + time required)
                    ts = row["timestamp"].strip()
                    if not ts or "/" not in ts or ":" not in ts:
                        continue
                    
                    rows.append({
                        "timestamp": ts,
                        "node": row["node"],
                        "device": device,
                        "r_per_s": float(row.get("r_per_s", 0) or 0),
                        "w_per_s": float(row.get("w_per_s", 0) or 0),
                        "rkB_per_s": float(row.get("rkB_per_s", 0) or 0),
                        "wkB_per_s": float(row.get("wkB_per_s", 0) or 0),
                        "r_await": float(row.get("r_await", 0) or 0),
                        "w_await": float(row.get("w_await", 0) or 0),
                        "rareq_sz": float(row.get("rareq_sz", 0) or 0),
                        "wareq_sz": float(row.get("wareq_sz", 0) or 0),
                        "aqu_sz": float(row.get("aqu_sz", 0) or 0),
                        "util": float(row.get("util", 0) or 0),
                    })
                except (ValueError, KeyError, AttributeError):
                    pass
        if rows:
            data[k] = rows
    return data


def _weighted_stats(values, weights):
    """IOPS-weighted mean and stddev. Returns (mean, std). Zero total weight -> (0, 0).

    iostat reports await=0 when no ops occurred in the interval; including those
    zeros in a plain mean biases latency downward. Weighting by IOPS (per-interval
    ops per second) treats each operation equally and excludes idle intervals.
    """
    values = np.asarray(values, dtype=float)
    weights = np.asarray(weights, dtype=float)
    total_w = weights.sum()
    if total_w <= 0:
        return 0.0, 0.0
    mean = float((values * weights).sum() / total_w)
    var = float((weights * (values - mean) ** 2).sum() / total_w)
    return mean, float(np.sqrt(max(var, 0.0)))


def plot_iostat_latency_vs_k(iostat_data, metadata, output_dir: Path):
    """Grouped bar chart: IOPS-weighted read and write latency (ms) for each k."""
    fig, ax = plt.subplots(figsize=(11, 7))

    k_vals = sorted(iostat_data.keys())
    avg_r_awaits, std_r_awaits = [], []
    avg_w_awaits, std_w_awaits = [], []

    for k in k_vals:
        rows = iostat_data[k]
        r_mean, r_std = _weighted_stats(
            [r["r_await"] for r in rows], [r["r_per_s"] for r in rows]
        )
        w_mean, w_std = _weighted_stats(
            [r["w_await"] for r in rows], [r["w_per_s"] for r in rows]
        )
        avg_r_awaits.append(r_mean)
        std_r_awaits.append(r_std)
        avg_w_awaits.append(w_mean)
        std_w_awaits.append(w_std)

    x = np.arange(len(k_vals))
    width = 0.35
    bars_r = ax.bar(x - width / 2, avg_r_awaits, width, yerr=std_r_awaits,
                    label="r_await (read)", color="#4C9BE8", alpha=0.85,
                    error_kw={"capsize": 5}, edgecolor="black", linewidth=0.5)
    bars_w = ax.bar(x + width / 2, avg_w_awaits, width, yerr=std_w_awaits,
                    label="w_await (write)", color="#E8724C", alpha=0.85,
                    error_kw={"capsize": 5}, edgecolor="black", linewidth=0.5)

    all_vals = avg_r_awaits + avg_w_awaits
    max_val = max(all_vals) if all_vals else 1
    for bar, avg, std in zip(list(bars_r) + list(bars_w),
                              avg_r_awaits + avg_w_awaits,
                              std_r_awaits + std_w_awaits):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + std + max_val * 0.02,
            f"{avg:.1f}",
            ha="center", va="bottom", fontsize=9,
        )

    ax.set_xticks(x)
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel("Average Disk Latency (ms)", fontsize=13)
    ax.set_title(
        f"Disk Read/Write Latency vs Storage Directory Count\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.legend(fontsize=11)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "iostat_latency_vs_k.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


def plot_iostat_util_vs_k(iostat_data, metadata, output_dir: Path):
    """Bar chart: average disk utilization (%) across active devices, per k.

    Excludes devices with no observed traffic (r/s + w/s == 0 throughout) so the
    mean reflects real busy-ness, not averaged against idle SSDs.
    """
    fig, ax = plt.subplots(figsize=(11, 7))

    k_vals = sorted(iostat_data.keys())
    avg_utils = []
    std_utils = []

    for k in k_vals:
        # Keep only rows from devices that did I/O in this interval.
        utils = [r["util"] for r in iostat_data[k] if (r["r_per_s"] + r["w_per_s"]) > 0]
        avg_utils.append(np.mean(utils) if utils else 0)
        std_utils.append(np.std(utils) if utils else 0)

    colors = plt.colormaps["plasma"](np.linspace(0.2, 0.8, len(k_vals)))
    bars = ax.bar(
        range(len(k_vals)), avg_utils,
        yerr=std_utils,
        color=colors, alpha=0.85,
        error_kw={"capsize": 6},
        edgecolor="black", linewidth=0.5,
    )

    for bar, avg, std in zip(bars, avg_utils, std_utils):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + std + max(avg_utils) * 0.03,
            f"{avg:.1f}%",
            ha="center", va="bottom", fontsize=10, fontweight="bold",
        )

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel("Average Disk Utilization (%)", fontsize=13)
    ax.set_title(
        f"Disk Utilization vs Storage Directory Count\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "iostat_util_vs_k.png"
    fig.savefig(out, dpi=DPI)
    print(f"Saved: {out}")
    plt.close(fig)


def _snap_ts(ts_str):
    """Convert an iostat timestamp string to a 5-second-snapped epoch integer.

    Nodes run independent iostat processes; their clocks may differ by 1-3 s.
    Snapping to the nearest 5 s grid (the iostat interval) ensures samples from
    all nodes land in the same bucket even when clocks are slightly out of sync.
    Falls back to the raw string if parsing fails (keeps old behaviour).
    """
    ts = (ts_str or "").strip()
    for fmt in (
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%y %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(ts, fmt)
            epoch = int(dt.timestamp())
            return (epoch // 5) * 5
        except (ValueError, AttributeError):
            pass
    return ts_str


def _ts_aggregate(rows, metric_key, mode):
    """Aggregate a per-device iostat metric over time.

    mode:
      "sum"   - cluster total at each timestamp (throughput/IOPS: rkB/s, wkB/s, r/s, w/s)
      "mean"  - average across devices that reported in that interval (%util, queue, req size)
      "wmean_r" - IOPS-weighted by r/s (read-side latency: r_await, rareq_sz)
      "wmean_w" - IOPS-weighted by w/s (write-side latency: w_await, wareq_sz)

    Returns (time_offsets_seconds, values) in timestamp order.
    Timestamps are snapped to the nearest 5-second epoch so that samples from
    different nodes (whose clocks may drift by 1-3 s) are bucketed together.
    """
    ts_order = []
    buckets = {}
    for r in rows:
        ts = _snap_ts(r["timestamp"])
        if ts not in buckets:
            buckets[ts] = []
            ts_order.append(ts)
        buckets[ts].append(r)

    # Sort chronologically. Keys are int epochs after _snap_ts; sorting makes
    # the order independent of which node's rows appear first in the input list.
    try:
        ts_order.sort()
    except TypeError:
        pass  # mixed int/str (fallback path) — keep insertion order

    values = []
    for ts in ts_order:
        group = buckets[ts]
        if mode == "sum":
            values.append(sum(r[metric_key] for r in group))
        elif mode == "mean":
            nonzero = [r[metric_key] for r in group if (r["r_per_s"] + r["w_per_s"]) > 0]
            values.append(np.mean(nonzero) if nonzero else 0.0)
        elif mode in ("wmean_r", "wmean_w"):
            w_key = "r_per_s" if mode == "wmean_r" else "w_per_s"
            tot = sum(r[w_key] for r in group)
            if tot > 0:
                values.append(sum(r[metric_key] * r[w_key] for r in group) / tot)
            else:
                values.append(0.0)
        else:
            raise ValueError(mode)

    time_offsets = list(range(0, len(ts_order) * 5, 5))
    return time_offsets, values


def plot_iostat_timeseries(iostat_data, metadata, output_dir: Path):
    """One PNG per metric: cluster-aggregated iostat value over time for each k."""
    metrics = [
        ("w_await",   "Write Latency w_await (ms)",    "wmean_w", "iostat_ts_w_await.png"),
        ("r_await",   "Read Latency r_await (ms)",     "wmean_r", "iostat_ts_r_await.png"),
        ("util",      "Disk Utilization %util",        "mean",    "iostat_ts_util.png"),
        ("wkB_per_s", "Cluster Write Throughput (KB/s)", "sum",   "iostat_ts_wkB_per_s.png"),
        ("rkB_per_s", "Cluster Read Throughput (KB/s)",  "sum",   "iostat_ts_rkB_per_s.png"),
        ("w_per_s",   "Cluster Write IOPS (w/s)",      "sum",     "iostat_ts_w_per_s.png"),
        ("r_per_s",   "Cluster Read IOPS (r/s)",       "sum",     "iostat_ts_r_per_s.png"),
        ("aqu_sz",    "I/O Queue Depth (aqu-sz)",      "mean",    "iostat_ts_aqu_sz.png"),
    ]

    k_vals = sorted(iostat_data.keys())
    colors = _k_colors(len(k_vals))
    datanode_hosts = metadata.get("datanode_hosts", metadata.get("physical_nodes", 5))
    many = len(k_vals) > 8

    for metric_key, ylabel, mode, filename in metrics:
        fig, ax = plt.subplots(figsize=(14 if many else 11, 7))
        has_data = False
        for idx, k in enumerate(k_vals):
            time_offsets, vals = _ts_aggregate(iostat_data[k], metric_key, mode)
            if time_offsets and vals:
                has_data = True
                ax.plot(
                    time_offsets, vals,
                    marker=MARKERS[idx % len(MARKERS)],
                    color=colors[idx],
                    linewidth=1.5, markersize=4, alpha=0.85,
                    label=f"k={k} ({k * datanode_hosts} dirs)",
                )
        
        ax.set_xlabel("Cumulative WordCount Active Time (seconds, runs concatenated)", fontsize=11)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.set_title(
            f"{ylabel} Over Time (During WordCount)\n({_subtitle(metadata)})",
            fontsize=12,
        )
        
        # If no data was plotted, show a message
        if not has_data:
            ax.text(0.5, 0.5, "No data available for this metric",
                   ha="center", va="center", transform=ax.transAxes,
                   fontsize=14, color="gray", style="italic")
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
        
        if many:
            ax.legend(fontsize=9, bbox_to_anchor=(1.01, 1), loc="upper left",
                      borderaxespad=0, title="k value")
        else:
            ax.legend(fontsize=10, loc="best")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()

        out = output_dir / filename
        fig.savefig(out, dpi=DPI)
        print(f"Saved: {out}")
        plt.close(fig)


def plot_iostat_per_node_comparison(iostat_data, metadata, output_dir: Path):
    """One PNG per metric: per-node/device bars grouped by k value.

    Aggregation per (node, device, k):
      - sum        : mean of per-interval sums (avg cluster throughput contribution)
      - mean       : mean over intervals where the device was active
      - wmean_r/_w : IOPS-weighted mean (latency, per-op request size)
    """
    metrics = [
        ("w_await",   "Write Latency w_await (ms)",      "lower is better",   "wmean_w", "iostat_node_w_await.png"),
        ("r_await",   "Read Latency r_await (ms)",       "lower is better",   "wmean_r", "iostat_node_r_await.png"),
        ("util",      "Disk Utilization %util",          "lower is better",   "mean",    "iostat_node_util.png"),
        ("rkB_per_s", "Read Throughput rkB/s",           "higher is better",  "mean",    "iostat_node_rkB_per_s.png"),
        ("wkB_per_s", "Write Throughput wkB/s",          "higher is better",  "mean",    "iostat_node_wkB_per_s.png"),
        ("rareq_sz",  "Read Request Size rareq-sz (KB)", "higher is better",  "wmean_r", "iostat_node_rareq_sz.png"),
        ("wareq_sz",  "Write Request Size wareq-sz (KB)","higher is better",  "wmean_w", "iostat_node_wareq_sz.png"),
        ("aqu_sz",    "I/O Queue Depth aqu-sz",          "lower is better",   "mean",    "iostat_node_aqu_sz.png"),
        ("r_per_s",   "Read IOPS r/s",                   "context-dependent", "mean",    "iostat_node_r_per_s.png"),
        ("w_per_s",   "Write IOPS w/s",                  "context-dependent", "mean",    "iostat_node_w_per_s.png"),
    ]

    k_vals = sorted(iostat_data.keys())

    from collections import defaultdict

    # For each k, bucket rows by "node/device" then by metric list
    node_rows = {}  # {k: {nd_key: [row, ...]}}
    for k in k_vals:
        bucket = defaultdict(list)
        for r in iostat_data[k]:
            bucket[f"{r['node']}/{r['device']}"].append(r)
        node_rows[k] = bucket

    # Collect all nodes that appear in any k value (even with low/zero util).
    # This ensures plots still render even when I/O is minimal.
    all_nodes = set()
    for k in k_vals:
        for nd_key, rows in node_rows[k].items():
            if rows:  # Include any node with at least one data point
                all_nodes.add(nd_key)
    nodes = sorted(all_nodes)
    if not nodes:
        return

    def _node_agg(rows, metric_key, mode):
        if not rows:
            return 0.0
        if mode == "mean":
            active = [r[metric_key] for r in rows if (r["r_per_s"] + r["w_per_s"]) > 0]
            return float(np.mean(active)) if active else 0.0
        w_key = "r_per_s" if mode == "wmean_r" else "w_per_s"
        tot = sum(r[w_key] for r in rows)
        if tot <= 0:
            return 0.0
        return float(sum(r[metric_key] * r[w_key] for r in rows) / tot)

    colors = _k_colors(len(k_vals))
    x = np.arange(len(nodes))
    width = 0.8 / max(len(k_vals), 1)
    short_labels = [n.replace("tapuz", "t") for n in nodes]
    many = len(k_vals) > 8

    for metric_key, ylabel, direction, mode, filename in metrics:
        fig, ax = plt.subplots(figsize=(max(14 if many else 10, len(nodes) * 1.2), 7))
        all_avgs = []
        for ki, k in enumerate(k_vals):
            avgs = [_node_agg(node_rows[k].get(nd, []), metric_key, mode) for nd in nodes]
            all_avgs.extend(avgs)
            bars = ax.bar(
                x + ki * width - 0.4 + width / 2, avgs, width,
                label=f"k={k}", color=colors[ki], alpha=0.85,
                edgecolor="black", linewidth=0.5,
            )
            peak = max(all_avgs) if all_avgs else 1.0
            for bar, avg in zip(bars, avgs):
                if avg > 0:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + peak * 0.02,
                        f"{avg:.1f}",
                        ha="center", va="bottom", fontsize=8,
                    )

        ax.set_xticks(x)
        ax.set_xticklabels(short_labels, fontsize=10)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.set_xlabel("Node / Device", fontsize=12)
        ax.set_title(
            f"{ylabel} per Node ({direction})\n({_subtitle(metadata)})",
            fontsize=12,
        )
        if many:
            ax.legend(fontsize=9, bbox_to_anchor=(1.01, 1), loc="upper left",
                      borderaxespad=0, title="k")
        else:
            ax.legend(fontsize=10, loc="best", title="k")
        ax.grid(True, axis="y", alpha=0.3)
        fig.tight_layout()

        out = output_dir / filename
        fig.savefig(out, dpi=DPI)
        print(f"Saved: {out}")
        plt.close(fig)


# ============================================================================
# MAIN
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Plot storage virtualization loopback experiment results."
    )
    parser.add_argument(
        "results_dir",
        type=Path,
        help="Path to the results directory (e.g., results/storage_virtualization_loopback/latest)",
    )
    args = parser.parse_args()

    results_dir = args.results_dir
    csv_path = results_dir / "results.csv"
    meta_path = results_dir / "metadata.json"
    nn_dir = results_dir / "namenode_memory"

    if not csv_path.exists():
        raise SystemExit(f"ERROR: {csv_path} not found")

    iostat_dir = results_dir / "iostat"

    results = read_results(csv_path)
    metadata = read_metadata(meta_path)
    timeseries = read_nn_memory_timeseries(nn_dir) if nn_dir.exists() else {}
    iostat_data = read_iostat_data(iostat_dir) if iostat_dir.exists() else {}
    nn_memory_in_csv = has_nn_memory_data(results)

    print(f"Loaded {len(results)} configurations from {csv_path}")
    print(f"k values: {[r['k'] for r in results]}")
    print(f"NN memory summary in CSV: {'yes' if nn_memory_in_csv else 'no'}")
    print(f"NN memory time series: {sorted(timeseries.keys()) if timeseries else 'none'}")
    print(f"iostat data: {sorted(iostat_data.keys()) if iostat_data else 'none'}")
    print()

    plot_runtime_vs_k(results, metadata, results_dir)
    plot_runtime_vs_total_dirs(results, metadata, results_dir)
    plot_speedup(results, metadata, results_dir)
    plot_individual_runs(results, metadata, results_dir)
    plot_runtime_vs_k_logscale(results, metadata, results_dir)

    if nn_memory_in_csv:
        plot_nn_memory_vs_k(results, metadata, results_dir)
        plot_runtime_and_memory(results, metadata, results_dir)
    else:
        print("NameNode summary memory columns not found in CSV, skipping summary memory plots.")

    plot_nn_memory_timeseries(timeseries, metadata, results_dir)

    # Block dilution chart (always generated if nn_block_count is populated)
    plot_block_distribution(results, metadata, results_dir)

    # Per-filesystem block distribution if data is present
    if any(r.get("block_counts_per_fs") for r in results):
        plot_per_fs_block_distribution(results, metadata, results_dir)

    # Input-only block distribution if data is present
    if any(r.get("input_block_counts_per_fs") for r in results):
        plot_input_blocks_per_fs(results, metadata, results_dir)

    # Filesystem capacity distribution if data is present
    if any(r.get("fs_used_mb_per_fs") for r in results):
        plot_fs_capacity_per_node(results, metadata, results_dir)

    # iostat disk I/O metrics if data is present
    if iostat_data:
        plot_iostat_latency_vs_k(iostat_data, metadata, results_dir)
        plot_iostat_util_vs_k(iostat_data, metadata, results_dir)
        plot_iostat_timeseries(iostat_data, metadata, results_dir)
        plot_iostat_per_node_comparison(iostat_data, metadata, results_dir)

    print()
    total = 5  # runtime_vs_k, runtime_vs_total_dirs, speedup, individual_runs, logscale
    total += 1  # block_distribution (dilution)
    if nn_memory_in_csv:
        total += 2
    if timeseries:
        total += 1
    if any(r.get("block_counts_per_fs") for r in results):
        total += 1
    if any(r.get("input_block_counts_per_fs") for r in results):
        total += 3  # per_fs + boxplot + balance
    if any(r.get("fs_used_mb_per_fs") for r in results):
        total += 2  # fs_capacity_per_node + fs_capacity_balance
    if iostat_data:
        # latency_vs_k + util_vs_k + 8 timeseries + 10 per-node
        total += 2 + 8 + 10
    print(f"All plots generated! ({total} total)")
    print(f"Output directory: {results_dir}")


if __name__ == "__main__":
    main()
