#!/usr/bin/env python3
"""
Plot Storage Virtualization Loopback Experiment Results

Generates visualizations showing how WordCount performance changes as the
number of storage directories (k loopback filesystems) per DataNode scales
from 2 to 512.

CSV format (produced by run-experiment-loopback-fs.sh):
    k_storage_dirs,total_storage_dirs,datanodes,avg_runtime_seconds,stddev_runtime,individual_runtimes,
    nn_heap_before_mb,nn_heap_peak_mb,nn_heap_avg_mb,nn_block_count

Usage:
    python3 plot-results.py <results_directory>
    python3 plot-results.py results/storage_virtualization_loopback/latest
"""

import argparse
import csv
import json
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
    """Bar plot showing block counts for each local filesystem (loopback) for each k."""
    fig, ax = plt.subplots(figsize=(max(10, len(results)*2), 7))
    labels = []
    values = []
    colors = []
    color_map = cm.get_cmap('tab10')
    for idx, r in enumerate(results):
        k = r["k"]
        counts = r.get("block_counts_per_fs", [])
        for i, cnt in enumerate(counts):
            labels.append(f"k={k}-fs{i+1}")
            values.append(cnt)
            colors.append(color_map(idx % 10))
    ax.bar(labels, values, color=colors)
    ax.set_ylabel("Blocks per Filesystem", fontsize=13)
    ax.set_xlabel("Filesystem (by k and index)", fontsize=13)
    ax.set_title("Block Distribution per Local Filesystem (All Files)", fontsize=14)
    ax.tick_params(axis='x', rotation=45)
    fig.tight_layout()
    out = output_dir / "per_fs_block_distribution.png"
    fig.savefig(out, dpi=150)
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

    # =========================================================================
    # PLOT A: Grouped bar chart with per-node coloring
    # =========================================================================
    fig, ax = plt.subplots(figsize=(14, 8))

    x_positions = []
    x_labels = []
    all_counts = []
    node_colors = []
    k_boundaries = []
    current_x = 0

    color_palette = cm.Set2(np.linspace(0, 1, num_datanode_hosts))

    for r in valid_results:
        k = r["k"]
        counts = r["input_block_counts_per_fs"]
        k_boundaries.append(current_x)

        # Each count corresponds to a filesystem
        # Filesystems are distributed across nodes: node1_fs1, node1_fs2, ..., node2_fs1, ...
        for i, cnt in enumerate(counts):
            node_idx = i // k if k > 0 else 0  # Which DataNode this FS belongs to
            fs_in_node = i % k + 1 if k > 0 else i + 1  # Which FS within the node

            x_positions.append(current_x)
            x_labels.append(f"N{node_idx+1}\nFS{fs_in_node}")
            all_counts.append(cnt)
            node_colors.append(color_palette[node_idx % num_datanode_hosts])
            current_x += 1

        current_x += 1.5  # Gap between k values

    bars = ax.bar(x_positions, all_counts, color=node_colors, edgecolor='black', linewidth=0.5, alpha=0.85)

    # Add k value labels above each group
    for i, r in enumerate(valid_results):
        k = r["k"]
        counts = r["input_block_counts_per_fs"]
        if counts:
            start_x = k_boundaries[i]
            end_x = start_x + len(counts) - 1
            mid_x = (start_x + end_x) / 2
            max_in_group = max(counts) if counts else 0
            ax.text(mid_x, max_in_group + max(all_counts) * 0.05, f"k={k}",
                    ha='center', va='bottom', fontsize=12, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgray', alpha=0.7))

            # Add statistics annotation
            mean_val = np.mean(counts)
            std_val = np.std(counts)
            cv = (std_val / mean_val * 100) if mean_val > 0 else 0
            ax.text(mid_x, -max(all_counts) * 0.12,
                    f"μ={mean_val:.0f}, σ={std_val:.1f}\nCV={cv:.1f}%",
                    ha='center', va='top', fontsize=9, color='gray')

    # Create legend for nodes
    legend_handles = [plt.Rectangle((0,0),1,1, color=color_palette[i], alpha=0.85)
                      for i in range(min(num_datanode_hosts, len(color_palette)))]
    legend_labels = [f"Node {i+1}" for i in range(num_datanode_hosts)]
    ax.legend(legend_handles, legend_labels, loc='upper right', fontsize=10, title="DataNodes")

    ax.set_ylabel("Input File Blocks (with replicas)", fontsize=13)
    ax.set_xlabel("Filesystem (Node / FS index)", fontsize=13)
    ax.set_title("Input File Block Distribution per Filesystem\n(Grouped by k, colored by DataNode)", fontsize=14)
    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels, fontsize=8, rotation=0)
    ax.grid(True, axis='y', alpha=0.3)
    fig.tight_layout()

    out = output_dir / "input_blocks_per_fs.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)

    # =========================================================================
    # PLOT B: Box plot showing distribution per k value
    # =========================================================================
    fig, ax = plt.subplots(figsize=(10, 7))

    box_data = []
    box_labels = []
    for r in valid_results:
        k = r["k"]
        counts = r["input_block_counts_per_fs"]
        if counts:
            box_data.append(counts)
            box_labels.append(f"k={k}\n({len(counts)} FSes)")

    if box_data:
        bp = ax.boxplot(box_data, labels=box_labels, patch_artist=True)
        colors = cm.viridis(np.linspace(0.2, 0.8, len(box_data)))
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)

        # Add mean markers
        means = [np.mean(d) for d in box_data]
        ax.scatter(range(1, len(means)+1), means, marker='D', color='red', s=50, zorder=5, label='Mean')

        ax.set_ylabel("Blocks per Filesystem", fontsize=13)
        ax.set_xlabel("Configuration", fontsize=13)
        ax.set_title("Input Block Distribution Statistics per k Value\n(Box shows quartiles, diamond shows mean)", fontsize=13)
        ax.legend(fontsize=10)
        ax.grid(True, axis='y', alpha=0.3)

        out = output_dir / "input_blocks_boxplot.png"
        fig.savefig(out, dpi=150)
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
        fig.savefig(out, dpi=150)
        print(f"Saved: {out}")
    plt.close(fig)


def plot_fs_capacity_per_node(results, metadata, output_dir: Path):
    """
    Histogram showing used filesystem capacity per loopback FS.
    This is Aviad's requested visualization:
    - X-axis: Filesystem 'number' (grouped by k value)
    - Y-axis: Used filesystem capacity (MB)
    - Different coloring per DataNode
    """
    # Filter results that have capacity data
    valid_results = [r for r in results if r.get("fs_used_mb_per_fs")]
    if not valid_results:
        print("No filesystem capacity data available, skipping capacity plot.")
        return

    num_datanode_hosts = metadata.get("datanode_hosts", 4)

    # =========================================================================
    # PLOT A: Grouped bar chart with per-node coloring
    # =========================================================================
    fig, ax = plt.subplots(figsize=(14, 8))

    x_positions = []
    x_labels = []
    all_capacities = []
    node_colors = []
    k_boundaries = []
    current_x = 0

    # Use same color palette as plot_input_blocks_per_fs for consistency
    color_palette = cm.Set2(np.linspace(0, 1, num_datanode_hosts))

    for r in valid_results:
        k = r["k"]
        capacities = r["fs_used_mb_per_fs"]
        k_boundaries.append(current_x)

        # Each capacity value corresponds to a filesystem
        # Filesystems are distributed across nodes: node1_fs1, node1_fs2, ..., node2_fs1, ...
        for i, cap in enumerate(capacities):
            node_idx = i // k if k > 0 else 0  # Which DataNode this FS belongs to
            fs_in_node = i % k + 1 if k > 0 else i + 1  # Which FS within the node

            x_positions.append(current_x)
            x_labels.append(f"N{node_idx+1}\nFS{fs_in_node}")
            all_capacities.append(cap)
            node_colors.append(color_palette[node_idx % num_datanode_hosts])
            current_x += 1

        current_x += 1.5  # Gap between k values

    bars = ax.bar(x_positions, all_capacities, color=node_colors,
                  edgecolor='black', linewidth=0.5, alpha=0.85)

    # Add k value labels above each group
    for i, r in enumerate(valid_results):
        k = r["k"]
        capacities = r["fs_used_mb_per_fs"]
        if capacities:
            start_x = k_boundaries[i]
            end_x = start_x + len(capacities) - 1
            mid_x = (start_x + end_x) / 2
            max_in_group = max(capacities) if capacities else 0
            ax.text(mid_x, max_in_group + max(all_capacities) * 0.05, f"k={k}",
                    ha='center', va='bottom', fontsize=12, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgray', alpha=0.7))

            # Add statistics annotation
            mean_val = np.mean(capacities)
            std_val = np.std(capacities)
            cv = (std_val / mean_val * 100) if mean_val > 0 else 0
            ax.text(mid_x, -max(all_capacities) * 0.12,
                    f"mean={mean_val:.0f}MB, std={std_val:.1f}\nCV={cv:.1f}%",
                    ha='center', va='top', fontsize=9, color='gray')

    # Create legend for nodes
    legend_handles = [plt.Rectangle((0,0),1,1, color=color_palette[i], alpha=0.85)
                      for i in range(min(num_datanode_hosts, len(color_palette)))]
    legend_labels = [f"Node {i+1}" for i in range(num_datanode_hosts)]
    ax.legend(legend_handles, legend_labels, loc='upper right', fontsize=10, title="DataNodes")

    ax.set_ylabel("Used Filesystem Capacity (MB)", fontsize=13)
    ax.set_xlabel("Filesystem (Node / FS index)", fontsize=13)
    ax.set_title("Filesystem Used Capacity per Loopback FS\n(Grouped by k, colored by DataNode)", fontsize=14)
    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels, fontsize=8, rotation=0)
    ax.grid(True, axis='y', alpha=0.3)
    fig.tight_layout()

    out = output_dir / "fs_capacity_per_node.png"
    fig.savefig(out, dpi=150)
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
        fig.savefig(out, dpi=150)
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
    input_gb = metadata.get("input_size_gb", 20)
    block_human = metadata.get("block_size_human", "128MB")
    num_dns = metadata.get("datanode_hosts", "?")
    replication = metadata.get("replication", 3)
    reps = metadata.get("repetitions", "?")
    return (
        f"{input_gb}GB input, {block_human} blocks, replication={replication}, "
        f"{num_dns} DataNodes, {reps} runs averaged"
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

    colors = cm.viridis(np.linspace(0.2, 0.8, len(k_vals)))

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
    fig.savefig(out, dpi=150)
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
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 3: Speedup vs k=2 baseline
# ============================================================================
def plot_speedup(results, metadata, output_dir: Path):
    if not results or results[0]["k"] != 2:
        print("WARNING: k=2 not found as first result, skipping speedup plot.")
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
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel("Speedup vs k=2", fontsize=13)
    ax.set_title(
        "Speedup from Storage Virtualization\n(>1.0 = faster than baseline k=2)",
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
    fig, ax = plt.subplots(figsize=(12, 7))

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
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
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
    fig.savefig(out, dpi=150)
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
    fig.savefig(out, dpi=150)
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
    fig.savefig(out, dpi=150)
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

    fig, ax = plt.subplots(figsize=(12, 7))

    colors = cm.tab10(np.linspace(0, 0.8, len(timeseries)))
    markers = ["o", "s", "^", "D", "v"]

    datanode_hosts = metadata.get("datanode_hosts", metadata.get("physical_nodes", 5))

    for idx, (k, rows) in enumerate(sorted(timeseries.items())):
        heap_values = [r["heap_used_mb"] for r in rows]
        time_offsets = list(range(0, len(heap_values) * 5, 5))

        ax.plot(
            time_offsets[:len(heap_values)],
            heap_values,
            marker=markers[idx % len(markers)],
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
    ax.legend(fontsize=10, loc="best")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    out = output_dir / "nn_memory_timeseries.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# PLOT 9: Block Distribution Across Loopback Filesystems
# ============================================================================
def plot_block_distribution(results, metadata, output_dir: Path):
    """Bar chart showing block distribution across loopback filesystems."""
    fig, ax = plt.subplots(figsize=(12, 7))

    k_vals = [r["k"] for r in results]
    block_counts = [r["nn_block_count"] for r in results]

    bars = ax.bar(
        range(len(k_vals)), block_counts,
        color="skyblue", alpha=0.85,
        edgecolor="black", linewidth=0.5,
    )

    for bar, blocks, k in zip(bars, block_counts, k_vals):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(block_counts) * 0.03,
            f"{blocks} blocks\nk={k}",
            ha="center", va="bottom", fontsize=10, fontweight="bold",
        )

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel("Total Blocks", fontsize=13)
    ax.set_title(
        f"Block Distribution Across Loopback Filesystems\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "block_distribution.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


# ============================================================================
# IOSTAT PLOTS
# ============================================================================

def read_iostat_data(iostat_dir: Path):
    """Read parsed iostat summary CSVs for each k value.

    Returns dict: {k: [row_dicts]} where each row has keys:
        timestamp, node, device, r_per_s, w_per_s, rkB_per_s, wkB_per_s,
        await, r_await, w_await, util
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
                    rows.append({
                        "timestamp": row["timestamp"],
                        "node": row["node"],
                        "device": row["device"],
                        "r_per_s": float(row.get("r_per_s", 0) or 0),
                        "w_per_s": float(row.get("w_per_s", 0) or 0),
                        "rkB_per_s": float(row.get("rkB_per_s", 0) or 0),
                        "wkB_per_s": float(row.get("wkB_per_s", 0) or 0),
                        "await": float(row.get("await", 0) or 0),
                        "r_await": float(row.get("r_await", 0) or 0),
                        "w_await": float(row.get("w_await", 0) or 0),
                        "util": float(row.get("util", 0) or 0),
                    })
                except (ValueError, KeyError):
                    pass
        if rows:
            data[k] = rows
    return data


def plot_iostat_await_vs_k(iostat_data, metadata, output_dir: Path):
    """Bar chart: average disk await time (ms) for each k value."""
    fig, ax = plt.subplots(figsize=(11, 7))

    k_vals = sorted(iostat_data.keys())
    avg_awaits = []
    std_awaits = []

    for k in k_vals:
        awaits = [r["await"] for r in iostat_data[k]]
        avg_awaits.append(np.mean(awaits) if awaits else 0)
        std_awaits.append(np.std(awaits) if awaits else 0)

    colors = cm.viridis(np.linspace(0.2, 0.8, len(k_vals)))
    bars = ax.bar(
        range(len(k_vals)), avg_awaits,
        yerr=std_awaits,
        color=colors, alpha=0.85,
        error_kw={"capsize": 6},
        edgecolor="black", linewidth=0.5,
    )

    for bar, avg, std in zip(bars, avg_awaits, std_awaits):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + std + max(avg_awaits) * 0.03,
            f"{avg:.2f}ms",
            ha="center", va="bottom", fontsize=10, fontweight="bold",
        )

    ax.set_xticks(range(len(k_vals)))
    ax.set_xticklabels([f"k={k}" for k in k_vals], fontsize=12)
    ax.set_xlabel("Storage Directories per DataNode (k)", fontsize=13)
    ax.set_ylabel("Average Disk Await Time (ms)", fontsize=13)
    ax.set_title(
        f"Disk I/O Wait Time vs Storage Directory Count\n({_subtitle(metadata)})",
        fontsize=12,
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = output_dir / "iostat_await_vs_k.png"
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


def plot_iostat_util_vs_k(iostat_data, metadata, output_dir: Path):
    """Bar chart: average disk utilization (%) for each k value."""
    fig, ax = plt.subplots(figsize=(11, 7))

    k_vals = sorted(iostat_data.keys())
    avg_utils = []
    std_utils = []

    for k in k_vals:
        utils = [r["util"] for r in iostat_data[k]]
        avg_utils.append(np.mean(utils) if utils else 0)
        std_utils.append(np.std(utils) if utils else 0)

    colors = cm.plasma(np.linspace(0.2, 0.8, len(k_vals)))
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
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)


def plot_iostat_timeseries(iostat_data, metadata, output_dir: Path):
    """2x2 subplot: await, %util, wkB/s, r/s over time for each k value."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    metrics = [
        ("await", "Avg Await (ms)", axes[0, 0]),
        ("util", "Avg %util", axes[0, 1]),
        ("wkB_per_s", "Avg Write KB/s", axes[1, 0]),
        ("r_per_s", "Avg Reads/s", axes[1, 1]),
    ]

    k_vals = sorted(iostat_data.keys())
    colors = cm.tab10(np.linspace(0, 0.8, len(k_vals)))
    markers = ["o", "s", "^", "D", "v", "P", "X", "h", "*", "d"]

    datanode_hosts = metadata.get("datanode_hosts", metadata.get("physical_nodes", 5))

    for metric_key, ylabel, ax in metrics:
        for idx, k in enumerate(k_vals):
            rows = iostat_data[k]
            # Group by timestamp order and average across all nodes/devices
            ts_values = {}
            ts_order = []
            for r in rows:
                ts = r["timestamp"]
                if ts not in ts_values:
                    ts_values[ts] = []
                    ts_order.append(ts)
                ts_values[ts].append(r[metric_key])

            time_offsets = list(range(0, len(ts_order) * 5, 5))
            avg_values = [np.mean(ts_values[ts]) for ts in ts_order]

            ax.plot(
                time_offsets[:len(avg_values)],
                avg_values,
                marker=markers[idx % len(markers)],
                color=colors[idx],
                linewidth=1.5,
                markersize=4,
                alpha=0.8,
                label=f"k={k} ({k * datanode_hosts} dirs)",
            )

        ax.set_xlabel("Time (seconds)", fontsize=11)
        ax.set_ylabel(ylabel, fontsize=11)
        ax.legend(fontsize=9, loc="best")
        ax.grid(True, alpha=0.3)

    fig.suptitle(
        f"Disk I/O Metrics Over Time (During WordCount)\n({_subtitle(metadata)})",
        fontsize=13,
    )
    fig.tight_layout()

    out = output_dir / "iostat_timeseries.png"
    fig.savefig(out, dpi=150)
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


    # Plot per-filesystem block distribution if data is present
    if any(r.get("block_counts_per_fs") for r in results):
        plot_per_fs_block_distribution(results, metadata, results_dir)

    # Plot input-only block distribution if data is present
    if any(r.get("input_block_counts_per_fs") for r in results):
        plot_input_blocks_per_fs(results, metadata, results_dir)

    # Plot filesystem capacity distribution if data is present
    if any(r.get("fs_used_mb_per_fs") for r in results):
        plot_fs_capacity_per_node(results, metadata, results_dir)

    # Plot iostat disk I/O metrics if data is present
    if iostat_data:
        plot_iostat_await_vs_k(iostat_data, metadata, results_dir)
        plot_iostat_util_vs_k(iostat_data, metadata, results_dir)
        plot_iostat_timeseries(iostat_data, metadata, results_dir)

    print()
    total = 5
    if nn_memory_in_csv:
        total += 2
    if timeseries:
        total += 1
    if any(r.get("block_counts_per_fs") for r in results):
        total += 1
    if any(r.get("input_block_counts_per_fs") for r in results):
        total += 1
    if any(r.get("fs_used_mb_per_fs") for r in results):
        total += 2  # fs_capacity_per_node.png + fs_capacity_balance.png
    if iostat_data:
        total += 3  # iostat_await_vs_k, iostat_util_vs_k, iostat_timeseries
    print(f"All plots generated! ({total} total)")
    print(f"Output directory: {results_dir}")


if __name__ == "__main__":
    main()
