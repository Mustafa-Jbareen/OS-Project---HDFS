"""Plot MiniDFSCluster fixed-blocks distribution experiment results.

Reads the CSV produced by MiniDFSFixedBlocksExperiment and generates:
  1. Memory vs DataNodes (with blocks/DN annotation)
  2. Memory vs Blocks-per-DataNode

Usage:
    python3 plot_fixed_blocks.py <csv_path> [-o output.png] [--log-y]
"""

import argparse
import csv
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError as exc:
    raise SystemExit(
        "matplotlib and numpy are required.  Install with:\n"
        "  pip install matplotlib numpy"
    ) from exc


def read_samples(csv_path: Path):
    """Return list of (datanodes, total_blocks, blocks_per_dn, memory_bytes)."""
    samples = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dn = int(row["DataNodes"])
                total = int(row["TotalBlocks"])
                per_dn = int(row["BlocksPerDataNode"])
                mem = int(row["MemoryUsed"])
            except (ValueError, KeyError):
                continue
            samples.append((dn, total, per_dn, mem))
    samples.sort(key=lambda x: x[0])
    return samples


def plot_memory_vs_datanodes(samples, output_path: Path, log_y: bool):
    """Memory (MiB) vs DataNode count, annotated with blocks/DN."""
    dns = [s[0] for s in samples]
    mem_mib = [s[3] / (1024 * 1024) for s in samples]
    per_dn = [s[2] for s in samples]
    total_blocks = samples[0][1] if samples else 0

    fig, ax1 = plt.subplots(figsize=(11, 6))

    color_mem = "steelblue"
    ax1.plot(dns, mem_mib, marker="o", linewidth=2, color=color_mem, label="Heap Used")
    ax1.set_xlabel("Number of DataNodes", fontsize=12)
    ax1.set_ylabel("Heap Used (MiB)", fontsize=12, color=color_mem)
    ax1.tick_params(axis="y", labelcolor=color_mem)
    ax1.set_xscale("log", base=2)
    if log_y:
        ax1.set_yscale("log", base=2)
    ax1.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.4)

    # Secondary y-axis: blocks per DataNode
    color_blocks = "darkorange"
    ax2 = ax1.twinx()
    ax2.plot(dns, per_dn, marker="s", linewidth=2, linestyle="--", color=color_blocks, label="Blocks/DN")
    ax2.set_ylabel("Blocks per DataNode", fontsize=12, color=color_blocks)
    ax2.tick_params(axis="y", labelcolor=color_blocks)

    fig.suptitle(
        f"Fixed-Blocks Experiment: {total_blocks} Total Blocks Distributed Across DataNodes",
        fontsize=13,
    )
    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=10)

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    print(f"Saved: {output_path}")
    plt.close(fig)


def plot_memory_vs_blocks_per_dn(samples, output_path: Path, log_y: bool):
    """Memory vs blocks-per-DataNode (inverse relationship expected)."""
    per_dn = [s[2] for s in samples]
    mem_mib = [s[3] / (1024 * 1024) for s in samples]
    dns = [s[0] for s in samples]
    total_blocks = samples[0][1] if samples else 0

    plt.figure(figsize=(10, 6))
    plt.plot(per_dn, mem_mib, marker="o", linewidth=2, color="teal")

    # Annotate each point with its DataNode count
    for x, y, dn in zip(per_dn, mem_mib, dns):
        plt.annotate(f"{dn} DNs", (x, y), textcoords="offset points",
                     xytext=(8, 6), fontsize=8, color="gray")

    plt.xlabel("Blocks per DataNode", fontsize=12)
    plt.ylabel("Heap Used (MiB)", fontsize=12)
    plt.title(
        f"Memory vs Blocks-per-DataNode\n({total_blocks} fixed blocks, replication=1)",
        fontsize=13,
    )
    if log_y:
        plt.yscale("log", base=2)
    plt.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.4)
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    print(f"Saved: {output_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Plot fixed-blocks MiniDFS experiment results."
    )
    parser.add_argument("csv", type=Path, help="Path to fixed_blocks_memory.csv")
    parser.add_argument("-o", "--output", type=Path, default=None,
                        help="Base output path (default: same dir as CSV)")
    parser.add_argument("--log-y", action="store_true",
                        help="Use log scale for memory axis")
    args = parser.parse_args()

    samples = read_samples(args.csv)
    if not samples:
        raise SystemExit(f"No data found in {args.csv}")

    base = args.output or args.csv.parent / "fixed_blocks"
    base = base.with_suffix("")  # strip .png if provided

    plot_memory_vs_datanodes(samples, base.with_name(base.name + "_memory_vs_dns.png"), args.log_y)
    plot_memory_vs_blocks_per_dn(samples, base.with_name(base.name + "_memory_vs_blocks_per_dn.png"), args.log_y)

    print("\nDone. Two plots generated.")


if __name__ == "__main__":
    main()
