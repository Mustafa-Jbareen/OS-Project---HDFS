#!/usr/bin/env python3
"""
Plot Multi-Node WordCount Benchmark Results

Generates multiple visualizations:
1. Combined chart with all node counts as different lines
2. Separate charts for each node count
3. Heatmap of runtime vs nodes vs block size

Usage:
    python3 plot-multinode-results.py <results_directory>
    python3 plot-multinode-results.py results/multinode-benchmark/latest
"""

import argparse
import csv
import os
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


def read_combined_results(csv_path: Path):
    """Read the combined results CSV."""
    results = {}  # {node_count: [(block_size_human, runtime), ...]}
    
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            node_count = int(row['node_count'])
            block_size_human = row['block_size_human']
            try:
                runtime = float(row['runtime_seconds'])
            except ValueError:
                continue  # Skip failed runs
            
            if node_count not in results:
                results[node_count] = []
            results[node_count].append((block_size_human, runtime))
    
    return results


def read_node_results(csv_path: Path):
    """Read a single node's results CSV."""
    results = []
    
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            block_size_human = row['block_size_human']
            try:
                runtime = float(row['runtime_seconds'])
            except ValueError:
                continue
            results.append((block_size_human, runtime))
    
    return results


def plot_combined(results: dict, output_path: Path):
    """Create combined chart with all node counts as different lines."""
    plt.figure(figsize=(12, 8))
    
    colors = cm.viridis(np.linspace(0, 0.8, len(results)))
    markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p']
    
    for idx, (node_count, data) in enumerate(sorted(results.items())):
        block_sizes = [d[0] for d in data]
        runtimes = [d[1] for d in data]
        
        plt.plot(
            range(len(block_sizes)), runtimes,
            marker=markers[idx % len(markers)],
            color=colors[idx],
            linewidth=2,
            markersize=10,
            label=f'{node_count} nodes'
        )
    
    # Get block sizes from first result for x-axis labels
    first_data = list(results.values())[0]
    block_sizes = [d[0] for d in first_data]
    
    plt.xticks(range(len(block_sizes)), block_sizes, rotation=45, ha='right')
    plt.xlabel('Block Size', fontsize=12)
    plt.ylabel('Runtime (seconds)', fontsize=12)
    plt.title('WordCount Performance: Block Size vs Runtime\n(5GB Input, Varying Node Count)', fontsize=14)
    plt.legend(loc='best', fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(output_path, dpi=150)
    print(f"Saved combined chart: {output_path}")
    plt.close()


def plot_individual(results: dict, output_dir: Path):
    """Create separate charts for each node count."""
    for node_count, data in sorted(results.items()):
        plt.figure(figsize=(10, 6))
        
        block_sizes = [d[0] for d in data]
        runtimes = [d[1] for d in data]
        
        bars = plt.bar(range(len(block_sizes)), runtimes, color='steelblue', alpha=0.8)
        
        # Add value labels on bars
        for bar, runtime in zip(bars, runtimes):
            plt.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(runtimes) * 0.02,
                f'{runtime:.1f}s',
                ha='center',
                fontsize=9
            )
        
        plt.xticks(range(len(block_sizes)), block_sizes, rotation=45, ha='right')
        plt.xlabel('Block Size', fontsize=12)
        plt.ylabel('Runtime (seconds)', fontsize=12)
        plt.title(f'WordCount Performance with {node_count} Nodes\n(5GB Input)', fontsize=14)
        plt.grid(True, axis='y', alpha=0.3)
        plt.tight_layout()
        
        output_path = output_dir / f'results_{node_count}nodes.png'
        plt.savefig(output_path, dpi=150)
        print(f"Saved {node_count}-node chart: {output_path}")
        plt.close()


def plot_heatmap(results: dict, output_path: Path):
    """Create a heatmap showing runtime vs nodes and block size."""
    # Prepare data matrix
    node_counts = sorted(results.keys())
    block_sizes = [d[0] for d in list(results.values())[0]]
    
    data_matrix = np.zeros((len(node_counts), len(block_sizes)))
    
    for i, node_count in enumerate(node_counts):
        for j, (_, runtime) in enumerate(results[node_count]):
            data_matrix[i, j] = runtime
    
    plt.figure(figsize=(12, 6))
    
    im = plt.imshow(data_matrix, aspect='auto', cmap='RdYlGn_r')
    plt.colorbar(im, label='Runtime (seconds)')
    
    plt.xticks(range(len(block_sizes)), block_sizes, rotation=45, ha='right')
    plt.yticks(range(len(node_counts)), [f'{n} nodes' for n in node_counts])
    
    # Add text annotations
    for i in range(len(node_counts)):
        for j in range(len(block_sizes)):
            text_color = 'white' if data_matrix[i, j] > np.median(data_matrix) else 'black'
            plt.text(j, i, f'{data_matrix[i, j]:.0f}s',
                     ha='center', va='center', color=text_color, fontsize=9)
    
    plt.xlabel('Block Size', fontsize=12)
    plt.ylabel('Node Count', fontsize=12)
    plt.title('WordCount Runtime Heatmap\n(5GB Input, Runtime in Seconds)', fontsize=14)
    plt.tight_layout()
    
    plt.savefig(output_path, dpi=150)
    print(f"Saved heatmap: {output_path}")
    plt.close()


def plot_speedup(results: dict, output_path: Path):
    """Plot speedup relative to 2-node configuration."""
    if 2 not in results:
        print("No 2-node baseline, skipping speedup chart")
        return
    
    baseline = {d[0]: d[1] for d in results[2]}
    
    plt.figure(figsize=(12, 8))
    
    colors = cm.viridis(np.linspace(0, 0.8, len(results)))
    markers = ['o', 's', '^', 'D', 'v']
    
    for idx, (node_count, data) in enumerate(sorted(results.items())):
        if node_count == 2:
            continue
        
        block_sizes = [d[0] for d in data]
        speedups = [baseline[d[0]] / d[1] for d in data]
        
        plt.plot(
            range(len(block_sizes)), speedups,
            marker=markers[idx % len(markers)],
            color=colors[idx],
            linewidth=2,
            markersize=10,
            label=f'{node_count} nodes'
        )
    
    # Ideal speedup lines
    first_data = list(results.values())[0]
    block_sizes = [d[0] for d in first_data]
    
    plt.xticks(range(len(block_sizes)), block_sizes, rotation=45, ha='right')
    plt.xlabel('Block Size', fontsize=12)
    plt.ylabel('Speedup (relative to 2 nodes)', fontsize=12)
    plt.title('WordCount Speedup Analysis\n(Relative to 2-Node Configuration)', fontsize=14)
    plt.axhline(y=1.0, color='gray', linestyle='--', alpha=0.5, label='Baseline (1x)')
    plt.legend(loc='best', fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(output_path, dpi=150)
    print(f"Saved speedup chart: {output_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate plots from multi-node WordCount benchmark results."
    )
    parser.add_argument(
        "results_dir",
        type=Path,
        help="Path to the results directory (e.g., results/multinode-benchmark/run_...)"
    )
    args = parser.parse_args()
    
    results_dir = args.results_dir.resolve()
    
    if not results_dir.exists():
        raise SystemExit(f"Results directory not found: {results_dir}")
    
    combined_csv = results_dir / "all_results.csv"
    
    if not combined_csv.exists():
        raise SystemExit(f"Combined results CSV not found: {combined_csv}")
    
    print(f"Reading results from: {results_dir}")
    print()
    
    results = read_combined_results(combined_csv)
    
    if not results:
        raise SystemExit("No valid results found in CSV")
    
    print(f"Found results for {len(results)} node configurations: {sorted(results.keys())}")
    print()
    
    # Generate all plots
    plot_combined(results, results_dir / "combined_results.png")
    plot_individual(results, results_dir)
    plot_heatmap(results, results_dir / "heatmap.png")
    plot_speedup(results, results_dir / "speedup.png")
    
    print()
    print("=" * 50)
    print("All plots generated successfully!")
    print("=" * 50)


if __name__ == "__main__":
    main()
