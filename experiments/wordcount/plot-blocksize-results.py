#!/usr/bin/env python3
"""
SCRIPT: plot-blocksize-results.py
DESCRIPTION: Plots WordCount runtime as a function of HDFS block size.
             Creates a figure showing how block size affects MapReduce performance.
             Supports both old and new CSV formats, and can compare multiple runs.
USAGE: python3 plot-blocksize-results.py [csv_file_or_run_dir]
PREREQUISITES:
    - matplotlib and pandas installed (pip install matplotlib pandas)
    - Benchmark results from benchmark-blocksize.sh
OUTPUT:
    - Plot image saved next to the CSV file
    - Console summary statistics
"""

import sys
import os
from pathlib import Path

# Try to import required libraries
try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError as e:
    print(f"Missing required library: {e}")
    print("Install with: pip install matplotlib pandas")
    sys.exit(1)


def find_csv_file(arg=None):
    """Find the CSV file to use for plotting."""
    script_dir = Path(__file__).parent
    results_dir = script_dir.parent.parent / "results"
    blocksize_results_dir = results_dir / "blocksize-benchmark"
    
    if arg:
        path = Path(arg)
        if path.is_file() and path.suffix == '.csv':
            return path
        if path.is_dir():
            # Check for results.csv in the directory
            results_csv = path / "results.csv"
            if results_csv.exists():
                return results_csv
        # Try as relative path
        if not path.is_absolute():
            for base in [Path.cwd(), script_dir, results_dir]:
                full_path = base / path
                if full_path.exists():
                    if full_path.is_file():
                        return full_path
                    elif (full_path / "results.csv").exists():
                        return full_path / "results.csv"
    
    # Try to find latest run
    latest_link = blocksize_results_dir / "latest"
    if latest_link.exists():
        latest_csv = latest_link / "results.csv"
        if latest_csv.exists():
            return latest_csv
    
    # Fall back to old format location
    old_csv = results_dir / "wordcount-blocksize.csv"
    if old_csv.exists():
        return old_csv
    
    return None


def main():
    # Determine CSV file path
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    csv_file = find_csv_file(arg)
    
    if csv_file is None:
        print("Error: No CSV file found!")
        print("Run the benchmark first: bash experiments/wordcount/benchmark-blocksize.sh")
        print("Or specify a CSV file: python3 plot-blocksize-results.py <path/to/results.csv>")
        sys.exit(1)
    
    csv_file = Path(csv_file)
    output_dir = csv_file.parent
    
    # Read data
    df = pd.read_csv(csv_file)
    
    # Handle both old and new CSV formats
    # New format has: block_size_exp,block_size_bytes,block_size_formula,block_size_human,runtime_seconds,num_splits
    # Old format has: block_size_bytes,block_size_human,runtime_seconds,num_splits
    
    if 'block_size_exp' not in df.columns and 'block_size_kb' not in df.columns and 'block_size_bytes' in df.columns:
        # Old format - add exponent column (approximate)
        import math
        df['block_size_exp'] = df['block_size_bytes'].apply(lambda x: int(math.log2(x)) if x > 0 else 0)
    elif 'block_size_kb' in df.columns:
        # Intermediate format with KB
        import math
        df['block_size_exp'] = (df['block_size_kb'] * 1024).apply(lambda x: int(math.log2(x)) if x > 0 else 0)
    
    # Filter out error/skipped rows
    df = df[~df['runtime_seconds'].isin(['ERROR', 'SKIPPED'])]
    df['runtime_seconds'] = pd.to_numeric(df['runtime_seconds'], errors='coerce')
    df = df.dropna(subset=['runtime_seconds'])
    df['block_size_bytes'] = df['block_size_bytes'].astype(int)
    
    if df.empty:
        print("Error: No valid data found in CSV")
        sys.exit(1)
    
    # Print summary statistics
    print("=" * 60)
    print("WordCount Block Size Benchmark Results")
    print("=" * 60)
    print(f"\nData from: {csv_file}")
    print(f"\nNumber of configurations tested: {len(df)}")
    print(f"\nBlock size range: {df['block_size_human'].iloc[0]} to {df['block_size_human'].iloc[-1]}")
    print(f"\nRuntime range: {df['runtime_seconds'].min():.1f}s to {df['runtime_seconds'].max():.1f}s")
    
    # Find optimal block size
    min_idx = df['runtime_seconds'].idxmin()
    optimal = df.loc[min_idx]
    print(f"\nOptimal block size: {optimal['block_size_human']} ({optimal['runtime_seconds']:.1f}s)")
    
    # Show formula if available
    if 'block_size_formula' in df.columns:
        print(f"  Formula: {optimal['block_size_formula']} = {optimal['block_size_bytes']} bytes")
    elif 'block_size_exp' in df.columns:
        print(f"  Formula: 2^{int(optimal['block_size_exp'])} = {optimal['block_size_bytes']} bytes")
    
    print("\n" + "-" * 60)
    print("Detailed Results:")
    print("-" * 60)
    print(df.to_string(index=False))
    print("=" * 60)
    
    # Create the plot
    fig, ax1 = plt.subplots(figsize=(12, 7))
    
    # Convert block size to MB for cleaner x-axis
    block_sizes_mb = df['block_size_bytes'] / (1024 * 1024)
    
    # Primary axis: Runtime
    color1 = '#2E86AB'
    ax1.set_xlabel('Block Size', fontsize=12)
    ax1.set_ylabel('Runtime (seconds)', color=color1, fontsize=12)
    line1 = ax1.plot(block_sizes_mb, df['runtime_seconds'], 
                     'o-', color=color1, linewidth=2, markersize=8, 
                     label='Runtime')
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.set_xscale('log', base=2)
    
    # Custom x-axis labels
    ax1.set_xticks(block_sizes_mb)
    ax1.set_xticklabels(df['block_size_human'], rotation=45, ha='right')
    
    # Secondary axis: Number of splits
    if 'num_splits' in df.columns:
        ax2 = ax1.twinx()
        color2 = '#E94F37'
        ax2.set_ylabel('Number of Blocks/Splits', color=color2, fontsize=12)
        line2 = ax2.plot(block_sizes_mb, df['num_splits'], 
                         's--', color=color2, linewidth=2, markersize=6,
                         label='Blocks/Splits')
        ax2.tick_params(axis='y', labelcolor=color2)
        
        # Combined legend
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc='upper right', fontsize=10)
    else:
        ax1.legend(loc='upper right', fontsize=10)
    
    # Mark optimal point
    ax1.axvline(x=optimal['block_size_bytes'] / (1024 * 1024), 
                color='green', linestyle=':', alpha=0.7, linewidth=2)
    ax1.annotate(f"Optimal: {optimal['block_size_human']}\n({optimal['runtime_seconds']:.1f}s)",
                 xy=(optimal['block_size_bytes'] / (1024 * 1024), optimal['runtime_seconds']),
                 xytext=(10, 30), textcoords='offset points',
                 fontsize=10, color='green',
                 arrowprops=dict(arrowstyle='->', color='green', alpha=0.7))
    
    # Title and grid
    plt.title('WordCount Runtime vs HDFS Block Size\n(Lower is better)', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_axisbelow(True)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save figure
    # Determine output filename based on input location
    run_name = output_dir.name if output_dir.name.startswith("run_") else "blocksize"
    output_file = output_dir / f"wordcount-blocksize-{run_name}.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")
    
    # Also save as PDF for higher quality
    pdf_file = output_dir / f"wordcount-blocksize-{run_name}.pdf"
    plt.savefig(pdf_file, bbox_inches='tight')
    print(f"PDF saved to: {pdf_file}")
    
    # Show plot
    plt.show()


if __name__ == "__main__":
    main()
