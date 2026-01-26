#!/usr/bin/env python3
"""
SCRIPT: plot-blocksize-results.py
DESCRIPTION: Plots WordCount runtime as a function of HDFS block size.
             Creates a figure showing how block size affects MapReduce performance.
USAGE: python3 plot-blocksize-results.py [csv_file]
PREREQUISITES:
    - matplotlib and pandas installed (pip install matplotlib pandas)
    - Benchmark results in results/wordcount-blocksize.csv
OUTPUT:
    - results/wordcount-blocksize.png (plot image)
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


def main():
    # Determine CSV file path
    script_dir = Path(__file__).parent
    results_dir = script_dir.parent.parent / "results"
    
    if len(sys.argv) > 1:
        csv_file = Path(sys.argv[1])
    else:
        csv_file = results_dir / "wordcount-blocksize.csv"
    
    if not csv_file.exists():
        print(f"Error: CSV file not found: {csv_file}")
        print("Run the benchmark first: bash experiments/wordcount/benchmark-blocksize.sh")
        sys.exit(1)
    
    # Read data
    df = pd.read_csv(csv_file)
    
    # Filter out error rows
    df = df[df['runtime_seconds'] != 'ERROR']
    df['runtime_seconds'] = df['runtime_seconds'].astype(float)
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
    output_file = results_dir / "wordcount-blocksize.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")
    
    # Also save as PDF for higher quality
    pdf_file = results_dir / "wordcount-blocksize.pdf"
    plt.savefig(pdf_file, bbox_inches='tight')
    print(f"PDF saved to: {pdf_file}")
    
    # Show plot
    plt.show()


if __name__ == "__main__":
    main()
