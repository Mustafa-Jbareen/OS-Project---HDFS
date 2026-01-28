#!/usr/bin/env python3
"""
SCRIPT: plot-storage-virtualization.py
DESCRIPTION: Unified plotting for storage virtualization experiments.
             Creates visualizations for:
             - Block scaling (NameNode memory vs block count)
             - Storage directory scaling (throughput vs storage dirs)
             - Memory over time monitoring
USAGE: python3 plot-storage-virtualization.py <csv_file> [output_dir]
"""

import sys
import os
from pathlib import Path

try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    import numpy as np
except ImportError as e:
    print(f"Missing required library: {e}")
    print("Install with: pip install matplotlib pandas numpy")
    sys.exit(1)


def detect_experiment_type(df):
    """Detect which experiment the CSV is from based on columns."""
    columns = set(df.columns)
    
    if 'num_dirs' in columns:
        return 'storage_dirs'
    elif 'target_blocks' in columns or 'block_size_exp' in columns:
        return 'block_scaling'
    elif 'heap_used_mb' in columns and 'timestamp' in columns:
        return 'memory_monitor'
    else:
        return 'unknown'


def plot_storage_dirs(df, output_dir):
    """Plot storage directory scaling results."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Plot 1: Throughput vs Storage Dirs
    ax1 = axes[0, 0]
    ax1.plot(df['num_dirs'], df['write_throughput_mbps'], 'o-', 
             label='Write', color='#2E86AB', linewidth=2, markersize=8)
    ax1.plot(df['num_dirs'], df['read_throughput_mbps'], 's-', 
             label='Read', color='#E94F37', linewidth=2, markersize=8)
    ax1.set_xlabel('Number of Storage Directories')
    ax1.set_ylabel('Throughput (MB/s)')
    ax1.set_title('I/O Throughput vs Virtual Storage Units')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale('log', base=2)
    
    # Plot 2: Block Report Time
    ax2 = axes[0, 1]
    ax2.bar(range(len(df)), df['block_report_ms'], color='#4ECDC4')
    ax2.set_xlabel('Number of Storage Directories')
    ax2.set_ylabel('Block Report Time (ms)')
    ax2.set_title('Block Report Latency vs Virtual Storage Units')
    ax2.set_xticks(range(len(df)))
    ax2.set_xticklabels(df['num_dirs'])
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Plot 3: NameNode Heap
    ax3 = axes[1, 0]
    ax3.plot(df['num_dirs'], df['namenode_heap_mb'], 'o-', 
             color='#9B59B6', linewidth=2, markersize=8)
    ax3.set_xlabel('Number of Storage Directories')
    ax3.set_ylabel('NameNode Heap (MB)')
    ax3.set_title('NameNode Memory vs Virtual Storage Units')
    ax3.grid(True, alpha=0.3)
    ax3.set_xscale('log', base=2)
    
    # Plot 4: Summary Table
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # Find optimal configuration
    if 'write_throughput_mbps' in df.columns:
        optimal_idx = df['write_throughput_mbps'].idxmax()
        optimal = df.loc[optimal_idx]
        
        summary_text = f"""
        Storage Directory Scaling Summary
        ─────────────────────────────────
        
        Tested configurations: {len(df)}
        Storage dirs range: {df['num_dirs'].min()} to {df['num_dirs'].max()}
        
        Optimal Configuration:
        • Storage directories: {int(optimal['num_dirs'])}
        • Write throughput: {optimal['write_throughput_mbps']:.1f} MB/s
        • Read throughput: {optimal['read_throughput_mbps']:.1f} MB/s
        • Block report time: {optimal['block_report_ms']:.1f} ms
        
        Scaling Behavior:
        • Write throughput change: {(df['write_throughput_mbps'].iloc[-1] / df['write_throughput_mbps'].iloc[0] - 1) * 100:.1f}%
        • Block report change: {(df['block_report_ms'].iloc[-1] / df['block_report_ms'].iloc[0] - 1) * 100:.1f}%
        """
        ax4.text(0.1, 0.5, summary_text, transform=ax4.transAxes, 
                 fontsize=11, verticalalignment='center', fontfamily='monospace',
                 bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.suptitle('Virtual Storage Scaling Experiment Results', fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    output_file = output_dir / "storage_dirs_results.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Plot saved to: {output_file}")
    plt.savefig(output_dir / "storage_dirs_results.pdf", bbox_inches='tight')


def plot_block_scaling(df, output_dir):
    """Plot block count scaling results."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Plot 1: Heap vs Blocks
    ax1 = axes[0, 0]
    ax1.plot(df['actual_blocks'], df['heap_mb'], 'o-', 
             color='#E74C3C', linewidth=2, markersize=8)
    ax1.set_xlabel('Block Count')
    ax1.set_ylabel('NameNode Heap (MB)')
    ax1.set_title('NameNode Memory vs Block Count')
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale('log')
    
    # Add trend line
    if len(df) > 2:
        z = np.polyfit(df['actual_blocks'], df['heap_mb'], 1)
        p = np.poly1d(z)
        ax1.plot(df['actual_blocks'], p(df['actual_blocks']), '--', 
                 color='gray', alpha=0.7, label=f'Trend: {z[0]:.4f} MB/block')
        ax1.legend()
    
    # Plot 2: Memory per Block
    ax2 = axes[0, 1]
    if 'heap_delta_mb' in df.columns and df['actual_blocks'].iloc[0] > 0:
        blocks_delta = df['actual_blocks'].diff().fillna(df['actual_blocks'])
        heap_delta = df['heap_delta_mb'].diff().fillna(df['heap_delta_mb'])
        bytes_per_block = (heap_delta * 1024 * 1024) / blocks_delta.replace(0, 1)
        ax2.bar(range(len(df)), bytes_per_block, color='#3498DB')
        ax2.set_xlabel('Measurement Point')
        ax2.set_ylabel('Bytes per Block (estimate)')
        ax2.set_title('Memory Cost per Block')
        ax2.axhline(y=150, color='r', linestyle='--', label='Typical: 150 bytes/block')
        ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Plot 3: Latency vs Blocks
    ax3 = axes[1, 0]
    if 'ls_latency_ms' in df.columns:
        ax3.plot(df['actual_blocks'], df['ls_latency_ms'], 'o-', 
                 label='ls -R', color='#2ECC71', linewidth=2, markersize=8)
    if 'fsck_latency_ms' in df.columns:
        ax3.plot(df['actual_blocks'], df['fsck_latency_ms'], 's-', 
                 label='fsck', color='#F39C12', linewidth=2, markersize=8)
    ax3.set_xlabel('Block Count')
    ax3.set_ylabel('Latency (ms)')
    ax3.set_title('Operation Latency vs Block Count')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.set_xscale('log')
    
    # Plot 4: Scalability Projections
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # Calculate projections
    if len(df) > 1:
        # Estimate bytes per block
        total_heap_delta = df['heap_mb'].iloc[-1] - df['heap_mb'].iloc[0]
        total_block_delta = df['actual_blocks'].iloc[-1] - df['actual_blocks'].iloc[0]
        if total_block_delta > 0:
            bytes_per_block_avg = (total_heap_delta * 1024 * 1024) / total_block_delta
        else:
            bytes_per_block_avg = 150  # HDFS default estimate
        
        # Projections for different heap sizes
        projections = ""
        for heap_gb in [4, 8, 16, 32, 64]:
            max_blocks = int((heap_gb * 1024 * 1024 * 1024) / bytes_per_block_avg)
            projections += f"        {heap_gb}GB heap → ~{max_blocks:,} blocks (~{max_blocks * 128 / 1024 / 1024:.1f}TB @ 128MB blocks)\n"
        
        summary_text = f"""
        Block Scaling Analysis
        ─────────────────────────────────
        
        Measured range: {df['actual_blocks'].min():,} to {df['actual_blocks'].max():,} blocks
        Memory growth: {df['heap_mb'].iloc[0]}MB → {df['heap_mb'].iloc[-1]}MB
        
        Estimated memory per block: ~{bytes_per_block_avg:.0f} bytes
        
        NameNode Capacity Projections:
{projections}
        
        Implications for Virtual Storage:
        • 1000 virtual storages × 10 blocks each = 10,000 blocks
        • Overhead: ~{10000 * bytes_per_block_avg / 1024 / 1024:.1f}MB NameNode heap
        """
        ax4.text(0.05, 0.5, summary_text, transform=ax4.transAxes, 
                 fontsize=10, verticalalignment='center', fontfamily='monospace',
                 bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
    
    plt.suptitle('Block Count Scaling Experiment Results', fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    output_file = output_dir / "block_scaling_results.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Plot saved to: {output_file}")
    plt.savefig(output_dir / "block_scaling_results.pdf", bbox_inches='tight')


def plot_memory_monitor(df, output_dir):
    """Plot NameNode memory monitoring over time."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Parse timestamp if needed
    if 'timestamp' in df.columns:
        df['time_idx'] = range(len(df))
    
    # Plot 1: Heap over time
    ax1 = axes[0, 0]
    ax1.plot(df['time_idx'], df['heap_used_mb'], '-', 
             color='#E74C3C', linewidth=1.5)
    ax1.fill_between(df['time_idx'], df['heap_used_mb'], alpha=0.3, color='#E74C3C')
    ax1.set_xlabel('Sample')
    ax1.set_ylabel('Heap Used (MB)')
    ax1.set_title('NameNode Heap Usage Over Time')
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Heap percentage
    ax2 = axes[0, 1]
    ax2.plot(df['time_idx'], df['heap_pct'], '-', color='#9B59B6', linewidth=1.5)
    ax2.axhline(y=80, color='orange', linestyle='--', label='Warning (80%)')
    ax2.axhline(y=95, color='red', linestyle='--', label='Critical (95%)')
    ax2.set_xlabel('Sample')
    ax2.set_ylabel('Heap Usage (%)')
    ax2.set_title('NameNode Heap Percentage')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, 100)
    
    # Plot 3: Block count over time
    ax3 = axes[1, 0]
    if 'block_count' in df.columns:
        ax3.plot(df['time_idx'], df['block_count'], '-', 
                 color='#3498DB', linewidth=1.5)
        ax3.set_ylabel('Block Count')
    ax3.set_xlabel('Sample')
    ax3.set_title('HDFS Block Count Over Time')
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Statistics
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    summary_text = f"""
    NameNode Memory Monitoring Summary
    ─────────────────────────────────
    
    Duration: {len(df)} samples
    
    Heap Usage:
    • Min: {df['heap_used_mb'].min():.0f} MB
    • Max: {df['heap_used_mb'].max():.0f} MB
    • Avg: {df['heap_used_mb'].mean():.0f} MB
    • Max %: {df['heap_pct'].max():.1f}%
    
    Block Count:
    • Start: {df['block_count'].iloc[0]:,}
    • End: {df['block_count'].iloc[-1]:,}
    • Delta: {df['block_count'].iloc[-1] - df['block_count'].iloc[0]:,}
    """
    ax4.text(0.1, 0.5, summary_text, transform=ax4.transAxes, 
             fontsize=11, verticalalignment='center', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))
    
    plt.suptitle('NameNode Memory Monitoring', fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    output_file = output_dir / "memory_monitor_results.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Plot saved to: {output_file}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 plot-storage-virtualization.py <csv_file> [output_dir]")
        sys.exit(1)
    
    csv_file = Path(sys.argv[1])
    if not csv_file.exists():
        print(f"Error: File not found: {csv_file}")
        sys.exit(1)
    
    output_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else csv_file.parent
    
    # Read data
    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} rows from {csv_file}")
    
    # Detect experiment type and plot
    exp_type = detect_experiment_type(df)
    print(f"Detected experiment type: {exp_type}")
    
    if exp_type == 'storage_dirs':
        plot_storage_dirs(df, output_dir)
    elif exp_type == 'block_scaling':
        plot_block_scaling(df, output_dir)
    elif exp_type == 'memory_monitor':
        plot_memory_monitor(df, output_dir)
    else:
        print(f"Unknown experiment type. Columns: {list(df.columns)}")
        sys.exit(1)
    
    plt.show()


if __name__ == "__main__":
    main()
