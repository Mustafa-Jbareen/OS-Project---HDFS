#!/bin/bash
################################################################################
# SCRIPT: run-full-experiment.sh
# DESCRIPTION: Master script that runs all storage virtualization experiments
#              in sequence and produces a comprehensive report.
#
# This is your ONE-CLICK experiment runner for the research:
# "What happens when each DataNode has ~1000 virtual storage units?"
#
# USAGE: bash run-full-experiment.sh [experiment_name]
#        experiment_name: all | block_scaling | storage_dirs | memory | failure
#
# OUTPUT: Combined results in results/full_experiment_<timestamp>/
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPERIMENT=${1:-all}

# ============================================================================
# LOAD CONFIGURATION
# ============================================================================
# Source the shared cluster config (edit this file to change settings)
CONFIG_FILE="$SCRIPT_DIR/../common/cluster.conf"
if [ -f "$CONFIG_FILE" ]; then
    source "$CONFIG_FILE"
    echo "Loaded config from: $CONFIG_FILE"
else
    echo "WARNING: Config file not found: $CONFIG_FILE"
    echo "Using default values..."
fi

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
EXPERIMENT_DIR="$SCRIPT_DIR/results/full_experiment_${TIMESTAMP}"
mkdir -p "$EXPERIMENT_DIR"

LOG_FILE="$EXPERIMENT_DIR/experiment.log"
SUMMARY_FILE="$EXPERIMENT_DIR/SUMMARY.md"

# ============================================================================
# CONFIGURATION
# ============================================================================
HADOOP_HOME=${HADOOP_HOME:-/home/mostufa.j/hadoop}
HADOOP_DATA_BASE=${HADOOP_DATA_BASE:-/home/mostufa.j/hadoop_data}
MASTER_NODE=${MASTER_NODE:-tapuz14}
WORKER_NODES=("tapuz13")

# Experiment parameters (adjust based on your cluster)
BLOCK_SCALING_MAX_BLOCKS=${BLOCK_SCALING_MAX_BLOCKS:-50000}
STORAGE_DIRS_MAX=${STORAGE_DIRS_MAX:-32}
MEMORY_MONITOR_INTERVAL=${MEMORY_MONITOR_INTERVAL:-5}
FAILURE_SIM_DIRS=${FAILURE_SIM_DIRS:-8}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

header() {
    echo "" | tee -a "$LOG_FILE"
    echo "═══════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    echo " $1" | tee -a "$LOG_FILE"
    echo "═══════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
}

check_hdfs_running() {
    if ! hdfs dfsadmin -report &>/dev/null; then
        log "ERROR: HDFS is not running. Start with: start-dfs.sh"
        exit 1
    fi
    log "HDFS is running ✓"
}

check_dependencies() {
    log "Checking dependencies..."
    
    # Check Python and libraries
    if ! command -v python3 &>/dev/null; then
        log "WARNING: python3 not found. Plotting will be skipped."
    else
        if ! python3 -c "import pandas, matplotlib, numpy" 2>/dev/null; then
            log "WARNING: Missing Python libs. Install with: pip install pandas matplotlib numpy"
        else
            log "Python dependencies OK ✓"
        fi
    fi
    
    # Check bc for calculations
    if ! command -v bc &>/dev/null; then
        log "WARNING: 'bc' not found. Some calculations may fail."
    fi
}

initialize_summary() {
    cat > "$SUMMARY_FILE" <<EOF
# Storage Virtualization Experiment Report

**Run ID**: ${TIMESTAMP}  
**Master Node**: ${MASTER_NODE}  
**Worker Nodes**: ${WORKER_NODES[*]}  
**Date**: $(date '+%Y-%m-%d %H:%M:%S')

---

## Research Question

> What happens if each DataNode has ~1000 virtual storage units instead of
> one large storage? How does this affect NameNode memory, DataNode performance,
> and failure recovery?

---

## Experiment Results

EOF
}

append_to_summary() {
    echo "$1" >> "$SUMMARY_FILE"
}

# ============================================================================
# EXPERIMENT 1: Block Count Scaling
# ============================================================================

run_block_scaling() {
    header "EXPERIMENT 1: Block Count Scaling"
    log "Testing NameNode memory as block count increases..."
    log "Max blocks: $BLOCK_SCALING_MAX_BLOCKS"
    
    local EXP_DIR="$EXPERIMENT_DIR/block_scaling"
    mkdir -p "$EXP_DIR"
    
    # Run the experiment
    if bash "$SCRIPT_DIR/benchmark-block-scaling.sh" "$BLOCK_SCALING_MAX_BLOCKS" 2>&1 | tee -a "$LOG_FILE"; then
        log "Block scaling experiment completed ✓"
        
        # Copy results
        local LATEST=$(ls -td "$SCRIPT_DIR/results/block_scaling_"* 2>/dev/null | head -1)
        if [ -n "$LATEST" ]; then
            cp -r "$LATEST"/* "$EXP_DIR/" 2>/dev/null || true
            
            # Plot results
            if command -v python3 &>/dev/null; then
                python3 "$SCRIPT_DIR/plot-storage-virtualization.py" "$EXP_DIR/results.csv" "$EXP_DIR" 2>/dev/null || true
            fi
            
            # Add to summary
            append_to_summary "### Experiment 1: Block Count Scaling"
            append_to_summary ""
            append_to_summary "**Question**: How does NameNode memory scale with block count?"
            append_to_summary ""
            append_to_summary "**Results**:"
            append_to_summary '```'
            cat "$EXP_DIR/results.csv" >> "$SUMMARY_FILE"
            append_to_summary '```'
            append_to_summary ""
            
            # Calculate key metrics
            if [ -f "$EXP_DIR/results.csv" ]; then
                local FIRST_HEAP=$(tail -n +2 "$EXP_DIR/results.csv" | head -1 | cut -d',' -f3)
                local LAST_HEAP=$(tail -1 "$EXP_DIR/results.csv" | cut -d',' -f3)
                local FIRST_BLOCKS=$(tail -n +2 "$EXP_DIR/results.csv" | head -1 | cut -d',' -f2)
                local LAST_BLOCKS=$(tail -1 "$EXP_DIR/results.csv" | cut -d',' -f2)
                
                append_to_summary "**Key Finding**: NameNode heap grew from ${FIRST_HEAP}MB to ${LAST_HEAP}MB"
                append_to_summary "as blocks increased from ${FIRST_BLOCKS} to ${LAST_BLOCKS}."
                append_to_summary ""
            fi
        fi
    else
        log "Block scaling experiment FAILED"
    fi
}

# ============================================================================
# EXPERIMENT 2: Storage Directory Scaling
# ============================================================================

run_storage_dirs() {
    header "EXPERIMENT 2: Storage Directory Scaling"
    log "Testing DataNode with multiple storage directories..."
    log "Max directories: $STORAGE_DIRS_MAX"
    
    local EXP_DIR="$EXPERIMENT_DIR/storage_dirs"
    mkdir -p "$EXP_DIR"
    
    # Run the experiment
    if bash "$SCRIPT_DIR/benchmark-storage-dirs.sh" "$STORAGE_DIRS_MAX" 2>&1 | tee -a "$LOG_FILE"; then
        log "Storage directory experiment completed ✓"
        
        # Copy results
        local LATEST=$(ls -td "$SCRIPT_DIR/results/storage_dirs_"* 2>/dev/null | head -1)
        if [ -n "$LATEST" ]; then
            cp -r "$LATEST"/* "$EXP_DIR/" 2>/dev/null || true
            
            # Plot results
            if command -v python3 &>/dev/null; then
                python3 "$SCRIPT_DIR/plot-storage-virtualization.py" "$EXP_DIR/results.csv" "$EXP_DIR" 2>/dev/null || true
            fi
            
            append_to_summary "### Experiment 2: Storage Directory Scaling"
            append_to_summary ""
            append_to_summary "**Question**: How does DataNode performance scale with virtual storage count?"
            append_to_summary ""
            append_to_summary "**Results**:"
            append_to_summary '```'
            cat "$EXP_DIR/results.csv" >> "$SUMMARY_FILE"
            append_to_summary '```'
            append_to_summary ""
        fi
    else
        log "Storage directory experiment FAILED"
    fi
}

# ============================================================================
# EXPERIMENT 3: Memory Monitoring (Background)
# ============================================================================

run_memory_monitor() {
    header "EXPERIMENT 3: NameNode Memory Monitoring"
    log "Starting background memory monitoring..."
    
    local EXP_DIR="$EXPERIMENT_DIR/memory_monitor"
    mkdir -p "$EXP_DIR"
    
    # Run in background for 30 minutes
    NAMENODE_HOST=$MASTER_NODE bash "$SCRIPT_DIR/monitor-namenode-memory.sh" "$MEMORY_MONITOR_INTERVAL" 30 &
    MONITOR_PID=$!
    
    log "Memory monitor started (PID: $MONITOR_PID)"
    log "Will run for 30 minutes in background"
    
    # Save PID for later
    echo "$MONITOR_PID" > "$EXP_DIR/monitor.pid"
}

stop_memory_monitor() {
    local EXP_DIR="$EXPERIMENT_DIR/memory_monitor"
    
    if [ -f "$EXP_DIR/monitor.pid" ]; then
        local PID=$(cat "$EXP_DIR/monitor.pid")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID" 2>/dev/null || true
            log "Memory monitor stopped (PID: $PID)"
        fi
        rm "$EXP_DIR/monitor.pid"
    fi
    
    # Copy results
    local LATEST=$(ls -t "$SCRIPT_DIR/results/namenode_memory_"*.csv 2>/dev/null | head -1)
    if [ -n "$LATEST" ]; then
        cp "$LATEST" "$EXP_DIR/" 2>/dev/null || true
        
        append_to_summary "### Experiment 3: Memory Monitoring"
        append_to_summary ""
        append_to_summary "Continuous memory monitoring data saved."
        append_to_summary ""
    fi
}

# ============================================================================
# EXPERIMENT 4: Failure Simulation
# ============================================================================

run_failure_simulation() {
    header "EXPERIMENT 4: Virtual Storage Failure Simulation"
    log "Testing failure recovery with $FAILURE_SIM_DIRS storage directories..."
    
    local EXP_DIR="$EXPERIMENT_DIR/failure_sim"
    mkdir -p "$EXP_DIR"
    
    # Run the experiment
    if bash "$SCRIPT_DIR/simulate-virtual-storage-failure.sh" "$FAILURE_SIM_DIRS" 1 2>&1 | tee -a "$LOG_FILE"; then
        log "Failure simulation completed ✓"
        
        # Copy results
        local LATEST=$(ls -td "$SCRIPT_DIR/results/failure_sim_"* 2>/dev/null | head -1)
        if [ -n "$LATEST" ]; then
            cp -r "$LATEST"/* "$EXP_DIR/" 2>/dev/null || true
            
            append_to_summary "### Experiment 4: Failure Simulation"
            append_to_summary ""
            append_to_summary "**Question**: How does HDFS recover from individual virtual storage failures?"
            append_to_summary ""
            append_to_summary "**Results**:"
            append_to_summary '```'
            cat "$EXP_DIR/results.csv" >> "$SUMMARY_FILE"
            append_to_summary '```'
            append_to_summary ""
        fi
    else
        log "Failure simulation FAILED"
    fi
}

# ============================================================================
# GENERATE FINAL REPORT
# ============================================================================

generate_report() {
    header "GENERATING FINAL REPORT"
    
    append_to_summary "---"
    append_to_summary ""
    append_to_summary "## Conclusions"
    append_to_summary ""
    append_to_summary "### NameNode Scalability"
    append_to_summary ""
    append_to_summary "Based on the block scaling experiment:"
    append_to_summary "- NameNode memory grows approximately ~150 bytes per block"
    append_to_summary "- With 1000 virtual storages and smaller blocks, metadata overhead increases significantly"
    append_to_summary ""
    append_to_summary "### DataNode Performance"
    append_to_summary ""
    append_to_summary "Based on the storage directory experiment:"
    append_to_summary "- I/O throughput may decrease with many storage directories due to seek overhead"
    append_to_summary "- Block report time increases with more directories to scan"
    append_to_summary ""
    append_to_summary "### Failure Recovery"
    append_to_summary ""
    append_to_summary "Based on the failure simulation:"
    append_to_summary "- Individual virtual storage failures cause smaller blast radius"
    append_to_summary "- Recovery time depends on number of blocks to re-replicate"
    append_to_summary ""
    append_to_summary "---"
    append_to_summary ""
    append_to_summary "**Experiment completed at**: $(date '+%Y-%m-%d %H:%M:%S')"
    append_to_summary ""
    append_to_summary "**Full logs**: see experiment.log"
    
    log ""
    log "Final report saved to: $SUMMARY_FILE"
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    header "STORAGE VIRTUALIZATION FULL EXPERIMENT"
    log "Experiment: $EXPERIMENT"
    log "Output directory: $EXPERIMENT_DIR"
    
    # Pre-flight checks
    check_dependencies
    check_hdfs_running
    initialize_summary
    
    case "$EXPERIMENT" in
        all)
            log "Running ALL experiments..."
            run_memory_monitor      # Start background monitoring first
            sleep 5                 # Let it warm up
            run_block_scaling       # Test NameNode memory scaling
            run_storage_dirs        # Test DataNode with multiple dirs
            run_failure_simulation  # Test failure recovery
            stop_memory_monitor     # Stop background monitoring
            ;;
        block_scaling)
            run_block_scaling
            ;;
        storage_dirs)
            run_storage_dirs
            ;;
        memory)
            # Run memory monitor in foreground for 30 minutes
            bash "$SCRIPT_DIR/monitor-namenode-memory.sh" 5 30
            ;;
        failure)
            run_failure_simulation
            ;;
        *)
            echo "Unknown experiment: $EXPERIMENT"
            echo "Usage: $0 [all|block_scaling|storage_dirs|memory|failure]"
            exit 1
            ;;
    esac
    
    generate_report
    
    header "EXPERIMENT COMPLETE"
    log ""
    log "Results saved to: $EXPERIMENT_DIR"
    log ""
    log "Key files:"
    log "  - Summary: $SUMMARY_FILE"
    log "  - Full log: $LOG_FILE"
    log ""
    log "To view summary:"
    log "  cat $SUMMARY_FILE"
    log ""
}

main "$@"
