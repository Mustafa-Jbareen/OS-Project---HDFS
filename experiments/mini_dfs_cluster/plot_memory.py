"""Plot MiniDFSCluster memory usage samples produced by MiniDFSClusterExperiment."""

import argparse
import csv
from pathlib import Path

try:
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit("matplotlib is required to generate the plot. Install it with `pip install matplotlib`." ) from exc


def read_samples(csv_path: Path):
    samples = []
    with csv_path.open() as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader, None)
        for row in reader:
            if not row or len(row) < 2:
                continue
            try:
                nodes = int(row[0])
                memory = int(row[1])
            except ValueError:
                continue
            samples.append((nodes, memory))
    return samples


def main():
    parser = argparse.ArgumentParser(
        description="Generate a memory-vs-DataNodes figure from the MiniDFSCluster experiment output."
    )
    parser.add_argument("csv", type=Path, help="Path to the memory_usage.csv produced by the Java experiment.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("mini_dfs_memory.png"),
        help="Path where the PNG figure will be saved.",
    )
    parser.add_argument(
        "--log-y",
        action="store_true",
        help="Use a logarithmic scale for the memory axis.",
    )
    args = parser.parse_args()

    samples = read_samples(args.csv)
    if not samples:
        raise SystemExit(f"No data found in {args.csv}")

    samples.sort(key=lambda item: item[0])
    nodes, memory = zip(*samples)
    memory_mib = [value / (1024 * 1024) for value in memory]

    plt.figure(figsize=(10, 6))
    plt.plot(nodes, memory_mib, marker="o", linewidth=2)
    plt.xlabel("DataNodes")
    plt.ylabel("Memory Used (MiB)")
    plt.title("MiniDFSCluster Memory Usage vs DataNodes")
    plt.xscale("log", base=2)
    if args.log_y:
        plt.yscale("log", base=2)
    plt.grid(True, which="both", linestyle="--", linewidth=0.6)
    plt.tight_layout()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.output, dpi=150)
    print(f"Saved figure to {args.output}")


if __name__ == "__main__":
    main()
