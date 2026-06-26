"""
Workload Generator: creates synthetic input files and submits
batches of Spark jobs to populate data/raw_metrics.csv.
"""
import os
import sys
import time
import random
import string
import argparse

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


# ─── Input File Generators ───────────────────────────────────────────────────

def generate_text_file(path: str, size_mb: float):
    """Generate a random text file of approximately `size_mb` megabytes."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    words = ["the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
             "spark", "hadoop", "data", "stream", "batch", "cluster", "node",
             "task", "stage", "executor", "driver", "partition", "shuffle",
             "memory", "compute", "analytics", "pipeline", "workflow"]
    target_bytes = int(size_mb * 1024 * 1024)
    chunk = " ".join(random.choices(words, k=100)) + "\n"
    chunk_bytes = len(chunk.encode())
    repeats = max(1, target_bytes // chunk_bytes)
    with open(path, "w") as f:
        for _ in range(repeats):
            f.write(chunk)


def generate_csv_pair(path_a: str, path_b: str, num_rows: int):
    """Generate two CSVs (with shared 'id') for join+aggregation jobs."""
    os.makedirs(os.path.dirname(path_a), exist_ok=True)
    categories = ["alpha", "beta", "gamma", "delta", "epsilon"]
    rng = np.random.default_rng(42)
    ids = np.arange(1, num_rows + 1)

    df_a = pd.DataFrame({
        "id": ids,
        "category": rng.choice(categories, size=num_rows),
        "value": rng.uniform(1.0, 1000.0, size=num_rows).round(2),
    })
    # df_b has about 80% overlap in IDs (simulates real join cardinality)
    b_ids = rng.choice(ids, size=int(num_rows * 0.8), replace=False)
    df_b = pd.DataFrame({
        "id": b_ids,
        "score": rng.uniform(0.0, 100.0, size=len(b_ids)).round(2),
    })
    df_a.to_csv(path_a, index=False)
    df_b.to_csv(path_b, index=False)


def generate_graph_file(path: str, num_nodes: int, avg_degree: int = 4):
    """Generate a random directed graph adjacency list for PageRank."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    rng = np.random.default_rng(42)
    lines = []
    nodes = [str(i) for i in range(num_nodes)]
    for node in nodes:
        num_neighbors = rng.integers(1, avg_degree * 2 + 1)
        neighbors = rng.choice(nodes, size=int(num_neighbors), replace=False)
        for neighbor in neighbors:
            if neighbor != node:
                lines.append(f"{node} {neighbor}")
    with open(path, "w") as f:
        f.write("\n".join(lines))


def generate_synthetic_inputs(data_dir: str) -> dict:
    """
    Create all synthetic input files. Returns a dict mapping size-variant
    names to their file paths.
    """
    print("Generating synthetic input files...")
    input_dir = os.path.join(data_dir, "input")

    # Text files for word count (small jobs)
    text_files = {}
    for size_mb in [2, 5, 10]:
        path = os.path.join(input_dir, f"text_{size_mb}mb.txt")
        if not os.path.exists(path):
            generate_text_file(path, size_mb)
            print(f"  Created {path} ({size_mb} MB)")
        text_files[size_mb] = path

    # CSV pairs for join+aggregation (medium jobs)
    csv_files = {}
    for num_rows in [50_000, 200_000, 500_000]:
        label = f"{num_rows // 1000}k"
        path_a = os.path.join(input_dir, f"medium_a_{label}.csv")
        path_b = os.path.join(input_dir, f"medium_b_{label}.csv")
        if not os.path.exists(path_a):
            generate_csv_pair(path_a, path_b, num_rows)
            print(f"  Created medium CSV pair ({label} rows)")
        approx_mb = round(num_rows * 30 / 1_000_000, 1)
        csv_files[label] = (path_a, path_b, approx_mb)

    # Graph files for PageRank (large jobs)
    graph_files = {}
    for num_nodes in [1_000, 5_000, 10_000]:
        path = os.path.join(input_dir, f"graph_{num_nodes}nodes.txt")
        if not os.path.exists(path):
            generate_graph_file(path, num_nodes)
            print(f"  Created graph file ({num_nodes} nodes)")
        approx_mb = round(num_nodes * 5 * 12 / 1_000_000, 2)
        graph_files[num_nodes] = (path, approx_mb)

    print("Input files ready.\n")
    return {"text": text_files, "csv": csv_files, "graph": graph_files}


# ─── Job Spec Builder ─────────────────────────────────────────────────────────

def build_job_specs(inputs: dict, n_runs: int = 40, seed: int = 0) -> list:
    """
    Build a diverse list of job specs by Latin-hypercube-like sampling
    over (job_type, size_variant, num_partitions).
    """
    rng = random.Random(seed)
    specs = []

    text_sizes = list(inputs["text"].keys())          # [2, 5, 10]
    csv_labels = list(inputs["csv"].keys())            # ['50k', '200k', '500k']
    graph_sizes = list(inputs["graph"].keys())         # [1000, 5000, 10000]
    partition_choices = [2, 4, 8, 16]
    pagerank_iters = [3, 5, 10]

    job_types = ["small", "medium", "large"]
    # Distribute roughly evenly
    per_type = n_runs // 3
    extras = n_runs - per_type * 3

    for jt, count in zip(job_types, [per_type, per_type, per_type + extras]):
        for _ in range(count):
            hour = rng.randint(0, 23)
            parts = rng.choice(partition_choices)

            if jt == "small":
                sz = rng.choice(text_sizes)
                path = inputs["text"][sz]
                specs.append({
                    "job_type": "small",
                    "input_path": path,
                    "input_path_b": None,
                    "input_size_mb": float(sz),
                    "num_partitions": parts,
                    "iterations": 1,
                    "hour_of_day": hour,
                })
            elif jt == "medium":
                label = rng.choice(csv_labels)
                path_a, path_b, size_mb = inputs["csv"][label]
                specs.append({
                    "job_type": "medium",
                    "input_path": path_a,
                    "input_path_b": path_b,
                    "input_size_mb": size_mb,
                    "num_partitions": parts,
                    "iterations": 1,
                    "hour_of_day": hour,
                })
            else:  # large
                n_nodes = rng.choice(graph_sizes)
                path, size_mb = inputs["graph"][n_nodes]
                iters = rng.choice(pagerank_iters)
                specs.append({
                    "job_type": "large",
                    "input_path": path,
                    "input_path_b": None,
                    "input_size_mb": size_mb,
                    "num_partitions": parts,
                    "iterations": iters,
                    "hour_of_day": hour,
                })

    rng.shuffle(specs)
    return specs


# ─── Batch Submission ─────────────────────────────────────────────────────────

def submit_batch(spark, job_specs: list, collector, verbose: bool = True):
    """Submit each job spec to Spark, collect and persist metrics."""
    from workload.jobs.small_job import run_word_count
    from workload.jobs.medium_job import run_join_aggregation
    from workload.jobs.large_job import run_iterative_pagerank

    results = []
    for i, spec in enumerate(job_specs):
        jt = spec["job_type"]
        if verbose:
            print(f"  [{i+1}/{len(job_specs)}] {jt:6s}  "
                  f"{spec['input_size_mb']:5.1f} MB  "
                  f"parts={spec['num_partitions']}", end="  ", flush=True)

        cpu_start = time.process_time()
        t_start = time.perf_counter()

        try:
            if jt == "small":
                run_word_count(spark, spec["input_path"], spec["num_partitions"])
            elif jt == "medium":
                run_join_aggregation(
                    spark, spec["input_path"], spec["input_path_b"],
                    spec["num_partitions"]
                )
            else:
                run_iterative_pagerank(
                    spark, spec["input_path"], spec["num_partitions"],
                    iterations=spec["iterations"]
                )
        except Exception as e:
            print(f"FAILED: {e}")
            continue

        t_end = time.perf_counter()
        cpu_end = time.process_time()

        row = collector.collect(spec, t_start, t_end, cpu_start, cpu_end)
        collector.append_to_csv(row)
        results.append(row)

        if verbose:
            print(f"→ {row['execution_time_sec']:.2f}s")

    return results


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate workload metrics dataset")
    parser.add_argument("--n-runs", type=int, default=40,
                        help="Number of job runs to generate (default: 40)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    from pyspark.sql import SparkSession
    from workload.metrics_collector import MetricsCollector

    data_dir = os.path.join(PROJECT_ROOT, "data")
    metrics_csv = os.path.join(data_dir, "raw_metrics.csv")

    # Generate input files
    inputs = generate_synthetic_inputs(data_dir)

    # Build job specs
    specs = build_job_specs(inputs, n_runs=args.n_runs, seed=args.seed)
    print(f"Submitting {len(specs)} jobs to Spark...\n")

    # Create SparkSession
    conf_dir = os.path.join(PROJECT_ROOT, "conf")
    spark = (
        SparkSession.builder
        .appName("WorkloadGenerator")
        .master("local[*]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/tmp/spark-events")
        .config("spark.scheduler.mode", "FAIR")
        .config(
            "spark.scheduler.allocation.file",
            os.path.join(conf_dir, "fairscheduler.xml")
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    collector = MetricsCollector(spark, metrics_csv)

    try:
        submit_batch(spark, specs, collector)
    finally:
        spark.stop()

    # Summary
    df = pd.read_csv(metrics_csv)
    print(f"\n✓ Dataset saved to {metrics_csv}")
    print(f"  Total rows : {len(df)}")
    print(f"  Job types  : {df['job_type'].value_counts().to_dict()}")
    print(f"  Exec time  : min={df['execution_time_sec'].min():.2f}s  "
          f"max={df['execution_time_sec'].max():.2f}s  "
          f"mean={df['execution_time_sec'].mean():.2f}s")


if __name__ == "__main__":
    main()
