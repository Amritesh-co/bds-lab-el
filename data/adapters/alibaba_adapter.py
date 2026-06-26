"""
Adapter: Convert Alibaba Cluster Trace v2018 batch_task.csv
to the raw_metrics.csv format used by this project's ML model.

Download the trace from: https://github.com/alibaba/clusterdata
File needed: batch_task.csv (inside cluster-trace-v2018/)

Usage:
    python -m data.adapters.alibaba_adapter \
        --input path/to/batch_task.csv \
        --output data/raw_metrics.csv
"""
import os
import sys
import argparse
import pandas as pd
import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

# Alibaba batch_task.csv columns (v2018):
# task_name, instance_num, job_name, status,
# start_time, end_time,
# plan_cpu (requested), plan_mem (requested)
ALIBABA_COLS = [
    "task_name", "instance_num", "job_name", "status",
    "start_time", "end_time", "plan_cpu", "plan_mem"
]


def classify_job_type(plan_cpu: float, plan_mem: float, exec_time: float) -> str:
    """
    Heuristically map Alibaba job attributes to small/medium/large.
    Uses execution time as primary signal (same thresholds as resource_policy.py).
    """
    if exec_time < 10:
        return "small"
    elif exec_time < 60:
        return "medium"
    return "large"


def adapt_alibaba(input_path: str, output_path: str, max_rows: int = 50_000):
    """
    Read Alibaba batch_task.csv, map to raw_metrics format, save CSV.
    """
    print(f"Reading {input_path}...")

    df = pd.read_csv(
        input_path,
        header=None,
        names=ALIBABA_COLS,
        nrows=max_rows,
    )

    print(f"  Loaded {len(df)} rows. Cleaning...")

    # Keep only completed tasks
    df = df[df["status"] == "Terminated"].copy()

    # Drop rows with missing times
    df = df.dropna(subset=["start_time", "end_time", "plan_cpu", "plan_mem"])

    # Execution time in seconds (Alibaba times are in seconds relative to trace start)
    df["execution_time_sec"] = (df["end_time"] - df["start_time"]).clip(lower=0.5)

    # Drop zero/negative durations
    df = df[df["execution_time_sec"] > 0.5]

    # Map to our features
    # plan_cpu is in fractional cores (Alibaba normalizes by 96-core machine)
    # plan_mem is normalized (0–1 fraction of total memory)
    df["input_size_mb"] = (df["plan_mem"] * 32_768).round(1)   # estimate: 32 GB machine
    df["num_partitions"] = (df["plan_cpu"] * 48).clip(1, 64).round(0).astype(int)
    df["num_stages"] = np.random.randint(1, 5, size=len(df))    # not in trace — estimated
    df["num_tasks"] = (df["instance_num"].clip(1, 200)).astype(int)
    df["peak_memory_mb"] = (df["plan_mem"] * 32_768 * 0.8).round(1)
    df["cpu_seconds"] = (df["plan_cpu"] * df["execution_time_sec"]).round(2)
    df["hour_of_day"] = (df["start_time"] % 86400 // 3600).astype(int).clip(0, 23)
    df["job_type"] = df.apply(
        lambda r: classify_job_type(r["plan_cpu"], r["plan_mem"], r["execution_time_sec"]),
        axis=1
    )

    # Select final columns in our schema order
    out_cols = [
        "job_type", "input_size_mb", "num_partitions", "num_stages",
        "num_tasks", "peak_memory_mb", "cpu_seconds", "execution_time_sec", "hour_of_day"
    ]
    result = df[out_cols].reset_index(drop=True)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Append to existing file if present
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        existing = pd.read_csv(output_path)
        result = pd.concat([existing, result], ignore_index=True)

    result.to_csv(output_path, index=False)

    print(f"\n✓ Adapted {len(df)} rows → {output_path}")
    print(f"  Job types : {result['job_type'].value_counts().to_dict()}")
    print(f"  Exec time : min={result['execution_time_sec'].min():.1f}s  "
          f"max={result['execution_time_sec'].max():.1f}s  "
          f"mean={result['execution_time_sec'].mean():.1f}s")


def main():
    parser = argparse.ArgumentParser(description="Adapt Alibaba Cluster Trace to raw_metrics format")
    parser.add_argument("--input", required=True, help="Path to Alibaba batch_task.csv")
    parser.add_argument("--output", default=os.path.join(PROJECT_ROOT, "data", "raw_metrics.csv"))
    parser.add_argument("--max-rows", type=int, default=50_000,
                        help="Max rows to read from source (default: 50000)")
    args = parser.parse_args()
    adapt_alibaba(args.input, args.output, args.max_rows)


if __name__ == "__main__":
    main()
