"""
Adapter: Google Cluster Trace 2011 → raw_metrics.csv
Uses task_events ONLY (no task_usage needed).

All features are derived from information available at scheduling time:
  - cpu_request  → num_partitions  (known before job runs)
  - ram_request  → input_size_mb   (known before job runs)
  - sched_class  → job_type        (known before job runs)
  - timestamp    → hour_of_day     (known before job runs)
  - task count   → num_tasks       (known before job runs)

Target: execution_time_sec = (FINISH_time - SCHEDULE_time) / 1e6

task_events columns (13, no header):
  0  timestamp(μs)  1  missing_info  2  job_id      3  task_index
  4  machine_id     5  event_type    6  user_hash   7  scheduling_class
  8  priority       9  cpu_request   10 ram_request 11 disk_request
  12 different_machine_constraint

event_type codes:
  0=SUBMIT  1=SCHEDULE  2=EVICT  3=FAIL  4=FINISH  5=KILL

Job type thresholds (execution time based):
  < 120s    → small
  120–1800s → medium
  > 1800s   → large

Machine capacity assumed: 4 cores, 25 GB RAM

Usage:
    python -m data.adapters.google_adapter            \\
        --events-dir data/google_cluster              \\
        --output     data/raw_metrics_google.csv      \\
        --parts      500
"""
import os, sys, glob, argparse
import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

SCHEDULE = 1
FINISH   = 4

MACHINE_CORES = 4.0    # cpu_request is fraction of this
MACHINE_RAM_GB = 25.0  # ram_request is fraction of this

# Columns we need from task_events (0-indexed)
EV_COLS   = [0, 2, 3, 5, 7, 9, 10]
EV_NAMES  = ["timestamp", "job_id", "task_index",
             "event_type", "sched_class", "cpu_request", "ram_request"]


def _read_events_chunk(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath, header=None, usecols=EV_COLS,
                       names=EV_NAMES, low_memory=False)


def adapt_google(events_dir: str, output_path: str, n_parts: int = 500,
                 chunksize: int = 50):
    """
    Process all task_events part files in batches, streaming results to CSV.
    Processes `chunksize` files at a time to control memory usage.
    """
    all_files = sorted(glob.glob(os.path.join(events_dir, "task_events_*.csv.gz")))
    files = all_files[:n_parts]

    if not files:
        raise FileNotFoundError(f"No task_events_*.csv.gz found in {events_dir}")

    print(f"Processing {len(files)} task_events parts in batches of {chunksize}...")
    print(f"Output → {output_path}\n")

    # Write header once; if file exists count existing rows for progress display
    header_written = os.path.exists(output_path) and os.path.getsize(output_path) > 0
    if header_written:
        existing_rows = sum(1 for _ in open(output_path)) - 1
        print(f"  Resuming — {existing_rows:,} rows already written.")
    total_written = 0

    for batch_start in range(0, len(files), chunksize):
        batch = files[batch_start: batch_start + chunksize]
        batch_end = batch_start + len(batch) - 1
        print(f"  Batch {batch_start//chunksize + 1}: parts {batch_start:03d}–{batch_end:03d}", end="  ", flush=True)

        # Read batch
        frames = [_read_events_chunk(fp) for fp in batch]
        ev = pd.concat(frames, ignore_index=True)

        # Coerce types
        ev["job_id"]      = pd.to_numeric(ev["job_id"],      errors="coerce")
        ev["task_index"]  = pd.to_numeric(ev["task_index"],  errors="coerce")
        ev["timestamp"]   = pd.to_numeric(ev["timestamp"],   errors="coerce")
        ev["cpu_request"] = pd.to_numeric(ev["cpu_request"], errors="coerce")
        ev["ram_request"] = pd.to_numeric(ev["ram_request"], errors="coerce")
        ev["sched_class"] = pd.to_numeric(ev["sched_class"], errors="coerce")
        ev = ev.dropna(subset=["job_id", "task_index", "timestamp"])

        # ── SCHEDULE events (start time + resource requests) ────────────────
        sched = (
            ev[ev["event_type"] == SCHEDULE]
            [["timestamp","job_id","task_index","sched_class","cpu_request","ram_request"]]
            .rename(columns={"timestamp": "t_start"})
            .sort_values("t_start")
            .groupby(["job_id","task_index"], as_index=False)
            .first()   # first schedule per task (ignore reschedules after eviction)
        )

        # ── FINISH events (end time) ─────────────────────────────────────────
        finish = (
            ev[ev["event_type"] == FINISH]
            [["timestamp","job_id","task_index"]]
            .rename(columns={"timestamp": "t_end"})
            .groupby(["job_id","task_index"], as_index=False)
            .first()
        )

        # ── Join, compute execution time ─────────────────────────────────────
        tasks = sched.merge(finish, on=["job_id","task_index"], how="inner")
        tasks["execution_time_sec"] = ((tasks["t_end"] - tasks["t_start"]) / 1_000_000).round(2)
        tasks = tasks[tasks["execution_time_sec"] >= 0.5].copy()

        if len(tasks) == 0:
            print(f"→ 0 tasks")
            continue

        # ── Task count per job (proxy for num_tasks) ─────────────────────────
        job_task_counts = (
            sched.groupby("job_id")["task_index"]
                 .count()
                 .rename("num_tasks")
                 .reset_index()
        )
        tasks = tasks.merge(job_task_counts, on="job_id", how="left")

        # ── Feature engineering ───────────────────────────────────────────────
        tasks["cpu_request"]  = tasks["cpu_request"].fillna(0.05).clip(lower=1e-4)
        tasks["ram_request"]  = tasks["ram_request"].fillna(0.01).clip(lower=1e-4)

        tasks["input_size_mb"]  = (tasks["ram_request"] * MACHINE_RAM_GB * 1024).round(1).clip(lower=1.0)
        tasks["num_partitions"] = (tasks["cpu_request"] * MACHINE_CORES * 4).clip(1, 32).round(0).astype(int)
        tasks["num_stages"]     = np.where(tasks["num_tasks"] > 10, 3,
                                   np.where(tasks["num_tasks"] > 2,  2, 1))
        tasks["num_tasks"]      = tasks["num_tasks"].fillna(1).clip(1, 500).astype(int)
        tasks["peak_memory_mb"] = (tasks["ram_request"] * MACHINE_RAM_GB * 1024 * 0.9).round(1).clip(lower=1.0)
        tasks["cpu_seconds"]    = (tasks["cpu_request"] * tasks["execution_time_sec"]).round(2)
        tasks["hour_of_day"]    = (tasks["t_start"] / 1_000_000 / 3600 % 24).astype(int).clip(0, 23)

        # ── Job type from execution time ──────────────────────────────────────
        tasks["job_type"] = pd.cut(
            tasks["execution_time_sec"],
            bins=[0, 120, 1800, float("inf")],
            labels=["small", "medium", "large"],
        ).astype(str)

        # ── Select output columns ─────────────────────────────────────────────
        OUT = ["job_type","input_size_mb","num_partitions","num_stages",
               "num_tasks","peak_memory_mb","cpu_seconds","execution_time_sec","hour_of_day"]
        result = tasks[OUT].dropna().reset_index(drop=True)

        # ── Stream-write to CSV ───────────────────────────────────────────────
        result.to_csv(output_path,
                      mode="a" if header_written else "w",
                      header=not header_written,
                      index=False)
        header_written = True
        total_written += len(result)
        print(f"→ {len(result):,} rows  (total: {total_written:,})")

    # ── Final summary ─────────────────────────────────────────────────────────
    df = pd.read_csv(output_path)
    print(f"\n{'='*58}")
    print(f"✓ Total rows: {len(df):,}  →  {output_path}")
    print(f"{'='*58}")
    print(f"  Job types : {df['job_type'].value_counts().to_dict()}")
    et = df["execution_time_sec"]
    print(f"  Exec time : min={et.min():.1f}s  mean={et.mean():.1f}s  "
          f"median={et.median():.1f}s  max={et.max():.1f}s")
    print(f"  Input MB  : min={df['input_size_mb'].min():.0f}  "
          f"max={df['input_size_mb'].max():.0f}")
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-dir", default=os.path.join(PROJECT_ROOT, "data", "google_cluster"))
    parser.add_argument("--output",     default=os.path.join(PROJECT_ROOT, "data", "raw_metrics_google.csv"))
    parser.add_argument("--parts",      type=int, default=500)
    parser.add_argument("--chunksize",  type=int, default=50,
                        help="Files processed per batch (controls RAM usage)")
    args = parser.parse_args()
    adapt_google(args.events_dir, args.output, args.parts, args.chunksize)


if __name__ == "__main__":
    main()
