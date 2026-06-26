"""
Benchmark runner: run the same 15-job queue under FIFO, Adaptive (SJF), and
PASTA scheduling, record timings, and save results to JSON.
"""
import os
import sys
import json
import time
import random
import argparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
ARTIFACTS_DIR = os.path.join(PROJECT_ROOT, "model", "artifacts")


# ─── Fixed Job Queue ──────────────────────────────────────────────────────────

def build_job_queue(n_jobs: int = 15) -> list:
    """
    Build a fixed, reproducible 15-job queue mixing all 3 job types.
    Seeded for reproducibility.
    """
    rng = random.Random(42)
    input_dir = os.path.join(DATA_DIR, "input")

    # Sizes bumped up well beyond the original 2-10MB/50-500k row/1-10k node
    # variants so a full `--mode all` run (FIFO + Adaptive + PASTA, each over
    # all 15 jobs) takes on the order of 15+ minutes instead of ~25 seconds.
    text_files = {
        500: os.path.join(input_dir, "text_500mb.txt"),
        1000: os.path.join(input_dir, "text_1000mb.txt"),
        1500: os.path.join(input_dir, "text_1500mb.txt"),
    }
    csv_files = {
        "5m": (
            os.path.join(input_dir, "medium_a_5m.csv"),
            os.path.join(input_dir, "medium_b_5m.csv"),
            99.0,
        ),
        "10m": (
            os.path.join(input_dir, "medium_a_10m.csv"),
            os.path.join(input_dir, "medium_b_10m.csv"),
            199.0,
        ),
        "15m": (
            os.path.join(input_dir, "medium_a_15m.csv"),
            os.path.join(input_dir, "medium_b_15m.csv"),
            304.0,
        ),
    }
    graph_files = {
        30000: (os.path.join(input_dir, "graph_30000nodes.txt"), 1.5),
    }

    specs = []
    job_type_cycle = (["small"] * 5 + ["medium"] * 5 + ["large"] * 5)
    rng.shuffle(job_type_cycle)

    for jt in job_type_cycle:
        parts = rng.choice([2, 4, 8])
        hour = rng.randint(8, 18)

        if jt == "small":
            sz = rng.choice([500, 1000, 1500])
            specs.append({
                "job_type": "small",
                "input_path": text_files[sz],
                "input_path_b": None,
                "input_size_mb": float(sz),
                "num_partitions": parts,
                "iterations": 1,
                "hour_of_day": hour,
            })
        elif jt == "medium":
            label = rng.choice(["5m", "10m", "15m"])
            pa, pb, size_mb = csv_files[label]
            specs.append({
                "job_type": "medium",
                "input_path": pa,
                "input_path_b": pb,
                "input_size_mb": size_mb,
                "num_partitions": parts,
                "iterations": 1,
                "hour_of_day": hour,
            })
        else:
            n_nodes = 30000
            path, size_mb = graph_files[n_nodes]
            iters = rng.choice([200, 350, 500])
            specs.append({
                "job_type": "large",
                "input_path": path,
                "input_path_b": None,
                "input_size_mb": size_mb,
                "num_partitions": parts,
                "iterations": iters,
                "hour_of_day": hour,
            })

    return specs


# ─── FIFO Runner ─────────────────────────────────────────────────────────────

def run_fifo(job_specs: list) -> dict:
    """
    Submit jobs in original order with default Spark configs (FIFO baseline).
    Returns dict with results list and total makespan.
    """
    from pyspark.sql import SparkSession
    from workload.jobs.small_job import run_word_count
    from workload.jobs.medium_job import run_join_aggregation
    from workload.jobs.large_job import run_iterative_pagerank

    spark = (
        SparkSession.builder
        .appName("FIFO_Benchmark")
        .master("local[*]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/tmp/spark-events")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    results = []
    wall_start = time.perf_counter()

    print(f"\nFIFO: submitting {len(job_specs)} jobs in original order...\n")
    for i, spec in enumerate(job_specs):
        jt = spec["job_type"]
        print(f"  [{i+1}/{len(job_specs)}] {jt:6s}  "
              f"{spec['input_size_mb']:5.1f} MB", end="  ", flush=True)

        t_start = time.perf_counter()
        try:
            if jt == "small":
                run_word_count(spark, spec["input_path"], spec["num_partitions"])
            elif jt == "medium":
                run_join_aggregation(spark, spec["input_path"],
                                     spec["input_path_b"], spec["num_partitions"])
            else:
                run_iterative_pagerank(spark, spec["input_path"],
                                       spec["num_partitions"],
                                       iterations=spec.get("iterations", 5))
        except Exception as e:
            print(f"FAILED: {e}")
            continue

        t_end = time.perf_counter()
        actual = round(t_end - t_start, 4)
        print(f"→ {actual:.2f}s")

        results.append({
            "job_index": i,
            "job_type": jt,
            "input_size_mb": spec["input_size_mb"],
            "num_partitions": spec["num_partitions"],
            "actual_time": actual,
            "start_ts": round(t_start - wall_start, 4),
            "end_ts": round(t_end - wall_start, 4),
        })

    spark.stop()
    makespan = round(time.perf_counter() - wall_start, 4)
    return {"results": results, "makespan": makespan}


# ─── Adaptive Runner ─────────────────────────────────────────────────────────

def run_adaptive(job_specs: list) -> dict:
    """
    Submit jobs using the AdaptiveScheduler (SJF + dynamic resource config).
    """
    from scheduler.adaptive_scheduler import AdaptiveScheduler

    rf_path = os.path.join(ARTIFACTS_DIR, "rf_model.joblib")
    enc_path = os.path.join(ARTIFACTS_DIR, "encoder.joblib")

    scheduler = AdaptiveScheduler(
        model_path=rf_path,
        encoder_path=enc_path,
        verbose=True,
    )

    wall_start = time.perf_counter()
    print(f"\nAdaptive: scheduling {len(job_specs)} jobs...\n")
    raw_results = scheduler.run(job_specs)
    makespan = round(time.perf_counter() - wall_start, 4)

    # Add wall-clock relative timestamps
    for i, r in enumerate(raw_results):
        r["job_index"] = i

    return {"results": raw_results, "makespan": makespan}


# ─── PASTA Runner ────────────────────────────────────────────────────────────

def run_pasta(job_specs: list, feedback: bool = False) -> dict:
    """
    Submit jobs using the PASTAScheduler (aging + dynamic tiers + feedback loop).
    """
    from scheduler.pasta_scheduler import PASTAScheduler

    rf_path  = os.path.join(ARTIFACTS_DIR, "rf_model.joblib")
    enc_path = os.path.join(ARTIFACTS_DIR, "encoder.joblib")

    scheduler = PASTAScheduler(
        model_path=rf_path,
        encoder_path=enc_path,
        verbose=True,
    )

    wall_start = time.perf_counter()
    print(f"\nPASTA: scheduling {len(job_specs)} jobs...\n")
    raw_results = scheduler.run(job_specs, current_time=0.0)
    makespan = round(time.perf_counter() - wall_start, 4)

    for i, r in enumerate(raw_results):
        r["job_index"] = i

    if feedback:
        scheduler.feedback_retrain(raw_results, retrain=True)

    return {"results": raw_results, "makespan": makespan}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run scheduling benchmark")
    parser.add_argument("--mode", choices=["fifo", "adaptive", "pasta", "all"],
                        default="all", help="Which mode to benchmark")
    parser.add_argument("--n-jobs", type=int, default=15)
    parser.add_argument("--feedback", action="store_true",
                        help="After PASTA run, append actuals to CSV and retrain model")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    job_specs = build_job_queue(args.n_jobs)

    if args.mode in ("fifo", "all"):
        fifo_out = run_fifo(job_specs)
        out_path = os.path.join(DATA_DIR, "benchmark_fifo.json")
        with open(out_path, "w") as f:
            json.dump(fifo_out, f, indent=2)
        print(f"\n✓ FIFO makespan: {fifo_out['makespan']:.2f}s  → {out_path}")

    if args.mode in ("adaptive", "all"):
        adaptive_out = run_adaptive(job_specs)
        out_path = os.path.join(DATA_DIR, "benchmark_adaptive.json")
        with open(out_path, "w") as f:
            json.dump(adaptive_out, f, indent=2)
        print(f"\n✓ Adaptive (SJF) makespan: {adaptive_out['makespan']:.2f}s  → {out_path}")

    if args.mode in ("pasta", "all"):
        pasta_out = run_pasta(job_specs, feedback=args.feedback)
        out_path = os.path.join(DATA_DIR, "benchmark_pasta.json")
        with open(out_path, "w") as f:
            json.dump(pasta_out, f, indent=2)
        print(f"\n✓ PASTA makespan: {pasta_out['makespan']:.2f}s  → {out_path}")

    if args.mode == "all":
        avg_fifo = sum(r["actual_time"] for r in fifo_out["results"]) / len(fifo_out["results"])
        avg_adap = sum(r["actual_time"] for r in adaptive_out["results"]) / len(adaptive_out["results"])
        avg_pasta = sum(r["actual_time"] for r in pasta_out["results"]) / len(pasta_out["results"])
        sp_adap  = fifo_out["makespan"] / adaptive_out["makespan"]
        sp_pasta = fifo_out["makespan"] / pasta_out["makespan"]
        print(f"\n{'='*60}")
        print(f"{'Metric':<30} {'FIFO':>8} {'Adaptive':>10} {'PASTA':>8}")
        print("-" * 60)
        print(f"{'Total makespan (s)':<30} {fifo_out['makespan']:>8.2f} {adaptive_out['makespan']:>10.2f} {pasta_out['makespan']:>8.2f}")
        print(f"{'Avg job latency (s)':<30} {avg_fifo:>8.2f} {avg_adap:>10.2f} {avg_pasta:>8.2f}")
        print(f"{'Speedup vs FIFO':<30} {'1.00x':>8} {sp_adap:>9.2f}x {sp_pasta:>7.2f}x")
        print("=" * 60)


if __name__ == "__main__":
    main()
