"""
PASTA — Predictive Adaptive Scheduling with Timed Aging
========================================================

Addresses THREE shortcomings of pure SJF scheduling in Apache Spark:

  1. STARVATION  — SJF can indefinitely delay large jobs when short jobs keep
                   arriving.  PASTA adds an aging term: the longer a job waits,
                   the higher its priority climbs, guaranteeing eventual service.

  2. PREDICTION  — Fixed tier thresholds (10s / 60s) are arbitrary and dataset-
     DRIFT          dependent.  PASTA computes dynamic tier boundaries from the
                   percentile distribution of current predictions, adapting to
                   whatever workload mix is present.

  3. FEEDBACK    — SJF never learns from its own mistakes.  PASTA records the
     LOOP           gap between predicted and actual execution times and can
                   retrain the underlying model, progressively improving accuracy.

Priority formula
----------------
  priority_i = α × norm_sjf_i  +  β × norm_aging_i

  norm_sjf_i   = 1 − (pred_i − min_pred) / (max_pred − min_pred + ε)
                 → 1.0 for shortest job, 0.0 for longest

  norm_aging_i = wait_i / (max_wait + ε)
                 → 0.0 on arrival, rises toward 1.0 as job waits

  α = 0.70  (SJF weight)   β = 0.30  (aging weight)

When all jobs arrive simultaneously (batch mode, wait_i = 0 for all):
  PASTA reduces to pure SJF — identical ordering.
When jobs have waited different amounts:
  Aging nudges long jobs forward, preventing starvation.

Dynamic tier boundaries
-----------------------
  Instead of hard-coded 10s / 60s:
    small  = predictions below the 33rd percentile of the current queue
    medium = 33rd–66th percentile
    large  = above 66th percentile

  This makes tiers relative to the workload at hand.

Feedback loop
-------------
  After every benchmark run, call feedback_retrain() to:
    1. Append (features, actual_time) rows to raw_metrics.csv
    2. Re-run model/train.py on the enriched dataset
  Subsequent predictions are then calibrated to local hardware.
"""

import os
import sys
import csv
import time
import subprocess
import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from model.predict import ExecutionTimePredictor
from scheduler.fair_pool_config import get_pool_name, generate_fairscheduler_xml, compute_adaptive_weights

# Priority weights
ALPHA = 0.70   # SJF component weight
BETA  = 0.30   # Aging component weight
EPS   = 1e-9   # Numerical stability


# ── Tier boundaries ───────────────────────────────────────────────────────────

def compute_dynamic_tiers(predictions: list) -> tuple:
    """
    Return (low_threshold, high_threshold) based on 33rd / 66th percentile
    of the current queue's predicted times.

    Example: if predictions are [0.1, 0.2, 0.5, 1.0, 3.0, 10.0]
      low  = p33 ≈ 0.35   → jobs < 0.35s are "small"
      high = p66 ≈ 2.0    → jobs > 2.0s are "large"
    """
    arr = np.array(predictions)
    low  = float(np.percentile(arr, 33))
    high = float(np.percentile(arr, 66))
    # Ensure at least a minimal gap so all three tiers are reachable
    if high <= low:
        high = low * 2 + EPS
    return low, high


def get_tier_dynamic(predicted: float, low: float, high: float) -> str:
    if predicted <= low:
        return "small"
    elif predicted <= high:
        return "medium"
    return "large"


def get_config_dynamic(predicted: float, low: float, high: float) -> dict:
    tier = get_tier_dynamic(predicted, low, high)
    if tier == "small":
        return {
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
            "spark.sql.shuffle.partitions": "4",
            "spark.driver.memory": "1g",
        }
    elif tier == "medium":
        return {
            "spark.executor.memory": "1g",
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.memory": "1g",
        }
    else:
        return {
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": "16",
            "spark.driver.memory": "2g",
        }


# ── Priority computation ─────────────────────────────────────────────────────

def compute_priority(predicted: float, wait_time: float,
                     min_pred: float, max_pred: float,
                     max_wait: float) -> float:
    """
    Compute PASTA priority score (higher = scheduled sooner).

    Args:
        predicted  : predicted execution time in seconds
        wait_time  : seconds this job has already waited in the queue
        min_pred   : minimum predicted time across all queued jobs
        max_pred   : maximum predicted time across all queued jobs
        max_wait   : maximum wait time across all queued jobs
    """
    pred_range = max_pred - min_pred + EPS
    norm_sjf   = 1.0 - (predicted - min_pred) / pred_range   # 1=shortest, 0=longest
    norm_aging = wait_time / (max_wait + EPS)                 # 0=new, 1=waited longest
    return ALPHA * norm_sjf + BETA * norm_aging


# ── PASTA Scheduler ───────────────────────────────────────────────────────────

class PASTAScheduler:
    """
    PASTA: Predictive Adaptive Scheduling with Timed Aging.
    Drop-in replacement for AdaptiveScheduler (SJF) with three enhancements:
      1. Aging-based priority (prevents starvation)
      2. Dynamic tier boundaries (percentile-based, not fixed)
      3. Post-run feedback loop (retrains model with actual times)
    """

    def __init__(
        self,
        model_path: str = None,
        encoder_path: str = None,
        metrics_csv: str = None,
        verbose: bool = True,
    ):
        self.predictor   = ExecutionTimePredictor(model_path, encoder_path)
        self.metrics_csv = metrics_csv or os.path.join(PROJECT_ROOT, "data", "raw_metrics.csv")
        self.verbose     = verbose

    # ── Step 1: Predict ───────────────────────────────────────────────────────

    def _predict_all(self, job_specs: list) -> list:
        """Add 'predicted_time' and 'submit_time' to each spec."""
        enriched = []
        for spec in job_specs:
            pt = self.predictor.predict_from_spec(spec)
            enriched.append({
                **spec,
                "predicted_time": round(pt, 4),
                "submit_time":    spec.get("submit_time", 0.0),
            })
        return enriched

    # ── Step 2: Prioritise (PASTA ordering) ──────────────────────────────────

    def prioritize(self, job_specs: list, current_time: float = 0.0) -> list:
        """
        Compute PASTA priority for every job and sort descending (best first).

        current_time — wall-clock reference for computing wait_time.
        In batch mode pass 0.0 (all jobs arrived simultaneously → no aging).
        In streaming mode pass time.perf_counter() so aged jobs get a boost.
        """
        enriched = self._predict_all(job_specs)

        predictions = [s["predicted_time"] for s in enriched]
        wait_times  = [max(0.0, current_time - s["submit_time"]) for s in enriched]

        min_pred = min(predictions)
        max_pred = max(predictions)
        max_wait = max(wait_times) if max(wait_times) > 0 else 1.0

        # Compute dynamic tier boundaries once for the whole queue
        low_thresh, high_thresh = compute_dynamic_tiers(predictions)

        for spec, pt, wt in zip(enriched, predictions, wait_times):
            spec["wait_time"]     = round(wt, 4)
            spec["priority"]      = round(compute_priority(pt, wt, min_pred, max_pred, max_wait), 6)
            spec["tier"]          = get_tier_dynamic(pt, low_thresh, high_thresh)
            spec["dyn_low"]       = round(low_thresh, 4)
            spec["dyn_high"]      = round(high_thresh, 4)

        enriched.sort(key=lambda s: s["priority"], reverse=True)
        return enriched, low_thresh, high_thresh

    # ── Step 3: Dispatch (same as AdaptiveScheduler) ─────────────────────────

    @staticmethod
    def _dispatch(spark, spec: dict):
        from workload.jobs.small_job  import run_word_count
        from workload.jobs.medium_job import run_join_aggregation
        from workload.jobs.large_job  import run_iterative_pagerank

        jt = spec["job_type"]
        if jt == "small":
            return run_word_count(spark, spec["input_path"], spec["num_partitions"])
        elif jt == "medium":
            return run_join_aggregation(
                spark, spec["input_path"], spec["input_path_b"], spec["num_partitions"])
        else:
            return run_iterative_pagerank(
                spark, spec["input_path"], spec["num_partitions"],
                iterations=spec.get("iterations", 10))

    # ── Step 4: Main run loop ─────────────────────────────────────────────────

    def run(self, job_specs: list, current_time: float = 0.0) -> list:
        """
        Full PASTA scheduling loop.
        Returns list of result dicts (compatible with benchmark/report.py).
        """
        from pyspark.sql import SparkSession

        prioritized, low_thresh, high_thresh = self.prioritize(job_specs, current_time)

        # Update FAIR pool weights
        weights = compute_adaptive_weights(prioritized)
        generate_fairscheduler_xml(weights)

        if self.verbose:
            print(f"\nPASTA order ({len(prioritized)} jobs):")
            print(f"  Dynamic tiers: small<{low_thresh:.2f}s  medium<{high_thresh:.2f}s  large≥{high_thresh:.2f}s")
            for i, s in enumerate(prioritized):
                print(f"  [{i+1:2d}] {s['job_type']:6s}  "
                      f"pred={s['predicted_time']:.2f}s  "
                      f"wait={s['wait_time']:.2f}s  "
                      f"priority={s['priority']:.4f}  "
                      f"tier={s['tier']}")
            print()

        conf_dir = os.path.join(PROJECT_ROOT, "conf")
        spark = (
            SparkSession.builder
            .appName("PASTA_Scheduler")
            .master("local[*]")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/tmp/spark-events")
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.scheduler.allocation.file",
                    os.path.join(conf_dir, "fairscheduler.xml"))
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        results     = []
        wall_start  = time.perf_counter()

        for i, spec in enumerate(prioritized):
            pt   = spec["predicted_time"]
            tier = spec["tier"]
            cfg  = get_config_dynamic(pt, low_thresh, high_thresh)
            pool = get_pool_name(pt)

            shuffle_parts = cfg.get("spark.sql.shuffle.partitions", "8")
            spark.conf.set("spark.sql.shuffle.partitions", shuffle_parts)
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool)

            if self.verbose:
                print(f"  [{i+1}/{len(prioritized)}] {spec['job_type']:6s}  "
                      f"pred={pt:.2f}s  tier={tier}  "
                      f"priority={spec['priority']:.4f}  "
                      f"shuffle={shuffle_parts}",
                      end="  ", flush=True)

            t_start = time.perf_counter()
            try:
                self._dispatch(spark, spec)
            except Exception as e:
                print(f"FAILED: {e}")
                continue
            t_end       = time.perf_counter()
            actual_time = round(t_end - t_start, 4)

            result = {
                "job_type":       spec["job_type"],
                "input_size_mb":  spec["input_size_mb"],
                "num_partitions": spec["num_partitions"],
                "predicted_time": pt,
                "actual_time":    actual_time,
                "wait_time":      spec["wait_time"],
                "priority":       spec["priority"],
                "tier":           tier,
                "pool":           pool,
                "config_used":    cfg,
                "start_ts":       round(t_start - wall_start, 4),
                "end_ts":         round(t_end   - wall_start, 4),
            }
            results.append(result)

            if self.verbose:
                print(f"→ actual={actual_time:.2f}s")

        spark.stop()
        return results

    # ── Step 5: Feedback loop ─────────────────────────────────────────────────

    def feedback_retrain(self, results: list, retrain: bool = True):
        """
        Append actual execution times from a completed run back into the
        training CSV, then optionally retrain the model.

        This closes the learning loop:
          Predict → Run → Record actual → Retrain → Better predictions
        """
        import datetime

        fieldnames = [
            "job_type", "input_size_mb", "num_partitions", "num_stages",
            "num_tasks", "peak_memory_mb", "cpu_seconds",
            "execution_time_sec", "hour_of_day",
        ]

        hour_now = datetime.datetime.now().hour
        new_rows = []
        for r in results:
            new_rows.append({
                "job_type":          r["job_type"],
                "input_size_mb":     r["input_size_mb"],
                "num_partitions":    r["num_partitions"],
                "num_stages":        3,     # approximate
                "num_tasks":         r["num_partitions"] * 3,
                "peak_memory_mb":    512 if r["tier"] == "small" else (1024 if r["tier"] == "medium" else 2048),
                "cpu_seconds":       round(r["actual_time"] * r["num_partitions"], 4),
                "execution_time_sec": r["actual_time"],
                "hour_of_day":       hour_now,
            })

        file_exists = os.path.exists(self.metrics_csv)
        with open(self.metrics_csv, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_rows)

        print(f"\n✓ Feedback: appended {len(new_rows)} rows → {self.metrics_csv}")

        if retrain:
            print("  Retraining model on enriched dataset…")
            env = os.environ.copy()
            env["PYTHONPATH"] = PROJECT_ROOT
            result = subprocess.run(
                [sys.executable, "-m", "model.train",
                 "--data", self.metrics_csv],
                cwd=PROJECT_ROOT,
                capture_output=True, text=True, env=env,
            )
            if result.returncode == 0:
                print("  ✓ Model retrained successfully.")
                # Print last few lines of training output
                for line in result.stdout.strip().splitlines()[-10:]:
                    print(f"    {line}")
            else:
                print(f"  ✗ Retraining failed:\n{result.stderr[-500:]}")
