"""
Adaptive Scheduler: predicts job execution time, prioritizes via SJF,
assigns resource configs, and runs each job on a freshly configured SparkSession.
"""
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from scheduler.resource_policy import predict_config, get_tier_label
from scheduler.fair_pool_config import get_pool_name, generate_fairscheduler_xml, compute_adaptive_weights
from model.predict import ExecutionTimePredictor


class AdaptiveScheduler:
    def __init__(
        self,
        model_path: str = None,
        encoder_path: str = None,
        metrics_csv: str = None,
        use_hdfs: bool = False,
        verbose: bool = True,
    ):
        self.predictor = ExecutionTimePredictor(model_path, encoder_path)
        self.metrics_csv = metrics_csv or os.path.join(PROJECT_ROOT, "data", "raw_metrics.csv")
        self.use_hdfs = use_hdfs
        self.verbose = verbose

    # ── Prediction ────────────────────────────────────────────────────────────

    def predict_execution_time(self, job_spec: dict) -> float:
        """Predict execution time in seconds for a single job spec."""
        return self.predictor.predict_from_spec(job_spec)

    # ── Prioritization (Shortest Job First) ───────────────────────────────────

    def prioritize(self, job_specs: list) -> list:
        """
        Add 'predicted_time' to each spec, sort ascending (SJF).
        Returns a new list — original list is not mutated.
        """
        enriched = []
        for spec in job_specs:
            pt = self.predict_execution_time(spec)
            enriched.append({**spec, "predicted_time": round(pt, 4)})
        enriched.sort(key=lambda s: s["predicted_time"])
        return enriched

    # ── SparkSession Factory ──────────────────────────────────────────────────

    def _stop_active_session(self):
        """Stop any currently active SparkSession."""
        from pyspark.sql import SparkSession
        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()

    def _build_spark_session(self, spark_configs: dict, pool_name: str):
        """
        Create a new SparkSession with the given resource configs.
        Stops the previous session first (required to change executor memory in local mode).
        """
        from pyspark.sql import SparkSession

        self._stop_active_session()

        conf_dir = os.path.join(PROJECT_ROOT, "conf")
        builder = (
            SparkSession.builder
            .appName("AdaptiveScheduler")
            .master("local[*]")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/tmp/spark-events")
            .config("spark.scheduler.mode", "FAIR")
            .config(
                "spark.scheduler.allocation.file",
                os.path.join(conf_dir, "fairscheduler.xml"),
            )
            .config("spark.ui.showConsoleProgress", "false")
        )

        for k, v in spark_configs.items():
            builder = builder.config(k, v)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        # Set scheduler pool for this job
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)
        return spark

    # ── Job Dispatch ─────────────────────────────────────────────────────────

    @staticmethod
    def _dispatch(spark, spec: dict):
        """Call the correct job function based on job_type."""
        from workload.jobs.small_job import run_word_count
        from workload.jobs.medium_job import run_join_aggregation
        from workload.jobs.large_job import run_iterative_pagerank

        jt = spec["job_type"]
        if jt == "small":
            return run_word_count(spark, spec["input_path"], spec["num_partitions"])
        elif jt == "medium":
            return run_join_aggregation(
                spark, spec["input_path"], spec["input_path_b"], spec["num_partitions"]
            )
        else:
            return run_iterative_pagerank(
                spark, spec["input_path"], spec["num_partitions"],
                iterations=spec.get("iterations", 10),
            )

    # ── Main Run Loop ─────────────────────────────────────────────────────────

    def run(self, job_specs: list, collector=None) -> list:
        """
        Full adaptive scheduling loop:
          1. Predict and prioritize (SJF)
          2. Update fairscheduler.xml with adaptive weights
          3. For each job: apply runtime-tunable configs, dispatch, collect metrics
          NOTE: We reuse a single SparkSession and apply per-job configs via
          spark.conf.set() for settings that support it (shuffle.partitions,
          scheduler.pool). executor.memory / executor.cores cannot be changed
          at runtime — the tier label is recorded for analysis even though the
          JVM-level memory is fixed at session creation time.
        Returns list of result dicts.
        """
        prioritized = self.prioritize(job_specs)

        # Update pool weights based on tier distribution
        weights = compute_adaptive_weights(prioritized)
        generate_fairscheduler_xml(weights)

        if self.verbose:
            print(f"\nAdaptive order ({len(prioritized)} jobs, SJF):")
            for i, s in enumerate(prioritized):
                print(f"  [{i+1}] {s['job_type']:6s}  "
                      f"predicted={s['predicted_time']:.2f}s  "
                      f"tier={get_tier_label(s['predicted_time'])}")
            print()

        # Build ONE shared SparkSession — avoids ~2s restart overhead per job.
        # Per-job tunable settings (shuffle partitions, pool) are applied inline.
        conf_dir = os.path.join(PROJECT_ROOT, "conf")
        spark = self._build_spark_session({}, "small_pool")

        results = []
        wall_start = time.perf_counter()

        for i, spec in enumerate(prioritized):
            pt = spec["predicted_time"]
            cfg = predict_config(pt)
            pool = get_pool_name(pt)
            tier = get_tier_label(pt)

            # Apply runtime-tunable configs
            shuffle_parts = cfg.get("spark.sql.shuffle.partitions", "8")
            spark.conf.set("spark.sql.shuffle.partitions", shuffle_parts)
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool)

            if self.verbose:
                print(f"  [{i+1}/{len(prioritized)}] {spec['job_type']:6s}  "
                      f"pred={pt:.2f}s  tier={tier}  shuffle={shuffle_parts}  pool={pool}",
                      end="  ", flush=True)

            cpu_start = time.process_time()
            t_start = time.perf_counter()

            try:
                self._dispatch(spark, spec)
            except Exception as e:
                print(f"FAILED: {e}")
                continue

            t_end = time.perf_counter()
            cpu_end = time.process_time()
            actual_time = round(t_end - t_start, 4)

            result = {
                "job_type": spec["job_type"],
                "input_size_mb": spec["input_size_mb"],
                "num_partitions": spec["num_partitions"],
                "predicted_time": pt,
                "actual_time": actual_time,
                "tier": tier,
                "pool": pool,
                "config_used": cfg,
                "start_ts": round(t_start - wall_start, 4),
                "end_ts": round(t_end - wall_start, 4),
            }
            results.append(result)

            if self.verbose:
                print(f"→ actual={actual_time:.2f}s")

            if collector is not None:
                if collector.spark is not spark:
                    collector.spark = spark
                row = collector.collect(spec, t_start, t_end, cpu_start, cpu_end)
                collector.append_to_csv(row)

        spark.stop()
        return results
