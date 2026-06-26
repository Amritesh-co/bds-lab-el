"""Collects Spark job execution metrics and appends to CSV."""
import os
import time
import psutil
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession


METRICS_COLUMNS = [
    "job_type", "input_size_mb", "num_partitions", "num_stages",
    "num_tasks", "peak_memory_mb", "cpu_seconds", "execution_time_sec", "hour_of_day"
]


class MetricsCollector:
    def __init__(self, spark: SparkSession, csv_path: str):
        self.spark = spark
        self.csv_path = csv_path
        self._process = psutil.Process(os.getpid())

    def _get_job_stats(self) -> dict:
        """Read stage and task counts from Spark's status tracker."""
        tracker = self.spark.sparkContext.statusTracker()
        job_ids = tracker.getJobIdsForGroup(None)

        total_stages = 0
        total_tasks = 0

        for job_id in job_ids:
            job_info = tracker.getJobInfo(job_id)
            if job_info is not None:
                try:
                    stage_ids = job_info.stageIds()
                except TypeError:
                    stage_ids = job_info.stageIds

                total_stages += len(stage_ids)
                for stage_id in stage_ids:
                    stage_info = tracker.getStageInfo(stage_id)
                    if stage_info is not None:
                        try:
                            tasks = stage_info.numTasks()
                        except TypeError:
                            tasks = stage_info.numTasks
                        total_tasks += tasks

        return {"num_stages": max(total_stages, 1), "num_tasks": max(total_tasks, 1)}

    def collect(self, job_spec: dict, start_time: float, end_time: float,
                cpu_start: float, cpu_end: float) -> dict:
        """Build a metrics row from a completed job."""
        stats = self._get_job_stats()
        peak_memory_mb = round(self._process.memory_info().rss / (1024 ** 2), 2)
        execution_time_sec = round(end_time - start_time, 4)
        cpu_seconds = round(cpu_end - cpu_start, 4)

        return {
            "job_type": job_spec["job_type"],
            "input_size_mb": job_spec["input_size_mb"],
            "num_partitions": job_spec["num_partitions"],
            "num_stages": stats["num_stages"],
            "num_tasks": stats["num_tasks"],
            "peak_memory_mb": peak_memory_mb,
            "cpu_seconds": cpu_seconds,
            "execution_time_sec": execution_time_sec,
            "hour_of_day": datetime.now().hour,
        }

    def append_to_csv(self, row: dict):
        """Append a single metrics row to the CSV file (creates it if missing)."""
        df_new = pd.DataFrame([row], columns=METRICS_COLUMNS)

        if os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0:
            df_existing = pd.read_csv(self.csv_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new

        df_combined.to_csv(self.csv_path, index=False)
