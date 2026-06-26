"""Thin inference wrapper: load a saved model and predict execution time."""
import os
import sys
import numpy as np
import joblib

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARTIFACTS_DIR = os.path.join(PROJECT_ROOT, "model", "artifacts")

# Must match FEATURE_COLS order in train.py
FEATURE_COLS = [
    "input_size_mb", "num_partitions", "job_type_encoded",
    "hour_of_day", "num_tasks",
]


class ExecutionTimePredictor:
    """
    Loads a trained model + encoder and predicts execution time in seconds.
    """

    def __init__(self, model_path: str = None, encoder_path: str = None):
        if model_path is None:
            model_path = os.path.join(ARTIFACTS_DIR, "rf_model.joblib")
        if encoder_path is None:
            encoder_path = os.path.join(ARTIFACTS_DIR, "encoder.joblib")

        self.model = joblib.load(model_path)
        self.encoder = joblib.load(encoder_path)

    def predict(self, job_type: str, input_size_mb: float,
                num_partitions: int, hour_of_day: int,
                num_tasks: int = 1) -> float:
        """
        Predict execution time in seconds (original scale).
        All inputs are known at job submission time.
        """
        job_type_encoded = self.encoder.transform([job_type])[0]
        features = np.array([[
            input_size_mb, num_partitions,
            job_type_encoded, hour_of_day, num_tasks,
        ]], dtype=float)
        log_pred = self.model.predict(features)[0]
        return float(np.expm1(log_pred))

    def predict_from_spec(self, job_spec: dict) -> float:
        """Convenience method: accepts a job_spec dict."""
        return self.predict(
            job_type      = job_spec["job_type"],
            input_size_mb = job_spec["input_size_mb"],
            num_partitions= job_spec["num_partitions"],
            hour_of_day   = job_spec.get("hour_of_day", 12),
            num_tasks     = job_spec.get("num_tasks", 1),
        )
