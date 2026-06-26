"""
Train Random Forest and XGBoost models to predict job execution time.

Features (all known at scheduling time, before the job runs):
  input_size_mb     — requested memory converted to MB
  num_partitions    — requested CPU converted to partition count
  job_type_encoded  — scheduling class encoded as integer
  hour_of_day       — hour the job was submitted
  num_tasks         — number of tasks in the job

Target: execution_time_sec (log1p-transformed during training)

Supports two data sources:
  - data/raw_metrics.csv         (synthetic local Spark jobs)
  - data/raw_metrics_google.csv  (Google Cluster Trace 2011, preferred)
"""
import os
import sys
import argparse
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

ARTIFACTS_DIR = os.path.join(PROJECT_ROOT, "model", "artifacts")
GOOGLE_CSV    = os.path.join(PROJECT_ROOT, "data", "raw_metrics_google.csv")
SYNTH_CSV     = os.path.join(PROJECT_ROOT, "data", "raw_metrics.csv")

# Features available before a job runs (no post-execution metrics)
FEATURE_COLS = [
    "input_size_mb", "num_partitions", "job_type_encoded",
    "hour_of_day", "num_tasks",
]
TARGET_COL = "execution_time_sec"


def load_and_prepare(csv_path: str = None, max_rows: int = 500_000):
    """
    Load metrics CSV, encode job_type, apply log1p to target.
    Auto-selects Google data if available (preferred), else synthetic.
    For large datasets (> max_rows), takes a stratified sample by job_type
    so all three classes are proportionally represented.
    Returns X (features DataFrame), y (log-transformed target), encoder.
    """
    if csv_path is None:
        csv_path = GOOGLE_CSV if os.path.exists(GOOGLE_CSV) else SYNTH_CSV

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} rows from {os.path.basename(csv_path)}")

    # Stratified sample for very large datasets (keeps class proportions)
    if max_rows and len(df) > max_rows:
        frac = max_rows / len(df)
        frames = []
        for jt in df["job_type"].unique():
            grp = df[df["job_type"] == jt]
            n_sample = max(1, int(round(len(grp) * frac)))
            frames.append(grp.sample(n=n_sample, random_state=42))
        df = pd.concat(frames, ignore_index=True)
        print(f"Stratified sample → {len(df):,} rows  "
              f"(frac={frac:.4f}, proportional per job_type)")

    # Minimum duration filter: 0.5s for Google cluster data, 0.05s for local
    min_time = 0.5 if "google" in csv_path else 0.05
    df = df[df[TARGET_COL] >= min_time].copy()
    print(f"After filtering (≥{min_time}s): {len(df):,} rows")

    # Encode job_type
    enc = LabelEncoder()
    df["job_type_encoded"] = enc.fit_transform(df["job_type"])
    joblib.dump(enc, os.path.join(ARTIFACTS_DIR, "encoder.joblib"))
    print(f"Job type classes: {dict(zip(enc.classes_, enc.transform(enc.classes_)))}")

    # Ensure num_tasks column exists (synthetic data may not have it)
    if "num_tasks" not in df.columns:
        df["num_tasks"] = 1

    X = df[FEATURE_COLS].astype(float)
    y = np.log1p(df[TARGET_COL].values)   # log-transform to reduce right skew

    print(f"Exec time: min={df[TARGET_COL].min():.2f}s  "
          f"mean={df[TARGET_COL].mean():.2f}s  "
          f"max={df[TARGET_COL].max():.2f}s")
    return X, y, enc


def train_random_forest(X_train, y_train) -> RandomForestRegressor:
    """Train a RandomForestRegressor and save to artifacts."""
    rf = RandomForestRegressor(
        n_estimators=300,
        max_depth=12,
        min_samples_leaf=3,
        random_state=42,
        n_jobs=-1,
    )
    rf.fit(X_train, y_train)
    joblib.dump(rf, os.path.join(ARTIFACTS_DIR, "rf_model.joblib"))
    print("✓ Random Forest trained and saved.")
    return rf


def train_xgboost(X_train, y_train) -> XGBRegressor:
    """Train an XGBRegressor and save to artifacts."""
    xgb = XGBRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        verbosity=0,
    )
    xgb.fit(X_train, y_train)
    joblib.dump(xgb, os.path.join(ARTIFACTS_DIR, "xgb_model.joblib"))
    print("✓ XGBoost trained and saved.")
    return xgb


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default=None,
                        help="Path to CSV (default: auto-select Google > synthetic)")
    parser.add_argument("--max-rows", type=int, default=500_000,
                        help="Stratified sample size for large datasets (default: 500000; 0=use all)")
    args = parser.parse_args()

    from model.evaluate import evaluate_model, plot_predictions, plot_feature_importance

    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    plots_dir = os.path.join(PROJECT_ROOT, "plots")
    os.makedirs(plots_dir, exist_ok=True)

    X, y, enc = load_and_prepare(args.data, max_rows=args.max_rows or None)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(f"\nTrain: {len(X_train):,}   Test: {len(X_test):,}\n")

    print("─── Random Forest ───────────────────────────────")
    rf = train_random_forest(X_train, y_train)
    rf_metrics = evaluate_model(rf, X_test, y_test, label="Random Forest")
    plot_predictions(rf, X_test, y_test, label="rf", plots_dir=plots_dir)
    plot_feature_importance(rf, FEATURE_COLS, label="rf", plots_dir=plots_dir)

    print("\n─── XGBoost ─────────────────────────────────────")
    xgb = train_xgboost(X_train, y_train)
    xgb_metrics = evaluate_model(xgb, X_test, y_test, label="XGBoost")
    plot_predictions(xgb, X_test, y_test, label="xgb", plots_dir=plots_dir)
    plot_feature_importance(xgb, FEATURE_COLS, label="xgb", plots_dir=plots_dir)

    print(f"\n{'='*52}")
    print(f"{'Metric':<20} {'Random Forest':>15} {'XGBoost':>15}")
    print("-" * 52)
    for k in ["MAE (s)", "RMSE (s)", "R²", "CV MAE (s)"]:
        print(f"{k:<20} {rf_metrics[k]:>15.2f} {xgb_metrics[k]:>15.2f}")
    print("=" * 52)

    best = "rf" if rf_metrics["R²"] >= xgb_metrics["R²"] else "xgb"
    print(f"\n✓ Best model by R²: {best.upper()} — saved to model/artifacts/")


if __name__ == "__main__":
    main()
