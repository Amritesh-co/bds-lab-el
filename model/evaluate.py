"""Model evaluation: metrics and plots."""
import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")  # non-interactive backend
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import cross_val_score


def evaluate_model(model, X_test, y_test_log, label: str = "") -> dict:
    """
    Evaluate a trained model. y_test_log is the log-transformed target.
    Reports MAE, RMSE, R² in original seconds (after np.expm1).
    """
    y_pred_log = model.predict(X_test)

    # Convert back to original scale
    y_pred = np.expm1(y_pred_log)
    y_test = np.expm1(y_test_log)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    # 5-fold CV on log-transformed data
    cv_scores = cross_val_score(
        model, X_test, y_test_log,
        cv=min(5, len(X_test)),
        scoring="neg_mean_absolute_error",
        n_jobs=-1,
    )
    cv_mae = float(np.expm1(-cv_scores.mean()))  # back to seconds

    metrics = {
        "MAE (s)": round(mae, 4),
        "RMSE (s)": round(rmse, 4),
        "R²": round(r2, 4),
        "CV MAE (s)": round(cv_mae, 4),
    }

    print(f"  {label}")
    for k, v in metrics.items():
        print(f"    {k:<12}: {v:.4f}")

    return metrics


def plot_predictions(model, X_test, y_test_log, label: str, plots_dir: str):
    """Scatter plot: predicted vs actual execution time."""
    y_pred = np.expm1(model.predict(X_test))
    y_true = np.expm1(y_test_log)

    fig, ax = plt.subplots(figsize=(7, 6))
    ax.scatter(y_true, y_pred, alpha=0.6, edgecolors="k", linewidths=0.4,
               color="steelblue", s=60)

    # y = x reference line
    lim_max = max(y_true.max(), y_pred.max()) * 1.05
    ax.plot([0, lim_max], [0, lim_max], "r--", linewidth=1.5, label="Perfect prediction")

    ax.set_xlabel("Actual Execution Time (s)", fontsize=12)
    ax.set_ylabel("Predicted Execution Time (s)", fontsize=12)
    ax.set_title(f"Predicted vs Actual — {label.upper()}", fontsize=13)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()

    path = os.path.join(plots_dir, f"prediction_vs_actual_{label}.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  Saved: {path}")


def plot_feature_importance(model, feature_names: list, label: str, plots_dir: str):
    """Horizontal bar chart of feature importances."""
    importances = model.feature_importances_
    sorted_idx = np.argsort(importances)
    colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(feature_names)))

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.barh(
        [feature_names[i] for i in sorted_idx],
        importances[sorted_idx],
        color=colors,
        edgecolor="white",
    )
    ax.set_xlabel("Importance", fontsize=12)
    ax.set_title(f"Feature Importance — {label.upper()}", fontsize=13)
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()

    path = os.path.join(plots_dir, f"feature_importance_{label}.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  Saved: {path}")
