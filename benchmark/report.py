"""
Generate all benchmark visualizations — 3-way comparison: FIFO vs Adaptive (SJF) vs PASTA.
"""
import os
import sys
import json
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR     = os.path.join(PROJECT_ROOT, "data")
PLOTS_DIR    = os.path.join(PROJECT_ROOT, "plots")

JOB_COLORS  = {"small": "#4CAF50", "medium": "#FF9800", "large": "#F44336"}
TIER_COLORS = {"small": "#2196F3", "medium": "#FF9800", "large": "#9C27B0"}
MODE_COLORS = {"FIFO": "#5C85D6", "Adaptive (SJF)": "#E05C5C", "PASTA": "#43A047"}


# ─── Gantt Chart ─────────────────────────────────────────────────────────────

def plot_gantt(results: list, title: str, out_path: str):
    fig, ax = plt.subplots(figsize=(12, max(5, len(results) * 0.5)))
    for i, r in enumerate(results):
        color    = JOB_COLORS.get(r["job_type"], "gray")
        start    = r.get("start_ts", 0)
        duration = r["actual_time"]
        ax.barh(i, duration, left=start, color=color,
                edgecolor="white", linewidth=0.5, height=0.6)
        ax.text(start + duration / 2, i, f"{duration:.2f}s",
                ha="center", va="center", fontsize=7, color="white", fontweight="bold")

    ax.set_yticks(range(len(results)))
    ax.set_yticklabels(
        [f"Job {r.get('job_index',i)+1} ({r['job_type']})" for i, r in enumerate(results)],
        fontsize=9)
    ax.set_xlabel("Wall-clock time (s)", fontsize=11)
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    legend_patches = [mpatches.Patch(color=c, label=jt.capitalize())
                      for jt, c in JOB_COLORS.items()]
    ax.legend(handles=legend_patches, loc="lower right", fontsize=9)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path}")


# ─── 3-Way Speedup Bar Chart ─────────────────────────────────────────────────

def plot_3way_comparison(fifo_results, adap_results, pasta_results, out_path: str):
    """Side-by-side per-job times for all three schedulers."""
    n = min(len(fifo_results), len(adap_results), len(pasta_results))
    x = np.arange(n)
    w = 0.25

    fifo_t  = [r["actual_time"] for r in fifo_results[:n]]
    adap_t  = [r["actual_time"] for r in adap_results[:n]]
    pasta_t = [r["actual_time"] for r in pasta_results[:n]]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10),
                                    gridspec_kw={"height_ratios": [3, 1]})

    ax1.bar(x - w, fifo_t,  w, label="FIFO",          color=MODE_COLORS["FIFO"],          edgecolor="white")
    ax1.bar(x,     adap_t,  w, label="Adaptive (SJF)", color=MODE_COLORS["Adaptive (SJF)"],edgecolor="white")
    ax1.bar(x + w, pasta_t, w, label="PASTA",          color=MODE_COLORS["PASTA"],         edgecolor="white")

    ax1.set_xticks(x)
    ax1.set_xticklabels(
        [f"J{i+1}\n({adap_results[i]['job_type']})" for i in range(n)], fontsize=8)
    ax1.set_ylabel("Execution Time (s)", fontsize=11)
    ax1.set_title("Per-Job Execution Time: FIFO vs Adaptive (SJF) vs PASTA", fontsize=13, fontweight="bold")
    ax1.legend(fontsize=10)
    ax1.grid(axis="y", alpha=0.3)

    # Bottom: speedup of PASTA vs FIFO per job
    sp_adap  = [f / a if a > 0 else 1.0 for f, a in zip(fifo_t, adap_t)]
    sp_pasta = [f / p if p > 0 else 1.0 for f, p in zip(fifo_t, pasta_t)]

    ax2.plot(x, sp_adap,  "o--", color=MODE_COLORS["Adaptive (SJF)"],
             label=f"Adaptive mean={np.mean(sp_adap):.2f}×", linewidth=1.5, markersize=5)
    ax2.plot(x, sp_pasta, "s-",  color=MODE_COLORS["PASTA"],
             label=f"PASTA mean={np.mean(sp_pasta):.2f}×",   linewidth=1.5, markersize=5)
    ax2.axhline(1.0, color="black", linewidth=1, linestyle="--", alpha=0.4)
    ax2.fill_between(x, 1, sp_pasta,
                     where=[p >= 1 for p in sp_pasta],
                     alpha=0.15, color=MODE_COLORS["PASTA"])
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"J{i+1}" for i in range(n)], fontsize=8)
    ax2.set_ylabel("Speedup vs FIFO (×)", fontsize=10)
    ax2.legend(fontsize=9)
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path}")


# ─── Makespan + Latency Summary Bar ──────────────────────────────────────────

def plot_summary_bars(fifo_data, adap_data, pasta_data, out_path: str):
    """Summary bar chart: makespan and avg latency for all three modes."""
    modes     = ["FIFO", "Adaptive (SJF)", "PASTA"]
    colors    = [MODE_COLORS[m] for m in modes]

    makespans = [
        fifo_data["makespan"],
        adap_data["makespan"],
        pasta_data["makespan"],
    ]
    latencies = [
        np.mean([r["actual_time"] for r in fifo_data["results"]]),
        np.mean([r["actual_time"] for r in adap_data["results"]]),
        np.mean([r["actual_time"] for r in pasta_data["results"]]),
    ]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    bars1 = ax1.bar(modes, makespans, color=colors, edgecolor="white", width=0.5)
    ax1.set_ylabel("Total Makespan (s)", fontsize=11)
    ax1.set_title("Total Makespan Comparison", fontsize=12, fontweight="bold")
    ax1.grid(axis="y", alpha=0.3)
    for bar, val in zip(bars1, makespans):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                 f"{val:.2f}s", ha="center", va="bottom", fontsize=10, fontweight="bold")

    bars2 = ax2.bar(modes, latencies, color=colors, edgecolor="white", width=0.5)
    ax2.set_ylabel("Average Job Latency (s)", fontsize=11)
    ax2.set_title("Average Job Latency Comparison", fontsize=12, fontweight="bold")
    ax2.grid(axis="y", alpha=0.3)
    for bar, val in zip(bars2, latencies):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
                 f"{val:.3f}s", ha="center", va="bottom", fontsize=10, fontweight="bold")

    plt.suptitle("FIFO  vs  Adaptive (SJF)  vs  PASTA", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path}")


# ─── Priority vs Execution Time Scatter (PASTA) ───────────────────────────────

def plot_pasta_priority(pasta_results: list, out_path: str):
    """Show how PASTA priority correlates with predicted time for each job."""
    if not any("priority" in r for r in pasta_results):
        return

    preds    = [r.get("predicted_time", 0) for r in pasta_results]
    actuals  = [r["actual_time"] for r in pasta_results]
    prios    = [r.get("priority", 0)       for r in pasta_results]
    types    = [r["job_type"]              for r in pasta_results]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Left: priority score per job (bar)
    bar_colors = [JOB_COLORS.get(t, "gray") for t in types]
    x = range(len(pasta_results))
    ax1.bar(x, prios, color=bar_colors, edgecolor="white")
    ax1.set_xticks(x)
    ax1.set_xticklabels(
        [f"J{r.get('job_index',i)+1}\n({r['job_type']})" for i, r in enumerate(pasta_results)],
        fontsize=8)
    ax1.set_ylabel("PASTA Priority Score", fontsize=11)
    ax1.set_title("Priority Score per Job (higher → runs sooner)", fontsize=12, fontweight="bold")
    ax1.grid(axis="y", alpha=0.3)
    legend_patches = [mpatches.Patch(color=c, label=jt.capitalize())
                      for jt, c in JOB_COLORS.items()]
    ax1.legend(handles=legend_patches, fontsize=9)

    # Right: predicted vs actual
    for jt in ["small", "medium", "large"]:
        px = [p for p, t in zip(preds, types) if t == jt]
        ay = [a for a, t in zip(actuals, types) if t == jt]
        ax2.scatter(px, ay, label=jt.capitalize(), color=JOB_COLORS[jt],
                    s=80, edgecolors="k", linewidths=0.4, alpha=0.8)
    lim = max(max(preds), max(actuals)) * 1.1
    ax2.plot([0, lim], [0, lim], "r--", linewidth=1.5, label="Perfect")
    ax2.set_xlabel("Predicted Execution Time (s)", fontsize=11)
    ax2.set_ylabel("Actual Execution Time (s)",    fontsize=11)
    ax2.set_title("Predicted vs Actual — PASTA", fontsize=12, fontweight="bold")
    ax2.legend(fontsize=9)
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path}")


# ─── Starvation Analysis ──────────────────────────────────────────────────────

def plot_starvation_analysis(fifo_results, adap_results, pasta_results, out_path: str):
    """
    Show wait time per job for each scheduler — demonstrates that PASTA
    gives large jobs earlier service than pure SJF.
    """
    fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharey=True)

    datasets = [
        (fifo_results,  "FIFO",          MODE_COLORS["FIFO"]),
        (adap_results,  "Adaptive (SJF)", MODE_COLORS["Adaptive (SJF)"]),
        (pasta_results, "PASTA",          MODE_COLORS["PASTA"]),
    ]

    for ax, (results, label, color) in zip(axes, datasets):
        job_types  = [r["job_type"] for r in results]
        start_times = [r.get("start_ts", 0) for r in results]
        bar_colors  = [JOB_COLORS.get(t, "gray") for t in job_types]

        x = range(len(results))
        ax.bar(x, start_times, color=bar_colors, edgecolor="white")
        ax.set_xticks(x)
        ax.set_xticklabels(
            [f"J{r.get('job_index',i)+1}\n({r['job_type'][:3]})" for i, r in enumerate(results)],
            fontsize=7)
        ax.set_title(label, fontsize=12, fontweight="bold", color=color)
        ax.set_xlabel("Job (in execution order)", fontsize=9)
        ax.grid(axis="y", alpha=0.3)

        # Annotate avg wait for large jobs
        large_waits = [s for s, t in zip(start_times, job_types) if t == "large"]
        if large_waits:
            ax.axhline(np.mean(large_waits), color="#F44336", linestyle="--",
                       linewidth=1.5, alpha=0.8,
                       label=f"Large avg wait={np.mean(large_waits):.2f}s")
            ax.legend(fontsize=8)

    axes[0].set_ylabel("Job Start Time (s) — lower = less wait", fontsize=10)
    legend_patches = [mpatches.Patch(color=c, label=jt.capitalize())
                      for jt, c in JOB_COLORS.items()]
    fig.legend(handles=legend_patches, loc="lower center", ncol=3,
               fontsize=10, bbox_to_anchor=(0.5, -0.05))
    plt.suptitle("Job Start Times — Starvation Analysis\n"
                 "(Large jobs wait longest under SJF; PASTA aging reduces their wait)",
                 fontsize=12, fontweight="bold")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path}")


# ─── Resource Utilization ─────────────────────────────────────────────────────

def plot_resource_utilization(adaptive_results: list, out_path: str):
    memory_map = {"512m": 0.5, "1g": 1.0, "2g": 2.0}
    labels   = [f"J{r.get('job_index',i)+1}\n({r['job_type']})" for i, r in enumerate(adaptive_results)]
    memories = [memory_map.get(r.get("config_used", {}).get("spark.executor.memory", "1g"), 1.0)
                for r in adaptive_results]
    tiers    = [r.get("tier", "medium") for r in adaptive_results]
    colors   = [TIER_COLORS.get(t, "gray") for t in tiers]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.bar(range(len(labels)), memories, color=colors, edgecolor="white")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Assigned Executor Memory (GB)", fontsize=11)
    ax.set_title("Resource Allocation — Adaptive Scheduler", fontsize=13, fontweight="bold")
    ax.set_yticks([0.5, 1.0, 2.0])
    ax.set_yticklabels(["512 MB", "1 GB", "2 GB"])
    ax.grid(axis="y", alpha=0.3)
    legend_patches = [mpatches.Patch(color=c, label=t.capitalize())
                      for t, c in TIER_COLORS.items()]
    ax.legend(handles=legend_patches, title="Tier", fontsize=9)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path}")


# ─── Prediction scatter (Adaptive) ───────────────────────────────────────────

def plot_prediction_scatter(adaptive_results: list, out_path: str):
    predicted = [r["predicted_time"] for r in adaptive_results]
    actual    = [r["actual_time"]    for r in adaptive_results]
    types     = [r["job_type"]       for r in adaptive_results]

    fig, ax = plt.subplots(figsize=(7, 6))
    for jt in ["small", "medium", "large"]:
        px = [p for p, t in zip(predicted, types) if t == jt]
        ay = [a for a, t in zip(actual,    types) if t == jt]
        ax.scatter(px, ay, label=jt.capitalize(), color=JOB_COLORS[jt],
                   s=80, edgecolors="k", linewidths=0.4, alpha=0.8)
    lim = max(max(predicted), max(actual)) * 1.1
    ax.plot([0, lim], [0, lim], "r--", linewidth=1.5, label="Perfect prediction")
    ax.set_xlabel("Predicted Execution Time (s)", fontsize=12)
    ax.set_ylabel("Actual Execution Time (s)",    fontsize=12)
    ax.set_title("Prediction Accuracy — Adaptive Scheduler", fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"  Saved: {out_path}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(PLOTS_DIR, exist_ok=True)

    fifo_path  = os.path.join(DATA_DIR, "benchmark_fifo.json")
    adap_path  = os.path.join(DATA_DIR, "benchmark_adaptive.json")
    pasta_path = os.path.join(DATA_DIR, "benchmark_pasta.json")

    missing = [p for p in [fifo_path, adap_path] if not os.path.exists(p)]
    if missing:
        print("ERROR: Run benchmark/runner.py first to generate benchmark data.")
        sys.exit(1)

    with open(fifo_path)  as f: fifo_data  = json.load(f)
    with open(adap_path)  as f: adap_data  = json.load(f)

    has_pasta = os.path.exists(pasta_path)
    if has_pasta:
        with open(pasta_path) as f: pasta_data = json.load(f)

    fifo_results = fifo_data["results"]
    adap_results = adap_data["results"]

    print("Generating plots...\n")

    # Gantt charts
    plot_gantt(fifo_results, "FIFO Scheduling — Job Timeline",
               os.path.join(PLOTS_DIR, "gantt_fifo.png"))
    plot_gantt(adap_results, "Adaptive (SJF) Scheduling — Job Timeline",
               os.path.join(PLOTS_DIR, "gantt_adaptive.png"))
    if has_pasta:
        plot_gantt(pasta_data["results"], "PASTA Scheduling — Job Timeline",
                   os.path.join(PLOTS_DIR, "gantt_pasta.png"))

    # 3-way comparison (or 2-way if no PASTA data)
    if has_pasta:
        plot_3way_comparison(fifo_results, adap_results, pasta_data["results"],
                             os.path.join(PLOTS_DIR, "speedup_comparison.png"))
        plot_summary_bars(fifo_data, adap_data, pasta_data,
                          os.path.join(PLOTS_DIR, "summary_bars.png"))
        plot_pasta_priority(pasta_data["results"],
                            os.path.join(PLOTS_DIR, "pasta_priority.png"))
        plot_starvation_analysis(fifo_results, adap_results, pasta_data["results"],
                                 os.path.join(PLOTS_DIR, "starvation_analysis.png"))
    else:
        # Fallback: 2-way speedup (original behaviour)
        n = min(len(fifo_results), len(adap_results))
        x = np.arange(n)
        w = 0.35
        fifo_t = [r["actual_time"] for r in fifo_results[:n]]
        adap_t = [r["actual_time"] for r in adap_results[:n]]
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.bar(x - w/2, fifo_t, w, label="FIFO",     color=MODE_COLORS["FIFO"],         edgecolor="white")
        ax.bar(x + w/2, adap_t, w, label="Adaptive",  color=MODE_COLORS["Adaptive (SJF)"],edgecolor="white")
        ax.set_xticks(x)
        ax.set_xticklabels([f"J{i+1}\n({adap_results[i]['job_type']})" for i in range(n)], fontsize=8)
        ax.set_ylabel("Execution Time (s)", fontsize=11)
        ax.set_title("Per-Job Execution Time: FIFO vs Adaptive", fontsize=13, fontweight="bold")
        ax.legend(fontsize=10)
        ax.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, "speedup_comparison.png"), dpi=150)
        plt.close()
        print(f"  Saved: {os.path.join(PLOTS_DIR, 'speedup_comparison.png')}")

    plot_prediction_scatter(adap_results,
                            os.path.join(PLOTS_DIR, "prediction_scatter.png"))
    plot_resource_utilization(adap_results,
                              os.path.join(PLOTS_DIR, "resource_utilization.png"))

    # ── Summary table ──────────────────────────────────────────────────────
    fifo_ms  = fifo_data["makespan"]
    adap_ms  = adap_data["makespan"]
    avg_fifo = np.mean([r["actual_time"] for r in fifo_results])
    avg_adap = np.mean([r["actual_time"] for r in adap_results])
    sp_adap  = fifo_ms / adap_ms if adap_ms > 0 else 0

    if has_pasta:
        pasta_ms  = pasta_data["makespan"]
        avg_pasta = np.mean([r["actual_time"] for r in pasta_data["results"]])
        sp_pasta  = fifo_ms / pasta_ms if pasta_ms > 0 else 0

        print(f"\n{'='*64}")
        print(f"{'BENCHMARK SUMMARY':^64}")
        print(f"{'='*64}")
        print(f"{'Metric':<30} {'FIFO':>8}  {'Adaptive':>10}  {'PASTA':>8}")
        print("-" * 64)
        print(f"{'Total makespan (s)':<30} {fifo_ms:>8.2f}  {adap_ms:>10.2f}  {pasta_ms:>8.2f}")
        print(f"{'Avg job latency (s)':<30} {avg_fifo:>8.3f}  {avg_adap:>10.3f}  {avg_pasta:>8.3f}")
        print(f"{'Speedup vs FIFO':<30} {'1.00×':>8}  {sp_adap:>9.2f}×  {sp_pasta:>7.2f}×")
        print(f"{'Jobs completed':<30} {len(fifo_results):>8}  {len(adap_results):>10}  {len(pasta_data['results']):>8}")
        print("=" * 64)
    else:
        print(f"\n{'='*54}")
        print(f"{'BENCHMARK SUMMARY':^54}")
        print(f"{'='*54}")
        print(f"{'Metric':<32} {'FIFO':>8}  {'Adaptive':>10}")
        print("-" * 54)
        print(f"{'Total makespan (s)':<32} {fifo_ms:>8.2f}  {adap_ms:>10.2f}")
        print(f"{'Avg job latency (s)':<32} {avg_fifo:>8.3f}  {avg_adap:>10.3f}")
        print(f"{'Speedup':<32} {'1.00×':>8}  {sp_adap:>9.2f}×")
        print(f"{'Jobs completed':<32} {len(fifo_results):>8}  {len(adap_results):>10}")
        print("=" * 54)

    print(f"\n✓ All plots saved to: {PLOTS_DIR}/")


if __name__ == "__main__":
    main()
