#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_demo.sh — End-to-end demo for Adaptive Task Scheduling in Apache Spark
# ─────────────────────────────────────────────────────────────────────────────
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_CONF_DIR="$PROJECT_ROOT/conf"
export PYSPARK_PYTHON="$PROJECT_ROOT/venv/bin/python3"
export PYSPARK_DRIVER_PYTHON="$PROJECT_ROOT/venv/bin/python3"

source venv/bin/activate

mkdir -p /tmp/spark-events plots data/input model/artifacts

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Adaptive Task Scheduling in Apache Spark            ║"
echo "║  Using Workload Prediction (RF / XGBoost)            ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Generate workload dataset ──────────────────────────────────────
echo "═══ Step 1/4: Generating workload data (40 Spark jobs) ═══"
python -m workload.generator --n-runs 40
echo ""

# ── Step 2: Train prediction models ───────────────────────────────────────
echo "═══ Step 2/4: Training prediction models ═══"
python -m model.train
echo ""

# ── Step 3: Run benchmarks ─────────────────────────────────────────────────
echo "═══ Step 3/4: Running FIFO benchmark ═══"
python -m benchmark.runner --mode fifo
echo ""

echo "═══ Step 3/4: Running Adaptive benchmark ═══"
python -m benchmark.runner --mode adaptive
echo ""

# ── Step 4: Generate plots ─────────────────────────────────────────────────
echo "═══ Step 4/4: Generating benchmark report and plots ═══"
python -m benchmark.report
echo ""

echo "╔══════════════════════════════════════════════════════╗"
echo "║  ✓  Demo complete! Opening plots directory...        ║"
echo "╚══════════════════════════════════════════════════════╝"
open "$PROJECT_ROOT/plots/" 2>/dev/null || echo "Plots saved to: $PROJECT_ROOT/plots/"
