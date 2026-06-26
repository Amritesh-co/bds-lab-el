# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A college Big Data Analytics Lab project: **PASTA** (Predictive Adaptive Scheduling with Timed Aging), an ML-driven alternative to Apache Spark's FIFO scheduler. It predicts job duration (Random Forest + XGBoost), sorts jobs shortest-first with an aging term to prevent starvation, and assigns each job a resource tier (executor memory/cores/partitions) before running it. A React dashboard visualizes FIFO vs SJF/Adaptive vs PASTA benchmark results, either from static seed data or from a live Spark run triggered through a Flask API.

Full narrative documentation lives in `DOCUMENTATION/` (start with `COMPLETE_PROJECT_EXPLANATION.md`); `README.md` has the same content condensed.

## Environment

- Python 3.11 venv at `./venv` — always use `./venv/bin/python`, not system Python.
- Java 17 required (`JAVA_HOME=/opt/homebrew/opt/openjdk@17`); PySpark 3.5.1 runs `local[*]`.
- **Critical gotcha**: PySpark workers can launch a different system Python than the driver venv, causing `PYTHON_VERSION_MISMATCH` (only some jobs survive silently). Always set `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` env vars to the venv interpreter when spawning Spark subprocesses (see `server.py`).
- Spark event logs go to `/tmp/spark-events/`.

## Common commands

```bash
source venv/bin/activate

# Full pipeline (generate data → train → benchmark → plots), ~15-30 min
bash run_demo.sh

# Individual phases
python -m workload.generator --n-runs 40        # generate synthetic job metrics
python -m model.train                            # train RF + XGBoost on data/raw_metrics.csv
python -m benchmark.runner --mode all            # FIFO + Adaptive(SJF) + PASTA, fixed 15-job seeded queue
python -m benchmark.runner --mode fifo|adaptive|pasta
python -m benchmark.runner --mode pasta --feedback  # PASTA + retrain model after run
python -m benchmark.report                       # generate plots/ from the 3 benchmark JSONs

# Live dashboard (two processes)
./venv/bin/python server.py        # Flask API on :5175
cd dashboard-react && npm run dev  # Vite dev server on :5174 (click "Run Benchmark" in navbar)

# Dashboard-only commands (run from dashboard-react/)
npm run build
npm run lint
```

There is no automated test suite in this repo (no pytest config, no JS test runner) — verification is done by running the pipeline/dashboard and checking output.

## Architecture

**Pipeline data flow**: `workload/generator.py` runs synthetic Spark jobs (small=RDD word count, medium=DataFrame join+agg, large=iterative PageRank, in `workload/jobs/`) and appends metrics to `data/raw_metrics.csv` → `model/train.py` trains RF+XGBoost (ensemble averaged) on that CSV, saving artifacts to `model/artifacts/*.joblib` → `model/predict.py`'s `ExecutionTimePredictor` loads those artifacts and predicts duration for new job specs → `scheduler/` consumes predictions to decide order and resources → `benchmark/runner.py` orchestrates FIFO vs Adaptive vs PASTA runs on the *same fixed seeded 15-job queue* (`build_job_queue`, `random.Random(42)`) and writes `data/benchmark_{fifo,adaptive,pasta}.json` → `benchmark/report.py` reads those JSONs and produces `plots/`.

**Scheduler layer** (`scheduler/`):
- `adaptive_scheduler.py` — `AdaptiveScheduler`: predicts time per job, sorts ascending (SJF), and for each job stops the active SparkSession and builds a new one with tier-specific config (`spark.executor.memory` is a JVM startup param — it cannot change on a running session, hence stop/rebuild per job).
- `pasta_scheduler.py` — adds two things on top of plain SJF: (1) an aging term so `priority = 0.70 × norm_sjf + 0.30 × norm_aging`, preventing long jobs from starving when short jobs keep arriving; (2) dynamic tier boundaries from the 33rd/66th percentile of current predictions instead of fixed 10s/60s cutoffs. Also supports a feedback loop that appends actual execution times back into `raw_metrics.csv` and retriggers `model/train.py`.
- `resource_policy.py` — maps predicted time → tier (`small`/`medium`/`large`) → Spark config (memory/cores/shuffle partitions).
- `fair_pool_config.py` — generates `conf/fairscheduler.xml` dynamically with pool weights proportional to how many jobs fall in each tier.

**Dashboard** (`dashboard-react/`, Vite + React 19 + Chakra/MUI + Recharts + Tailwind):
- `src/data/BenchmarkContext.jsx` — `BenchmarkProvider`/`useBenchmark()`. Seeds from static `src/data/benchmarkData.js` (works with the API server off), pulls `/api/benchmark/latest` on mount, and `runBenchmark()` POSTs to `/api/benchmark/run` then polls `/status` every 1.5s until done, then fetches `/results`. All pages derive makespan/latency/speedup from this context rather than hardcoding values — don't reintroduce hardcoded benchmark numbers in components.
- Pages (`src/pages/`): `Overview.jsx`, `Simulation.jsx` (Gantt-style scheduler visualization), `PastaAlgorithm.jsx`, `HowSparkWorks.jsx`.
- `src/components/AlgorithmShowdown.jsx` — animated FIFO/SJF/Adaptive/PASTA race comparison.
- In `Simulation.jsx`, `ALGO_BASE` is ordered `[FIFO, Adaptive(SJF), SJF+Aging, PASTA]` — index 3 is PASTA; index-based lookups elsewhere in that file must match this order.
- `server.py` (project root, Flask) bridges the dashboard to the real pipeline: `POST /api/benchmark/run` spawns `python -m benchmark.runner --mode all` as a subprocess (with the venv-pinned `PYSPARK_PYTHON`), streams stdout to infer stage/progress via `STAGE_MARKERS` substring matching, and `build_payload()` normalizes the three `data/benchmark_*.json` files into the shape the dashboard expects (note: the dashboard's "SJF" series is just an alias for the "adaptive" results — there is no separate SJF-only JSON file).
