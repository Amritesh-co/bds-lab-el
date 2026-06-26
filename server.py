"""
PASTA Dashboard API server.

Bridges the React dashboard and the real Spark benchmark pipeline.

Endpoints
─────────
POST /api/benchmark/run      → start a benchmark run in the background, returns {task_id}
GET  /api/benchmark/status   → {status, progress, stage, log, error}  (?task_id=...)
GET  /api/benchmark/results  → normalized {fifo, sjf, adaptive, pasta, makespans, max_t}
GET  /api/benchmark/latest   → same shape, read straight from the JSON files on disk

Run:
    ./venv/bin/python server.py
    → http://localhost:5175
"""
import os
import re
import sys
import json
import time
import threading
import subprocess

from flask import Flask, jsonify, request
from flask_cors import CORS

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
# Prefer the project venv interpreter; fall back to whatever launched us.
VENV_PY = os.path.join(PROJECT_ROOT, "venv", "bin", "python")
PYTHON = VENV_PY if os.path.exists(VENV_PY) else sys.executable

app = Flask(__name__)
CORS(app)

# task_id -> {status, progress, stage, log, error, results}
tasks = {}
_lock = threading.Lock()
_running = False  # only allow one benchmark at a time


# ─── Normalization ────────────────────────────────────────────────────────────

def _load_json(name):
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _makespan(results):
    """Makespan = latest end_ts in the result list."""
    if not results:
        return 0.0
    return round(max(r.get("end_ts", 0.0) for r in results), 4)


def build_payload():
    """
    Read the three benchmark_*.json files and return the shape the dashboard
    consumes (mirrors src/data/benchmarkData.js).

    The runner produces fifo / adaptive / pasta. The dashboard also shows an
    "SJF" series which, in this project, is the same ordering as Adaptive
    (SJF + dynamic config) — so sjf mirrors adaptive, matching the static data.
    """
    fifo = _load_json("benchmark_fifo.json")
    adaptive = _load_json("benchmark_adaptive.json")
    pasta = _load_json("benchmark_pasta.json")

    fifo_r = (fifo or {}).get("results", [])
    adaptive_r = (adaptive or {}).get("results", [])
    pasta_r = (pasta or {}).get("results", [])

    makespans = {
        "fifo": (fifo or {}).get("makespan") or _makespan(fifo_r),
        "adaptive": (adaptive or {}).get("makespan") or _makespan(adaptive_r),
        "sjf": (adaptive or {}).get("makespan") or _makespan(adaptive_r),
        "pasta": (pasta or {}).get("makespan") or _makespan(pasta_r),
    }

    # Axis upper bound: a little headroom above the slowest run.
    all_ends = [makespans["fifo"], makespans["adaptive"], makespans["pasta"]]
    max_t = round(max(all_ends or [1.0]) * 1.12, 2)

    return {
        "fifo": fifo_r,
        "sjf": adaptive_r,        # SJF == Adaptive ordering in this project
        "adaptive": adaptive_r,
        "pasta": pasta_r,
        "makespans": makespans,
        "max_t": max_t,
        "generated_at": time.time(),
    }


# ─── Background runner ────────────────────────────────────────────────────────

# Maps a substring of the runner's stdout to a (progress, stage) checkpoint.
STAGE_MARKERS = [
    ("FIFO: submitting", 10, "Running FIFO baseline"),
    ("FIFO makespan", 40, "FIFO done"),
    ("Adaptive: scheduling", 45, "Running Adaptive (SJF)"),
    ("Adaptive (SJF) makespan", 70, "Adaptive done"),
    ("PASTA: scheduling", 75, "Running PASTA"),
    ("PASTA makespan", 95, "PASTA done"),
]


def _run_benchmark(task_id):
    global _running
    task = tasks[task_id]
    task["status"] = "running"
    task["stage"] = "Starting Spark…"
    task["progress"] = 3

    cmd = [PYTHON, "-m", "benchmark.runner", "--mode", "all"]
    # Pin PySpark workers AND driver to this same interpreter, otherwise Spark
    # may launch a different system Python and crash with PYTHON_VERSION_MISMATCH.
    env = dict(os.environ)
    env["PYSPARK_PYTHON"] = PYTHON
    env["PYSPARK_DRIVER_PYTHON"] = PYTHON
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            task["log"].append(line)
            if len(task["log"]) > 400:
                task["log"] = task["log"][-400:]
            for marker, pct, stage in STAGE_MARKERS:
                if marker in line and pct > task["progress"]:
                    task["progress"] = pct
                    task["stage"] = stage
            # Nudge progress within a stage on per-job completion lines.
            if re.search(r"→\s*[\d.]+s", line) and task["progress"] < 95:
                task["progress"] = min(task["progress"] + 1, 95)

        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"benchmark.runner exited with code {proc.returncode}")

        task["results"] = build_payload()
        task["progress"] = 100
        task["stage"] = "Complete"
        task["status"] = "done"
    except Exception as e:  # noqa: BLE001
        task["status"] = "error"
        task["error"] = str(e)
        task["stage"] = "Failed"
        task["log"].append(f"ERROR: {e}")
    finally:
        with _lock:
            _running = False


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/api/benchmark/run", methods=["POST"])
def run_benchmark():
    global _running
    with _lock:
        if _running:
            return jsonify({"error": "a benchmark is already running"}), 409
        _running = True

    task_id = f"run_{int(time.time())}"
    tasks[task_id] = {
        "status": "queued",
        "progress": 0,
        "stage": "Queued",
        "log": [],
        "error": None,
        "results": None,
    }
    threading.Thread(target=_run_benchmark, args=(task_id,), daemon=True).start()
    return jsonify({"task_id": task_id})


@app.route("/api/benchmark/status", methods=["GET"])
def get_status():
    task_id = request.args.get("task_id")
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "task not found"}), 404
    return jsonify({
        "status": task["status"],
        "progress": task["progress"],
        "stage": task["stage"],
        "error": task["error"],
        "log": task["log"][-12:],
    })


@app.route("/api/benchmark/results", methods=["GET"])
def get_results():
    task_id = request.args.get("task_id")
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "task not found"}), 404
    if task["status"] != "done":
        return jsonify({"error": "not ready", "status": task["status"]}), 202
    return jsonify(task["results"])


@app.route("/api/benchmark/latest", methods=["GET"])
def get_latest():
    """Read the JSON files currently on disk — used for initial dashboard load."""
    return jsonify(build_payload())


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "python": PYTHON, "running": _running})


if __name__ == "__main__":
    print(f"PASTA API server → http://localhost:5175  (python: {PYTHON})")
    app.run(host="127.0.0.1", port=5175, debug=False, threaded=True)
