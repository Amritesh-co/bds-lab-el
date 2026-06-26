# Dynamic Dashboard with Live Backend Integration — Implementation Plan

> ✅ **IMPLEMENTED & VERIFIED (2026-06-24).** The dashboard now runs the real Spark
> pipeline from a "Run Benchmark" button and updates every stat from live data.
> See "How to run it" at the bottom. The rest of this file is the original design notes.

**Goal:** Make the dashboard trigger real Spark benchmark runs and display live results dynamically.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                       REACT DASHBOARD                           │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Overview Page                                            │   │
│  │ ┌────────────────────────────────────────────────────┐   │   │
│  │ │ [RUN BENCHMARK] Button                             │   │   │
│  │ │ • Shows loading spinner during execution           │   │   │
│  │ │ • Polls /api/benchmark/status for progress         │   │   │
│  │ └────────────────────────────────────────────────────┘   │   │
│  │                                                            │   │
│  │ ┌────────────────────────────────────────────────────┐   │   │
│  │ │ AlgorithmShowdown (Dynamic)                        │   │   │
│  │ │ • Accepts benchmark results as props               │   │   │
│  │ │ • Displays real FIFO/SJF/Adaptive/PASTA stats      │   │   │
│  │ │ • Re-animates on data change                       │   │   │
│  │ └────────────────────────────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
         ↓ HTTP POST /api/benchmark/run
         ↓ HTTP GET /api/benchmark/status (polling)
         ↓ HTTP GET /api/benchmark/results (when done)
┌─────────────────────────────────────────────────────────────────┐
│                      FLASK API SERVER                           │
│  Port: 5175 (separate from Vite dev server on 5174)             │
│                                                                 │
│  ┌─ POST /api/benchmark/run ─────────────────────────────────┐  │
│  │ • Spawns async task to run benchmark.runner               │  │
│  │ • Returns task_id                                         │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌─ GET /api/benchmark/status?task_id=... ──────────────────┐  │
│  │ • Returns { status: "running|done", progress: 0-100 }     │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌─ GET /api/benchmark/results?task_id=... ─────────────────┐  │
│  │ • Returns { fifo: [...], sjf: [...], pasta: [...] }       │  │
│  │ • Data from JSON files or in-memory store                 │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
         ↓ imports & calls
┌─────────────────────────────────────────────────────────────────┐
│                   PYTHON BACKEND (Existing)                     │
│  benchmark.runner (--mode fifo / --mode adaptive)               │
│  • Generates 15-job queue                                       │
│  • Runs FIFO scheduler                                          │
│  • Runs SJF scheduler                                           │
│  • Runs Adaptive scheduler                                      │
│  • Runs PASTA scheduler                                         │
│  • Saves results to data/benchmark_*.json                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Steps

### Phase 1: Flask API Server (15 mins)
**File:** `server.py` (root of EL/)

```python
from flask import Flask, jsonify, request
from flask_cors import CORS
import subprocess
import json
import threading
import os
import time
from pathlib import Path

app = Flask(__name__)
CORS(app)

# Store task state in memory (task_id -> {status, progress, results})
tasks = {}

@app.route("/api/benchmark/run", methods=["POST"])
def run_benchmark():
    """Trigger a new benchmark run."""
    task_id = f"run_{int(time.time())}"
    tasks[task_id] = {"status": "queued", "progress": 0, "results": None}
    
    def run_in_bg():
        tasks[task_id]["status"] = "running"
        tasks[task_id]["progress"] = 25
        
        # Run FIFO
        subprocess.run([...benchmark runner --mode fifo...])
        tasks[task_id]["progress"] = 50
        
        # Run Adaptive (which includes SJF + PASTA)
        subprocess.run([...benchmark runner --mode adaptive...])
        tasks[task_id]["progress"] = 75
        
        # Load results
        fifo = json.load(open("data/benchmark_fifo.json"))["jobs"]
        adaptive = json.load(open("data/benchmark_adaptive.json"))["jobs"]
        
        tasks[task_id]["results"] = {
            "fifo": fifo,
            "sjf": adaptive,     # SJF is first phase of adaptive
            "adaptive": adaptive,
            "pasta": adaptive,   # PASTA is final phase
        }
        tasks[task_id]["progress"] = 100
        tasks[task_id]["status"] = "done"
    
    thread = threading.Thread(target=run_in_bg, daemon=True)
    thread.start()
    return jsonify({"task_id": task_id})

@app.route("/api/benchmark/status", methods=["GET"])
def get_status():
    """Poll for task progress."""
    task_id = request.args.get("task_id")
    if not task_id or task_id not in tasks:
        return jsonify({"error": "not found"}), 404
    task = tasks[task_id]
    return jsonify({
        "status": task["status"],
        "progress": task["progress"]
    })

@app.route("/api/benchmark/results", methods=["GET"])
def get_results():
    """Fetch completed results."""
    task_id = request.args.get("task_id")
    if not task_id or task_id not in tasks:
        return jsonify({"error": "not found"}), 404
    task = tasks[task_id]
    if task["status"] != "done":
        return jsonify({"error": "still running"}), 202
    return jsonify(task["results"])

if __name__ == "__main__":
    app.run(port=5175, debug=False)
```

### Phase 2: Update AlgorithmShowdown Component (20 mins)
**File:** `src/components/AlgorithmShowdown.jsx`

- Accept `data` prop (can be hardcoded or from API)
- If data is null, show loading skeleton
- Calculate stats from data arrays (makespan, latency, speedup)
- Re-animate when data changes

```javascript
export default function AlgorithmShowdown({ data = null, isLoading = false }) {
  const [algos, setAlgos] = useState(HARDCODED_ALGOS);
  
  useEffect(() => {
    if (data) {
      // Transform API data into ALGOS format
      const calculated = calculateStats(data);
      setAlgos(calculated);
    }
  }, [data]);
  
  if (isLoading) {
    return <LoadingSkeleton />;
  }
  
  // Rest of component uses algos (dynamic or hardcoded)
  return (...);
}
```

### Phase 3: Add Run Button & State to Overview (15 mins)
**File:** `src/pages/Overview.jsx`

```javascript
export default function Overview() {
  const [benchmarkData, setBenchmarkData] = useState(null);
  const [isRunning, setIsRunning] = useState(false);
  const [taskId, setTaskId] = useState(null);
  
  const handleRunBenchmark = async () => {
    setIsRunning(true);
    const res = await fetch("http://localhost:5175/api/benchmark/run", {
      method: "POST"
    });
    const { task_id } = await res.json();
    setTaskId(task_id);
    
    // Poll for completion
    const pollInterval = setInterval(async () => {
      const statusRes = await fetch(
        `http://localhost:5175/api/benchmark/status?task_id=${task_id}`
      );
      const status = await statusRes.json();
      
      if (status.status === "done") {
        const resultsRes = await fetch(
          `http://localhost:5175/api/benchmark/results?task_id=${task_id}`
        );
        const results = await resultsRes.json();
        setBenchmarkData(results);
        setIsRunning(false);
        clearInterval(pollInterval);
      }
    }, 2000);
  };
  
  return (
    <>
      <button onClick={handleRunBenchmark} disabled={isRunning}>
        {isRunning ? "Running..." : "🚀 Run Live Benchmark"}
      </button>
      
      <AlgorithmShowdown 
        data={benchmarkData} 
        isLoading={isRunning}
      />
      {/* ... rest of page */}
    </>
  );
}
```

### Phase 4: Polish & Styling (10 mins)
- Add loading skeleton with pulse animations
- Progress bar during execution
- Error handling with retry
- Toast notifications for success/failure
- Disable other buttons while running

---

## Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `server.py` | CREATE | Flask API with /api/benchmark/* endpoints |
| `src/components/AlgorithmShowdown.jsx` | MODIFY | Accept data prop, add loading state |
| `src/pages/Overview.jsx` | MODIFY | Add run button & state management |
| `src/components/BenchmarkRunButton.jsx` | CREATE (optional) | Reusable run button component |
| `requirements.txt` | MODIFY | Add flask, flask-cors |

---

## Running It All

```bash
# Terminal 1: Start Flask server
cd "/Users/ignite/College/6TH SEM/bda lab/EL"
source venv/bin/activate
pip install flask flask-cors
python server.py
# → Running on http://localhost:5175

# Terminal 2: Start React dev server
cd dashboard-react
npm run dev
# → http://localhost:5174
```

Then click the **Run Benchmark** button on the dashboard → watch the background tasks → see live results populate in AlgorithmShowdown.

---

## Data Flow Example

1. User clicks "Run Benchmark"
2. → POST to `/api/benchmark/run` 
3. → Returns `task_id: "run_1719268..." `
4. → Dashboard shows loading spinner
5. → Poll `/api/benchmark/status?task_id=run_1719268...` every 2s
6. → Flask runs `benchmark.runner --mode fifo` (25% done)
7. → Flask runs `benchmark.runner --mode adaptive` (75% done)
8. → Status returns `{ status: "done", progress: 100 }`
9. → GET `/api/benchmark/results?task_id=...`
10. → Returns `{ fifo: [...], sjf: [...], adaptive: [...], pasta: [...] }`
11. → React component re-renders with real data
12. → AlgorithmShowdown animates in with live makespan/latency/speedup

---

## How to run it (final, as built)

```bash
# Terminal 1 — API server (runs the real Spark benchmark)
cd "/Users/ignite/College/6TH SEM/bda lab/EL"
./venv/bin/python server.py            # → http://localhost:5175

# Terminal 2 — dashboard
cd dashboard-react
npm run dev                            # → http://localhost:5174
```

Click **Run Benchmark** (top-right of the navbar). A progress bar + live stage
text track FIFO → Adaptive → PASTA. When it finishes, every stat (hero cards,
showdown race/podium, speedups, latency chart, simulation Gantt, PASTA table)
re-renders from the live JSON. The **SEED/LIVE** pill shows which data you're on.

### What was built
- `server.py` — Flask API: `/api/benchmark/run|status|results|latest|health`.
  Streams runner stdout to derive progress; normalizes the 3 JSONs into the
  dashboard shape. Pins `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to the venv.
- `src/data/BenchmarkContext.jsx` — provider/hook. Seeds from the static
  `benchmarkData.js` so the UI works with the server off; pulls `/latest` on
  mount; `runBenchmark()` POSTs + polls.
- Navbar Run button + progress bar + error toast + SEED/LIVE pill.
- Overview, AlgorithmShowdown, Simulation, PastaAlgorithm now read the hook;
  makespan/latency/speedup are all derived from the data, not hardcoded.

### Gotcha fixed
Spark workers were launching system Python 3.14 while the driver was venv 3.11
(`PYTHON_VERSION_MISMATCH`) — only `medium` jobs survived. Setting
`PYSPARK_PYTHON` to the venv interpreter in `server.py` makes all 15 jobs run.

## Questions (original)

- Should the "Run" button be prominent (hero section) or subtle (menu)?
- How long should each benchmark take? (Typical: 10-20 mins for full run)
- Do you want progress logging (stdout from subprocess)?
- Should results persist across page reloads, or reset each time?
- Do you want the ability to queue multiple runs or just one at a time?
