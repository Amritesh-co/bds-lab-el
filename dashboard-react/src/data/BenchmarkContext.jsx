import { createContext, useContext, useCallback, useEffect, useMemo, useState } from "react";
import {
  FIFO as DEFAULT_FIFO,
  SJF as DEFAULT_SJF,
  ADAPTIVE as DEFAULT_ADAPTIVE,
  PASTA as DEFAULT_PASTA,
  MAX_T as DEFAULT_MAX_T,
} from "./benchmarkData";

// Flask API base. Override with VITE_API_BASE if you host it elsewhere.
const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:5175";

const POLL_MS = 1500;

// ── Derive makespan = latest end_ts in a job list ─────────────────────────────
const makespanOf = (jobs) =>
  jobs && jobs.length ? Math.max(...jobs.map((j) => j.end_ts ?? 0)) : 0;

// ── Default dataset (static seed so the dashboard works with no server) ───────
const DEFAULT_DATASET = {
  fifo: DEFAULT_FIFO,
  sjf: DEFAULT_SJF,
  adaptive: DEFAULT_ADAPTIVE,
  pasta: DEFAULT_PASTA,
  maxT: DEFAULT_MAX_T,
  makespans: {
    fifo: makespanOf(DEFAULT_FIFO),
    sjf: makespanOf(DEFAULT_SJF),
    adaptive: makespanOf(DEFAULT_ADAPTIVE),
    pasta: makespanOf(DEFAULT_PASTA),
  },
  source: "static",
};

// Normalize an API payload (which uses max_t / makespans) into our shape.
function normalize(payload) {
  if (!payload || !payload.fifo) return null;
  return {
    fifo: payload.fifo,
    sjf: payload.sjf,
    adaptive: payload.adaptive,
    pasta: payload.pasta,
    maxT: payload.max_t ?? DEFAULT_MAX_T,
    makespans: payload.makespans ?? {
      fifo: makespanOf(payload.fifo),
      sjf: makespanOf(payload.sjf),
      adaptive: makespanOf(payload.adaptive),
      pasta: makespanOf(payload.pasta),
    },
    source: "live",
  };
}

const BenchmarkContext = createContext(null);

export function BenchmarkProvider({ children }) {
  const [dataset, setDataset] = useState(DEFAULT_DATASET);
  const [isRunning, setIsRunning] = useState(false);
  const [progress, setProgress] = useState(0);
  const [stage, setStage] = useState("");
  const [log, setLog] = useState([]);
  const [error, setError] = useState(null);
  const [apiOnline, setApiOnline] = useState(false);
  const [lastRunAt, setLastRunAt] = useState(null);

  // On mount: pull the latest results already on disk (if the API is up).
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/benchmark/latest`, { cache: "no-store" });
        if (!res.ok) throw new Error("no api");
        const payload = await res.json();
        const norm = normalize(payload);
        if (!cancelled && norm) {
          setDataset(norm);
          setApiOnline(true);
          setLastRunAt(payload.generated_at ? payload.generated_at * 1000 : Date.now());
        }
      } catch {
        // Server not running — stay on static seed data. Not an error.
        if (!cancelled) setApiOnline(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const runBenchmark = useCallback(async () => {
    if (isRunning) return;
    setError(null);
    setLog([]);
    setProgress(0);
    setStage("Queued");
    setIsRunning(true);

    let taskId;
    try {
      const res = await fetch(`${API_BASE}/api/benchmark/run`, { method: "POST" });
      if (res.status === 409) throw new Error("A benchmark is already running.");
      if (!res.ok) throw new Error(`Failed to start (HTTP ${res.status}).`);
      taskId = (await res.json()).task_id;
      setApiOnline(true);
    } catch (e) {
      setError(
        e.message?.includes("fetch")
          ? "Cannot reach the API server on :5175. Start it with ./venv/bin/python server.py"
          : e.message,
      );
      setIsRunning(false);
      return;
    }

    // Poll status until done / error.
    await new Promise((resolve) => {
      const poll = setInterval(async () => {
        try {
          const sres = await fetch(
            `${API_BASE}/api/benchmark/status?task_id=${taskId}`,
            { cache: "no-store" },
          );
          const s = await sres.json();
          setProgress(s.progress ?? 0);
          setStage(s.stage ?? "");
          if (Array.isArray(s.log)) setLog(s.log);

          if (s.status === "done") {
            clearInterval(poll);
            const rres = await fetch(
              `${API_BASE}/api/benchmark/results?task_id=${taskId}`,
              { cache: "no-store" },
            );
            const payload = await rres.json();
            const norm = normalize(payload);
            if (norm) {
              setDataset(norm);
              setLastRunAt(Date.now());
            }
            setIsRunning(false);
            resolve();
          } else if (s.status === "error") {
            clearInterval(poll);
            setError(s.error || "Benchmark failed. Check the server log.");
            setIsRunning(false);
            resolve();
          }
        } catch {
          clearInterval(poll);
          setError("Lost connection to the API server during the run.");
          setIsRunning(false);
          resolve();
        }
      }, POLL_MS);
    });
  }, [isRunning]);

  const value = useMemo(
    () => ({
      // Datasets (live or static seed)
      FIFO: dataset.fifo,
      SJF: dataset.sjf,
      ADAPTIVE: dataset.adaptive,
      PASTA: dataset.pasta,
      MAX_T: dataset.maxT,
      makespans: dataset.makespans,
      isLive: dataset.source === "live",
      // Run controls / state
      runBenchmark,
      isRunning,
      progress,
      stage,
      log,
      error,
      apiOnline,
      lastRunAt,
    }),
    [dataset, runBenchmark, isRunning, progress, stage, log, error, apiOnline, lastRunAt],
  );

  return <BenchmarkContext.Provider value={value}>{children}</BenchmarkContext.Provider>;
}

export function useBenchmark() {
  const ctx = useContext(BenchmarkContext);
  if (!ctx) throw new Error("useBenchmark must be used within <BenchmarkProvider>");
  return ctx;
}
