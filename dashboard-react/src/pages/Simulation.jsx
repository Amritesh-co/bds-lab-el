import { useCallback, useEffect, useRef, useState, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  AreaChart, Area, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, ReferenceLine,
} from "recharts";
import { typeColor, fmtT } from "../data/benchmarkData";
import { useBenchmark } from "../data/BenchmarkContext";
import TypeChip, { TierChip } from "../components/TypeChip";

// ─────────────────────────────────────────────────────────────────────────────
// DATASET SCALING HELPERS
// ─────────────────────────────────────────────────────────────────────────────

/** Tile a job list N times, offsetting timestamps by the batch makespan. */
function scaleDataset(jobs, n) {
  if (n === 1) return jobs;
  const makespan = jobs[jobs.length - 1].end_ts;
  const result = [...jobs];
  for (let batch = 1; batch < n; batch++) {
    const offset = batch * makespan;
    jobs.forEach((j) =>
      result.push({ ...j, start_ts: j.start_ts + offset, end_ts: j.end_ts + offset })
    );
  }
  return result;
}

// makespan = latest end_ts in a job list
const lastEnd = (jobs) =>
  jobs && jobs.length ? parseFloat(jobs[jobs.length - 1].end_ts ?? Math.max(...jobs.map((j) => j.end_ts || 0))) : 0;

/** Build the algorithm rows from the (live or seed) benchmark datasets. */
function buildAlgoBase({ FIFO, ADAPTIVE, SJF, PASTA }) {
  return [
    { id: "fifo",     label: "FIFO",           color: "#ffb4ab", data: FIFO,     makespan: lastEnd(FIFO) },
    { id: "adaptive", label: "Adaptive (SJF)", color: "#ffc59f", data: ADAPTIVE, makespan: lastEnd(ADAPTIVE) },
    { id: "sjf",      label: "SJF + Aging",    color: "#ffb95f", data: SJF,      makespan: lastEnd(SJF) },
    { id: "pasta",    label: "PASTA 🏆",       color: "#4edea3", data: PASTA,    makespan: lastEnd(PASTA) },
  ];
}

// ── useContainerWidth: watches a ref with ResizeObserver ──────────────────────
function useContainerWidth(ref) {
  const [width, setWidth] = useState(900);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((entries) => {
      setWidth(entries[0].contentRect.width);
    });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, [ref]);
  return width;
}

// ─────────────────────────────────────────────────────────────────────────────
// COMET-TAIL DATA PACKETS (the "flies")
// ─────────────────────────────────────────────────────────────────────────────

/** 6 glowing comet-tail packets trailing behind the processing cursor. */
function DataPackets({ job, simTime, color, pxPerSec }) {
  const isRunning = job.start_ts <= simTime && job.end_ts > simTime;
  if (!isRunning) return null;

  const TRAIL = 6;
  const GAP   = 0.07; // seconds between each dot

  return (
    <>
      {Array.from({ length: TRAIL }, (_, i) => {
        const t = simTime - i * GAP;
        if (t < job.start_ts || t > job.end_ts) return null;
        const px      = t * pxPerSec;
        const opacity = 1 - (i / TRAIL) * 0.85;
        const size    = Math.round(9 - i * 1.3);
        return (
          <div key={i} style={{
            position: "absolute",
            left: px,
            top: "50%",
            transform: "translate(-50%, -50%)",
            width: size, height: size,
            borderRadius: "50%",
            background: color,
            opacity,
            boxShadow: i === 0
              ? `0 0 ${size * 3}px ${color}, 0 0 ${size * 6}px ${color}55`
              : `0 0 ${size * 2}px ${color}70`,
            zIndex: 20,
            pointerEvents: "none",
          }} />
        );
      }).filter(Boolean)}
    </>
  );
}

/** Ring burst that expands at the pixel where a job just finished. */
function CompletionBurst({ job, simTime, color, pxPerSec }) {
  const justDone = simTime > 0 && job.end_ts <= simTime && simTime - job.end_ts < 0.14;
  if (!justDone) return null;

  return (
    <div style={{
      position: "absolute",
      left: job.end_ts * pxPerSec,
      top: "50%",
      transform: "translate(-50%,-50%)",
      width: 26, height: 26,
      borderRadius: "50%",
      background: "transparent",
      border: `2px solid ${color}`,
      animation: "burst 0.4s ease-out forwards",
      zIndex: 25,
      pointerEvents: "none",
    }} />
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// GANTT ROW — no scroll, always fits the container width
// ─────────────────────────────────────────────────────────────────────────────

const ROW_H = 44;

function GanttRow({ cfg, simTime, pxPerSec, containerW, totalPx, scrollRef }) {
  const [tooltip, setTooltip] = useState(null);
  const trackRef = useRef(null);

  const hasRunning = cfg.data.some((j) => j.start_ts <= simTime && j.end_ts > simTime);
  const donePct    = Math.round(
    (cfg.data.filter((j) => j.end_ts <= simTime).length / cfg.data.length) * 100
  );

  // Minimum bar width in pixels — always at least 4 px so bars stay visible
  const minPx = Math.max(4, pxPerSec * 0.04);

  return (
    <div className="mb-4 relative">
      {/* Header row */}
      <div className="flex items-center gap-3 mb-1.5">
        <div className="w-36 flex-shrink-0 flex items-center gap-2">
          <motion.div
            className="w-2 h-2 rounded-full flex-shrink-0"
            style={{ background: cfg.color }}
            animate={hasRunning ? { scale: [1, 1.7, 1], opacity: [1, 0.4, 1] } : {}}
            transition={{ repeat: Infinity, duration: 0.7 }}
          />
          <span className="text-xs font-bold truncate" style={{ color: cfg.color }}>
            {cfg.label}
          </span>
        </div>
        <span className="text-xs mono text-on-surface-variant">
          Makespan:{" "}
          <span className="font-bold" style={{ color: cfg.color }}>
            {(cfg.data[cfg.data.length - 1]?.end_ts ?? cfg.makespan).toFixed(2)}s
          </span>
        </span>
        <span
          className="ml-auto text-[10px] font-bold mono px-2 py-0.5 rounded-full"
          style={{ background: `${cfg.color}18`, color: cfg.color, border: `1px solid ${cfg.color}30` }}
        >
          {donePct}%
        </span>
      </div>

      {/* Track — fills container, no overflow */}
      <div style={{ marginLeft: 144 }}>
        <div
          ref={trackRef}
          className="relative"
          style={{
            width: "100%",
            height: ROW_H,
            background: `${cfg.color}07`,
            border: `1px solid ${cfg.color}18`,
            borderRadius: 8,
            overflow: "hidden",       // clip content to the track bounds
          }}
        >
          {/* ── Job bars ── */}
          {cfg.data.map((job, i) => {
            const barLeft  = job.start_ts * pxPerSec;
            const barW     = Math.max(minPx, (job.end_ts - job.start_ts) * pxPerSec - 1);
            const done     = job.end_ts <= simTime;
            const running  = job.start_ts <= simTime && job.end_ts > simTime;
            const opacity  = simTime === 0 ? 0.28 : done ? 0.92 : running ? 0.85 : 0.22;
            const c        = typeColor(job.job_type);

            return (
              <motion.div
                key={i}
                animate={{ opacity }}
                style={{
                  position: "absolute",
                  left:   barLeft,
                  width:  barW,
                  top: 4, bottom: 4,
                  background: done
                    ? `linear-gradient(180deg, ${c} 0%, ${c}cc 100%)`
                    : c,
                  borderRadius: 4,
                  boxShadow: running
                    ? `0 0 14px ${c}90, 0 0 4px ${c}`
                    : done ? `0 0 3px ${c}30` : "none",
                  cursor: "pointer",
                  zIndex: 2,
                }}
                onMouseEnter={(e) => {
                  const rect = trackRef.current?.getBoundingClientRect() ?? { left: 0, top: 0 };
                  setTooltip({ job, x: e.clientX - rect.left, y: e.clientY - rect.top });
                }}
                onMouseLeave={() => setTooltip(null)}
              />
            );
          })}

          {/* ✈  Comet-tail data packets */}
          {cfg.data.map((job, i) => (
            <DataPackets key={`pk-${i}`} job={job} simTime={simTime} color={cfg.color} pxPerSec={pxPerSec} />
          ))}

          {/* 💥  Completion bursts */}
          {cfg.data.map((job, i) => (
            <CompletionBurst key={`br-${i}`} job={job} simTime={simTime} color={cfg.color} pxPerSec={pxPerSec} />
          ))}

          {/* Cursor line */}
          {simTime > 0 && (
            <div style={{
              position: "absolute",
              top: -2, bottom: -2,
              left: simTime * pxPerSec,
              width: 2,
              borderRadius: 2,
              background: cfg.color,
              boxShadow: `0 0 10px ${cfg.color}, 0 0 20px ${cfg.color}50`,
              zIndex: 30,
              pointerEvents: "none",
            }} />
          )}
        </div>
      </div>

      {/* Tooltip */}
      <AnimatePresence>
        {tooltip && (
          <motion.div
            key="tip"
            initial={{ opacity: 0, scale: 0.9, y: 4 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.9 }}
            transition={{ duration: 0.1 }}
            className="absolute z-50 glass rounded-xl p-3 text-xs shadow-2xl border pointer-events-none"
            style={{
              left: Math.min(tooltip.x + 160, (containerW ?? 600) - 200),
              top:  tooltip.y - 70,
              borderColor: `${typeColor(tooltip.job.job_type)}44`,
              minWidth: 170,
            }}
          >
            <div className="font-bold mb-1.5" style={{ color: typeColor(tooltip.job.job_type) }}>
              {tooltip.job.job_type} job
            </div>
            <div className="space-y-0.5 text-on-surface-variant mono">
              <div className="flex justify-between gap-4"><span>Size</span><span className="text-on-surface">{tooltip.job.input_size_mb} MB</span></div>
              <div className="flex justify-between gap-4"><span>Actual</span><span className="text-on-surface">{fmtT(tooltip.job.actual_time)}</span></div>
              {tooltip.job.predicted_time != null && (
                <div className="flex justify-between gap-4"><span>Predicted</span><span style={{ color: typeColor(tooltip.job.job_type) }}>{fmtT(tooltip.job.predicted_time)}</span></div>
              )}
              {tooltip.job.priority != null && (
                <div className="flex justify-between gap-4"><span>Priority</span><span className="text-secondary">{tooltip.job.priority.toFixed(4)}</span></div>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// LIVE STATS CARD
// ─────────────────────────────────────────────────────────────────────────────

function StatsCard({ cfg, simTime }) {
  const done    = cfg.data.filter((j) => j.end_ts <= simTime).length;
  const total   = cfg.data.length;
  const cur     = cfg.data.find((j) => j.start_ts <= simTime && j.end_ts > simTime);
  const elapsed = Math.min(simTime, cfg.makespan).toFixed(2);
  const mbDone  = cfg.data.filter((j) => j.end_ts <= simTime)
                           .reduce((s, j) => s + j.input_size_mb, 0);
  const mbCur   = cur
    ? cur.input_size_mb * ((simTime - cur.start_ts) / (cur.end_ts - cur.start_ts))
    : 0;
  const mbLive  = (mbDone + mbCur).toFixed(2);
  const jobPct  = Math.round((done / total) * 100);

  return (
    <motion.div
      className="glass rounded-2xl p-4"
      animate={{
        boxShadow: cur
          ? `0 0 24px ${cfg.color}25`
          : done === total && simTime > 0
          ? `0 0 16px ${cfg.color}18`
          : "none",
      }}
    >
      <div className="flex items-center gap-2 mb-3">
        <motion.div
          className="w-2 h-2 rounded-full"
          style={{ background: cfg.color }}
          animate={cur ? { scale: [1, 1.8, 1], opacity: [1, 0.3, 1] } : {}}
          transition={{ repeat: Infinity, duration: 0.6 }}
        />
        <span className="text-xs font-bold uppercase tracking-wider" style={{ color: cfg.color }}>
          {cfg.label}
        </span>
        {done === total && simTime > 0 && (
          <motion.span
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            className="ml-auto text-[9px] font-bold text-secondary bg-secondary/10 px-2 py-0.5 rounded-full"
          >
            ✓ DONE
          </motion.span>
        )}
      </div>

      <div className="space-y-2">
        <div className="flex justify-between text-xs">
          <span className="text-on-surface-variant">Jobs Done</span>
          <span className="mono font-bold text-on-surface">{done} / {total}</span>
        </div>
        {/* progress bar */}
        <div className="h-2.5 bg-surface-container-high rounded-full overflow-hidden">
          <motion.div
            className="h-full rounded-full shadow-lg"
            style={{
              background: `linear-gradient(90deg, ${cfg.color}60, ${cfg.color})`,
            }}
            animate={{ width: `${jobPct}%` }}
            transition={{ duration: 0.1, ease: "linear" }}
            key={`progress-${jobPct}`}
          />
        </div>

        <div className="flex justify-between text-xs">
          <span className="text-on-surface-variant">Current</span>
          <span className="mono text-[10px] text-on-surface">
            {cur ? `${cur.job_type} ${cur.input_size_mb}MB` : "—"}
          </span>
        </div>

        <div className="flex justify-between text-xs">
          <span className="text-on-surface-variant">Elapsed</span>
          <span className="mono font-bold" style={{ color: cfg.color }}>{elapsed}s</span>
        </div>

        {/* LIVE data processed */}
        <div
          className="mt-2 rounded-lg px-2 py-1.5 border flex justify-between items-center"
          style={{ background: `${cfg.color}08`, borderColor: `${cfg.color}20` }}
        >
          <span className="text-[9px] text-on-surface-variant flex items-center gap-1">
            <span className="material-symbols-outlined" style={{ fontSize: 11 }}>database</span>
            Data processed
          </span>
          <motion.span
            className="mono text-xs font-bold"
            style={{ color: cfg.color }}
            key={Math.floor(mbLive * 10)} // re-animate on change
            initial={{ opacity: 0.5 }}
            animate={{ opacity: 1 }}
          >
            {mbLive} MB
          </motion.span>
        </div>
      </div>
    </motion.div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// LIVE THROUGHPUT SPARKLINE
// ─────────────────────────────────────────────────────────────────────────────

function ThroughputChart({ history }) {
  if (history.length < 2) {
    return (
      <div className="flex items-center justify-center h-full text-xs text-on-surface-variant/40">
        Press Play to see live throughput
      </div>
    );
  }
  return (
    <ResponsiveContainer width="100%" height="100%">
      <AreaChart data={history} margin={{ top: 4, right: 4, left: -28, bottom: 0 }}>
        <defs>
          <linearGradient id="gFifo"  x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%"  stopColor="#ffb4ab" stopOpacity={0.4} />
            <stop offset="95%" stopColor="#ffb4ab" stopOpacity={0} />
          </linearGradient>
          <linearGradient id="gAdaptive" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%"  stopColor="#ffc59f" stopOpacity={0.4} />
            <stop offset="95%" stopColor="#ffc59f" stopOpacity={0} />
          </linearGradient>
          <linearGradient id="gSjf"   x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%"  stopColor="#ffb95f" stopOpacity={0.4} />
            <stop offset="95%" stopColor="#ffb95f" stopOpacity={0} />
          </linearGradient>
          <linearGradient id="gPasta" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%"  stopColor="#4edea3" stopOpacity={0.5} />
            <stop offset="95%" stopColor="#4edea3" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(66,71,84,0.2)" />
        <XAxis dataKey="t" tick={{ fill: "#8c909f", fontSize: 8 }} tickFormatter={(v) => `${v.toFixed(1)}s`} interval="preserveStartEnd" />
        <YAxis tick={{ fill: "#8c909f", fontSize: 8 }} />
        <Tooltip
          contentStyle={{ background: "#122131", border: "1px solid #424754", borderRadius: 8, fontSize: 10 }}
          formatter={(v, name) => [`${v} jobs`, name]}
          labelFormatter={(l) => `t=${l.toFixed(2)}s`}
        />
        <Area type="monotone" dataKey="fifo"      name="FIFO"          stroke="#ffb4ab" fill="url(#gFifo)"      strokeWidth={1.5} dot={false} isAnimationActive={false} />
        <Area type="monotone" dataKey="adaptive"  name="Adaptive (SJF)" stroke="#ffc59f" fill="url(#gAdaptive)"  strokeWidth={1.5} dot={false} isAnimationActive={false} />
        <Area type="monotone" dataKey="sjf"       name="SJF + Aging"   stroke="#ffb95f" fill="url(#gSjf)"       strokeWidth={1.5} dot={false} isAnimationActive={false} />
        <Area type="monotone" dataKey="pasta"     name="PASTA"         stroke="#4edea3" fill="url(#gPasta)"     strokeWidth={2}   dot={false} isAnimationActive={false} />
      </AreaChart>
    </ResponsiveContainer>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// DATASET SCALE PANEL
// ─────────────────────────────────────────────────────────────────────────────

const SCALE_LABELS = ["1×", "2×", "3×", "4×", "5×"];

function DatasetScalePanel({ scale, onScale }) {
  const bench = useBenchmark();
  const ALGO_BASE = useMemo(() => buildAlgoBase(bench), [bench]);
  const makespans = ALGO_BASE.map((a) => ({
    id:      a.id,
    label:   a.label,
    color:   a.color,
    base:    a.makespan,
    scaled:  parseFloat((a.makespan * scale).toFixed(2)),
    jobs:    15 * scale,
    mbTotal: parseFloat(
      (ALGO_BASE[0].data.reduce((s, j) => s + j.input_size_mb, 0) * scale).toFixed(1)
    ),
  }));

  const pastaVsFifo = (
    ((makespans[0].scaled - makespans[3].scaled) / makespans[0].scaled) * 100
  ).toFixed(1);

  return (
    <div className="glass rounded-2xl p-5">
      <div className="flex items-center gap-2 mb-4">
        <span className="material-symbols-outlined text-secondary" style={{ fontSize: 18 }}>dataset</span>
        <h3 className="font-bold text-sm text-on-surface">Dataset Scale</h3>
        <span className="ml-auto text-[10px] text-on-surface-variant">
          Repeat benchmark batches to simulate larger workloads
        </span>
      </div>

      {/* Scale selector */}
      <div className="flex gap-2 mb-5">
        {SCALE_LABELS.map((l, i) => {
          const n = i + 1;
          const active = scale === n;
          return (
            <motion.button
              key={n}
              whileHover={{ y: -2 }}
              whileTap={{ scale: 0.95 }}
              onClick={() => onScale(n)}
              className="flex-1 py-2 rounded-xl text-xs font-bold border transition-all"
              style={active ? {
                background: "rgba(78,222,163,0.15)",
                borderColor: "rgba(78,222,163,0.45)",
                color: "#4edea3",
                boxShadow: "0 0 14px rgba(78,222,163,0.2)",
              } : {
                background: "rgba(18,33,49,0.4)",
                borderColor: "rgba(66,71,84,0.25)",
                color: "#8c909f",
              }}
            >
              {l}
            </motion.button>
          );
        })}
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-3 gap-3 mb-5">
        {[
          { label: "Total Jobs",  val: 15 * scale,                               color: "#adc6ff" },
          { label: "Data Volume", val: `${makespans[0].mbTotal} MB`,              color: "#4edea3" },
          { label: "PASTA Saves", val: `${pastaVsFifo}% vs FIFO`,                color: "#4edea3" },
        ].map((s) => (
          <div key={s.label} className="bg-surface-container rounded-xl p-3 text-center border border-outline-variant/15">
            <motion.div
              key={s.val}
              initial={{ scale: 0.8, opacity: 0.5 }}
              animate={{ scale: 1, opacity: 1 }}
              className="text-xl font-black mono"
              style={{ color: s.color }}
            >
              {s.val}
            </motion.div>
            <div className="text-[9px] text-on-surface-variant mt-0.5">{s.label}</div>
          </div>
        ))}
      </div>

      {/* Scaled makespan bars */}
      <div className="space-y-3">
        {makespans.map((a, i) => {
          const maxMs = makespans[0].scaled;
          const pct   = (a.scaled / maxMs) * 100;
          return (
            <div key={a.id}>
              <div className="flex justify-between text-xs mb-1">
                <span className="font-semibold" style={{ color: a.color }}>{a.label}</span>
                <span className="mono font-bold" style={{ color: a.color }}>{a.scaled}s</span>
              </div>
              <div className="h-3 bg-surface-container-high rounded-full overflow-hidden">
                <motion.div
                  className="h-full rounded-full"
                  style={{
                    background: i === 3
                      ? `linear-gradient(90deg, ${a.color}80, ${a.color})`
                      : a.color,
                  }}
                  animate={{ width: `${pct}%` }}
                  transition={{ duration: 0.6, ease: [0.22, 1, 0.36, 1] }}
                />
              </div>
            </div>
          );
        })}
      </div>

      {scale > 1 && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="mt-4 text-[10px] text-on-surface-variant leading-relaxed rounded-xl p-3 border border-secondary/20"
          style={{ background: "rgba(78,222,163,0.04)" }}
        >
          With <strong className="text-secondary">{scale}× dataset</strong> ({15 * scale} jobs,{" "}
          {makespans[0].mbTotal} MB): PASTA finishes in{" "}
          <strong className="text-secondary">{makespans[3].scaled}s</strong> vs FIFO's{" "}
          <strong className="text-error">{makespans[0].scaled}s</strong> —{" "}
          saving <strong className="text-secondary">{(makespans[0].scaled - makespans[3].scaled).toFixed(2)}s</strong>.
          {scale >= 3 && " At scale, aging prevents starvation of the many large jobs in later batches."}
        </motion.div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN PAGE
// ─────────────────────────────────────────────────────────────────────────────

export default function Simulation() {
  const [scale,    setScale]    = useState(1);
  const [simTime,  setSimTime]  = useState(0);
  const [running,  setRunning]  = useState(false);
  const [speed,    setSpeed]    = useState(1);
  const [history,  setHistory]  = useState([]); // throughput chart data
  const timerRef          = useRef(null);
  const historyRef        = useRef([]);
  const lastHistRef       = useRef(0);
  const ganttContainerRef = useRef(null);
  const ganttScrollRef    = useRef(null);
  const FPS = 60;

  // Scale the datasets
  const bench = useBenchmark();
  const ALGO_BASE = useMemo(() => buildAlgoBase(bench), [bench]);
  const algos = useMemo(() =>
    ALGO_BASE.map((a) => ({
      ...a,
      data:     scaleDataset(a.data, scale),
      makespan: parseFloat((a.makespan * scale).toFixed(2)),
    })),
  [ALGO_BASE, scale]);

  const scaledMaxT = algos[0].makespan; // FIFO makespan is our timeline ref
  const containerW = useContainerWidth(ganttContainerRef);
  const pxPerSec   = containerW > 144 ? (containerW - 144) / scaledMaxT : 80;

  // Reset when scale changes
  useEffect(() => {
    setRunning(false);
    clearInterval(timerRef.current);
    setSimTime(0);
    setHistory([]);
    historyRef.current = [];
    lastHistRef.current = 0;
  }, [scale]);

  // Tick
  const tick = useCallback(() => {
    setSimTime((t) => {
      const dt    = (1 / FPS) * speed;  // Real-time: 1 second per second at speed=1
      const next  = Math.min(t + dt, scaledMaxT);

      // Record history snapshot every 0.25s of sim time
      if (next - lastHistRef.current >= 0.25 || next >= scaledMaxT) {
        const snap = {
          t:        parseFloat(next.toFixed(2)),
          fifo:     algos[0].data.filter((j) => j.end_ts <= next).length,
          adaptive: algos[1].data.filter((j) => j.end_ts <= next).length,
          sjf:      algos[2].data.filter((j) => j.end_ts <= next).length,
          pasta:    algos[3].data.filter((j) => j.end_ts <= next).length,
        };
        historyRef.current = [...historyRef.current, snap];
        setHistory([...historyRef.current]);
        lastHistRef.current = next;
      }

      if (next >= scaledMaxT) {
        setRunning(false);
        clearInterval(timerRef.current);
      }
      return next;
    });
  }, [speed, scaledMaxT, algos]);

  useEffect(() => {
    if (running) {
      timerRef.current = setInterval(tick, 1000 / FPS);
    } else {
      clearInterval(timerRef.current);
    }
    return () => clearInterval(timerRef.current);
  }, [running, tick]);

  // (no auto-scroll needed — Gantt always fits in the container)

  const play  = () => { if (simTime >= scaledMaxT) resetSim(); setRunning(true); };
  const pause = () => setRunning(false);
  const resetSim = () => {
    setRunning(false);
    setSimTime(0);
    setHistory([]);
    historyRef.current = [];
    lastHistRef.current = 0;
  };

  const pct = scaledMaxT > 0 ? (simTime / scaledMaxT) * 100 : 0;

  // Global live MB across all algos (use PASTA as the "live" feed)
  const pastaDone   = algos[3].data.filter((j) => j.end_ts <= simTime);
  const pastaCur    = algos[3].data.find((j) => j.start_ts <= simTime && j.end_ts > simTime);
  const liveMbPasta = parseFloat((
    pastaDone.reduce((s, j) => s + j.input_size_mb, 0) +
    (pastaCur
      ? pastaCur.input_size_mb * ((simTime - pastaCur.start_ts) / (pastaCur.end_ts - pastaCur.start_ts))
      : 0)
  ).toFixed(2));

  const totalJobs   = algos[0].data.length;
  const doneJobs    = algos[3].data.filter((j) => j.end_ts <= simTime).length;
  const throughput  = simTime > 0.5 ? (liveMbPasta / simTime).toFixed(2) : "0.00";

  const axisLabels = Array.from({ length: 10 }, (_, i) =>
    ((scaledMaxT / 9) * i).toFixed(1)
  );

  // CSS for burst animation
  const burstCSS = `
    @keyframes burst {
      0%   { transform: translate(-50%,-50%) scale(0.2); opacity: 1; }
      100% { transform: translate(-50%,-50%) scale(2.5); opacity: 0; }
    }
  `;

  return (
    <>
      <style>{burstCSS}</style>
      <div className="max-w-[1600px] mx-auto px-6 py-6 space-y-5">

        {/* ── Header ── */}
        <div className="flex items-end justify-between flex-wrap gap-4">
          <div>
            <h1 className="text-3xl font-black text-on-surface">Live Simulation</h1>
            <p className="text-sm text-on-surface-variant mt-1">
              Animated data packets fly through each algorithm in real time · Scale the dataset to see PASTA's advantage grow
            </p>
          </div>

          {/* Playback controls */}
          <div className="glass rounded-2xl p-2 flex items-center gap-2">
            <motion.button
              whileHover={{ scale: 1.06 }}
              whileTap={{ scale: 0.94 }}
              onClick={play}
              disabled={running}
              className="bg-primary-container text-[#002e6a] px-5 py-2 rounded-xl flex items-center gap-2 text-sm font-black disabled:opacity-40 hover:brightness-110 transition-all"
            >
              <motion.span
                className="material-symbols-outlined fill-icon"
                style={{ fontSize: 18 }}
                animate={running ? { rotate: 360 } : {}}
                transition={{ repeat: Infinity, duration: 1.5, ease: "linear" }}
              >
                {running ? "autorenew" : "play_arrow"}
              </motion.span>
              {running ? "Running…" : "Play"}
            </motion.button>
            <motion.button whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}
              onClick={pause} disabled={!running}
              className="border border-outline-variant text-on-surface hover:bg-surface-variant rounded-xl p-2 disabled:opacity-40 transition-colors">
              <span className="material-symbols-outlined" style={{ fontSize: 18 }}>pause</span>
            </motion.button>
            <motion.button whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}
              onClick={resetSim}
              className="border border-outline-variant text-on-surface hover:bg-surface-variant rounded-xl p-2 transition-colors">
              <span className="material-symbols-outlined" style={{ fontSize: 18 }}>refresh</span>
            </motion.button>

            <div className="h-6 w-px bg-outline-variant/30 mx-1" />

            <select value={speed} onChange={(e) => setSpeed(Number(e.target.value))}
              className="bg-surface-container border border-outline-variant text-on-surface rounded-xl text-xs py-2 px-3 mono cursor-pointer">
              {[1, 2, 5, 10].map((s) => <option key={s} value={s}>{s}× Speed</option>)}
            </select>
          </div>
        </div>

        {/* ── Live KPI strip ── */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {[
            { icon: "timer",     label: "Simulation Time", val: `${simTime.toFixed(2)}s`,   color: "#adc6ff" },
            { icon: "database",  label: "Data Processed",  val: `${liveMbPasta} MB`,         color: "#4edea3" },
            { icon: "task_alt",  label: "Jobs Complete",   val: `${doneJobs} / ${totalJobs}`, color: "#4edea3" },
            { icon: "speed",     label: "Throughput",      val: `${throughput} MB/s`,          color: "#ffb95f" },
          ].map((k) => (
            <motion.div
              key={k.label}
              className="glass rounded-2xl px-4 py-3 flex items-center gap-3"
              animate={{ boxShadow: running ? `0 0 16px ${k.color}15` : "none" }}
            >
              <span className="material-symbols-outlined" style={{ fontSize: 20, color: k.color }}>{k.icon}</span>
              <div>
                <motion.div
                  key={k.val}
                  initial={{ y: -6, opacity: 0 }}
                  animate={{ y: 0, opacity: 1 }}
                  className="text-sm font-black mono"
                  style={{ color: k.color }}
                >
                  {k.val}
                </motion.div>
                <div className="text-[9px] text-on-surface-variant">{k.label}</div>
              </div>
            </motion.div>
          ))}
        </div>

        {/* ── Scrubber ── */}
        <div className="glass rounded-2xl px-5 py-4">
          <div className="flex justify-between text-xs mono text-on-surface-variant mb-2">
            <span>0.00s</span>
            <span className="text-primary font-bold">{simTime.toFixed(2)}s / {scaledMaxT.toFixed(2)}s</span>
            <span>FIFO makespan: {scaledMaxT.toFixed(2)}s</span>
          </div>
          <div
            className="relative h-3 bg-surface-container-high rounded-full cursor-pointer"
            onClick={(e) => {
              const rect = e.currentTarget.getBoundingClientRect();
              const p = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
              setSimTime(p * scaledMaxT);
              setRunning(false);
            }}
          >
            <motion.div
              className="absolute top-0 left-0 h-full rounded-full"
              style={{ background: "linear-gradient(90deg, #adc6ff, #4edea3)" }}
              animate={{ width: `${pct}%` }}
              transition={{ duration: 0, ease: "linear" }}
            />
            {/* Progress packets on scrubber */}
            {running && [0, 1, 2].map((i) => {
              const offset = i * 2.5;
              const pp = Math.max(0, pct - offset);
              return (
                <div key={i} style={{
                  position: "absolute",
                  top: "50%",
                  left: `${pp}%`,
                  transform: "translate(-50%, -50%)",
                  width: 8 - i * 2,
                  height: 8 - i * 2,
                  borderRadius: "50%",
                  background: "#4edea3",
                  opacity: 1 - i * 0.3,
                  boxShadow: `0 0 8px #4edea3`,
                  zIndex: 5,
                  pointerEvents: "none",
                }} />
              );
            })}
            <motion.div
              className="absolute top-1/2 -translate-y-1/2 w-5 h-5 bg-primary rounded-full border-2 border-surface shadow-lg"
              style={{ left: `calc(${pct}% - 10px)`, boxShadow: "0 0 12px #adc6ff60" }}
              whileHover={{ scale: 1.2 }}
            />
          </div>
          <div className="flex justify-between text-[9px] mono text-on-surface-variant/40 mt-1.5 px-0.5">
            {axisLabels.map((l) => <span key={l}>{l}s</span>)}
          </div>
        </div>

        {/* ── Gantt Charts ── */}
        {(() => {
          // Total pixel width = FIFO makespan × pxPerSec (shared across all rows)
          const totalPx = scaledMaxT * pxPerSec;
          // Time-axis ticks: one tick every second (capped at 20 ticks)
          const tickCount  = Math.min(Math.ceil(scaledMaxT) + 1, 21);
          const tickStep   = scaledMaxT / (tickCount - 1);

          return (
            <div ref={ganttContainerRef} className="glass rounded-2xl p-5 pb-3">
              {/* Header */}
              <div className="flex items-center justify-between mb-3">
                <h3 className="font-semibold text-sm flex items-center gap-2">
                  <span className="material-symbols-outlined text-primary" style={{ fontSize: 16 }}>view_timeline</span>
                  Gantt — Job Execution Timeline
                  <span className="text-[10px] text-on-surface-variant/40 font-normal hidden sm:inline">
                    · hover bars · glowing dots = data packets in flight
                  </span>
                </h3>
                <div className="flex items-center gap-4">
                  {[["#adc6ff","Small"],["#4edea3","Medium"],["#ffb95f","Large"]].map(([c, l]) => (
                    <div key={l} className="flex items-center gap-1 text-[10px] text-on-surface-variant">
                      <div className="w-3 h-3 rounded-sm" style={{ background: c }} />
                      {l}
                    </div>
                  ))}
                  {scale > 1 && (
                    <span className="text-[10px] font-bold text-secondary bg-secondary/10 border border-secondary/25 px-2 py-0.5 rounded-full">
                      ← scroll →
                    </span>
                  )}
                </div>
              </div>

              {/* Rows — each has its own scroll track; they sync via ganttScrollRef */}
              <div ref={ganttScrollRef} className="relative">
                {algos.map((cfg) => (
                  <GanttRow
                    key={cfg.id}
                    cfg={cfg}
                    simTime={simTime}
                    pxPerSec={pxPerSec}
                    totalPx={totalPx}
                    scrollRef={ganttScrollRef}
                  />
                ))}

                {/* Shared time-axis (also scrolls) */}
                <div
                  className="overflow-x-auto mt-1"
                  style={{ marginLeft: 144, scrollbarWidth: "thin" }}
                  onScroll={(e) => {
                    ganttScrollRef.current?.querySelectorAll(".gantt-track").forEach(
                      (el) => { el.scrollLeft = e.currentTarget.scrollLeft; }
                    );
                  }}
                >
                  <div
                    className="relative"
                    style={{ width: totalPx, height: 20 }}
                  >
                    {Array.from({ length: tickCount }, (_, i) => {
                      const t   = i * tickStep;
                      const px  = t * pxPerSec;
                      return (
                        <div
                          key={i}
                          style={{
                            position: "absolute",
                            left: px,
                            transform: i === tickCount - 1 ? "translateX(-100%)" : "translateX(-50%)",
                            fontSize: 9,
                            color: "rgba(140,144,159,0.7)",
                            fontFamily: "JetBrains Mono, monospace",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {t.toFixed(1)}s
                        </div>
                      );
                    })}
                    {/* cursor tick on axis */}
                    {simTime > 0 && (
                      <div style={{
                        position: "absolute",
                        left: simTime * pxPerSec,
                        transform: "translateX(-50%)",
                        fontSize: 9,
                        color: "#adc6ff",
                        fontFamily: "JetBrains Mono, monospace",
                        fontWeight: 700,
                        whiteSpace: "nowrap",
                        textShadow: "0 0 8px #adc6ff",
                      }}>
                        ▲ {simTime.toFixed(2)}s
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          );
        })()}

        {/* ── Live stats cards ── */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {algos.map((cfg) => <StatsCard key={cfg.id} cfg={cfg} simTime={simTime} />)}
        </div>

        {/* ── Throughput chart + Dataset scale ── */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
          {/* Throughput area chart */}
          <div className="glass rounded-2xl p-5">
            <h3 className="font-semibold text-sm flex items-center gap-2 mb-4">
              <span className="material-symbols-outlined text-tertiary" style={{ fontSize: 16 }}>show_chart</span>
              Live Jobs Completed — All 4 Algorithms
            </h3>
            <div style={{ height: 180 }}>
              <ThroughputChart history={history} />
            </div>
            <div className="flex gap-4 justify-center mt-2">
              {[["#ffb4ab","FIFO"],["#ffc59f","Adaptive"],["#ffb95f","SJF+Aging"],["#4edea3","PASTA"]].map(([c,n]) => (
                <div key={n} className="flex items-center gap-1.5 text-[10px] text-on-surface-variant">
                  <div className="w-6 h-1.5 rounded-full" style={{ background: c }} />
                  {n}
                </div>
              ))}
            </div>
          </div>

          {/* Dataset scale */}
          <DatasetScalePanel scale={scale} onScale={(n) => { resetSim(); setScale(n); }} />
        </div>

        {/* ── Completed jobs log ── */}
        <div className="glass rounded-2xl overflow-hidden">
          <div className="px-5 py-3.5 border-b border-outline-variant/15 flex items-center justify-between">
            <h3 className="font-semibold text-sm flex items-center gap-2">
              <span className="material-symbols-outlined text-secondary" style={{ fontSize: 16 }}>task_alt</span>
              Completed Jobs Log — PASTA Scheduler
            </h3>
            <div className="flex items-center gap-3">
              <span className="text-xs mono text-secondary">{doneJobs} / {totalJobs} done</span>
              {/* rolling MB counter */}
              <span className="text-[10px] text-on-surface-variant mono">
                {liveMbPasta} MB ingested
              </span>
            </div>
          </div>
          <div className="overflow-x-auto max-h-52">
            <table className="w-full text-left">
              <thead>
                <tr className="bg-surface-container-highest/50 text-[10px] font-bold uppercase tracking-widest text-on-surface-variant border-b border-outline-variant/10">
                  {["Pos","Type","Size MB","Predicted","Actual","Priority","Tier"].map((h) => (
                    <th key={h} className={`px-4 py-2 ${["Size MB","Predicted","Actual","Priority"].includes(h) ? "text-right" : h === "Tier" ? "text-center" : ""}`}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="text-xs mono divide-y divide-outline-variant/5">
                <AnimatePresence>
                  {pastaDone.map((j, i) => (
                    <motion.tr
                      key={`${scale}-${i}`}
                      initial={{ opacity: 0, backgroundColor: "rgba(78,222,163,0.12)" }}
                      animate={{ opacity: 1, backgroundColor: "rgba(0,0,0,0)" }}
                      transition={{ duration: 0.6 }}
                      className="hover:bg-surface-container/50 transition-colors"
                    >
                      <td className="px-4 py-1.5 text-secondary font-bold">{i + 1}</td>
                      <td className="px-4 py-1.5"><TypeChip type={j.job_type} size="xs" /></td>
                      <td className="px-4 py-1.5 text-right text-on-surface-variant">{j.input_size_mb}</td>
                      <td className="px-4 py-1.5 text-right text-on-surface-variant">{fmtT(j.predicted_time)}</td>
                      <td className="px-4 py-1.5 text-right text-on-surface">{fmtT(j.actual_time)}</td>
                      <td className="px-4 py-1.5 text-right text-secondary">
                        {j.priority != null ? j.priority.toFixed(4) : "—"}
                      </td>
                      <td className="px-4 py-1.5 text-center">
                        {j.tier ? <TierChip tier={j.tier} size="xs" /> : "—"}
                      </td>
                    </motion.tr>
                  ))}
                </AnimatePresence>
                {pastaDone.length === 0 && (
                  <tr>
                    <td colSpan={7} className="px-4 py-6 text-center text-on-surface-variant/40 text-xs">
                      Press <strong className="text-primary">Play</strong> to launch the simulation…
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </>
  );
}
