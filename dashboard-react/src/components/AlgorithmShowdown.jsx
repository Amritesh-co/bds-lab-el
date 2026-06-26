import { useEffect, useRef, useState } from "react";
import { motion, useInView, AnimatePresence } from "framer-motion";
import { useBenchmark } from "../data/BenchmarkContext";

// ── Static visual metadata per scheduler (numbers come from live data) ────────
const ALGO_META = [
  {
    key: "fifo",
    name: "FIFO",
    sub: "Baseline · arrival order",
    color: "#ffb4ab",
    icon: "low_priority",
    blurb: "No prediction, no reordering. Short jobs stall behind long ones — the convoy effect.",
    tags: ["No ML", "Static"],
  },
  {
    key: "sjf",
    name: "SJF",
    sub: "Shortest-Job-First",
    color: "#ffb95f",
    icon: "trending_down",
    blurb: "ML predicts duration, runs shortest first. Great latency — but long jobs can starve.",
    tags: ["XGBoost", "SJF"],
  },
  {
    key: "adaptive",
    name: "Adaptive",
    sub: "SJF + dynamic tiers",
    color: "#adc6ff",
    icon: "tune",
    blurb: "Adds per-job resource tiers (memory/cores/partitions) on top of SJF ordering.",
    tags: ["Tiers", "Adaptive"],
  },
  {
    key: "pasta",
    name: "PASTA",
    sub: "Aging + tiers + feedback",
    color: "#4edea3",
    icon: "auto_awesome",
    blurb: "Aging prevents starvation, percentile tiers adapt to the workload, feedback retrains the model.",
    tags: ["Aging α=0.7", "Feedback"],
    winner: true,
  },
];

const mean = (jobs) =>
  jobs && jobs.length ? jobs.reduce((s, j) => s + (j.actual_time ?? 0), 0) / jobs.length : 0;

// ── Animated count-up that fires when scrolled into view ──────────────────────
function CountUp({ to, decimals = 2, suffix = "", duration = 1.4, start }) {
  const [val, setVal] = useState(0);
  useEffect(() => {
    if (!start) return;
    const t0 = Date.now();
    const tick = () => {
      const p = Math.min((Date.now() - t0) / (duration * 1000), 1);
      const e = 1 - Math.pow(1 - p, 3);
      setVal(to * e);
      if (p < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  }, [start, to, duration]);
  return (
    <span>
      {val.toFixed(decimals)}
      {suffix}
    </span>
  );
}

// ── One lane of the race track ────────────────────────────────────────────────
function RaceLane({ algo, inView, i, best }) {
  // Faster algorithm => longer bar. Normalise so the winner fills the track.
  const pct = algo.makespan ? (best / algo.makespan) * 100 : 0;
  return (
    <motion.div
      initial={{ opacity: 0, x: -30 }}
      animate={inView ? { opacity: 1, x: 0 } : {}}
      transition={{ delay: 0.15 + i * 0.12, duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
      className="flex items-center gap-3"
    >
      {/* Label */}
      <div className="w-28 shrink-0 text-right">
        <div className="flex items-center justify-end gap-1.5">
          <span className="text-sm font-black" style={{ color: algo.color }}>
            {algo.name}
          </span>
          {algo.winner && (
            <span className="material-symbols-outlined fill-icon sparkle" style={{ fontSize: 15, color: algo.color }}>
              workspace_premium
            </span>
          )}
        </div>
        <div className="text-[9px] text-on-surface-variant truncate">{algo.sub}</div>
      </div>

      {/* Track */}
      <div className="relative flex-1 h-9">
        <div className="absolute inset-0 rounded-full bg-surface-container-high/60 border border-outline-variant/15" />
        <motion.div
          className="race-fill absolute inset-y-0 left-0 flex items-center justify-end pr-3"
          style={{
            background: `linear-gradient(90deg, ${algo.color}55, ${algo.color})`,
            boxShadow: `0 0 18px ${algo.color}66`,
          }}
          initial={{ width: "0%" }}
          animate={inView ? { width: `${pct}%` } : {}}
          transition={{ delay: 0.3 + i * 0.12, duration: 1.3, ease: [0.22, 1, 0.36, 1] }}
        >
          <span className="text-[11px] font-black mono text-[#051424] drop-shadow">
            {inView && <CountUp to={algo.makespan} suffix="s" start={inView} />}
          </span>
        </motion.div>
        {/* Runner emoji at the bar tip */}
        <motion.div
          className="absolute top-1/2 -translate-y-1/2 text-base"
          initial={{ left: "0%", opacity: 0 }}
          animate={inView ? { left: `calc(${pct}% - 14px)`, opacity: 1 } : {}}
          transition={{ delay: 0.3 + i * 0.12, duration: 1.3, ease: [0.22, 1, 0.36, 1] }}
        >
          {algo.winner ? "🏆" : "▸"}
        </motion.div>
      </div>

      {/* Speedup pill */}
      <div className="w-16 shrink-0 text-center">
        <span
          className="inline-block text-[11px] font-black mono px-2 py-0.5 rounded-full"
          style={{ background: `${algo.color}1f`, color: algo.color, border: `1px solid ${algo.color}40` }}
        >
          {algo.speedup.toFixed(2)}×
        </span>
      </div>
    </motion.div>
  );
}

// ── Podium block (1st / 2nd / 3rd by makespan) ────────────────────────────────
function Podium({ inView, algos }) {
  const ranked = [...algos].sort((a, b) => a.makespan - b.makespan).slice(0, 3);
  // Visual order: 2nd, 1st, 3rd
  const order = [ranked[1], ranked[0], ranked[2]];
  const heights = [78, 116, 56];
  const places = [2, 1, 3];
  const medals = ["🥈", "🥇", "🥉"];

  return (
    <div className="flex items-end justify-center gap-3 h-44 pt-2">
      {order.map((a, idx) => (
        <div key={a.key} className="flex flex-col items-center gap-1.5" style={{ width: 78 }}>
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            transition={{ delay: 0.6 + idx * 0.15 }}
            className="text-xl"
          >
            {medals[idx]}
          </motion.div>
          <motion.div
            initial={{ opacity: 0 }}
            animate={inView ? { opacity: 1 } : {}}
            transition={{ delay: 0.7 + idx * 0.15 }}
            className="text-[11px] font-black"
            style={{ color: a.color }}
          >
            {a.name}
          </motion.div>
          <motion.div
            className="w-full rounded-t-lg origin-bottom relative overflow-hidden"
            style={{
              height: heights[idx],
              background: `linear-gradient(180deg, ${a.color}, ${a.color}40)`,
              boxShadow: places[idx] === 1 ? `0 0 26px ${a.color}77` : "none",
            }}
            initial={{ scaleY: 0 }}
            animate={inView ? { scaleY: 1 } : {}}
            transition={{ delay: 0.55 + idx * 0.15, duration: 0.7, ease: [0.22, 1, 0.36, 1] }}
          >
            <div className="absolute inset-x-0 top-1 text-center text-[10px] font-black text-[#051424] mono">
              {a.makespan.toFixed(2)}s
            </div>
            <div className="absolute inset-x-0 bottom-1 text-center text-2xl font-black text-[#051424]/30">
              {places[idx]}
            </div>
          </motion.div>
        </div>
      ))}
    </div>
  );
}

export default function AlgorithmShowdown() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, margin: "-80px" });
  const { FIFO, SJF, ADAPTIVE, PASTA, makespans } = useBenchmark();

  // Merge static visual metadata with live makespan / latency / speedup.
  const jobsByKey = { fifo: FIFO, sjf: SJF, adaptive: ADAPTIVE, pasta: PASTA };
  const fifoMs = makespans.fifo || 1;
  const ALGOS = ALGO_META.map((m) => {
    const ms = makespans[m.key] || 0;
    return {
      ...m,
      makespan: ms,
      latency: mean(jobsByKey[m.key]),
      speedup: ms ? fifoMs / ms : 1,
    };
  });
  const WORST = Math.max(...ALGOS.map((a) => a.makespan));
  const BEST = Math.min(...ALGOS.map((a) => a.makespan));
  const reduction = WORST ? (((WORST - BEST) / WORST) * 100).toFixed(0) : "0";

  return (
    <motion.section
      ref={ref}
      initial={{ opacity: 0, y: 24 }}
      animate={inView ? { opacity: 1, y: 0 } : {}}
      transition={{ duration: 0.6, ease: [0.22, 1, 0.36, 1] }}
      className="aurora-border"
      style={{ "--aurora": "#4edea3" }}
    >
      <div className="glass-dark rounded-2xl p-6 overflow-hidden relative">
        {/* Header */}
        <div className="flex items-center justify-between flex-wrap gap-3 mb-5">
          <div>
            <h2 className="text-lg font-black flex items-center gap-2 text-on-surface">
              <span className="material-symbols-outlined text-secondary breathe rounded-full" style={{ fontSize: 22 }}>
                trophy
              </span>
              Algorithm Showdown
              <span className="text-[10px] font-bold text-secondary bg-secondary/10 border border-secondary/25 px-2 py-0.5 rounded-full tracking-widest">
                4 SCHEDULERS
              </span>
            </h2>
            <p className="text-xs text-on-surface-variant mt-1">
              Head-to-head on the same 15-job queue — longer bar = faster finish.
            </p>
          </div>
          <div className="text-right">
            <div className="text-3xl font-black mono text-secondary leading-none">
              <CountUp to={Number(reduction)} decimals={0} suffix="%" start={inView} />
            </div>
            <div className="text-[10px] uppercase tracking-widest text-on-surface-variant mt-1">
              faster makespan
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Race track */}
          <div className="lg:col-span-2 space-y-3 flex flex-col justify-center">
            {ALGOS.map((a, i) => (
              <RaceLane key={a.key} algo={a} inView={inView} i={i} best={BEST} />
            ))}
            <div className="flex justify-between text-[9px] text-on-surface-variant/60 mono pl-32 pr-16 pt-1">
              <span>0s</span>
              <span>finish line →</span>
            </div>
          </div>

          {/* Podium */}
          <div className="glass rounded-xl p-4 flex flex-col">
            <h3 className="text-xs font-bold uppercase tracking-widest text-on-surface-variant mb-1 text-center">
              Makespan Podium
            </h3>
            <Podium inView={inView} algos={ALGOS} />
            <div className="mt-3 pt-3 border-t border-outline-variant/10 text-center">
              <p className="text-[10px] text-on-surface-variant leading-relaxed">
                <span className="text-secondary font-bold">PASTA</span> wins on speed{" "}
                <span className="text-on-surface">and</span> fairness — the only scheduler
                that is both fast and starvation-free.
              </p>
            </div>
          </div>
        </div>

        {/* Detail strip */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-5">
          {ALGOS.map((a, i) => (
            <motion.div
              key={a.key}
              initial={{ opacity: 0, y: 16 }}
              animate={inView ? { opacity: 1, y: 0 } : {}}
              transition={{ delay: 0.8 + i * 0.1 }}
              whileHover={{ y: -4 }}
              className={`shine rounded-xl p-3.5 border relative ${a.winner ? "breathe" : ""}`}
              style={{ background: `${a.color}0d`, borderColor: `${a.color}33` }}
            >
              <div className="flex items-center justify-between mb-2">
                <span className="material-symbols-outlined" style={{ fontSize: 18, color: a.color }}>
                  {a.icon}
                </span>
                {a.winner && (
                  <span className="text-[8px] font-black px-1.5 py-0.5 rounded-full bg-secondary text-[#051424]">
                    BEST
                  </span>
                )}
              </div>
              <div className="text-sm font-black" style={{ color: a.color }}>
                {a.name}
              </div>
              <div className="flex items-baseline gap-1 mt-1">
                <span className="text-2xl font-black mono text-on-surface">
                  {inView && <CountUp to={a.makespan} suffix="s" start={inView} />}
                </span>
              </div>
              <div className="text-[9px] text-on-surface-variant mt-0.5">
                {a.latency.toFixed(2)}s avg latency
              </div>
              <p className="text-[10px] text-on-surface-variant/80 leading-snug mt-2">{a.blurb}</p>
              <div className="flex gap-1 flex-wrap mt-2">
                {a.tags.map((t) => (
                  <span
                    key={t}
                    className="text-[8px] px-1.5 py-0.5 rounded-full font-bold"
                    style={{ background: `${a.color}1a`, color: a.color }}
                  >
                    {t}
                  </span>
                ))}
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </motion.section>
  );
}
