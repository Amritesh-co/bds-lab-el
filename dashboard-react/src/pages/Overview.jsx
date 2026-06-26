import { useEffect, useRef, useState } from "react";
import { motion, useInView } from "framer-motion";
import {
  RadarChart, PolarGrid, PolarAngleAxis, Radar, ResponsiveContainer,
  BarChart, Bar, XAxis, YAxis, Tooltip, Cell, CartesianGrid,
} from "recharts";
import { fmtT, typeColor } from "../data/benchmarkData";
import { useBenchmark } from "../data/BenchmarkContext";
import TypeChip, { TierChip } from "../components/TypeChip";
import AlgorithmShowdown from "../components/AlgorithmShowdown";

// Mean of actual_time across a job list (avg job latency).
const avgLatency = (jobs) =>
  jobs && jobs.length ? jobs.reduce((s, j) => s + (j.actual_time ?? 0), 0) / jobs.length : 0;

// ── Animated counter ──────────────────────────────────────────────────────────
function AnimCounter({ to, suffix = "", decimals = 2, duration = 1.5 }) {
  const [val, setVal] = useState(0);
  const ref = useRef(null);
  const inView = useInView(ref, { once: true });

  useEffect(() => {
    if (!inView) return;
    const start = Date.now();
    const tick = () => {
      const p = Math.min((Date.now() - start) / (duration * 1000), 1);
      const ease = 1 - Math.pow(1 - p, 3);
      setVal(parseFloat((to * ease).toFixed(decimals)));
      if (p < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  }, [inView, to, duration, decimals]);

  return (
    <span ref={ref}>
      {val.toFixed(decimals)}
      {suffix}
    </span>
  );
}

// ── Speedup bar (animated on mount) ──────────────────────────────────────────
function SpeedupBar({ label, value, max, color, sublabel, delay = 0 }) {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true });
  return (
    <div ref={ref}>
      <div className="flex justify-between text-xs mb-1.5">
        <span className="text-on-surface-variant">{label}</span>
        <span className="mono font-bold" style={{ color }}>
          {value}×
        </span>
      </div>
      <div className="h-2.5 bg-surface-container-high rounded-full overflow-hidden">
        <motion.div
          className="h-full rounded-full"
          style={{ background: color }}
          initial={{ width: 0 }}
          animate={inView ? { width: `${(value / max) * 100}%` } : {}}
          transition={{ duration: 1, delay, ease: [0.22, 1, 0.36, 1] }}
        />
      </div>
      {sublabel && (
        <p className="text-[10px] text-on-surface-variant mt-1">{sublabel}</p>
      )}
    </div>
  );
}

// ── Radar data ────────────────────────────────────────────────────────────────
const radarData = [
  { metric: "Throughput",    FIFO: 40, SJF: 72, PASTA: 90 },
  { metric: "Fairness",      FIFO: 30, SJF: 45, PASTA: 88 },
  { metric: "Latency",       FIFO: 35, SJF: 75, PASTA: 85 },
  { metric: "Starvation",    FIFO: 20, SJF: 35, PASTA: 95 },
  { metric: "Adaptability",  FIFO: 25, SJF: 60, PASTA: 92 },
];

const cardVariants = {
  hidden: { opacity: 0, y: 24 },
  visible: (i) => ({
    opacity: 1,
    y: 0,
    transition: { delay: i * 0.12, duration: 0.5, ease: [0.22, 1, 0.36, 1] },
  }),
};

export default function Overview() {
  const { FIFO, SJF, PASTA, makespans } = useBenchmark();

  // Derived, live stats
  const fifoMs = makespans.fifo, sjfMs = makespans.sjf, pastaMs = makespans.pasta;
  const spSjf = sjfMs ? fifoMs / sjfMs : 1;
  const spPasta = pastaMs ? fifoMs / pastaMs : 1;
  const spMax = Math.max(1.6, spPasta * 1.04);
  const latFifo = avgLatency(FIFO), latSjf = avgLatency(SJF), latPasta = avgLatency(PASTA);
  const latencyData = [
    { algo: "FIFO",  avg: +latFifo.toFixed(2),  color: "#ffb4ab" },
    { algo: "SJF",   avg: +latSjf.toFixed(2),   color: "#ffb95f" },
    { algo: "PASTA", avg: +latPasta.toFixed(2), color: "#4edea3" },
  ];

  return (
    <div className="max-w-[1600px] mx-auto px-6 py-6 space-y-6">
      {/* Status bar */}
      <motion.div
        initial={{ opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex items-center justify-between flex-wrap gap-3"
      >
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-secondary pulse-dot" />
          <span className="text-xs mono text-secondary tracking-widest uppercase">
            Benchmark Complete · 15-Job Queue · Local[*] Mode
          </span>
        </div>
        <div className="flex gap-2">
          {[
            { icon: "database", label: "Google Cluster Trace 2011" },
            { icon: "psychology", label: "XGBoost + Random Forest" },
          ].map((b) => (
            <div
              key={b.label}
              className="glass rounded-lg px-3 py-1 flex items-center gap-1.5 text-xs text-on-surface-variant"
            >
              <span className="material-symbols-outlined" style={{ fontSize: 14 }}>
                {b.icon}
              </span>
              {b.label}
            </div>
          ))}
        </div>
      </motion.div>

      {/* ── Algorithm Showdown (all 4 schedulers) ── */}
      <AlgorithmShowdown />

      {/* ── Hero metric cards ── */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
        {/* FIFO */}
        <motion.div
          custom={0}
          variants={cardVariants}
          initial="hidden"
          animate="visible"
          whileHover={{ y: -4, transition: { duration: 0.2 } }}
          className="glass rounded-2xl p-6 border-t-4 border-error glow-red relative overflow-hidden group"
        >
          <div className="absolute inset-0 bg-gradient-to-br from-error/5 to-transparent" />
          <div className="relative">
            <div className="flex justify-between items-start mb-4">
              <span className="text-[11px] font-bold uppercase tracking-widest text-error/80">
                FIFO · Baseline
              </span>
              <span className="material-symbols-outlined text-error/30 group-hover:text-error/60 transition-colors" style={{ fontSize: 22 }}>
                low_priority
              </span>
            </div>
            <div className="text-6xl font-black text-on-surface mb-1 mono">
              <AnimCounter to={fifoMs} suffix="s" />
            </div>
            <div className="text-[10px] uppercase tracking-wider text-on-surface-variant mb-3">
              Total Makespan
            </div>
            <p className="text-xs text-on-surface-variant leading-relaxed">
              Jobs run in arrival order. No prediction. No reordering. The{" "}
              <em className="text-error">Convoy Effect</em> delays short jobs.
            </p>
            <div className="mt-4 flex gap-1.5 flex-wrap">
              {["No ML", "Static Order", "Convoy Effect"].map((tag) => (
                <span key={tag} className="bg-error/10 border border-error/25 text-error text-[10px] px-2 py-0.5 rounded-full font-medium">
                  {tag}
                </span>
              ))}
            </div>
          </div>
          {/* Watermark icon */}
          <div className="absolute -right-6 -bottom-6 opacity-[0.04] group-hover:opacity-[0.07] transition-opacity">
            <span className="material-symbols-outlined" style={{ fontSize: 130 }}>sort</span>
          </div>
        </motion.div>

        {/* SJF */}
        <motion.div
          custom={1}
          variants={cardVariants}
          initial="hidden"
          animate="visible"
          whileHover={{ y: -4, transition: { duration: 0.2 } }}
          className="glass rounded-2xl p-6 border-t-4 border-tertiary glow-yellow relative overflow-hidden group"
        >
          <div className="absolute inset-0 bg-gradient-to-br from-tertiary/5 to-transparent" />
          <div className="relative">
            <div className="flex justify-between items-start mb-4">
              <span className="text-[11px] font-bold uppercase tracking-widest text-tertiary/80">
                SJF / Adaptive
              </span>
              <span className="material-symbols-outlined text-tertiary/30 group-hover:text-tertiary/60 transition-colors" style={{ fontSize: 22 }}>
                analytics
              </span>
            </div>
            <div className="text-6xl font-black text-on-surface mb-1 mono">
              <AnimCounter to={sjfMs} suffix="s" />
            </div>
            <div className="text-[10px] uppercase tracking-wider text-on-surface-variant mb-3">
              Total Makespan
            </div>
            <div className="flex items-center gap-1.5 mb-3 bg-tertiary/10 border border-tertiary/25 rounded-lg px-2.5 py-1 w-fit">
              <span className="material-symbols-outlined text-tertiary" style={{ fontSize: 13 }}>bolt</span>
              <span className="text-[10px] text-tertiary font-bold tracking-wider">ML PREDICTION ACTIVE</span>
            </div>
            <p className="text-xs text-on-surface-variant leading-relaxed">
              ML predicts job duration → reorders shortest-first. Optimal average wait time but risks starvation.
            </p>
            <div className="mt-4 flex gap-1.5 flex-wrap">
              {["XGBoost Predict", "SJF Order", "Fixed Tiers"].map((tag) => (
                <span key={tag} className="bg-tertiary/10 border border-tertiary/25 text-tertiary text-[10px] px-2 py-0.5 rounded-full font-medium">
                  {tag}
                </span>
              ))}
            </div>
          </div>
        </motion.div>

        {/* PASTA */}
        <motion.div
          custom={2}
          variants={cardVariants}
          initial="hidden"
          animate="visible"
          whileHover={{ y: -4, transition: { duration: 0.2 } }}
          className="glass rounded-2xl p-6 border-t-4 border-secondary glow-green relative overflow-hidden group"
        >
          <div className="absolute inset-0 bg-gradient-to-br from-secondary/8 via-secondary/3 to-transparent" />
          <div className="relative">
            <div className="flex justify-between items-start mb-4">
              <span className="text-[11px] font-bold uppercase tracking-widest text-secondary">
                PASTA · Winner
              </span>
              <div className="bg-secondary text-[#051424] px-2.5 py-0.5 rounded-full flex items-center gap-1">
                <span className="material-symbols-outlined fill-icon" style={{ fontSize: 13 }}>emoji_events</span>
                <span className="text-[10px] font-black">BEST</span>
              </div>
            </div>
            <div className="text-6xl font-black text-secondary mb-1 mono">
              <AnimCounter to={pastaMs} suffix="s" />
            </div>
            <div className="text-[10px] uppercase tracking-wider text-on-surface-variant mb-3">
              Total Makespan
            </div>
            <p className="text-xs text-on-surface-variant leading-relaxed">
              Aging prevents starvation. Dynamic percentile tiers adapt to workload. Feedback loop retrains model.
            </p>
            <div className="mt-4 flex gap-1.5 flex-wrap">
              {["Aging α=0.70", "Dynamic Tiers", "Feedback Loop"].map((tag) => (
                <span key={tag} className="bg-secondary/10 border border-secondary/30 text-secondary text-[10px] px-2 py-0.5 rounded-full font-medium">
                  {tag}
                </span>
              ))}
            </div>
          </div>
          <div className="absolute -right-6 -bottom-6 opacity-[0.05] group-hover:opacity-[0.1] transition-opacity">
            <span className="material-symbols-outlined" style={{ fontSize: 130 }}>auto_awesome</span>
          </div>
        </motion.div>
      </div>

      {/* ── Middle: table + charts ── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        {/* Job Queue Table */}
        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.3, duration: 0.5 }}
          className="col-span-2 glass rounded-2xl overflow-hidden flex flex-col"
        >
          <div className="px-5 py-3.5 border-b border-outline-variant/15 flex justify-between items-center">
            <h3 className="font-semibold text-sm flex items-center gap-2">
              <span className="material-symbols-outlined text-primary" style={{ fontSize: 18 }}>reorder</span>
              15-Job Benchmark Queue — Actual Execution Data
            </h3>
            <span className="text-[10px] text-on-surface-variant mono bg-surface-container-high px-2 py-0.5 rounded-full">
              seed=42 · reproducible
            </span>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="bg-surface-container-highest/50 text-[10px] font-bold uppercase tracking-widest text-on-surface-variant border-b border-outline-variant/10">
                  {["#", "Type", "Size MB", "Parts", "FIFO Actual", "SJF Actual", "PASTA Actual", "PASTA Tier"].map((h) => (
                    <th key={h} className={`px-4 py-2.5 ${["Size MB","Parts","FIFO Actual","SJF Actual","PASTA Actual"].includes(h) ? "text-right" : ""}`}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="text-xs mono divide-y divide-outline-variant/5">
                {FIFO.map((f, i) => {
                  const s = SJF[i];
                  const p = PASTA[i];
                  return (
                    <motion.tr
                      key={i}
                      initial={{ opacity: 0, x: -10 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: 0.05 * i }}
                      className="hover:bg-surface-container/60 transition-colors group"
                    >
                      <td className="px-4 py-2 text-primary font-bold">{i + 1}</td>
                      <td className="px-4 py-2"><TypeChip type={f.job_type} /></td>
                      <td className="px-4 py-2 text-right text-on-surface-variant">{f.input_size_mb}</td>
                      <td className="px-4 py-2 text-right text-on-surface-variant">{f.num_partitions}</td>
                      <td className="px-4 py-2 text-right text-error">{fmtT(f.actual_time)}</td>
                      <td className="px-4 py-2 text-right text-tertiary">{s ? fmtT(s.actual_time) : "—"}</td>
                      <td className="px-4 py-2 text-right text-secondary">{p ? fmtT(p.actual_time) : "—"}</td>
                      <td className="px-4 py-2 text-center">
                        {p ? <TierChip tier={p.tier} /> : "—"}
                      </td>
                    </motion.tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </motion.div>

        {/* Right col */}
        <div className="flex flex-col gap-5">
          {/* Speedup */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: 0.35 }}
            className="glass rounded-2xl p-5"
          >
            <h4 className="font-semibold text-sm mb-4 flex items-center gap-2">
              <span className="material-symbols-outlined text-primary" style={{ fontSize: 16 }}>speed</span>
              Speedup vs FIFO
            </h4>
            <div className="space-y-4">
              <SpeedupBar label="FIFO"           value={1.00}                  max={spMax} color="#ffb4ab" delay={0.4} />
              <SpeedupBar label="SJF / Adaptive" value={+spSjf.toFixed(2)}      max={spMax} color="#ffb95f" delay={0.55} />
              <SpeedupBar label="PASTA 🏆"       value={+spPasta.toFixed(2)}    max={spMax} color="#4edea3" delay={0.7} />
            </div>
            {/* Avg latency mini-chart */}
            <div className="mt-4 pt-4 border-t border-outline-variant/10">
              <p className="text-[10px] text-on-surface-variant uppercase tracking-widest mb-2">Avg Job Latency</p>
              <ResponsiveContainer width="100%" height={80}>
                <BarChart data={latencyData} barSize={28} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(66,71,84,0.3)" />
                  <XAxis dataKey="algo" tick={{ fill: "#c2c6d6", fontSize: 10 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: "#c2c6d6", fontSize: 10 }} axisLine={false} tickLine={false} />
                  <Tooltip
                    contentStyle={{ background: "#122131", border: "1px solid #424754", borderRadius: 8 }}
                    labelStyle={{ color: "#d4e4fa", fontSize: 11 }}
                    formatter={(v) => [`${v}s`, "Avg Latency"]}
                  />
                  <Bar dataKey="avg" radius={[4, 4, 0, 0]}>
                    {latencyData.map((e, i) => (
                      <Cell key={i} fill={e.color} fillOpacity={0.85} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </motion.div>

          {/* Radar / scoring */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: 0.5 }}
            className="glass rounded-2xl p-5"
          >
            <h4 className="font-semibold text-sm mb-2 flex items-center gap-2">
              <span className="material-symbols-outlined text-secondary" style={{ fontSize: 16 }}>radar</span>
              Algorithm Comparison
            </h4>
            <ResponsiveContainer width="100%" height={180}>
              <RadarChart data={radarData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                <PolarGrid stroke="rgba(66,71,84,0.5)" />
                <PolarAngleAxis dataKey="metric" tick={{ fill: "#8c909f", fontSize: 9 }} />
                <Radar name="FIFO"  dataKey="FIFO"  stroke="#ffb4ab" fill="#ffb4ab" fillOpacity={0.1} strokeWidth={1.5} />
                <Radar name="SJF"   dataKey="SJF"   stroke="#ffb95f" fill="#ffb95f" fillOpacity={0.1} strokeWidth={1.5} />
                <Radar name="PASTA" dataKey="PASTA" stroke="#4edea3" fill="#4edea3" fillOpacity={0.25} strokeWidth={2} />
              </RadarChart>
            </ResponsiveContainer>
            <div className="flex justify-center gap-4 text-[10px] text-on-surface-variant mt-1">
              {[["#ffb4ab","FIFO"],["#ffb95f","SJF"],["#4edea3","PASTA"]].map(([c,n]) => (
                <div key={n} className="flex items-center gap-1">
                  <div className="w-2.5 h-2.5 rounded-sm" style={{ background: c }} />
                  {n}
                </div>
              ))}
            </div>
          </motion.div>

          {/* Dataset sources */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: 0.6 }}
            className="glass rounded-2xl p-5"
          >
            <h4 className="font-semibold text-sm mb-3 flex items-center gap-2">
              <span className="material-symbols-outlined text-primary" style={{ fontSize: 16 }}>dataset</span>
              Training Datasets
            </h4>
            <div className="space-y-3">
              <div className="bg-primary/5 border border-primary/20 rounded-xl p-3.5">
                <div className="text-xs font-bold text-primary mb-1">📊 Google Cluster Trace 2011</div>
                <div className="text-3xl font-black mono text-on-surface">34.9M</div>
                <div className="text-[10px] text-on-surface-variant mt-1">rows · real production cluster workload</div>
              </div>
              <div className="bg-secondary/5 border border-secondary/20 rounded-xl p-3.5">
                <div className="text-xs font-bold text-secondary mb-1">🔄 Local Feedback Loop</div>
                <div className="text-3xl font-black mono text-on-surface">70</div>
                <div className="text-[10px] text-on-surface-variant mt-1">rows · actual Spark timings + feedback</div>
                <div className="mt-1.5 text-[9px] bg-secondary/10 border border-secondary/20 rounded-full px-2 py-0.5 text-secondary w-fit">
                  Auto-retrains after each run
                </div>
              </div>
            </div>
          </motion.div>
        </div>
      </div>
    </div>
  );
}
