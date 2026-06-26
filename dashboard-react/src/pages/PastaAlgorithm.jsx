import { useRef } from "react";
import { motion, useInView } from "framer-motion";
import { typeColor, fmtT } from "../data/benchmarkData";
import { useBenchmark } from "../data/BenchmarkContext";
import TypeChip, { TierChip } from "../components/TypeChip";

// ── Priority bar ──────────────────────────────────────────────────────────────
function PriorityBar({ value, color, delay = 0 }) {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true });
  return (
    <div ref={ref} className="h-2 bg-surface-container-high rounded-full overflow-hidden" style={{ minWidth: 80 }}>
      <motion.div
        className="h-full rounded-full"
        style={{ background: color }}
        initial={{ width: 0 }}
        animate={inView ? { width: `${value * 100}%` } : {}}
        transition={{ duration: 1, delay, ease: [0.22, 1, 0.36, 1] }}
      />
    </div>
  );
}

// ── Feedback loop step ────────────────────────────────────────────────────────
function FlowStep({ icon, label, color, fill = false }) {
  return (
    <motion.div
      whileHover={{ y: -3, scale: 1.05 }}
      className="bg-surface-container rounded-xl px-4 py-3 flex flex-col items-center gap-1.5 min-w-[90px] cursor-default"
      style={{ border: `1px solid ${color}25` }}
    >
      <span
        className={`material-symbols-outlined ${fill ? "fill-icon" : ""}`}
        style={{ fontSize: 26, color }}
      >
        {icon}
      </span>
      <span className="text-[10px] font-bold text-center leading-tight" style={{ color }}>
        {label}
      </span>
    </motion.div>
  );
}

const problems = [
  {
    title: "Starvation",
    desc: "In pure SJF, large jobs can wait indefinitely if short jobs keep arriving. A 10-minute PageRank job might never run.",
    solution: "Aging term: as wait_time grows, priority climbs toward 1.0. Every job is guaranteed to eventually reach the top.",
  },
  {
    title: "Tier Drift",
    desc: "SJF uses fixed thresholds: small < 10s, large > 60s. On this hardware (all jobs 0.1–2s), every job lands in 'small' — defeating the purpose.",
    solution: "Dynamic tiers: percentile-based boundaries (p33/p66) adapt to whatever workload is present. Always assigns 3 tiers correctly.",
  },
  {
    title: "No Learning",
    desc: "SJF uses the model as-is. If predictions are off (trained on Google Cloud but running locally), errors accumulate and ordering degrades.",
    solution: "Feedback loop: after each run, actual times are appended to raw_metrics.csv and the model retrains. Predictions improve run-over-run.",
  },
];

export default function PastaAlgorithm() {
  const { PASTA } = useBenchmark();
  const MIN_P = 0.135, MAX_P = 1.1191;

  return (
    <div className="max-w-[1600px] mx-auto px-6 py-6 space-y-6">
      <motion.div initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }}>
        <h1 className="text-3xl font-black text-on-surface">PASTA Algorithm — Deep Dive</h1>
        <p className="text-sm text-on-surface-variant mt-1">
          Predictive Adaptive Scheduling with Timed Aging · Three enhancements over pure SJF
        </p>
      </motion.div>

      {/* ── Priority Formula ── */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="glass rounded-2xl p-6"
      >
        <h3 className="font-semibold text-xs text-on-surface-variant uppercase tracking-widest mb-5">
          Priority Formula
        </h3>
        <div className="bg-surface-container rounded-2xl p-6 text-center space-y-5 border border-outline-variant/20">
          {/* Main formula */}
          <div className="text-xl font-bold mono space-x-2 leading-loose">
            <span className="text-on-surface">priority</span>
            <span className="text-outline mx-2">=</span>
            <span className="text-primary text-3xl font-black">α</span>
            <span className="text-outline">×</span>
            <span className="text-primary">norm_sjf</span>
            <span className="text-outline mx-2">+</span>
            <span className="text-tertiary text-3xl font-black">β</span>
            <span className="text-outline">×</span>
            <span className="text-tertiary">norm_aging</span>
          </div>

          {/* Sub-formulas */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-2">
            <div className="bg-primary/5 border border-primary/20 rounded-xl p-4 text-left">
              <div className="text-xs font-bold text-primary mb-2">SJF Component (α = 0.70)</div>
              <div className="mono text-xs text-on-surface-variant mb-3 leading-relaxed">
                norm_sjf = 1 − (pred − min_pred) / (max_pred − min_pred + ε)
              </div>
              <div className="text-xs text-on-surface-variant">
                → <span className="text-primary font-bold">1.0</span> for the shortest predicted job,{" "}
                <span className="text-primary font-bold">0.0</span> for the longest
              </div>
            </div>
            <div className="bg-tertiary/5 border border-tertiary/20 rounded-xl p-4 text-left">
              <div className="text-xs font-bold text-tertiary mb-2">Aging Component (β = 0.30)</div>
              <div className="mono text-xs text-on-surface-variant mb-3 leading-relaxed">
                norm_aging = wait_time / (max_wait + ε)
              </div>
              <div className="text-xs text-on-surface-variant">
                → <span className="text-tertiary font-bold">0.0</span> when just arrived, rises toward{" "}
                <span className="text-tertiary font-bold">1.0</span> as job waits longer
              </div>
            </div>
          </div>

          <div className="text-xs text-on-surface-variant bg-secondary/5 border border-secondary/20 rounded-xl p-3">
            <strong className="text-secondary">In batch mode</strong> (all 15 jobs arrive at t=0): wait_time=0 for all
            → norm_aging=0 → PASTA reduces to <strong className="text-on-surface">pure SJF</strong>.
            In streaming mode, aged jobs get a priority boost, preventing starvation of large jobs.
          </div>
        </div>
      </motion.div>

      {/* ── 3 Problems → 3 Solutions ── */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
        {problems.map((p, i) => (
          <motion.div
            key={p.title}
            initial={{ opacity: 0, y: 24 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.15 + i * 0.1 }}
            whileHover={{ y: -3 }}
            className="glass rounded-2xl p-5 border border-error/20 relative overflow-hidden group"
          >
            <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-error/60 to-secondary/40 opacity-0 group-hover:opacity-100 transition-opacity" />
            <div className="flex items-center gap-2 mb-3">
              <span className="material-symbols-outlined text-error" style={{ fontSize: 20 }}>warning</span>
              <span className="text-xs font-bold text-error uppercase tracking-widest">Problem {i + 1} · {p.title}</span>
            </div>
            <p className="text-xs text-on-surface-variant mb-4 leading-relaxed">{p.desc}</p>
            <div className="border-t border-outline-variant/15 pt-4">
              <div className="flex items-center gap-2 mb-2">
                <span className="material-symbols-outlined text-secondary" style={{ fontSize: 16 }}>check_circle</span>
                <span className="text-xs font-bold text-secondary">PASTA Solution</span>
              </div>
              <p className="text-xs text-secondary/80 leading-relaxed">{p.solution}</p>
            </div>
          </motion.div>
        ))}
      </div>

      {/* ── Priority Ranking Table ── */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.35 }}
        className="glass rounded-2xl overflow-hidden"
      >
        <div className="px-5 py-3.5 border-b border-outline-variant/15 flex justify-between items-center">
          <h3 className="font-semibold text-sm flex items-center gap-2">
            <span className="material-symbols-outlined text-secondary" style={{ fontSize: 16 }}>format_list_numbered</span>
            15-Job PASTA Priority Ranking (Actual Run)
          </h3>
          <span className="text-xs text-secondary mono">Higher score = scheduled first</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left">
            <thead>
              <tr className="bg-surface-container-highest/50 text-[10px] font-bold uppercase tracking-widest text-on-surface-variant border-b border-outline-variant/10">
                {["Pos","Type","Size MB","Predicted","Actual","Priority","Tier","Shuffle Parts","Priority Bar"].map((h) => (
                  <th key={h} className={`px-4 py-2.5 ${["Size MB","Predicted","Actual","Priority"].includes(h) ? "text-right" : ["Tier","Shuffle Parts","Priority Bar"].includes(h) ? "text-center" : ""}`}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="text-xs mono divide-y divide-outline-variant/5">
              {PASTA.map((j, i) => {
                const parts = { small: "4", medium: "8", large: "16" }[j.tier] || "?";
                const c = typeColor(j.job_type);
                return (
                  <motion.tr
                    key={i}
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: 0.04 * i }}
                    className="hover:bg-surface-container/50 transition-colors"
                  >
                    <td className="px-4 py-2.5 text-secondary font-bold">{i + 1}</td>
                    <td className="px-4 py-2.5"><TypeChip type={j.job_type} /></td>
                    <td className="px-4 py-2.5 text-right text-on-surface-variant">{j.input_size_mb}</td>
                    <td className="px-4 py-2.5 text-right text-tertiary">{fmtT(j.predicted_time)}</td>
                    <td className="px-4 py-2.5 text-right text-on-surface">{fmtT(j.actual_time)}</td>
                    <td className="px-4 py-2.5 text-right text-secondary font-bold">{j.priority.toFixed(4)}</td>
                    <td className="px-4 py-2.5 text-center"><TierChip tier={j.tier} /></td>
                    <td className="px-4 py-2.5 text-center text-on-surface">{parts}</td>
                    <td className="px-4 py-2.5" style={{ minWidth: 100 }}>
                      <PriorityBar value={j.priority} color={c} delay={0.05 * i} />
                    </td>
                  </motion.tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </motion.div>

      {/* ── Dynamic Tier Boundaries ── */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.45 }}
        className="glass rounded-2xl p-6"
      >
        <h3 className="font-semibold text-xs text-on-surface-variant uppercase tracking-widest mb-4">
          Dynamic Tier Boundaries (p33 / p66)
        </h3>
        <div className="mb-4 grid grid-cols-3 gap-4 text-xs">
          {[
            { label: "SMALL tier",  color: "#adc6ff", range: "predicted < p33 = 0.53s",                 parts: 4 },
            { label: "MEDIUM tier", color: "#4edea3", range: "0.53s ≤ predicted < p66 = 0.75s",          parts: 8 },
            { label: "LARGE tier",  color: "#ffb95f", range: "predicted ≥ p66 = 0.75s",                  parts: 16 },
          ].map((t) => (
            <motion.div
              key={t.label}
              whileHover={{ scale: 1.02 }}
              className="rounded-xl p-4 text-center"
              style={{ background: `${t.color}10`, border: `1px solid ${t.color}25` }}
            >
              <div className="font-bold mb-2" style={{ color: t.color }}>{t.label}</div>
              <div className="text-on-surface-variant text-[11px] mb-1 mono">{t.range}</div>
              <div className="text-[10px] text-on-surface-variant">
                shuffle.partitions ={" "}
                <span className="font-bold" style={{ color: t.color }}>{t.parts}</span>
              </div>
            </motion.div>
          ))}
        </div>

        {/* Number line with dots */}
        <div
          className="relative h-14 rounded-xl overflow-hidden border border-outline-variant/15"
          style={{ background: "#0d1c2d" }}
        >
          {/* Segments */}
          <div className="absolute inset-0 flex">
            <div className="flex items-center justify-center text-[10px] font-bold text-primary" style={{ width: "47.6%", background: "rgba(173,198,255,0.08)" }}>
              SMALL (0–0.53s)
            </div>
            <div className="flex items-center justify-center text-[10px] font-bold text-secondary" style={{ width: "19.6%", background: "rgba(78,222,163,0.08)" }}>
              MEDIUM
            </div>
            <div className="flex items-center justify-center text-[10px] font-bold text-tertiary flex-1" style={{ background: "rgba(255,185,95,0.08)" }}>
              LARGE (&gt;0.75s)
            </div>
          </div>

          {/* Boundary lines */}
          <div className="absolute top-0 bottom-0 w-px bg-primary/40" style={{ left: "47.6%" }} />
          <div className="absolute top-0 bottom-0 w-px bg-secondary/40" style={{ left: "67.2%" }} />

          {/* Predicted time dots */}
          {PASTA.map((j, i) => {
            const pct = ((j.predicted_time - MIN_P) / (MAX_P - MIN_P)) * 100;
            const c = typeColor(j.job_type);
            return (
              <motion.div
                key={i}
                initial={{ scale: 0 }}
                animate={{ scale: 1 }}
                transition={{ delay: 0.05 * i, type: "spring" }}
                title={`${j.job_type} · pred=${fmtT(j.predicted_time)}`}
                className="absolute rounded-full cursor-pointer z-10"
                style={{
                  left: `${pct}%`,
                  top: "50%",
                  width: 9,
                  height: 9,
                  background: c,
                  border: "2px solid rgba(5,20,36,0.8)",
                  transform: "translate(-50%,-50%)",
                  boxShadow: `0 0 6px ${c}80`,
                }}
              />
            );
          })}
        </div>

        <div className="flex justify-between text-[9px] mono text-on-surface-variant/60 mt-1.5 px-1">
          {["0.13s", "0.26s", "0.39s", "0.53s p33", "0.66s", "0.75s p66", "1.12s"].map((l) => (
            <span key={l}>{l}</span>
          ))}
        </div>
      </motion.div>

      {/* ── Feedback Loop ── */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.55 }}
        className="glass rounded-2xl p-6"
      >
        <h3 className="font-semibold text-xs text-on-surface-variant uppercase tracking-widest mb-5">
          Feedback Loop — How PASTA Learns
        </h3>
        <div className="flex items-center gap-2 flex-wrap">
          <FlowStep icon="queue"          label="Job Spec"         color="#adc6ff" />
          <span className="material-symbols-outlined text-outline-variant/60">arrow_forward</span>
          <FlowStep icon="psychology"     label="ML Predict"       color="#ffb95f" />
          <span className="material-symbols-outlined text-outline-variant/60">arrow_forward</span>
          <FlowStep icon="sort"           label="PASTA Order"      color="#4edea3" />
          <span className="material-symbols-outlined text-outline-variant/60">arrow_forward</span>
          <FlowStep icon="bolt"           label="Run Jobs"         color="#d4e4fa" />
          <span className="material-symbols-outlined text-outline-variant/60">arrow_forward</span>
          <FlowStep icon="timer"          label="Record Actual"    color="#ffb95f" />
          <span className="material-symbols-outlined text-outline-variant/60">arrow_forward</span>
          <FlowStep icon="model_training" label="Retrain Model"    color="#4edea3" />
          <span className="material-symbols-outlined text-outline-variant/60">arrow_forward</span>
          <FlowStep icon="trending_up"    label="Better Predictions" color="#4edea3" fill />
        </div>
        <div className="mt-5 text-xs text-on-surface-variant bg-surface-container rounded-xl p-3.5 border border-outline-variant/15 leading-relaxed">
          Run{" "}
          <code className="text-secondary bg-surface-container-high px-1.5 py-0.5 rounded font-mono">
            python -m benchmark.runner --mode pasta --feedback
          </code>{" "}
          to trigger feedback. Appends 15 rows to{" "}
          <code className="text-primary bg-surface-container-high px-1.5 py-0.5 rounded font-mono">
            data/raw_metrics.csv
          </code>{" "}
          then calls{" "}
          <code className="text-primary bg-surface-container-high px-1.5 py-0.5 rounded font-mono">
            model/train.py
          </code>{" "}
          automatically.
        </div>
      </motion.div>
    </div>
  );
}
