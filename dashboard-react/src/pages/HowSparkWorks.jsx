import { useState, useEffect, useRef } from "react";
import { motion, AnimatePresence, useInView } from "framer-motion";

// ─────────────────────────────────────────────────────────────────────────────
// CONSTANTS & DATA
// ─────────────────────────────────────────────────────────────────────────────
const COLORS = { blue: "#adc6ff", green: "#4edea3", yellow: "#ffb95f", red: "#ffb4ab", dim: "#8c909f" };

const LIFECYCLE_STEPS = [
  {
    num: "01", label: "Submit",
    icon: "upload_file", color: COLORS.blue,
    title: "User Submits a Spark Job",
    body: "Your driver program (main.py / .jar) is launched via spark-submit. SparkContext initializes, registers with the Cluster Manager, and requests executor resources.",
    code: "spark-submit --master local[*] \\\n  --executor-memory 2g \\\n  scheduler.py",
    badge: "Entry Point",
  },
  {
    num: "02", label: "DAG Build",
    icon: "account_tree", color: COLORS.yellow,
    title: "Build a Directed Acyclic Graph",
    body: "Spark's Catalyst optimizer analyzes transformations lazily. No data moves yet — Spark builds a DAG of logical operations: map → filter → groupBy → join.",
    code: "df.filter(...)\n  .groupBy('type')\n  .agg(sum('size'))   # Nothing runs yet!",
    badge: "Lazy Eval",
  },
  {
    num: "03", label: "Stage Split",
    icon: "call_split", color: COLORS.green,
    title: "Split DAG into Stages at Shuffle Boundaries",
    body: "Every wide transformation (groupBy, join, sort) forces a shuffle — data must cross the network. Spark cuts the DAG at each shuffle, creating separate stages that run sequentially.",
    code: "Stage 0: map + filter  (narrow, no shuffle)\nStage 1: groupBy + agg (SHUFFLE BOUNDARY)\nStage 2: collect result",
    badge: "Shuffle Split",
  },
  {
    num: "04", label: "Tasks",
    icon: "settings", color: COLORS.yellow,
    title: "Launch One Task per Partition",
    body: "Each stage is parallelized into tasks — one task per partition. Tasks run concurrently on executor cores. PASTA controls partition count: small=4, medium=8, large=16.",
    code: "spark.sql.shuffle.partitions = 8\n# → 8 parallel tasks per stage\n# PASTA sets this per-job tier!",
    badge: "Parallelism",
  },
  {
    num: "05", label: "Result",
    icon: "check_circle", color: COLORS.green,
    title: "Collect Results & Record Metrics",
    body: "After all tasks complete, results are collected to the driver. PASTA records actual_time → appends to raw_metrics.csv → triggers model retrain for better future predictions.",
    code: "actual_time = time.time() - start\nwrite_metrics(job_id, actual_time)\nretrain_model()   # feedback loop!",
    badge: "Feedback",
  },
];

const DAG_NODES = [
  { id: "A", label: "read CSV", type: "narrow",  x: 80,  y: 30,  color: COLORS.blue   },
  { id: "B", label: "filter()",  type: "narrow",  x: 200, y: 30,  color: COLORS.blue   },
  { id: "C", label: "map()",     type: "narrow",  x: 320, y: 30,  color: COLORS.blue   },
  { id: "D", label: "groupBy()", type: "shuffle", x: 440, y: 30,  color: COLORS.yellow },
  { id: "E", label: "agg(sum)",  type: "narrow",  x: 560, y: 30,  color: COLORS.green  },
  { id: "F", label: "join()",    type: "shuffle", x: 440, y: 110, color: COLORS.yellow },
  { id: "G", label: "sort()",    type: "shuffle", x: 560, y: 110, color: COLORS.red    },
  { id: "H", label: "collect()", type: "action",  x: 680, y: 70,  color: COLORS.green  },
];
const DAG_EDGES = [
  { from: "A", to: "B" }, { from: "B", to: "C" }, { from: "C", to: "D" },
  { from: "D", to: "E" }, { from: "C", to: "F" }, { from: "F", to: "G" },
  { from: "E", to: "H" }, { from: "G", to: "H" },
];

const CONCEPTS = [
  { icon: "hourglass_disabled", title: "Lazy Evaluation",  color: COLORS.blue,   body: "Transformations (map, filter) are recorded but not executed until an action (count, collect) is called. Spark can optimize the full plan before running anything." },
  { icon: "recycling",          title: "RDD / DataFrame",  color: COLORS.green,  body: "Resilient Distributed Dataset — the fundamental abstraction. DataFrames add schema and SQL-like API. Both are immutable and partitioned across nodes." },
  { icon: "shuffle",            title: "Shuffle",           color: COLORS.yellow, body: "The most expensive operation: data from every partition must be sent to other nodes to group/join by key. PASTA minimises shuffle cost by sizing partitions to the job." },
  { icon: "memory",             title: "In-Memory Cache",   color: COLORS.green,  body: "df.cache() keeps data in RAM between stages, avoiding repeated disk reads. Spark's Tungsten engine uses off-heap memory for ultra-fast serialization." },
  { icon: "schema",             title: "Catalyst Optimizer",color: COLORS.blue,   body: "SQL/DataFrame queries are converted to logical plans, then Catalyst rewrites them (predicate pushdown, column pruning) before creating the physical execution plan." },
  { icon: "account_tree",       title: "DAG Scheduler",     color: COLORS.yellow, body: "Converts the logical plan into stages and tasks. Handles fault tolerance by re-running failed tasks from their parent RDD lineage without restarting the whole job." },
];

const MEMORY_REGIONS = [
  { label: "Execution Memory",  pct: 60, color: COLORS.green,  desc: "Shuffle, sort, aggregation buffers. Spills to disk when full." },
  { label: "Storage Memory",    pct: 40, color: COLORS.blue,   desc: "Cached RDDs / DataFrames. Evicted by LRU when execution needs space." },
  { label: "User Memory",       pct: 25, color: COLORS.yellow, desc: "Your data structures: Python objects, UDF closures, dictionaries." },
  { label: "Reserved Memory",   pct: 15, color: COLORS.dim,    desc: "300 MB hardcoded for Spark internals. Non-negotiable." },
];

// ─────────────────────────────────────────────────────────────────────────────
// SUB-COMPONENTS
// ─────────────────────────────────────────────────────────────────────────────

// Animated flowing connection arrow between two SVG points
function FlowArrow({ x1, y1, x2, y2, color = "#424754", animated = false, delay = 0 }) {
  const id = `arrow-${x1}-${y1}-${x2}-${y2}`;
  const dx = x2 - x1, dy = y2 - y1;
  const mx = x1 + dx * 0.5, my = y1;
  const d = `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;

  return (
    <g>
      <defs>
        <marker id={id} markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
          <path d="M0,0 L0,6 L6,3 z" fill={color} />
        </marker>
      </defs>
      <path d={d} stroke={color} strokeWidth={1.5} fill="none" markerEnd={`url(#${id})`} opacity={0.5} />
      {animated && (
        <path d={d} stroke={color} strokeWidth={2} fill="none" opacity={0.9}
          strokeDasharray="6 12"
          style={{ animation: `dash-flow 1.8s linear ${delay}s infinite` }}
        />
      )}
    </g>
  );
}

// Section header with accent line
function SectionHeader({ icon, title, subtitle, color = COLORS.blue }) {
  return (
    <div className="flex items-start gap-3 mb-6">
      <div className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0"
        style={{ background: `${color}15`, border: `1px solid ${color}30` }}>
        <span className="material-symbols-outlined fill-icon" style={{ fontSize: 20, color }}>{icon}</span>
      </div>
      <div>
        <h3 className="font-black text-base text-on-surface">{title}</h3>
        {subtitle && <p className="text-xs text-on-surface-variant mt-0.5">{subtitle}</p>}
      </div>
    </div>
  );
}

// Inline code tag
function Code({ children, color = COLORS.green }) {
  return (
    <code className="px-1.5 py-0.5 rounded text-[11px] font-mono"
      style={{ background: `${color}15`, color }}>
      {children}
    </code>
  );
}

// ── 1. HERO BANNER ────────────────────────────────────────────────────────────
function HeroBanner() {
  const stats = [
    { val: "In-Memory", sub: "computation engine", color: COLORS.green },
    { val: "100×",      sub: "faster than MapReduce", color: COLORS.yellow },
    { val: "local[*]",  sub: "our benchmark mode", color: COLORS.blue },
    { val: "JVM",       sub: "single process, all cores", color: COLORS.red },
  ];
  return (
    <div className="glass rounded-2xl p-6 overflow-hidden relative">
      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-secondary/5" />
      <div className="relative flex flex-col md:flex-row md:items-center gap-6">
        <div className="flex-1">
          <div className="flex items-center gap-3 mb-3">
            <div className="w-10 h-10 rounded-2xl bg-secondary/15 border border-secondary/30 flex items-center justify-center">
              <span className="material-symbols-outlined fill-icon text-secondary" style={{ fontSize: 24 }}>local_fire_department</span>
            </div>
            <div>
              <h1 className="text-3xl font-black text-on-surface leading-tight">
                How Apache <span className="text-secondary">Spark</span> Works
              </h1>
              <p className="text-sm text-on-surface-variant mt-0.5">
                Distributed in-memory computing engine · Powers our PASTA scheduler
              </p>
            </div>
          </div>
          <p className="text-xs text-on-surface-variant leading-relaxed max-w-xl">
            Apache Spark processes data <em className="text-primary">in RAM</em> across a cluster of machines using a
            Directed Acyclic Graph (DAG) execution model. Unlike Hadoop MapReduce which writes to disk between every step,
            Spark chains transformations lazily and executes them in an optimized pipeline.
          </p>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {stats.map((s) => (
            <motion.div key={s.val} whileHover={{ y: -3 }}
              className="glass rounded-xl p-3 text-center border"
              style={{ borderColor: `${s.color}25` }}>
              <div className="text-lg font-black mono" style={{ color: s.color }}>{s.val}</div>
              <div className="text-[9px] text-on-surface-variant mt-0.5 leading-tight">{s.sub}</div>
            </motion.div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ── 2. EXECUTION LIFECYCLE STEPPER ───────────────────────────────────────────
function ExecutionLifecycle() {
  const [active, setActive] = useState(0);
  const step = LIFECYCLE_STEPS[active];

  return (
    <div className="glass rounded-2xl p-6">
      <SectionHeader icon="play_circle" title="Execution Lifecycle — Step by Step"
        subtitle="Click any step to explore what Spark does at each phase" color={COLORS.blue} />

      {/* Step pills */}
      <div className="flex gap-2 mb-6 flex-wrap">
        {LIFECYCLE_STEPS.map((s, i) => (
          <motion.button key={i}
            whileHover={{ y: -2 }}
            whileTap={{ scale: 0.97 }}
            onClick={() => setActive(i)}
            className="flex items-center gap-2 px-3 py-2 rounded-xl text-xs font-bold transition-all border"
            style={active === i ? {
              background: `${s.color}20`,
              borderColor: `${s.color}50`,
              color: s.color,
              boxShadow: `0 0 16px ${s.color}25`,
            } : {
              background: "rgba(18,33,49,0.4)",
              borderColor: "rgba(66,71,84,0.3)",
              color: "#8c909f",
            }}>
            <span className="mono text-[10px] opacity-70">{s.num}</span>
            <span className="material-symbols-outlined" style={{ fontSize: 14 }}>{s.icon}</span>
            {s.label}
          </motion.button>
        ))}
      </div>

      {/* Progress track */}
      <div className="relative h-1.5 bg-surface-container-high rounded-full mb-6 overflow-hidden">
        <motion.div
          className="absolute top-0 left-0 h-full rounded-full"
          style={{ background: `linear-gradient(90deg, ${COLORS.blue}, ${step.color})` }}
          animate={{ width: `${((active + 1) / LIFECYCLE_STEPS.length) * 100}%` }}
          transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
        />
      </div>

      {/* Step detail */}
      <AnimatePresence mode="wait">
        <motion.div key={active}
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          exit={{ opacity: 0, x: -20 }}
          transition={{ duration: 0.25 }}
          className="grid grid-cols-1 md:grid-cols-2 gap-5"
        >
          <div>
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-2xl flex items-center justify-center"
                style={{ background: `${step.color}20`, border: `1px solid ${step.color}40` }}>
                <span className="material-symbols-outlined fill-icon" style={{ fontSize: 22, color: step.color }}>{step.icon}</span>
              </div>
              <div>
                <div className="text-[10px] font-bold uppercase tracking-widest" style={{ color: step.color }}>
                  Step {step.num} · {step.badge}
                </div>
                <h4 className="text-sm font-black text-on-surface">{step.title}</h4>
              </div>
            </div>
            <p className="text-xs text-on-surface-variant leading-relaxed">{step.body}</p>

            {/* Nav buttons */}
            <div className="flex gap-2 mt-4">
              <button onClick={() => setActive(Math.max(0, active - 1))}
                disabled={active === 0}
                className="px-3 py-1.5 rounded-lg text-xs font-bold border border-outline-variant/40 text-on-surface-variant hover:bg-surface-container-high transition-colors disabled:opacity-30">
                ← Prev
              </button>
              <button onClick={() => setActive(Math.min(LIFECYCLE_STEPS.length - 1, active + 1))}
                disabled={active === LIFECYCLE_STEPS.length - 1}
                className="px-3 py-1.5 rounded-lg text-xs font-bold transition-colors"
                style={{
                  background: active < LIFECYCLE_STEPS.length - 1 ? `${step.color}20` : undefined,
                  borderColor: `${step.color}40`,
                  color: step.color,
                  border: `1px solid ${step.color}40`,
                  opacity: active === LIFECYCLE_STEPS.length - 1 ? 0.3 : 1,
                }}>
                Next →
              </button>
            </div>
          </div>

          {/* Code block */}
          <div className="bg-surface-container-lowest rounded-xl p-4 border border-outline-variant/20">
            <div className="flex items-center gap-2 mb-3">
              <div className="flex gap-1.5">
                <div className="w-2.5 h-2.5 rounded-full bg-error/60" />
                <div className="w-2.5 h-2.5 rounded-full bg-tertiary/60" />
                <div className="w-2.5 h-2.5 rounded-full bg-secondary/60" />
              </div>
              <span className="text-[9px] text-on-surface-variant/50 mono ml-1">spark_exec.py</span>
            </div>
            <pre className="text-[11px] mono text-on-surface-variant leading-relaxed whitespace-pre-wrap"
              style={{ color: step.color === COLORS.green ? "#4edea3" : "#adc6ff" }}>
              {step.code}
            </pre>
          </div>
        </motion.div>
      </AnimatePresence>
    </div>
  );
}

// ── 3. ARCHITECTURE DIAGRAM (SVG-based) ──────────────────────────────────────
function ArchitectureDiagram() {
  const [hovered, setHovered] = useState(null);
  const [animated, setAnimated] = useState(true);

  const nodes = [
    {
      id: "driver",
      x: 80, y: 100, w: 160, h: 160,
      color: COLORS.blue, icon: "computer",
      title: "Driver Program",
      items: ["SparkContext", "DAG Scheduler", "Task Scheduler", "your main() code"],
    },
    {
      id: "cluster",
      x: 320, y: 120, w: 150, h: 120,
      color: COLORS.yellow, icon: "hub",
      title: "Cluster Manager",
      items: ["local[*] ← ours", "YARN / Mesos / K8s", "Allocates resources"],
    },
    {
      id: "worker1",
      x: 560, y: 60, w: 160, h: 110,
      color: COLORS.green, icon: "memory",
      title: "Worker Node 1",
      items: ["Executor JVM", "Task 1 · Task 2", "In-memory cache"],
    },
    {
      id: "worker2",
      x: 560, y: 190, w: 160, h: 90,
      color: COLORS.green, icon: "memory",
      title: "Worker Node 2",
      items: ["Executor JVM", "Task 3 · Task 4"],
    },
  ];

  const connections = [
    { x1: 240, y1: 155, x2: 320, y2: 165, label: "register & request", color: COLORS.blue,   animated: animated, delay: 0 },
    { x1: 470, y1: 140, x2: 560, y2: 105, label: "launch executor",    color: COLORS.yellow, animated: animated, delay: 0.4 },
    { x1: 470, y1: 155, x2: 560, y2: 230, label: "launch executor",    color: COLORS.yellow, animated: animated, delay: 0.8 },
    { x1: 240, y1: 170, x2: 560, y2: 120, label: "submit tasks",       color: COLORS.blue,   animated: animated, delay: 0.2 },
    { x1: 240, y1: 180, x2: 560, y2: 250, label: "submit tasks",       color: COLORS.blue,   animated: animated, delay: 0.6 },
  ];

  return (
    <div className="glass rounded-2xl p-6">
      <div className="flex items-start justify-between mb-6">
        <SectionHeader icon="schema" title="Spark Architecture"
          subtitle="Driver orchestrates; Cluster Manager allocates; Workers execute" color={COLORS.yellow} />
        <button onClick={() => setAnimated((v) => !v)}
          className="flex items-center gap-1.5 text-[10px] font-bold px-3 py-1.5 rounded-lg border border-outline-variant/30 text-on-surface-variant hover:bg-surface-container-high transition-colors">
          <span className="material-symbols-outlined" style={{ fontSize: 13 }}>
            {animated ? "pause_circle" : "play_circle"}
          </span>
          {animated ? "Pause flow" : "Animate flow"}
        </button>
      </div>

      <div className="overflow-x-auto">
        <div style={{ minWidth: 780 }}>
          <svg width="780" height="320" viewBox="0 0 780 320"
            style={{ overflow: "visible" }}>

            {/* Grid background */}
            <defs>
              <pattern id="grid" width="24" height="24" patternUnits="userSpaceOnUse">
                <circle cx="0" cy="0" r="0.8" fill="rgba(66,71,84,0.4)" />
              </pattern>
            </defs>
            <rect width="780" height="320" fill="url(#grid)" rx="12" />

            {/* Connections */}
            {connections.map((c, i) => (
              <FlowArrow key={i} {...c} />
            ))}

            {/* Labels on connections */}
            <text x="270" y="148" fill={COLORS.blue} fontSize="9" textAnchor="middle" className="mono" opacity={0.6}>register</text>
            <text x="390" y="80"  fill={COLORS.blue} fontSize="9" textAnchor="middle" className="mono" opacity={0.6}>tasks →</text>

            {/* Node boxes */}
            {nodes.map((n) => (
              <g key={n.id}
                onMouseEnter={() => setHovered(n.id)}
                onMouseLeave={() => setHovered(null)}
                style={{ cursor: "pointer" }}>
                <rect x={n.x} y={n.y} width={n.w} height={n.h} rx={12}
                  fill={hovered === n.id ? `${n.color}18` : `${n.color}0e`}
                  stroke={hovered === n.id ? n.color : `${n.color}35`}
                  strokeWidth={hovered === n.id ? 1.5 : 1}
                  style={{ transition: "all 0.2s" }}
                />
                {/* Icon placeholder circle */}
                <circle cx={n.x + n.w / 2} cy={n.y + 28} r={16}
                  fill={`${n.color}20`} stroke={`${n.color}40`} strokeWidth={1} />
                <text x={n.x + n.w / 2} y={n.y + 33}
                  fill={n.color} fontSize="16" textAnchor="middle" fontFamily="Material Symbols Outlined">
                </text>
                <text x={n.x + n.w / 2} y={n.y + 33}
                  fill={n.color} fontSize="13" textAnchor="middle" fontWeight="900" opacity={0.9}>
                  {n.id === "driver" ? "⬛" : n.id === "cluster" ? "◉" : "▣"}
                </text>
                <text x={n.x + n.w / 2} y={n.y + 54}
                  fill={n.color} fontSize="11" textAnchor="middle" fontWeight="900">
                  {n.title}
                </text>
                {n.items.map((item, ii) => (
                  <text key={ii} x={n.x + n.w / 2} y={n.y + 72 + ii * 16}
                    fill={item.includes("←") ? n.color : "rgba(194,198,214,0.8)"}
                    fontSize="9" textAnchor="middle"
                    fontWeight={item.includes("←") ? "700" : "400"}>
                    {item}
                  </text>
                ))}
              </g>
            ))}

            {/* Animated data packets */}
            {animated && (
              <>
                <circle r="4" fill={COLORS.blue} opacity="0.9">
                  <animateMotion dur="2s" repeatCount="indefinite" begin="0s"
                    path="M240,155 C280,155 280,165 320,165" />
                </circle>
                <circle r="3" fill={COLORS.yellow} opacity="0.9">
                  <animateMotion dur="1.8s" repeatCount="indefinite" begin="0.5s"
                    path="M470,140 C515,140 515,105 560,105" />
                </circle>
                <circle r="3" fill={COLORS.blue} opacity="0.8">
                  <animateMotion dur="2.2s" repeatCount="indefinite" begin="0.2s"
                    path="M240,170 C400,170 400,120 560,120" />
                </circle>
              </>
            )}
          </svg>
        </div>
      </div>

      {/* local[*] callout */}
      <div className="mt-4 rounded-xl p-3.5 border text-xs text-on-surface-variant leading-relaxed"
        style={{ background: "rgba(173,198,255,0.05)", borderColor: "rgba(173,198,255,0.15)" }}>
        <strong className="text-on-surface">local[*] mode</strong> (our benchmark) collapses Driver + Cluster Manager + all Workers
        into a <em className="text-primary">single JVM process</em> on your machine. All CPU cores become virtual executors — zero network overhead.{" "}
        <Code color={COLORS.red}>spark.executor.memory</Code> is fixed at JVM launch;
        only <Code>spark.sql.shuffle.partitions</Code> and <Code>spark.scheduler.pool</Code> can change per-job.
      </div>
    </div>
  );
}

// ── 4. DAG VISUALIZER ────────────────────────────────────────────────────────
function DAGVisualizer() {
  const [selected, setSelected] = useState(null);
  const [animStep, setAnimStep] = useState(-1);

  const sel = DAG_NODES.find((n) => n.id === selected);

  const typeInfo = {
    narrow:  { label: "Narrow Transform", color: COLORS.blue,   desc: "Data stays in same partition. No network. Fast.", badge: "No Shuffle" },
    shuffle: { label: "Wide Transform",   color: COLORS.yellow, desc: "Data crosses partitions and nodes. Expensive.", badge: "SHUFFLE!" },
    action:  { label: "Action",           color: COLORS.green,  desc: "Triggers execution of the whole DAG. One Job per action.", badge: "Triggers DAG" },
  };

  // Animate through nodes
  useEffect(() => {
    if (animStep >= 0 && animStep < DAG_NODES.length) {
      const t = setTimeout(() => setAnimStep((s) => s + 1), 500);
      return () => clearTimeout(t);
    }
  }, [animStep]);

  const nodePos = Object.fromEntries(DAG_NODES.map((n) => [n.id, { x: n.x, y: n.y }]));

  return (
    <div className="glass rounded-2xl p-6">
      <div className="flex items-start justify-between mb-5">
        <SectionHeader icon="account_tree" title="DAG Visualizer — Directed Acyclic Graph"
          subtitle="Click any node to inspect · Blue = narrow (fast) · Yellow = shuffle (expensive) · Green = action" color={COLORS.green} />
        <button onClick={() => setAnimStep(0)}
          className="text-[10px] font-bold px-3 py-1.5 rounded-lg flex items-center gap-1.5 transition-colors"
          style={{ background: `${COLORS.green}15`, border: `1px solid ${COLORS.green}30`, color: COLORS.green }}>
          <span className="material-symbols-outlined" style={{ fontSize: 13 }}>play_arrow</span>
          Run DAG
        </button>
      </div>

      <div className="overflow-x-auto">
        <svg width="780" height="160" viewBox="0 20 780 160" style={{ overflow: "visible", minWidth: 780 }}>
          <defs>
            {DAG_NODES.map((n) => (
              <marker key={n.id} id={`dag-arrow-${n.id}`} markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
                <path d="M0,0 L0,6 L6,3 z" fill={n.color} opacity={0.5} />
              </marker>
            ))}
          </defs>

          {/* Stage shading */}
          <rect x={50} y={22} width={360} height={56} rx={6} fill="rgba(173,198,255,0.04)" stroke="rgba(173,198,255,0.1)" strokeWidth={1} />
          <text x={230} y={18} fill={COLORS.blue} fontSize={9} textAnchor="middle" opacity={0.6}>Stage 0 · narrow</text>
          <rect x={420} y={22} width={150} height={56} rx={6} fill="rgba(255,185,95,0.04)" stroke="rgba(255,185,95,0.1)" strokeWidth={1} />
          <text x={495} y={18} fill={COLORS.yellow} fontSize={9} textAnchor="middle" opacity={0.6}>Stage 1 · shuffle</text>
          <rect x={420} y={96} width={150} height={56} rx={6} fill="rgba(255,185,95,0.04)" stroke="rgba(255,185,95,0.1)" strokeWidth={1} />
          <text x={495} y={92} fill={COLORS.yellow} fontSize={9} textAnchor="middle" opacity={0.6}>Stage 2 · shuffle</text>
          <rect x={650} y={45} width={100} height={82} rx={6} fill="rgba(78,222,163,0.04)" stroke="rgba(78,222,163,0.1)" strokeWidth={1} />
          <text x={700} y={41} fill={COLORS.green} fontSize={9} textAnchor="middle" opacity={0.6}>Result</text>

          {/* Edges */}
          {DAG_EDGES.map((e, i) => {
            const from = DAG_NODES.find((n) => n.id === e.from);
            const to   = DAG_NODES.find((n) => n.id === e.to);
            if (!from || !to) return null;
            const isShuffleBoundary = from.type === "shuffle" || to.type === "shuffle";
            return (
              <line key={i}
                x1={from.x + 32} y1={from.y + 14}
                x2={to.x - 32}   y2={to.y + 14}
                stroke={isShuffleBoundary ? COLORS.yellow : COLORS.blue}
                strokeWidth={isShuffleBoundary ? 1.5 : 1}
                strokeDasharray={isShuffleBoundary ? "4 3" : "none"}
                opacity={0.4}
                markerEnd={`url(#dag-arrow-${e.from})`}
              />
            );
          })}

          {/* Nodes */}
          {DAG_NODES.map((n, i) => {
            const isActive = animStep > i || selected === n.id;
            return (
              <g key={n.id} onClick={() => setSelected(selected === n.id ? null : n.id)}
                style={{ cursor: "pointer" }}>
                <rect x={n.x - 34} y={n.y + 2} width={68} height={24} rx={5}
                  fill={isActive ? `${n.color}25` : `${n.color}10`}
                  stroke={selected === n.id ? n.color : isActive ? `${n.color}60` : `${n.color}30`}
                  strokeWidth={selected === n.id ? 2 : 1}
                  style={{ transition: "all 0.25s" }}
                />
                <text x={n.x} y={n.y + 18}
                  fill={isActive ? n.color : `${n.color}80`}
                  fontSize={9.5} textAnchor="middle" fontWeight="700"
                  fontFamily="JetBrains Mono, monospace">
                  {n.label}
                </text>
                {/* Shuffle indicator */}
                {n.type === "shuffle" && (
                  <circle cx={n.x + 30} cy={n.y + 5} r={4} fill={COLORS.yellow} opacity={0.8}>
                    {animStep > i && <animate attributeName="opacity" values="0.8;0.2;0.8" dur="1.2s" repeatCount="indefinite" />}
                  </circle>
                )}
              </g>
            );
          })}
        </svg>
      </div>

      {/* Selected node detail */}
      <AnimatePresence>
        {sel && (
          <motion.div key={sel.id}
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="mt-4 overflow-hidden"
          >
            <div className="rounded-xl p-4 border text-xs"
              style={{ background: `${sel.color}08`, borderColor: `${sel.color}25` }}>
              <div className="flex items-center gap-2 mb-2">
                <span className="font-black mono" style={{ color: sel.color }}>{sel.label}()</span>
                <span className="text-[9px] font-bold px-2 py-0.5 rounded-full"
                  style={{ background: `${sel.color}20`, color: sel.color }}>
                  {typeInfo[sel.type]?.badge}
                </span>
              </div>
              <p className="text-on-surface-variant">{typeInfo[sel.type]?.desc}</p>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ── 5. JOB → STAGE → TASK INTERACTIVE TREE ───────────────────────────────────
function JobTree() {
  const [openJob, setOpenJob] = useState(0);
  const [openStage, setOpenStage] = useState(null);

  const jobs = [
    {
      id: 0, label: "Job #0 — wordcount.count()", color: COLORS.blue, duration: "0.26s",
      trigger: "df.count()", partitions: 4,
      stages: [
        {
          id: "0-0", label: "Stage 0: read + map", type: "narrow", color: COLORS.blue,
          tasks: 4, desc: "Read CSV partitions, apply word tokenizer. No shuffle needed.",
          taskList: ["Task 0: rows 0–2500", "Task 1: rows 2500–5000", "Task 2: rows 5000–7500", "Task 3: rows 7500–10000"],
        },
        {
          id: "0-1", label: "Stage 1: reduceByKey ⚡ SHUFFLE", type: "shuffle", color: COLORS.yellow,
          tasks: 4, desc: "Redistributes words by key hash across partitions. Most expensive step.",
          taskList: ["Task 0: keys a–f", "Task 1: keys g–m", "Task 2: keys n–s", "Task 3: keys t–z"],
        },
      ],
    },
    {
      id: 1, label: "Job #1 — join + aggregate", color: COLORS.green, duration: "0.48s",
      trigger: "df_joined.agg()", partitions: 8,
      stages: [
        {
          id: "1-0", label: "Stage 0: filter + project", type: "narrow", color: COLORS.blue,
          tasks: 8, desc: "Push filters down to scan level. Catalyst prunes unused columns.",
          taskList: ["Tasks 0–7: filter partitions in parallel"],
        },
        {
          id: "1-1", label: "Stage 1: hash join ⚡ SHUFFLE", type: "shuffle", color: COLORS.yellow,
          tasks: 8, desc: "Shuffle both tables by join key. Each partition gets matching rows.",
          taskList: ["Tasks 0–7: process join partitions"],
        },
        {
          id: "1-2", label: "Stage 2: aggregate ⚡ SHUFFLE", type: "shuffle", color: COLORS.yellow,
          tasks: 8, desc: "Group and sum results. Second shuffle needed for global aggregation.",
          taskList: ["Tasks 0–7: aggregate by group"],
        },
      ],
    },
    {
      id: 2, label: "Job #2 — PageRank iteration", color: COLORS.yellow, duration: "0.75s",
      trigger: "ranks.saveAsTextFile()", partitions: 16,
      stages: [
        {
          id: "2-0", label: "Stage 0: build link graph", type: "narrow", color: COLORS.blue, tasks: 16, desc: "Parse adjacency list. Narrow — each partition independently.",
          taskList: ["Tasks 0–15: build subgraph"],
        },
        {
          id: "2-1", label: "Stage 1: rank propagation ⚡ SHUFFLE", type: "shuffle", color: COLORS.yellow, tasks: 16,
          desc: "Each node distributes rank to its neighbors. Requires full graph shuffle.",
          taskList: ["Tasks 0–15: propagate ranks"],
        },
        {
          id: "2-2", label: "Stage 2: normalize + save", type: "narrow", color: COLORS.blue, tasks: 16,
          desc: "Normalize rank values. Write 16 output part files in parallel.",
          taskList: ["Tasks 0–15: write part-00000 … part-00015"],
        },
      ],
    },
  ];

  return (
    <div className="glass rounded-2xl p-6">
      <SectionHeader icon="account_tree" title="Job → Stage → Task Explorer"
        subtitle="Click a job to expand stages · click a stage to inspect tasks" color={COLORS.yellow} />

      <div className="space-y-3">
        {jobs.map((job) => (
          <div key={job.id}>
            {/* Job row */}
            <motion.button whileHover={{ x: 2 }} whileTap={{ scale: 0.99 }}
              onClick={() => setOpenJob(openJob === job.id ? null : job.id)}
              className="w-full flex items-center gap-3 p-3 rounded-xl border text-left transition-all"
              style={openJob === job.id ? {
                background: `${job.color}12`,
                borderColor: `${job.color}40`,
              } : { background: "rgba(18,33,49,0.4)", borderColor: "rgba(66,71,84,0.2)" }}>
              <span className="material-symbols-outlined fill-icon" style={{ fontSize: 20, color: job.color }}>workspaces</span>
              <div className="flex-1">
                <div className="text-xs font-bold" style={{ color: openJob === job.id ? job.color : "#d4e4fa" }}>
                  {job.label}
                </div>
                <div className="text-[9px] text-on-surface-variant mono mt-0.5">
                  trigger: {job.trigger} · partitions: {job.partitions} · {job.stages.length} stages
                </div>
              </div>
              <span className="mono text-xs font-bold" style={{ color: job.color }}>{job.duration}</span>
              <span className="material-symbols-outlined text-on-surface-variant" style={{ fontSize: 16 }}>
                {openJob === job.id ? "expand_less" : "expand_more"}
              </span>
            </motion.button>

            {/* Stages */}
            <AnimatePresence>
              {openJob === job.id && (
                <motion.div key="stages"
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: "auto" }}
                  exit={{ opacity: 0, height: 0 }}
                  className="overflow-hidden ml-6 mt-2 space-y-2"
                >
                  {job.stages.map((stage) => (
                    <div key={stage.id}>
                      <motion.button whileHover={{ x: 2 }}
                        onClick={() => setOpenStage(openStage === stage.id ? null : stage.id)}
                        className="w-full flex items-center gap-3 p-2.5 rounded-xl border text-left transition-all"
                        style={openStage === stage.id ? {
                          background: `${stage.color}10`,
                          borderColor: `${stage.color}35`,
                        } : { background: "rgba(18,33,49,0.3)", borderColor: "rgba(66,71,84,0.15)" }}>
                        <span className="material-symbols-outlined" style={{ fontSize: 16, color: stage.color }}>stacked_bar_chart</span>
                        <div className="flex-1">
                          <div className="text-[11px] font-bold" style={{ color: stage.color }}>{stage.label}</div>
                          <div className="text-[9px] text-on-surface-variant">{stage.tasks} tasks · {stage.type}</div>
                        </div>
                        <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-full`}
                          style={{ background: `${stage.color}20`, color: stage.color }}>
                          {stage.type === "shuffle" ? "⚡ SHUFFLE" : "narrow"}
                        </span>
                        <span className="material-symbols-outlined text-on-surface-variant" style={{ fontSize: 14 }}>
                          {openStage === stage.id ? "expand_less" : "expand_more"}
                        </span>
                      </motion.button>

                      {/* Tasks */}
                      <AnimatePresence>
                        {openStage === stage.id && (
                          <motion.div key="tasks"
                            initial={{ opacity: 0, height: 0 }}
                            animate={{ opacity: 1, height: "auto" }}
                            exit={{ opacity: 0, height: 0 }}
                            className="overflow-hidden ml-6 mt-1.5"
                          >
                            <div className="rounded-xl p-3 border text-xs"
                              style={{ background: `${stage.color}05`, borderColor: `${stage.color}20` }}>
                              <p className="text-on-surface-variant mb-2 leading-relaxed">{stage.desc}</p>
                              <div className="flex gap-1 flex-wrap mt-2">
                                {stage.taskList.map((t, i) => (
                                  <span key={i} className="text-[9px] mono px-2 py-0.5 rounded-lg"
                                    style={{ background: `${stage.color}15`, color: stage.color, border: `1px solid ${stage.color}25` }}>
                                    {t}
                                  </span>
                                ))}
                              </div>
                            </div>
                          </motion.div>
                        )}
                      </AnimatePresence>
                    </div>
                  ))}
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── 6. PARTITION SHUFFLE ANIMATION ───────────────────────────────────────────
function ShuffleVisualizer() {
  const [phase, setPhase] = useState("before"); // before | shuffling | after
  const [running, setRunning] = useState(false);

  const run = () => {
    if (running) return;
    setRunning(true);
    setPhase("before");
    setTimeout(() => setPhase("shuffling"), 600);
    setTimeout(() => setPhase("after"), 2200);
    setTimeout(() => setRunning(false), 2800);
  };

  const before = [
    { key: "cat",   partition: 0, color: COLORS.blue   },
    { key: "dog",   partition: 0, color: COLORS.green  },
    { key: "cat",   partition: 1, color: COLORS.blue   },
    { key: "bird",  partition: 1, color: COLORS.yellow },
    { key: "dog",   partition: 2, color: COLORS.green  },
    { key: "bird",  partition: 2, color: COLORS.yellow },
    { key: "cat",   partition: 3, color: COLORS.blue   },
    { key: "bird",  partition: 3, color: COLORS.yellow },
  ];

  const after = [
    { key: "cat ×3",  partition: 0, color: COLORS.blue   },
    { key: "dog ×2",  partition: 1, color: COLORS.green  },
    { key: "bird ×3", partition: 2, color: COLORS.yellow },
    { key: "(empty)", partition: 3, color: COLORS.dim    },
  ];

  const data = phase === "after" ? after : before;

  return (
    <div className="glass rounded-2xl p-6">
      <div className="flex items-start justify-between mb-5">
        <SectionHeader icon="shuffle" title="Shuffle Visualizer"
          subtitle="See how data redistributes across partitions during groupBy / join" color={COLORS.yellow} />
        <button onClick={run} disabled={running}
          className="text-[10px] font-bold px-3 py-1.5 rounded-lg flex items-center gap-1.5 transition-all disabled:opacity-50"
          style={{ background: `${COLORS.yellow}15`, border: `1px solid ${COLORS.yellow}30`, color: COLORS.yellow }}>
          <span className="material-symbols-outlined" style={{ fontSize: 13 }}>shuffle</span>
          {running ? "Shuffling…" : "Run Shuffle"}
        </button>
      </div>

      {/* Phase labels */}
      <div className="flex items-center gap-2 mb-4 text-[10px] font-bold uppercase tracking-widest">
        {["before", "shuffling", "after"].map((p) => (
          <div key={p} className="flex items-center gap-1.5">
            <div className="w-2 h-2 rounded-full transition-all"
              style={{ background: phase === p ? COLORS.yellow : "rgba(66,71,84,0.5)" }} />
            <span style={{ color: phase === p ? COLORS.yellow : "#424754" }}>{p}</span>
          </div>
        ))}
      </div>

      {/* Partitions grid */}
      <div className="grid grid-cols-4 gap-3">
        {[0, 1, 2, 3].map((pidx) => (
          <div key={pidx} className="rounded-xl p-3 border"
            style={{ background: "rgba(18,33,49,0.5)", borderColor: "rgba(66,71,84,0.2)" }}>
            <div className="text-[9px] font-bold text-on-surface-variant mono mb-2 uppercase tracking-wider">
              Partition {pidx}
            </div>
            <div className="space-y-1.5 min-h-[80px]">
              <AnimatePresence mode="popLayout">
                {data.filter((d) => d.partition === pidx).map((item, i) => (
                  <motion.div key={`${phase}-${pidx}-${i}`}
                    initial={{ opacity: 0, scale: 0.7, y: -10 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.5, y: 10 }}
                    transition={{ type: "spring", stiffness: 300, damping: 20, delay: i * 0.08 }}
                    className="px-2 py-1 rounded-lg text-[10px] font-bold mono"
                    style={{
                      background: `${item.color}18`,
                      border: `1px solid ${item.color}35`,
                      color: item.color,
                    }}>
                    {item.key}
                  </motion.div>
                ))}
                {data.filter((d) => d.partition === pidx).length === 0 && phase === "shuffling" && (
                  <motion.div key="spinning"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    className="text-[10px] text-on-surface-variant/40 text-center pt-4">
                    ⟳ moving…
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </div>
        ))}
      </div>

      <div className="mt-3 text-[10px] text-on-surface-variant leading-relaxed">
        <span className="text-yellow-400 font-bold">Before:</span> keys scattered randomly across partitions.{" "}
        <span className="text-yellow-400 font-bold">After:</span> each key lands in exactly one partition (by hash).
        This is why shuffle is slow — every executor sends data to every other executor over the network.
      </div>
    </div>
  );
}

// ── 7. MEMORY MODEL ───────────────────────────────────────────────────────────
function MemoryModel() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true });

  return (
    <div ref={ref} className="glass rounded-2xl p-6">
      <SectionHeader icon="memory" title="Spark Memory Model"
        subtitle="How the JVM heap is divided across executor memory regions" color={COLORS.blue} />

      <div className="space-y-3">
        {MEMORY_REGIONS.map((r, i) => (
          <motion.div key={r.label}
            initial={{ opacity: 0, x: -20 }}
            animate={inView ? { opacity: 1, x: 0 } : {}}
            transition={{ delay: i * 0.1 }}
          >
            <div className="flex items-center justify-between mb-1.5">
              <span className="text-xs font-bold" style={{ color: r.color }}>{r.label}</span>
              <span className="mono text-xs font-bold" style={{ color: r.color }}>{r.pct}%</span>
            </div>
            <div className="h-3 bg-surface-container-high rounded-full overflow-hidden mb-1">
              <motion.div className="h-full rounded-full"
                style={{ background: `linear-gradient(90deg, ${r.color}, ${r.color}80)` }}
                initial={{ width: 0 }}
                animate={inView ? { width: `${r.pct}%` } : {}}
                transition={{ duration: 0.8, delay: i * 0.12, ease: [0.22, 1, 0.36, 1] }}
              />
            </div>
            <p className="text-[10px] text-on-surface-variant">{r.desc}</p>
          </motion.div>
        ))}
      </div>

      <div className="mt-4 rounded-xl p-3.5 text-xs text-on-surface-variant leading-relaxed border"
        style={{ background: "rgba(78,222,163,0.04)", borderColor: "rgba(78,222,163,0.15)" }}>
        <strong className="text-secondary">PASTA's approach:</strong> small jobs use 4 shuffle partitions (less memory pressure),
        large jobs use 16 (more parallelism). This balances execution memory usage across job tiers.
      </div>
    </div>
  );
}

// ── 8. KEY CONCEPTS GLOSSARY ─────────────────────────────────────────────────
function KeyConcepts() {
  const [expanded, setExpanded] = useState(null);

  return (
    <div className="glass rounded-2xl p-6">
      <SectionHeader icon="library_books" title="Key Spark Concepts"
        subtitle="Hover or click to explore core ideas" color={COLORS.green} />

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {CONCEPTS.map((c, i) => (
          <motion.div key={c.title}
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: i * 0.06 }}
            whileHover={{ y: -3 }}
            onClick={() => setExpanded(expanded === c.title ? null : c.title)}
            className="rounded-xl p-4 border cursor-pointer transition-all"
            style={expanded === c.title ? {
              background: `${c.color}12`,
              borderColor: `${c.color}40`,
              boxShadow: `0 0 20px ${c.color}15`,
            } : { background: "rgba(18,33,49,0.4)", borderColor: "rgba(66,71,84,0.2)" }}>
            <div className="flex items-center gap-2 mb-2">
              <span className="material-symbols-outlined" style={{ fontSize: 18, color: c.color }}>{c.icon}</span>
              <span className="text-xs font-black" style={{ color: c.color }}>{c.title}</span>
            </div>
            <AnimatePresence>
              {expanded === c.title ? (
                <motion.p key="body"
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: "auto" }}
                  exit={{ opacity: 0, height: 0 }}
                  className="text-[11px] text-on-surface-variant leading-relaxed overflow-hidden">
                  {c.body}
                </motion.p>
              ) : (
                <p className="text-[11px] text-on-surface-variant/50 line-clamp-1">{c.body.slice(0, 55)}…</p>
              )}
            </AnimatePresence>
          </motion.div>
        ))}
      </div>
    </div>
  );
}

// ── 9. SCHEDULING COMPARISON ─────────────────────────────────────────────────
function SchedulingComparison() {
  const [mode, setMode] = useState("pasta");

  const queues = {
    fifo: {
      label: "FIFO Queue",
      desc: "Jobs processed strictly in arrival order. Large job at head blocks everything.",
      jobs: [
        { id: "A", size: 3, label: "Large (2min)",  color: COLORS.red,    status: "running" },
        { id: "B", size: 1, label: "Small (0.1s)",  color: COLORS.blue,   status: "blocked" },
        { id: "C", size: 1, label: "Small (0.1s)",  color: COLORS.blue,   status: "blocked" },
        { id: "D", size: 1, label: "Medium (30s)",  color: COLORS.yellow, status: "blocked" },
      ],
    },
    fair: {
      label: "FAIR Scheduler",
      desc: "3 pools sharing resources by weight. Pools run concurrently — no convoy effect.",
      jobs: [
        { id: "small_pool",  size: 1, label: "small_pool (w=3)", color: COLORS.blue,   status: "running" },
        { id: "medium_pool", size: 1, label: "medium_pool (w=2)", color: COLORS.yellow, status: "running" },
        { id: "large_pool",  size: 1, label: "large_pool (w=1)", color: COLORS.green,  status: "waiting" },
      ],
    },
    pasta: {
      label: "PASTA Priority Queue",
      desc: "Priority = α×norm_sjf + β×norm_aging. Short jobs first, starvation prevented.",
      jobs: [
        { id: "1", size: 1, label: "p=0.70 small",   color: COLORS.blue,   status: "running" },
        { id: "2", size: 1, label: "p=0.61 small",   color: COLORS.blue,   status: "next" },
        { id: "3", size: 1, label: "p=0.42 medium",  color: COLORS.green,  status: "waiting" },
        { id: "4", size: 2, label: "p=0.00→0.85 large⚡", color: COLORS.yellow, status: "aging" },
      ],
    },
  };

  const q = queues[mode];

  return (
    <div className="glass rounded-2xl p-6">
      <SectionHeader icon="sort" title="Scheduling Mode Comparison"
        subtitle="Toggle between modes to see how jobs are ordered and executed" color={COLORS.red} />

      {/* Mode selector */}
      <div className="flex gap-2 mb-5">
        {Object.entries(queues).map(([k, v]) => {
          const c = k === "fifo" ? COLORS.red : k === "fair" ? COLORS.yellow : COLORS.green;
          return (
            <motion.button key={k} whileHover={{ y: -1 }} whileTap={{ scale: 0.97 }}
              onClick={() => setMode(k)}
              className="px-4 py-2 rounded-xl text-xs font-bold border transition-all"
              style={mode === k ? {
                background: `${c}20`, borderColor: `${c}50`, color: c,
                boxShadow: `0 0 14px ${c}20`,
              } : { background: "rgba(18,33,49,0.4)", borderColor: "rgba(66,71,84,0.25)", color: "#8c909f" }}>
              {v.label}
            </motion.button>
          );
        })}
      </div>

      <AnimatePresence mode="wait">
        <motion.div key={mode}
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -10 }}
          transition={{ duration: 0.2 }}>
          <p className="text-xs text-on-surface-variant mb-4">{q.desc}</p>

          {/* Queue visualization */}
          <div className="flex flex-col gap-2">
            {q.jobs.map((job, i) => (
              <motion.div key={job.id}
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: i * 0.07 }}
                className="flex items-center gap-3 rounded-xl p-3 border"
                style={{
                  background: `${job.color}08`,
                  borderColor: `${job.color}25`,
                  opacity: job.status === "blocked" ? 0.5 : 1,
                }}>
                {/* Relative size bar */}
                <div className="flex gap-1">
                  {Array.from({ length: job.size }).map((_, j) => (
                    <div key={j} className="w-4 h-6 rounded"
                      style={{ background: `${job.color}50` }} />
                  ))}
                </div>
                <span className="text-xs font-bold" style={{ color: job.color }}>{job.label}</span>
                <span className="ml-auto text-[9px] font-bold px-2 py-0.5 rounded-full"
                  style={{
                    background: job.status === "running" ? `${COLORS.green}20` :
                                job.status === "aging"   ? `${COLORS.yellow}20` :
                                job.status === "next"    ? `${COLORS.blue}20` : "rgba(66,71,84,0.3)",
                    color: job.status === "running" ? COLORS.green :
                           job.status === "aging"   ? COLORS.yellow :
                           job.status === "next"    ? COLORS.blue : "#8c909f",
                  }}>
                  {job.status === "running" ? "▶ RUNNING" :
                   job.status === "aging"   ? "⬆ AGING" :
                   job.status === "next"    ? "→ NEXT" :
                   job.status === "blocked" ? "⛔ BLOCKED" : "⏳ WAITING"}
                </span>
              </motion.div>
            ))}
          </div>
        </motion.div>
      </AnimatePresence>
    </div>
  );
}

// ── 10. ML PIPELINE (enhanced) ────────────────────────────────────────────────
function MLPipeline() {
  const [hoveredModel, setHoveredModel] = useState(null);

  const features = [
    { label: "MB",    full: "input_size_mb",      color: COLORS.blue,   desc: "Data volume in megabytes" },
    { label: "P",     full: "num_partitions",      color: COLORS.green,  desc: "Spark shuffle partitions" },
    { label: "type",  full: "job_type_encoded",    color: COLORS.yellow, desc: "0=word, 1=join, 2=pagerank" },
    { label: "H",     full: "hour_of_day",         color: COLORS.blue,   desc: "System load proxy" },
    { label: "T",     full: "num_tasks",           color: COLORS.green,  desc: "Total task count" },
  ];

  const models = [
    { id: "rf",  name: "Random Forest", color: COLORS.yellow, mae: "0.24s", r2: "0.14", trees: "300", detail: "depth 12" },
    { id: "xgb", name: "XGBoost ✓ Best", color: COLORS.green, mae: "0.24s", r2: "0.23", trees: "500", detail: "η=0.03, γ=0.1" },
  ];

  return (
    <div className="glass rounded-2xl p-6">
      <SectionHeader icon="psychology" title="ML Prediction Pipeline"
        subtitle="5 features → predicted execution time → PASTA priority score" color={COLORS.green} />

      {/* Feature vector */}
      <div className="mb-5">
        <div className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-2">Input Features</div>
        <div className="grid grid-cols-5 gap-2">
          {features.map((f) => (
            <motion.div key={f.label} whileHover={{ y: -4, scale: 1.05 }}
              className="rounded-xl p-3 text-center border cursor-default"
              style={{ background: `${f.color}08`, borderColor: `${f.color}20` }}
              title={f.desc}>
              <div className="text-xl font-black mono" style={{ color: f.color }}>{f.label}</div>
              <div className="text-[8px] text-on-surface-variant mt-1 leading-tight">{f.full}</div>
            </motion.div>
          ))}
        </div>
      </div>

      {/* Flow */}
      <div className="flex items-center gap-2 mb-5 flex-wrap">
        <div className="flex items-center gap-1">
          {features.map((f) => (
            <div key={f.label} className="w-3 h-8 rounded-sm" style={{ background: `${f.color}40` }} />
          ))}
        </div>

        <span className="material-symbols-outlined text-outline-variant" style={{ fontSize: 20 }}>arrow_forward</span>

        {/* log1p box */}
        <motion.div whileHover={{ scale: 1.05 }}
          className="rounded-xl px-3 py-2 text-xs border"
          style={{ background: "rgba(18,33,49,0.6)", borderColor: "rgba(66,71,84,0.3)" }}>
          <div className="mono text-on-surface font-bold">log1p(y)</div>
          <div className="text-[9px] text-on-surface-variant">target transform</div>
        </motion.div>

        <span className="material-symbols-outlined text-outline-variant" style={{ fontSize: 20 }}>arrow_forward</span>

        {/* Models */}
        <div className="flex gap-2">
          {models.map((m) => (
            <motion.div key={m.id}
              whileHover={{ scale: 1.05, y: -2 }}
              onHoverStart={() => setHoveredModel(m.id)}
              onHoverEnd={() => setHoveredModel(null)}
              className="rounded-xl px-4 py-3 text-xs cursor-default border transition-all"
              style={hoveredModel === m.id ? {
                background: `${m.color}18`, borderColor: `${m.color}50`,
                boxShadow: `0 0 16px ${m.color}20`,
              } : { background: `${m.color}08`, borderColor: `${m.color}25` }}>
              <div className="font-black" style={{ color: m.color }}>{m.name}</div>
              <div className="text-[9px] text-on-surface-variant">{m.trees} trees · {m.detail}</div>
              <div className="text-[9px] mt-1 mono" style={{ color: m.color }}>
                MAE={m.mae} R²={m.r2}
              </div>
            </motion.div>
          ))}
        </div>

        <span className="material-symbols-outlined text-outline-variant" style={{ fontSize: 20 }}>arrow_forward</span>

        {/* expm1 box */}
        <motion.div whileHover={{ scale: 1.05 }}
          className="rounded-xl px-3 py-2 text-xs border"
          style={{ background: "rgba(18,33,49,0.6)", borderColor: "rgba(66,71,84,0.3)" }}>
          <div className="mono text-on-surface font-bold">expm1(ŷ)</div>
          <div className="text-[9px] text-on-surface-variant">→ seconds</div>
        </motion.div>

        <span className="material-symbols-outlined text-outline-variant" style={{ fontSize: 20 }}>arrow_forward</span>

        {/* Priority output */}
        <motion.div whileHover={{ scale: 1.05 }}
          className="rounded-xl px-4 py-3 text-xs border"
          style={{ background: `${COLORS.green}10`, borderColor: `${COLORS.green}30` }}>
          <div className="font-black text-secondary">PASTA Priority</div>
          <div className="text-[9px] text-on-surface-variant mono mt-0.5">α×norm_sjf + β×norm_aging</div>
        </motion.div>
      </div>

      {/* Why log1p */}
      <div className="rounded-xl p-4 border text-xs"
        style={{ background: "rgba(173,198,255,0.04)", borderColor: "rgba(173,198,255,0.15)" }}>
        <strong className="text-primary">Why log1p transform?</strong>
        <span className="text-on-surface-variant ml-1 leading-relaxed">
          Execution times are right-skewed — a few large jobs dominate (0.1s–2s range with outliers).
          log1p compresses the range so the model doesn't over-weight large jobs during training.
          <Code color={COLORS.green}>expm1(ŷ)</Code> reverses the transform at inference time.
        </span>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN PAGE
// ─────────────────────────────────────────────────────────────────────────────
export default function HowSparkWorks() {
  return (
    <>
      {/* CSS for animated dash-flow */}
      <style>{`
        @keyframes dash-flow {
          to { stroke-dashoffset: -18; }
        }
      `}</style>

      <div className="max-w-[1600px] mx-auto px-6 py-6 space-y-6">
        <HeroBanner />
        <ExecutionLifecycle />

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <ArchitectureDiagram />
          <DAGVisualizer />
        </div>

        <JobTree />

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <ShuffleVisualizer />
          <MemoryModel />
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <SchedulingComparison />
          <KeyConcepts />
        </div>

        <MLPipeline />
      </div>
    </>
  );
}
