import { motion, AnimatePresence } from "framer-motion";
import { useBenchmark } from "../data/BenchmarkContext";

const TABS = [
  { id: "overview",    label: "Overview",        icon: "dashboard" },
  { id: "simulation",  label: "Simulation",       icon: "play_circle" },
  { id: "pasta",       label: "PASTA Algorithm",  icon: "auto_awesome" },
  { id: "spark",       label: "How Spark Works",  icon: "bolt" },
];

export default function Navbar({ active, onNavigate }) {
  const { runBenchmark, isRunning, progress, stage, error, isLive } = useBenchmark();
  return (
    <header className="sticky top-0 z-50 glass-dark border-b border-[#424754]/30">
      {/* Run progress bar (spans the header bottom edge) */}
      <AnimatePresence>
        {isRunning && (
          <motion.div
            initial={{ scaleX: 0, opacity: 0 }}
            animate={{ scaleX: progress / 100, opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ ease: [0.22, 1, 0.36, 1], duration: 0.4 }}
            className="absolute bottom-0 left-0 right-0 h-[3px] bg-secondary origin-left z-[60]"
            style={{ boxShadow: "0 0 12px #4edea3" }}
          />
        )}
      </AnimatePresence>
      <div className="max-w-[1600px] mx-auto px-6 h-14 flex items-center justify-between">
        {/* Brand */}
        <motion.div
          className="flex items-center gap-3 cursor-pointer"
          whileHover={{ scale: 1.02 }}
          onClick={() => onNavigate("overview")}
        >
          <div className="relative">
            <span
              className="material-symbols-outlined fill-icon text-secondary float-icon"
              style={{ fontSize: 22 }}
            >
              local_fire_department
            </span>
            <div className="absolute inset-0 blur-md bg-secondary/30 rounded-full" />
          </div>
          <span className="font-black text-sm tracking-tight text-on-surface">
            PASTA{" "}
            <span className="shimmer-text">Spark</span>
          </span>
          <span className="text-outline text-xs hidden sm:inline font-medium">
            · Adaptive Task Scheduling
          </span>
        </motion.div>

        {/* Tabs */}
        <nav className="flex items-center gap-1">
          {TABS.map((tab) => (
            <motion.button
              key={tab.id}
              onClick={() => onNavigate(tab.id)}
              whileHover={{ y: -1 }}
              whileTap={{ scale: 0.97 }}
              className={`relative px-3 py-2 text-[11px] font-semibold uppercase tracking-wider transition-colors flex items-center gap-1.5 ${
                active === tab.id
                  ? "text-primary"
                  : "text-on-surface-variant hover:text-on-surface"
              }`}
            >
              <span
                className="material-symbols-outlined"
                style={{ fontSize: 14 }}
              >
                {tab.icon}
              </span>
              <span className="hidden sm:inline">{tab.label}</span>
              {active === tab.id && (
                <motion.div
                  layoutId="nav-indicator"
                  className="absolute bottom-0 left-0 right-0 h-[2px] bg-primary rounded-full"
                  transition={{ type: "spring", stiffness: 500, damping: 30 }}
                />
              )}
            </motion.button>
          ))}
        </nav>

        {/* Run benchmark control */}
        <div className="flex items-center gap-3">
          {/* Live / static data source pill */}
          <div
            className={`hidden md:flex items-center gap-1.5 rounded-full px-2.5 py-1 border ${
              isLive
                ? "bg-secondary/10 border-secondary/20 text-secondary"
                : "bg-on-surface-variant/10 border-on-surface-variant/20 text-on-surface-variant"
            }`}
            title={isLive ? "Showing live Spark run data" : "Showing seed data — run a benchmark for live results"}
          >
            <div className={`w-1.5 h-1.5 rounded-full ${isLive ? "bg-secondary pulse-dot" : "bg-on-surface-variant"}`} />
            <span className="text-[10px] font-bold mono tracking-widest">
              {isLive ? "LIVE" : "SEED"}
            </span>
          </div>

          <motion.button
            onClick={runBenchmark}
            disabled={isRunning}
            whileHover={!isRunning ? { scale: 1.04 } : {}}
            whileTap={!isRunning ? { scale: 0.96 } : {}}
            className={`relative flex items-center gap-2 rounded-full pl-3 pr-4 py-1.5 text-[11px] font-black uppercase tracking-wider transition-colors overflow-hidden ${
              isRunning
                ? "bg-secondary/15 border border-secondary/30 text-secondary cursor-wait"
                : "bg-secondary text-[#051424] shine hover:brightness-110"
            }`}
            title={error || stage || "Run the real Spark benchmark pipeline"}
          >
            {isRunning ? (
              <>
                <span className="material-symbols-outlined animate-spin" style={{ fontSize: 15 }}>
                  progress_activity
                </span>
                <span className="mono">{progress}%</span>
                <span className="hidden lg:inline normal-case font-semibold tracking-normal opacity-80 max-w-[140px] truncate">
                  {stage}
                </span>
              </>
            ) : (
              <>
                <span className="material-symbols-outlined fill-icon" style={{ fontSize: 16 }}>
                  rocket_launch
                </span>
                Run Benchmark
              </>
            )}
          </motion.button>
        </div>
      </div>

      {/* Error toast */}
      <AnimatePresence>
        {error && !isRunning && (
          <motion.div
            initial={{ opacity: 0, y: -8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            className="absolute right-6 top-16 max-w-sm glass-dark border border-error/40 rounded-xl px-4 py-2.5 text-[11px] text-error flex items-start gap-2 z-[60]"
          >
            <span className="material-symbols-outlined" style={{ fontSize: 16 }}>error</span>
            <span>{error}</span>
          </motion.div>
        )}
      </AnimatePresence>
    </header>
  );
}
