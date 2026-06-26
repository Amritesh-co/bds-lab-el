import { useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import Navbar from "./components/Navbar";
import Overview from "./pages/Overview";
import Simulation from "./pages/Simulation";
import PastaAlgorithm from "./pages/PastaAlgorithm";
import HowSparkWorks from "./pages/HowSparkWorks";

const PAGES = {
  overview:   <Overview />,
  simulation: <Simulation />,
  pasta:      <PastaAlgorithm />,
  spark:      <HowSparkWorks />,
};

const pageVariants = {
  initial: { opacity: 0, y: 18 },
  enter:   { opacity: 1, y: 0, transition: { duration: 0.35, ease: [0.22, 1, 0.36, 1] } },
  exit:    { opacity: 0, y: -10, transition: { duration: 0.2, ease: "easeIn" } },
};

export default function App() {
  const [active, setActive] = useState("overview");

  return (
    <div className="dark min-h-screen bg-surface mesh-bg">
      {/* Ambient glow blobs */}
      <div className="fixed inset-0 pointer-events-none overflow-hidden z-0">
        <div
          className="absolute rounded-full blur-[120px] opacity-[0.06]"
          style={{ width: 600, height: 600, top: -150, left: -100, background: "#4edea3" }}
        />
        <div
          className="absolute rounded-full blur-[160px] opacity-[0.05]"
          style={{ width: 500, height: 500, bottom: -100, right: -50, background: "#adc6ff" }}
        />
        <div
          className="absolute rounded-full blur-[100px] opacity-[0.04]"
          style={{ width: 400, height: 400, top: "40%", left: "40%", background: "#ffb95f" }}
        />
      </div>

      {/* Dot grid overlay */}
      <div className="fixed inset-0 pointer-events-none dot-grid opacity-30 z-0" />

      <div className="relative z-10">
        <Navbar active={active} onNavigate={setActive} />

        <AnimatePresence mode="wait">
          <motion.main
            key={active}
            variants={pageVariants}
            initial="initial"
            animate="enter"
            exit="exit"
          >
            {PAGES[active]}
          </motion.main>
        </AnimatePresence>

        {/* Footer */}
        <footer className="border-t border-outline-variant/15 mt-12 py-4 px-6">
          <div className="max-w-[1600px] mx-auto flex items-center justify-between text-[10px] text-on-surface-variant/50">
            <span className="mono">PASTA Spark Scheduler · BDA Lab EL · seed=42</span>
            <span>XGBoost MAE=0.24s · PASTA makespan 5.03s · 1.54× speedup over FIFO</span>
          </div>
        </footer>
      </div>
    </div>
  );
}
