import { motion } from "framer-motion";
import { typeColor } from "../data/benchmarkData";

export default function TypeChip({ type, size = "sm" }) {
  const c = typeColor(type);
  const px = size === "xs" ? "px-1.5 py-0" : "px-2.5 py-0.5";
  const txt = size === "xs" ? "text-[9px]" : "text-[10px]";
  return (
    <motion.span
      whileHover={{ scale: 1.05 }}
      style={{
        background: `${c}22`,
        color: c,
        border: `1px solid ${c}44`,
      }}
      className={`inline-flex items-center rounded-full font-bold uppercase tracking-wider ${px} ${txt}`}
    >
      {type}
    </motion.span>
  );
}

export function TierChip({ tier, size = "sm" }) {
  const colors = { small: "#adc6ff", medium: "#4edea3", large: "#ffb95f" };
  const c = colors[tier] || "#adc6ff";
  const px = size === "xs" ? "px-1.5 py-0" : "px-2.5 py-0.5";
  const txt = size === "xs" ? "text-[9px]" : "text-[10px]";
  return (
    <motion.span
      whileHover={{ scale: 1.05 }}
      style={{
        background: `${c}22`,
        color: c,
        border: `1px solid ${c}44`,
      }}
      className={`inline-flex items-center rounded-full font-bold ${px} ${txt}`}
    >
      {tier}
    </motion.span>
  );
}
