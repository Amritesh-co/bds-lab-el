// ── Real benchmark data from actual Spark runs ────────────────────────────────
export const MAX_T = 8.75;

export const FIFO = [
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 2, actual_time: 2.0858, start_ts: 0,      end_ts: 2.0859 },
  { job_type: "large",  input_size_mb: 0.6,  num_partitions: 2, actual_time: 1.216,  start_ts: 2.0859, end_ts: 3.3019 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 2, actual_time: 0.2671, start_ts: 3.3019, end_ts: 3.5689 },
  { job_type: "medium", input_size_mb: 15.0, num_partitions: 8, actual_time: 0.7684, start_ts: 3.569,  end_ts: 4.3374 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 8, actual_time: 0.6643, start_ts: 4.3374, end_ts: 5.0017 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 8, actual_time: 0.4221, start_ts: 5.0017, end_ts: 5.4238 },
  { job_type: "medium", input_size_mb: 6.0,  num_partitions: 8, actual_time: 0.5449, start_ts: 5.4239, end_ts: 5.9688 },
  { job_type: "small",  input_size_mb: 2.0,  num_partitions: 4, actual_time: 0.0815, start_ts: 5.9688, end_ts: 6.0503 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 4, actual_time: 0.2113, start_ts: 6.0503, end_ts: 6.2617 },
  { job_type: "small",  input_size_mb: 5.0,  num_partitions: 4, actual_time: 0.1133, start_ts: 6.2617, end_ts: 6.375  },
  { job_type: "small",  input_size_mb: 5.0,  num_partitions: 4, actual_time: 0.1083, start_ts: 6.375,  end_ts: 6.4833 },
  { job_type: "large",  input_size_mb: 0.6,  num_partitions: 2, actual_time: 0.3255, start_ts: 6.4834, end_ts: 6.8089 },
  { job_type: "small",  input_size_mb: 10.0, num_partitions: 4, actual_time: 0.1707, start_ts: 6.8089, end_ts: 6.9796 },
  { job_type: "small",  input_size_mb: 10.0, num_partitions: 4, actual_time: 0.1665, start_ts: 6.9797, end_ts: 7.1462 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 4, actual_time: 0.5966, start_ts: 7.1462, end_ts: 7.7428 },
];

export const SJF = [
  { job_type: "small",  input_size_mb: 2.0,  num_partitions: 4, predicted_time: 0.135,  actual_time: 0.2753, tier: "small", start_ts: 0.0019, end_ts: 0.2772 },
  { job_type: "small",  input_size_mb: 10.0, num_partitions: 4, predicted_time: 0.2578, actual_time: 0.1626, tier: "small", start_ts: 0.2775, end_ts: 0.4401 },
  { job_type: "small",  input_size_mb: 10.0, num_partitions: 4, predicted_time: 0.2578, actual_time: 0.1577, tier: "small", start_ts: 0.4403, end_ts: 0.598  },
  { job_type: "small",  input_size_mb: 5.0,  num_partitions: 4, predicted_time: 0.2942, actual_time: 0.1039, tier: "small", start_ts: 0.5983, end_ts: 0.7022 },
  { job_type: "small",  input_size_mb: 5.0,  num_partitions: 4, predicted_time: 0.2942, actual_time: 0.0992, tier: "small", start_ts: 0.7025, end_ts: 0.8017 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 4, predicted_time: 0.529,  actual_time: 0.2751, tier: "small", start_ts: 0.8019, end_ts: 1.0771 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 2, predicted_time: 0.5839, actual_time: 0.2011, tier: "small", start_ts: 1.0774, end_ts: 1.2785 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 2, predicted_time: 0.5839, actual_time: 0.1994, tier: "small", start_ts: 1.2787, end_ts: 1.4781 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 4, predicted_time: 0.646,  actual_time: 0.6286, tier: "small", start_ts: 1.4784, end_ts: 2.1071 },
  { job_type: "large",  input_size_mb: 0.6,  num_partitions: 2, predicted_time: 0.6885, actual_time: 0.7545, tier: "small", start_ts: 2.1073, end_ts: 2.8618 },
  { job_type: "large",  input_size_mb: 0.6,  num_partitions: 2, predicted_time: 0.6885, actual_time: 0.3158, tier: "small", start_ts: 2.8621, end_ts: 3.1779 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 8, predicted_time: 0.7524, actual_time: 0.5742, tier: "small", start_ts: 3.1781, end_ts: 3.7523 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 8, predicted_time: 0.7524, actual_time: 0.3773, tier: "small", start_ts: 3.7527, end_ts: 4.13   },
  { job_type: "medium", input_size_mb: 6.0,  num_partitions: 8, predicted_time: 1.022,  actual_time: 0.4721, tier: "small", start_ts: 4.1303, end_ts: 4.6024 },
  { job_type: "medium", input_size_mb: 15.0, num_partitions: 8, predicted_time: 1.1191, actual_time: 0.5949, tier: "small", start_ts: 4.6027, end_ts: 5.1976 },
];

export const ADAPTIVE = [
  { job_type: "small",  input_size_mb: 2.0,  num_partitions: 4, predicted_time: 0.135,  actual_time: 0.2753, tier: "small",  start_ts: 0.0019, end_ts: 0.2772 },
  { job_type: "small",  input_size_mb: 10.0, num_partitions: 4, predicted_time: 0.2578, actual_time: 0.1626, tier: "small",  start_ts: 0.2775, end_ts: 0.4401 },
  { job_type: "small",  input_size_mb: 10.0, num_partitions: 4, predicted_time: 0.2578, actual_time: 0.1577, tier: "small",  start_ts: 0.4403, end_ts: 0.598  },
  { job_type: "small",  input_size_mb: 5.0,  num_partitions: 4, predicted_time: 0.2942, actual_time: 0.1039, tier: "small",  start_ts: 0.5983, end_ts: 0.7022 },
  { job_type: "small",  input_size_mb: 5.0,  num_partitions: 4, predicted_time: 0.2942, actual_time: 0.0992, tier: "small",  start_ts: 0.7025, end_ts: 0.8017 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 4, predicted_time: 0.529,  actual_time: 0.2751, tier: "small",  start_ts: 0.8019, end_ts: 1.0771 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 2, predicted_time: 0.5839, actual_time: 0.2011, tier: "small",  start_ts: 1.0774, end_ts: 1.2785 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 2, predicted_time: 0.5839, actual_time: 0.1994, tier: "small",  start_ts: 1.2787, end_ts: 1.4781 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 4, predicted_time: 0.646,  actual_time: 0.6286, tier: "small",  start_ts: 1.4784, end_ts: 2.1071 },
  { job_type: "large",  input_size_mb: 0.6,  num_partitions: 2, predicted_time: 0.6885, actual_time: 0.7545, tier: "small",  start_ts: 2.1073, end_ts: 2.8618 },
  { job_type: "large",  input_size_mb: 0.6,  num_partitions: 2, predicted_time: 0.6885, actual_time: 0.3158, tier: "small",  start_ts: 2.8621, end_ts: 3.1779 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 8, predicted_time: 0.7524, actual_time: 0.5742, tier: "small",  start_ts: 3.1781, end_ts: 3.7523 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 8, predicted_time: 0.7524, actual_time: 0.3773, tier: "small",  start_ts: 3.7527, end_ts: 4.13   },
  { job_type: "medium", input_size_mb: 6.0,  num_partitions: 8, predicted_time: 1.022,  actual_time: 0.4721, tier: "small",  start_ts: 4.1303, end_ts: 4.6024 },
  { job_type: "medium", input_size_mb: 15.0, num_partitions: 8, predicted_time: 1.1191, actual_time: 0.5949, tier: "small",  start_ts: 4.6027, end_ts: 5.1976 },
];

export const PASTA = [
  { job_type: "small",  input_size_mb: 2.0,  num_partitions: 4, predicted_time: 0.135,  actual_time: 0.2599, priority: 0.7,       tier: "small",  start_ts: 0.0015, end_ts: 0.2615 },
  { job_type: "small",  input_size_mb: 10.0, num_partitions: 4, predicted_time: 0.2578, actual_time: 0.1761, priority: 0.612651,  tier: "small",  start_ts: 0.2618, end_ts: 0.4379 },
  { job_type: "small",  input_size_mb: 10.0, num_partitions: 4, predicted_time: 0.2578, actual_time: 0.161,  priority: 0.612651,  tier: "small",  start_ts: 0.4382, end_ts: 0.5991 },
  { job_type: "small",  input_size_mb: 5.0,  num_partitions: 4, predicted_time: 0.2942, actual_time: 0.0946, priority: 0.586759,  tier: "small",  start_ts: 0.5993, end_ts: 0.6939 },
  { job_type: "small",  input_size_mb: 5.0,  num_partitions: 4, predicted_time: 0.2942, actual_time: 0.0936, priority: 0.586759,  tier: "small",  start_ts: 0.6941, end_ts: 0.7877 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 4, predicted_time: 0.529,  actual_time: 0.2382, priority: 0.419744,  tier: "medium", start_ts: 0.7879, end_ts: 1.0261 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 2, predicted_time: 0.5839, actual_time: 0.1881, priority: 0.380693,  tier: "medium", start_ts: 1.0263, end_ts: 1.2143 },
  { job_type: "medium", input_size_mb: 1.5,  num_partitions: 2, predicted_time: 0.5839, actual_time: 0.1876, priority: 0.380693,  tier: "medium", start_ts: 1.2146, end_ts: 1.4021 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 4, predicted_time: 0.646,  actual_time: 0.5692, priority: 0.336521,  tier: "medium", start_ts: 1.4023, end_ts: 1.9715 },
  { job_type: "large",  input_size_mb: 0.6,  num_partitions: 2, predicted_time: 0.6885, actual_time: 0.7382, priority: 0.30629,   tier: "medium", start_ts: 1.9718, end_ts: 2.71   },
  { job_type: "large",  input_size_mb: 0.6,  num_partitions: 2, predicted_time: 0.6885, actual_time: 0.3102, priority: 0.30629,   tier: "medium", start_ts: 2.7102, end_ts: 3.0204 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 8, predicted_time: 0.7524, actual_time: 0.5458, priority: 0.260837,  tier: "large",  start_ts: 3.0207, end_ts: 3.5665 },
  { job_type: "large",  input_size_mb: 0.06, num_partitions: 8, predicted_time: 0.7524, actual_time: 0.3703, priority: 0.260837,  tier: "large",  start_ts: 3.571,  end_ts: 3.9413 },
  { job_type: "medium", input_size_mb: 6.0,  num_partitions: 8, predicted_time: 1.022,  actual_time: 0.4846, priority: 0.069068,  tier: "large",  start_ts: 3.9416, end_ts: 4.4262 },
  { job_type: "medium", input_size_mb: 15.0, num_partitions: 8, predicted_time: 1.1191, actual_time: 0.6076, priority: 0.0,       tier: "large",  start_ts: 4.4264, end_ts: 5.034  },
];

// ── Color helpers ─────────────────────────────────────────────────────────────
export const TYPE_COLORS = {
  small:  "#adc6ff",
  medium: "#4edea3",
  large:  "#ffb95f",
};

export const ALGO_COLORS = {
  fifo:  "#ffb4ab",
  sjf:   "#ffb95f",
  pasta: "#4edea3",
};

export const TIER_PARTITIONS = { small: 4, medium: 8, large: 16 };

export const typeColor = (t) => TYPE_COLORS[t] || "#adc6ff";
export const fmtT = (t) => (typeof t === "number" ? t.toFixed(3) + "s" : "—");
