# PASTA Scheduling Pipeline

## Complete End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PHASE 1: JOB QUEUE SETUP                            │
└─────────────────────────────────────────────────────────────────────────────┘

   benchmark/runner.py:build_job_queue()
   ├─ Seed: random.Random(42)              [Reproducible]
   ├─ Mix 5 small + 5 medium + 5 large jobs
   ├─ Randomize job order
   └─ Output: 15 job specs with:
      ├─ job_type          (small/medium/large)
      ├─ input_path        (500-1500MB text, 5M-15M row CSV, 30K-node graph)
      ├─ num_partitions    (2, 4, or 8)
      ├─ iterations        (1 for small/medium, 200-500 for large PageRank)
      ├─ hour_of_day       (8-18, random)
      └─ input_size_mb     (calculated)

┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 2: SCHEDULER SELECTION                           │
└─────────────────────────────────────────────────────────────────────────────┘

   Based on --mode flag:
   
   ┌─────────────────────────────────────────────────────────────────────┐
   │ MODE: FIFO (run_fifo)                                               │
   │ ─────────────────────────────────────────────────────────────────── │
   │ NO prediction, NO optimization                                      │
   │ • Run jobs in original order (job_index 0→14)                       │
   │ • Single fixed SparkSession with default config                     │
   │   └─ spark.executor.memory=512m, spark.executor.cores=1            │
   │ • No tier assignment                                                │
   │                                                                     │
   │ Result: Baseline performance (usually slowest)                      │
   └─────────────────────────────────────────────────────────────────────┘
   
   ┌─────────────────────────────────────────────────────────────────────┐
   │ MODE: ADAPTIVE (run_adaptive)  — SJF Scheduler                      │
   │ ─────────────────────────────────────────────────────────────────── │
   │                                                                     │
   │ Step 1: PREDICTION                                                  │
   │   for each job_spec:                                                │
   │     predicted_time = ML_Model.predict(                              │
   │       input_size_mb, num_partitions,                                │
   │       job_type, hour_of_day, num_tasks                              │
   │     )  [from model/predict.py]                                      │
   │                                                                     │
   │ Step 2: PRIORITIZATION (Shortest Job First)                         │
   │   Sort job_specs by predicted_time (ascending)                      │
   │   Shortest predicted → Scheduled first                              │
   │                                                                     │
   │ Step 3: TIER ASSIGNMENT (static)                                    │
   │   if predicted_time < 10s  → "small" tier                           │
   │   elif predicted_time < 60s → "medium" tier                         │
   │   else → "large" tier                                               │
   │                                                                     │
   │ Step 4: RESOURCE CONFIG                                             │
   │   small:  512m memory, 1 core, 4 partitions                         │
   │   medium: 1g memory, 2 cores, 8 partitions                          │
   │   large:  2g memory, 2 cores, 16 partitions                         │
   │                                                                     │
   │ Step 5: FAIR SCHEDULER POOL                                         │
   │   Generate conf/fairscheduler.xml with pool weights:                │
   │     weight(small_pool)  ∝ count(small jobs)                         │
   │     weight(medium_pool) ∝ count(medium jobs)                        │
   │     weight(large_pool)  ∝ count(large jobs)                         │
   │                                                                     │
   │ Result: Better than FIFO (SJF principle works)                      │
   └─────────────────────────────────────────────────────────────────────┘
   
   ┌─────────────────────────────────────────────────────────────────────┐
   │ MODE: PASTA (run_pasta)  — Adaptive + Aging + Feedback              │
   │ ─────────────────────────────────────────────────────────────────── │
   │                                                                     │
   │ Step 1: PREDICTION (same as Adaptive)                               │
   │   predicted_time = ML_Model.predict(job_spec)                       │
   │                                                                     │
   │ Step 2: COMPUTE DYNAMIC TIER BOUNDARIES                             │
   │   Percentiles of current predictions:                               │
   │   low  = 33rd percentile of all predictions                         │
   │   high = 66th percentile of all predictions                         │
   │   Example: [0.1, 0.2, 0.5, 1.0, 3.0, 10.0]                          │
   │     → low=0.35s, high=2.0s                                          │
   │                                                                     │
   │ Step 3: PRIORITY COMPUTATION (with Aging)                           │
   │   For each job at time t:                                           │
   │     wait_time_i = t - arrival_time_i                                │
   │     norm_sjf_i = 1 - (pred_i - min_pred) / (max_pred - min_pred)    │
   │     norm_aging_i = wait_i / max_wait                                │
   │     priority_i = 0.70 * norm_sjf_i + 0.30 * norm_aging_i            │
   │                                                                     │
   │   Higher priority → Scheduled sooner                                │
   │   Short jobs get high priority via SJF term                         │
   │   Jobs that waited long get boosted via aging term                  │
   │   Result: Prevents starvation of long jobs                          │
   │                                                                     │
   │ Step 4: SORT by priority (descending)                               │
   │   Schedule highest priority job first                               │
   │                                                                     │
   │ Step 5: DYNAMIC TIER ASSIGNMENT                                     │
   │   if predicted_time <= low  → "small" tier                          │
   │   elif predicted_time <= high → "medium" tier                       │
   │   else → "large" tier                                               │
   │   (Same config as Adaptive: 512m/1g/2g memory)                      │
   │                                                                     │
   │ Step 6: FAIR SCHEDULER POOL (adaptive weights)                      │
   │   Pool weights computed based on current tier distribution          │
   │                                                                     │
   │ Step 7: OPTIONAL FEEDBACK LOOP (--feedback flag)                    │
   │   After run completes:                                              │
   │   1. Append actual times to data/raw_metrics.csv                    │
   │   2. Re-run model/train.py (model retrains)                         │
   │   3. Next run uses improved model                                   │
   │                                                                     │
   │ Result: Best performance (SJF + starvation prevention + learning)   │
   └─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 3: JOB EXECUTION LOOP                            │
└─────────────────────────────────────────────────────────────────────────────┘

   For each scheduled job (in priority order):
   
   ┌─────────────────────────────────────────────────────────────────┐
   │ Step A: STOP PREVIOUS SparkSession                              │
   │   (Required: can't change executor.memory on running JVM)        │
   │   SparkSession.getActiveSession().stop()                        │
   └─────────────────────────────────────────────────────────────────┘
            ↓
   ┌─────────────────────────────────────────────────────────────────┐
   │ Step B: BUILD NEW SparkSession with tier-specific config        │
   │                                                                 │
   │ SparkSession.builder                                            │
   │   .master("local[*]")  ← All CPU cores                           │
   │   .config("spark.executor.memory", tier_memory)  ← Tier-specific │
   │   .config("spark.executor.cores", tier_cores)    ← Tier-specific │
   │   .config("spark.scheduler.mode", "FAIR")        ← Fair pools    │
   │   .config("spark.scheduler.allocation.file",     ← Pool weights  │
   │           "conf/fairscheduler.xml")                              │
   │   .getOrCreate()                                                 │
   │                                                                 │
   │ Local mode execution:                                            │
   │   • Driver runs in main JVM                                      │
   │   • Executors = threads in same JVM (one per core)               │
   │   • Tasks parallelized via ThreadPoolExecutor                    │
   │   • No network I/O for shuffle (shared JVM memory)               │
   └─────────────────────────────────────────────────────────────────┘
            ↓
   ┌─────────────────────────────────────────────────────────────────┐
   │ Step C: DISPATCH JOB to correct handler                          │
   │                                                                 │
   │ if job_type == "small":                                          │
   │   run_word_count(spark, input_path, num_partitions)              │
   │   → Read text file, split into words, count per partition        │
   │                                                                 │
   │ elif job_type == "medium":                                       │
   │   run_join_aggregation(spark, path_a, path_b, num_partitions)    │
   │   → Read 2 CSVs, join on key, aggregate by group                 │
   │                                                                 │
   │ else:  # "large"                                                 │
   │   run_iterative_pagerank(spark, graph_path, partitions, iters)   │
   │   → Load graph, run 200-500 iterations of:                       │
   │     join(links, ranks).flatMap(...).reduceByKey(...)             │
   │   → Checkpoint every 20 iterations to prevent StackOverflow      │
   │                                                                 │
   │ Execution:                                                       │
   │   • Task DAG built from job RDD lineage                          │
   │   • Spark scheduler distributes tasks to executor threads        │
   │   • Each core runs 1 task at a time (no true parallelism in     │
   │     local mode, but realistic Spark behavior simulated)          │
   │   • Return job result (e.g., word count, aggregated data)        │
   └─────────────────────────────────────────────────────────────────┘
            ↓
   ┌─────────────────────────────────────────────────────────────────┐
   │ Step D: RECORD METRICS                                           │
   │                                                                 │
   │ For each job result:                                             │
   │   {                                                              │
   │     "job_index": 0,                                              │
   │     "job_type": "small",                                         │
   │     "input_size_mb": 1000.0,                                     │
   │     "predicted_time": 0.26,  ← ML prediction                     │
   │     "actual_time": 16.22,    ← Measured wall-clock               │
   │     "wait_time": 0.0,        ← Time spent waiting                │
   │     "start_ts": 0.0,         ← Global start timestamp            │
   │     "end_ts": 16.22,         ← Global end timestamp              │
   │     "tier": "small",         ← Assigned tier                     │
   │     "config_used": {...},    ← Executor config                   │
   │   }                                                              │
   │                                                                 │
   │ Append to results list                                           │
   └─────────────────────────────────────────────────────────────────┘
            ↓
            └─→ Repeat for next job in queue

┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 4: RESULTS AGGREGATION                           │
└─────────────────────────────────────────────────────────────────────────────┘

   After all jobs complete:
   
   Calculate:
   • makespan = max(end_ts across all jobs)
   • avg_latency = mean(actual_time)
   • speedup = FIFO_makespan / this_makespan
   
   Save to JSON:
   data/benchmark_fifo.json
   data/benchmark_adaptive.json
   data/benchmark_pasta.json
   
   Each JSON contains:
   {
     "results": [job_record_1, job_record_2, ...],
     "makespan": 418.57
   }

┌─────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 5: OPTIONAL FEEDBACK LOOP                        │
└─────────────────────────────────────────────────────────────────────────────┘

   If --feedback flag set:
   
   1. READ benchmark_pasta.json results
   
   2. TRANSFORM to ML format:
      For each job:
        {
          "input_size_mb": 1000.0,
          "num_partitions": 4,
          "job_type": "small",
          "hour_of_day": 12,
          "num_tasks": 100,
          "execution_time_sec": 16.22  ← Measured actual time
        }
   
   3. APPEND to data/raw_metrics.csv
      (Raw metrics now have real local execution data)
   
   4. RETRAIN model:
      python -m model.train
      • Loads updated raw_metrics.csv
      • Trains new RandomForest + XGBoost
      • Saves to model/artifacts/
   
   5. NEXT RUN uses improved model
      (Model becomes better calibrated to local hardware)
      
   Benefit: Prediction accuracy improves over multiple runs
            (PASTA learns from its own predictions)
```

## Key Design Decisions

### Why Stop/Rebuild SparkSession Per Job?

```
Problem: spark.executor.memory is a JVM startup parameter
         Cannot change while JVM is running
         
Solution: Stop active session (kill JVM)
         Create new session with different config
         
Cost: ~5-10 seconds overhead per job = ~75s total for 15 jobs
      Justified by better resource allocation per job tier
```

### Why Local Mode [*] Instead of Distributed Spark?

```
Advantages:
✓ No cluster needed (run on laptop)
✓ Realistic Spark DAG scheduling + task distribution
✓ Realistic shuffle (gather data between stages)
✓ Fast feedback loop for algorithm testing
✓ All CPU cores utilized (8-16 on modern machine)

Limitations:
✗ No network I/O between nodes
✗ All executor memory must fit in single machine
✗ No node failures / recovery testing
```

### Why 0.70 SJF + 0.30 Aging in PASTA?

```
Balance between two goals:

1. Optimize for makespan (SJF principle, weight=0.70)
   → Run shortest jobs first to finish queue quickly
   
2. Prevent starvation (aging, weight=0.30)
   → Long jobs can't be postponed forever
   
Tuning: 70/30 split empirically chosen
        Could be 80/20 (more aggressive SJF)
        Or 60/40 (more starvation prevention)
```

### Why Dynamic Tier Boundaries (33rd/66th percentile)?

```
Problem with fixed 10s/60s thresholds:
  • Small workload (all jobs 1-5s): 10s threshold too high
  • Large workload (all jobs 50-500s): 60s threshold too low
  • Tier assignment becomes meaningless

Solution: Dynamic percentiles adapt to current queue:
  • Always use 33% of jobs as "small" tier
  • Always use 33-66% as "medium" tier  
  • Always use top 33% as "large" tier
  • Tier assignment meaningful regardless of workload scale
```

## Example: 15-Job Benchmark Run

```
Queue (seeded, reproducible):
  Job 0: small   500MB  → predicted=0.26s
  Job 1: medium  99MB   → predicted=0.99s
  Job 2: large   1.5MB  200iter → predicted=0.79s
  ... (15 total)

PASTA Scheduling (ALPHA=0.70, BETA=0.30):

Time t=0 (all jobs arrive):
  Compute dynamic tiers:
    low  = p33([0.26, 0.99, 0.79, ...]) ≈ 0.61s
    high = p66(...) ≈ 0.84s
  
  For each job (wait_time = 0 initially):
    priority = 0.70 * norm_sjf + 0.30 * norm_aging
             = 0.70 * norm_sjf + 0.30 * 0
             = 0.70 * norm_sjf
  
  Sort by priority → [Job0, Job2, Job5, ...]
  (Shortest predicted times first, same as SJF)
  
  Assign tiers:
    Job 0: 0.26s < 0.61s → "small"  (512m, 1 core)
    Job 2: 0.79s > 0.84s → "large"  (2g, 2 cores)
    ...

Execution:
  t=0.0:  Start Job 0  (small tier)  end_ts=16.22s
  t=16.22: Stop Spark, rebuild for Job2 config
  t=16.3:  Start Job 2  (large tier)  end_ts=98.59s
  ...
  t=418.6: All jobs done
  
Makespan = 418.6s
Speedup = 544.5s (FIFO) / 418.6s = 1.30x
```
