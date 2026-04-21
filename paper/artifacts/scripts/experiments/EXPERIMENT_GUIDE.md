# Experiment Guide: Strengthening the Paper

This guide covers three experiments to add baselines and state-of-the-art comparisons.

## Time Estimate Summary

| Experiment | Who runs it | HPC time | Your active time |
|-----------|-------------|----------|-----------------|
| **1. Broker Scaling + RF** | You on HPC | ~3–4 hours | ~30 min setup + monitor |
| **2. Latency Analysis** | Already done | 0 | 0 |
| **3. SProBench Comparison** | You on HPC | ~2–3 hours | ~1 hour setup + monitor |
| **Total** | | **~5–7 hours HPC** | **~2 hours active** |

---

## Experiment 1: Broker Scaling + Replication Factor

### What it tests
- 1 broker, 2 brokers, 3 brokers
- RF=1, RF=2, RF=3 (up to broker count)
- Same parameter grid: partitions {1,4,8} × message sizes {100B, 1KB, 10KB} × acks {1, all}
- Producer, consumer, and end-to-end tests

### Step-by-step

**Step 1: Transfer scripts to HPC**
```bash
scp deploy_kafka_N_brokers.sh kafka_scaling_bench.sh your_user@aion-cluster:~/
```

**Step 2: Deploy 1-broker cluster and run benchmark**
```bash
# Submit 1-broker deployment (2 nodes: 1 ZK + 1 broker)
NUM_BROKERS=1 sbatch --nodes=2 deploy_kafka_N_brokers.sh

# Wait for "Cluster ready" in the SLURM output, then SSH to coordinator:
ssh $(cat ~/coordinatorNode)

# Run benchmark (from inside the running SLURM job)
bash ~/kafka_scaling_bench.sh 1
# Results → ~/kafka_bench/results/scaling_bench_1br_*.csv

# Cancel the SLURM job when done
scancel <jobid>
```

**Step 3: Deploy 2-broker cluster and run benchmark**
```bash
NUM_BROKERS=2 sbatch --nodes=3 deploy_kafka_N_brokers.sh

# Wait, SSH, run:
bash ~/kafka_scaling_bench.sh 2
# Results → ~/kafka_bench/results/scaling_bench_2br_*.csv

scancel <jobid>
```

**Step 4: Deploy 3-broker cluster and run benchmark**
```bash
NUM_BROKERS=3 sbatch --nodes=4 deploy_kafka_N_brokers.sh

# Wait, SSH, run:
bash ~/kafka_scaling_bench.sh 3
# Results → ~/kafka_bench/results/scaling_bench_3br_*.csv

scancel <jobid>
```

**Step 5: Download results**
```bash
scp your_user@aion-cluster:~/kafka_bench/results/scaling_bench_*.csv ./
```

### Expected time per broker config
| Brokers | RF levels | Producer tests | Consumer tests | E2E tests | ~Total |
|---------|-----------|---------------|----------------|-----------|--------|
| 1 | 1 | 18 | 9 | 9 | ~30 min |
| 2 | 2 | 36 | 18 | 18 | ~60 min |
| 3 | 3 | 54 | 27 | 27 | ~90 min |
| | | | | **Grand total** | **~3 hours** |

Plus ~20 minutes deployment time per cluster = **~4 hours total HPC time**.

### CSV output format
```
num_brokers,replication_factor,test_type,partitions,message_size_bytes,producer_acks,consumer_groups,records_per_sec,mb_per_sec,avg_latency_ms,max_latency_ms
3,2,producer,4,10240,1,N/A,10500.5,102.54,178.3,259.0
```

---

## Experiment 2: Latency Analysis (DONE — No HPC needed)

Two new figures have already been generated from the existing CSV data:

- **fig6_latency_analysis.pdf** — Bar chart: avg latency by partition count and acks setting
- **fig7_throughput_vs_latency.pdf** — Scatter: throughput vs. latency tradeoff

Key findings from the existing data:
- 100B messages: very low latency (1–14 ms) but low throughput
- 1KB messages: high latency (~180–220 ms) — Kafka batches internally
- 10KB messages: moderate latency (~175–185 ms) with highest throughput
- acks=all adds 2–5% latency overhead for 1KB/10KB, larger relative impact for 100B
- Latency decreases with more partitions for medium messages (parallelism helps)

---

## Experiment 3: SProBench Comparison

### What it tests
Run SProBench's pass-through benchmark on Aion to get a direct comparison with a published benchmark tool on the SAME hardware. This shows:
- Containerized (your setup) vs native (SProBench) deployment overhead
- How your Kafka throughput compares to SProBench's Kafka throughput

### Prerequisites
- Java 11+ (should be available on Aion via `module load`)
- Git access on login node

### Step-by-step

**Step 1: Clone SProBench**
```bash
ssh your_user@aion-cluster
cd ~
git clone https://github.com/apurvkulkarni7/SProBench.git
cd SProBench
```

**Step 2: Check available modules**
```bash
module avail java
module avail jdk
# Load Java 11 or higher
module load lang/Java/11.0.2   # or whatever is available
java -version
```

**Step 3: Build SProBench**
```bash
# SProBench has a build script — follow their README
# Typically:
./scripts/build.sh
# or
mvn clean package    # if Maven-based
```

**Step 4: Configure SProBench**
Edit the SProBench configuration file to match your Aion setup:
- Set number of nodes to match your experiments (e.g., 2-4 nodes)
- Set Kafka topic partitions: 1, 4, 8
- Set message/event size: 27 bytes (SProBench default), then also 100B, 1KB, 10KB
- Set workload: 100K events or matching your NUM_MESSAGES
- Pipeline: pass-through (baseline)

**Step 5: Run SProBench via SLURM**
```bash
# SProBench has native SLURM support
# Use their entrypoint script with batch mode
# Refer to: https://github.com/apurvkulkarni7/SProBench/blob/main/README.md
```

**Step 6: Collect results**
SProBench outputs throughput and latency metrics. Record:
- Kafka broker throughput (events/sec and MB/s)
- Processing latency
- Compare against your containerized Kafka results

### What to compare
| Metric | SProBench (native) | Your setup (Singularity) | Difference |
|--------|-------------------|-------------------------|------------|
| Kafka producer throughput (10KB, 4 part) | ? MB/s | 103.8 MB/s | ?% |
| Kafka producer throughput (1KB, 4 part) | ? MB/s | 83.5 MB/s | ?% |
| Kafka producer latency | ? ms | 177 ms | ?% |

If SProBench achieves similar throughput, it validates that containerization overhead is minimal.
If SProBench achieves higher throughput, it quantifies the Singularity overhead.
Both are interesting findings for the paper.

### Estimated time
- Setup and build: ~30-60 minutes
- Running experiments: ~1-2 hours
- Total: ~2-3 hours

### If SProBench setup is too complex
Fall back to a **literature comparison table** using SProBench's published numbers from their paper:
- Their HPC cluster (Barnard): Intel Xeon Platinum 8470, 104 cores/node, 512 GB RAM
- Your HPC cluster (Aion): AMD EPYC 7H12, 128 cores/node, 256 GB RAM
- Compare throughput numbers, acknowledging hardware differences

---

## After All Experiments

Copy the new CSV results to `paper/artifacts/data/` and update the figure generation script.
The new data enables:
1. A **broker scaling figure** showing throughput vs. broker count
2. A **replication overhead figure** showing RF=1 vs RF=2 vs RF=3
3. A **comparison table** with SProBench and published benchmarks
4. **Latency figures** (already generated)

These additions transform the paper from "single-broker characterization" to a comprehensive scalability and comparison study.
