# Artifact: Kafka-Spark Streaming Integration on HPC Infrastructure

---

## Overview

This artifact contains scripts, raw data, and instructions needed to reproduce Kafka-Spark streaming benchmarks on HPC infrastructure. The experiments benchmark Apache Kafka and Apache Spark Structured Streaming deployed via Singularity containers on HPC infrastructure managed by SLURM.

## Directory Structure

```
artifacts/
├── README.md                          # This file
├── ENVIRONMENT.md                     # Detailed environment/software specs
├── container/
│   └── sparkhdfs.def                  # Singularity container definition
├── scripts/
│   ├── deployment/
│   │   ├── deploy_cluster.sh          # Full cluster: Spark + HDFS + Kafka + ZooKeeper
│   │   ├── deploy_kafka_only.sh       # Kafka-only deployment (2 nodes)
│   │   └── deploy_spark_hdfs.sh       # Spark + HDFS deployment (no Kafka)
│   ├── benchmarks/
│   │   ├── native_kafka/
│   │   │   ├── kafka_producer_bench.sh    # Native Kafka producer throughput
│   │   │   ├── kafka_consumer_bench.sh    # Native Kafka consumer throughput
│   │   │   └── kafka_full_bench.sh        # Complete benchmark suite (prod+cons+e2e)
│   │   └── spark_kafka/
│   │       ├── spark_producer_bench.sh    # Spark Structured Streaming producer
│   │       ├── spark_consumer_bench.sh    # Spark Structured Streaming consumer
│   │       ├── spark_streaming_consumer.py  # PySpark streaming consumer example
│   │       └── spark_to_kafka_example.py    # PySpark + kafka-python example
│   └── utils/
│       ├── clean.sh                   # Cleanup logs and temp files
│       ├── cleanup_kafka.sh           # Delete Kafka topics and consumer groups
│       └── start_zookeeper.sh         # Standalone ZooKeeper startup
├── data/
│   ├── native_kafka/
│   │   └── kafka_perf_results_20250317_175725.csv   # Complete native Kafka results (168 rows)
│   └── spark_kafka/
│       ├── spark_kafka_test_20250327_1307/          # Run 1
│       ├── spark_kafka_test_20250327_1325/          # Run 2
│       └── spark_kafka_test_20250327_1340/          # Run 3
└── figures/
    └── generate_all_figures.py        # Python script to regenerate all paper figures from CSVs
```

## Prerequisites

### Hardware Requirements
- Minimum 3 HPC nodes (5 recommended for full deployment)
- Each node: 16+ CPU cores, 16+ GB RAM
- 10 Gbps Ethernet (minimum) or InfiniBand interconnect
- SLURM workload manager

### Software Requirements
- Singularity/Apptainer 3.x+ (container runtime)
- SLURM workload manager
- Python 3.8+ with `matplotlib`, `numpy` (for figure generation only)

### Container Image
The experiments use a custom Singularity image (`hsk.sif` or `sparkhdfs.sif`) containing:
- Apache Spark 3.4.x
- Apache Kafka 2.8.x
- Apache ZooKeeper 3.7.x
- Apache Hadoop HDFS 3.3.x
- Python 3.x with kafka-python, pyspark

See `container/sparkhdfs.def` for the full container definition.

## Reproducing the Experiments

### Step 1: Build the Container Image

```bash
# On a machine with root/sudo access (not HPC login node)
sudo singularity build sparkhdfs.sif container/sparkhdfs.def

# Transfer to HPC
scp sparkhdfs.sif user@hpc-cluster:~/
```

### Step 2: Deploy the Cluster

```bash
# Edit deploy_cluster.sh to set:
#   - Number of nodes (SBATCH -N)
#   - Container path (CONTAINER variable)
#   - Time limit

# Submit the deployment job
sbatch scripts/deployment/deploy_cluster.sh

# Wait for "All services started" in the SLURM output
# The coordinator node hostname will be printed
```

### Step 3: Run Native Kafka Benchmarks

```bash
# SSH into the coordinator node (check SLURM output for hostname)
# Run from within a Singularity shell:
singularity shell --bind $HOME/kafka/config:/opt/kafka/config sparkhdfs.sif

# Producer benchmark
bash scripts/benchmarks/native_kafka/kafka_producer_bench.sh

# Consumer benchmark
bash scripts/benchmarks/native_kafka/kafka_consumer_bench.sh

# Full suite (producer + consumer + end-to-end)
bash scripts/benchmarks/native_kafka/kafka_full_bench.sh
```

### Step 4: Run Spark-Kafka Benchmarks

```bash
# From the coordinator node:
bash scripts/benchmarks/spark_kafka/spark_producer_bench.sh

# Results are written to CSV files in ~/data/spark_kafka_test_YYYYMMDD_HHMM/
```

### Step 5: Generate Figures

```bash
# On any machine with Python 3 + matplotlib
pip install matplotlib numpy

# From the artifacts/figures/ directory:
python3 generate_all_figures.py

# Or specify paths explicitly:
python3 generate_all_figures.py --data-dir ../data --output-dir .

# Outputs: fig2_*.pdf/.png through fig5_*.pdf/.png
```

The script reads raw CSV data from the `data/` directory and generates all four paper figures:
- `fig2_native_kafka_throughput` — Producer vs. consumer throughput across partitions
- `fig3_acks_comparison` — Impact of `acks=1` vs. `acks=all`
- `fig4_spark_kafka_throughput` — Spark-Kafka throughput (mean ± std of 3 runs)
- `fig5_spark_vs_native` — Spark Structured Streaming vs. native Kafka CLI

## Benchmark Configuration Matrix

### Native Kafka (168 configurations)
| Parameter | Values |
|-----------|--------|
| Partitions | 1, 2, 4, 8 |
| Message Size | 100 B, 1 KB, 10 KB |
| Acks | 1, all |
| Throughput Target | 100K, 500K, 1M msg/sec |
| Test Types | Isolated producer (72), isolated consumer (36), end-to-end (60) |

### Spark-Kafka Streaming (24 configs × 3 runs = 72 experiments)
| Parameter | Values |
|-----------|--------|
| Partitions | 1, 2, 4, 8 |
| Record Size | 100 B, 1 KB, 10 KB |
| Batch Size | disabled (0), 16 KB |

## Raw Data Format

### Native Kafka CSV (`kafka_perf_results_*.csv`)
```
test_type,partitions,message_size_bytes,producer_throughput,producer_acks,consumer_groups,records_per_sec,mb_per_sec,avg_latency_ms,max_latency_ms,test_duration_sec
producer,1,100,100000,1,N/A,97087.378641,9.26,22.34,284.00,
consumer,1,100,N/A,N/A,1,28768.6997,2.7436,N/A,N/A,N/A
e2e-producer,1,100,1000000,1,1,206185.567010,19.66,15.41,261.00,
e2e-consumer,1,100,N/A,N/A,1,18268.1768,1.7422,N/A,N/A,N/A
```

### Spark-Kafka CSV (`spark_producer_results.csv`)
```
partitions,record_size_bytes,batch_size,records_per_sec,mb_per_sec,duration_sec,total_records
1,100,16384b,815072.9291652299,71.42653153423463,12.268840789794922,10000000
```

## Key Results Summary

| Experiment | Key Finding | Peak Value |
|------------|-------------|------------|
| Native Kafka Producer | Message size dominates throughput | 105 MB/s (10 KB, 8 partitions) |
| Native Kafka Consumer | Partition-insensitive | 79 MB/s (10 KB) |
| Spark-Kafka Producer | 1.7–3.6× faster than native CLI | 180 MB/s (10 KB, 4 partitions) |
| Acks=1 vs Acks=all | <1% difference for 10 KB | Negligible |

## License

This artifact is released under the MIT License for the scripts and documentation. The raw experimental data is provided as-is for research reproducibility.
