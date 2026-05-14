# HPC-to-Cloud Spark-Kafka Streaming Pipelines

Evaluation of Apache Spark and Apache Kafka for streaming HPC-processed scientific data to the cloud. The repository is organized around the two parts of the study:

1. **`hpc-characterization/`** — Spark-Kafka streaming performance on an HPC cluster deployed via Singularity containers on SLURM (60 native Kafka measurements, 12 Spark Structured Streaming configurations, and 36 broker-scaling configurations under RF 1--3).
2. **`hpc-to-cloud-pipeline/`** — Five end-to-end delivery pipelines from HPC to AWS EC2 over WAN, evaluated on a 5 GB EEA air-quality dataset.

## Pipeline Approaches

| #  | Approach              | Type   | Description                                                     |
| -- | --------------------- | ------ | --------------------------------------------------------------- |
| A1 | Cloud-Side Processing | Batch  | Transfer raw data via scp, filter with Spark on cloud           |
| A2 | Direct Kafka over WAN | Stream | Spark produces filtered records directly to remote Kafka broker |
| A3 | HPC-Side Processing   | Batch  | Spark filters on HPC, scp filtered output, ingest to Kafka      |
| A4 | MirrorMaker 2         | Stream | Spark to regional Kafka broker, MM2 replicates cross-region     |
| A5 | SkyHOST               | Stream | Spark to regional Kafka broker, SkyHOST transfers cross-region  |

## Key Results (5 GB EEA air-quality dataset)

| # | Approach              | Type   | E2E (s) | First Record Latency (s) |
| - | --------------------- | ------ | ------- | ------------------------ |
| 1 | Cloud-Side Processing | Batch  | 368     | 339                      |
| 2 | Direct Producer       | Stream | 667     | 35                       |
| 3 | HPC-Side Processing   | Batch  | 183     | 103                      |
| 4 | MM2 Replication       | Stream | 219     | 35                       |
| 5 | SkyHOST               | Stream | 186     | 35                       |

HPC-Side batch processing achieves the fastest completion (183 s), but no record reaches the destination until the file transfer stage finishes. SkyHOST matches this within 2% while exposing the first record 3x earlier (35 s vs 103 s), which makes it practical for recurring HPC-to-cloud workloads where incremental data availability matters.

## Repository Layout

```
hpc-to-cloud-spark-kafka/
├── README.md                              # This file
├── LICENSE                                # Apache-2.0
├── docs/
│   └── ENVIRONMENT.md                     # Full environment specification
├── container/
│   └── hsk.def                            # Singularity container recipe
├── figures/
│   ├── fig2_native_kafka_throughput.*     # Native Kafka producer/consumer
│   ├── fig3_acks_comparison.*             # acks=1 vs acks=all
│   ├── fig4_spark_kafka_throughput.*      # Spark Structured Streaming
│   ├── fig5_spark_vs_native.*             # Spark vs native Kafka CLI
│   ├── fig6_latency_analysis.*            # Latency breakdown
│   ├── fig7_throughput_vs_latency.*       # Throughput vs latency
│   └── generate_all_figures.py            # Regenerate from raw CSVs
├── hpc-characterization/
│   ├── README.md                          # HPC streaming benchmarks (Section 4)
│   ├── EXPERIMENT_GUIDE.md                # Broker scaling and replication notes
│   ├── scripts/                           # Deployment and benchmark scripts
│   └── data/
│       ├── native_kafka/                  # Native Kafka raw CSV
│       ├── spark_kafka/                   # Spark Structured Streaming raw CSVs
│       └── multi_broker/                  # Broker scaling raw CSVs (RF 1--3)
└── hpc-to-cloud-pipeline/
    ├── README.md                          # Five-approach pipeline (Section 5)
    ├── HPC_COMMANDS.md                    # Aion-side command reference
    ├── configs/experiment.conf            # Shared pipeline configuration
    ├── scripts/                           # All five approach scripts + helpers
    └── results/                           # 3-run averaged CSV results + analysis
```

## Environment

- **HPC:** Multi-node SLURM cluster with Singularity/Apptainer (tested on AMD EPYC 7H12, 128 cores per node, 10 Gbps Ethernet).
- **Cloud:** AWS EC2 m5.8xlarge in eu-central-1 (Frankfurt) and us-east-1 (Virginia).
- **HPC software (inside container):** Apache Kafka 2.8, Apache ZooKeeper 3.7, Apache Spark 3.4.
- **EC2 software:** single Apache Kafka 3.7 broker in Docker (KRaft mode) and Apache Spark 3.4.
- **Dataset:** Synthetic EEA air quality monitoring data (deterministic generator included).

Full specification in `docs/ENVIRONMENT.md`.

## Reproducing Results

### Build the Singularity container

```bash
sudo singularity build hsk.sif container/hsk.def
```

The recipe installs Kafka 2.8, Spark 3.4, ZooKeeper 3.7, and Python dependencies. The `.sif` image itself is not in the repo because it is a 1-2 GB binary.

### HPC characterization (Section 4)

See `hpc-characterization/README.md` for native Kafka, Spark-Kafka, and broker-scaling benchmarks. Raw CSVs are under `hpc-characterization/data/`.

### HPC-to-cloud pipeline (Section 5)

See `hpc-to-cloud-pipeline/README.md` for step-by-step instructions to run the five approaches on a 5 GB dataset. Raw CSVs are under `hpc-to-cloud-pipeline/results/`.

### Regenerate figures

```bash
python figures/generate_all_figures.py
```

The script reads the raw CSVs in `hpc-characterization/data/` and writes the PDFs and PNGs in `figures/`.

## Raw Data Traceability

| Paper element              | Source                                                                            |
| -------------------------- | --------------------------------------------------------------------------------- |
| Native Kafka table         | `hpc-characterization/data/native_kafka/kafka_perf_results.csv`                   |
| Spark Structured Streaming | `hpc-characterization/data/spark_kafka/spark_kafka_test_*/spark_producer_results.csv` |
| Broker scaling and RF      | `hpc-characterization/data/multi_broker/scaling_bench_{1,2,3}br_*.csv`            |
| Pipeline results table     | `hpc-to-cloud-pipeline/results/approach[1-5]*.csv`                                |
| Pipeline timeline figure   | Derived from the same pipeline CSVs                                               |

## License

Apache License 2.0. See `LICENSE`.
