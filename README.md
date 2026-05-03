# HPC-to-Cloud Spark-Kafka Streaming Pipelines

Evaluation of five end-to-end data delivery pipelines for moving HPC-processed scientific data to the cloud using Apache Spark and Apache Kafka. The study covers two dimensions: (1) characterizing Spark-Kafka streaming performance on an HPC cluster deployed via Singularity containers on SLURM, and (2) comparing batch and streaming pipeline architectures for cross-site data delivery over WAN.

## Pipeline Approaches

| # | Approach | Type | Description |
|---|---------|------|-------------|
| A1 | Cloud-Side Processing | Batch | Transfer raw data via scp, filter with Spark on cloud |
| A2 | Direct Kafka over WAN | Stream | Spark produces filtered records directly to remote Kafka broker |
| A3 | HPC-Side Processing | Batch | Spark filters on HPC, scp filtered output, ingest to Kafka |
| A4 | MirrorMaker 2 | Stream | Spark to regional Kafka broker, MM2 replicates cross-region |
| A5 | SkyHOST | Stream | Spark to regional Kafka broker, SkyHOST transfers cross-region |

## Key Results (5 GB EEA air-quality dataset)

| # | Approach | Type | E2E (s) | First Record Latency (s) |
|---|---------|------|---------|--------------------------|
| 1 | Cloud-Side Processing | Batch | 368 | 339 |
| 2 | Direct Producer | Stream | 670 | 36 |
| 3 | HPC-Side Processing | Batch | 183 | 103 |
| 4 | MM2 Replication | Stream | 219 | 35 |
| 5 | SkyHOST | Stream | 186 | 35 |

HPC-Side batch processing achieves the fastest completion (183s) but holds all records until the pipeline finishes. SkyHOST matches this within 2% while delivering the first record 3x earlier (35s vs. 103s), making it practical for recurring HPC-to-cloud workloads where incremental data availability matters.

## Repository Structure

```
├── paper/
│   ├── figures/                         # Included figures (PDF + PNG)
│   ├── generate_all_figures.py          # Regenerate benchmark figures from raw data
│   └── artifacts/
│       ├── ENVIRONMENT.md               # Full environment specification
│       ├── container/sparkhdfs.def      # Singularity container definition (build .sif from this)
│       ├── data/                        # Raw benchmark data (native Kafka, Spark, multi-broker)
│       └── scripts/                     # Deployment, benchmark, and experiment scripts
│
├── experiments/
│   ├── README.md                        # Step-by-step experiment instructions
│   ├── HPC_COMMANDS.md                  # HPC-specific SLURM commands reference
│   ├── configs/                         # Experiment configuration files
│   ├── data/generate_eea_dataset.py     # Deterministic synthetic dataset generator
│   ├── scripts/                         # Shared support scripts
│   │   ├── spark_filter_produce.py      # Core PySpark filter + Kafka producer
│   │   ├── setup_ec2_kafka.sh           # EC2 Kafka/Spark deployment
│   │   └── kafka_dir_ingest.sh          # File-to-Kafka ingestion helper
│   ├── results/5gb/                     # Raw per-run CSV results for all approaches
│   └── 5gb/                             # 5 GB experiment scripts and configs
│       ├── scripts/approach[1-5]*.sh
│       ├── scripts/first_record_consumer.py  # Consumer instrumentation
│       └── configs/experiment_5gb.conf
│
├── benchmarks/
│   ├── kafka_perf_results/              # Raw Kafka benchmark CSV
│   ├── kafka_perf_test.sh               # Native Kafka producer benchmark
│   ├── kafka_consumer_perf_test.sh      # Native Kafka consumer benchmark
│   ├── spark_kafka_iperf_test_*.sh      # Spark producer/consumer benchmarks
│   └── running_kafka_iperf.py           # Orchestration script
│
└── hpc-scripts/
    ├── slurm_main.sh                    # Main SLURM job (Kafka + Spark + ZooKeeper)
    └── spark_filter_produce.py          # PySpark filter script (HPC version)
```

## Environment

- **HPC:** Multi-node SLURM cluster with Singularity/Apptainer (tested on AMD EPYC, 128 cores/node, 10 Gbps Ethernet)
- **Cloud:** AWS EC2 m5.8xlarge in eu-central-1 (Frankfurt) and us-east-1 (Virginia)
- **Software:** HPC container uses Apache Kafka 2.8 + ZooKeeper 3.7 and Apache Spark 3.4. EC2 setup uses a single Kafka 3.7 broker in Docker and Spark 3.4.
- **Dataset:** Synthetic EEA air quality monitoring data (deterministic generator included)

Full environment specification in `paper/artifacts/ENVIRONMENT.md`.

## Reproducing Results

### Generate the dataset

```bash
python3 experiments/data/generate_eea_dataset.py --output eea_airquality_5gb.csv --size-gb 5 --seed 42
```

The generator uses a fixed seed, so output is deterministic and matches the experiments.

### Build the Singularity container

```bash
sudo singularity build sparkhdfs.sif paper/artifacts/container/sparkhdfs.def
```

Builds a container with Kafka 2.8, Spark 3.4, ZooKeeper 3.7, Hadoop 3.3, and Python dependencies. The `.sif` image is not included in the repo (it is ~1-2 GB binary). The `.def` recipe is sufficient to rebuild it.

### HPC benchmarks

Deploy the cluster on SLURM using `hpc-scripts/slurm_main.sh`, then run the benchmark scripts in `benchmarks/`.

### Pipeline experiments

Follow `experiments/README.md` for step-by-step instructions. All 5 approach scripts are in `experiments/5gb/scripts/` with shared configuration in `experiments/5gb/configs/experiment_5gb.conf`.

### Figures

The repository includes the benchmark figures in `paper/figures/`. To regenerate the benchmark PDFs from raw data:

```bash
python paper/generate_all_figures.py
```

## Infrastructure Requirements

| Component | Required for | Notes |
|-----------|-------------|-------|
| SLURM HPC cluster | Benchmarks + all approaches | Any multi-node cluster with Singularity |
| AWS EC2 (2 instances) | Pipeline approaches | m5.8xlarge in eu-central-1 and us-east-1 |
| SkyHOST / Skyplane | Approach 5 only | See `experiments/5gb/scripts/approach5_skyhost.sh` |

## Raw Data Traceability

| Paper Element | Source File |
|--------------|-------------|
| Kafka benchmark table | `benchmarks/kafka_perf_results/kafka_perf_results_20250317_175725.csv` |
| Pipeline results table | `experiments/results/5gb/approach[1-5]*.csv` |
| Pipeline timeline figure | Derived from the same 5 GB CSVs |
| Benchmark figures | `paper/artifacts/data/` |

## License

This repository is provided for academic reproducibility.
