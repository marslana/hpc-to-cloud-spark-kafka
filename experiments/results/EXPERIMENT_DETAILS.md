# Experiment Details

This document records the final 5 GB experiment configuration used by the paper.

## Infrastructure

### HPC Source

- Cluster: Aion, University of Luxembourg HPC
- Scheduler: SLURM with dedicated allocations
- Container runtime: Singularity/Apptainer
- Container image: `sparkhdfs.sif` or `hsk.sif`
- HPC software inside container: Spark 3.4, Kafka 2.8, ZooKeeper 3.7
- Spark allocation for pipeline runs: 2 workers, 4 cores per worker, 6 GB executor memory

### Cloud Destination and Regional Broker

- Region 1: AWS `us-east-1`, EC2 `m5.8xlarge`
- Region 2: AWS `eu-central-1`, EC2 `m5.8xlarge`
- EC2 setup script: `experiments/scripts/setup_ec2_kafka.sh`
- EC2 Kafka: single Kafka 3.7 broker in Docker KRaft mode
- EC2 Spark: Spark 3.4 for cloud-side processing

### Network and TCP

- HPC to Frankfurt: approximately 10 ms RTT
- Frankfurt to US-East: approximately 85 ms RTT
- HPC to US-East: approximately 90 ms RTT
- Cross-region endpoints use BBR congestion control and 128 MB socket buffers.

## Dataset

| Property | 5 GB dataset |
|---|---:|
| File | `eea_airquality_5gb.csv` |
| Raw size | 5,291.7 MB |
| Raw records | 83,886,080 |
| Filter | Countries in `{LU, DE, BE}` |
| Filtered records | 9,317,527 |
| Filter ratio | 11.11% |
| Estimated filtered CSV size | 587.8 MB |
| Consumer-measured Kafka payload | 1,571.75 MB |

The dataset can be regenerated with:

```bash
python3 experiments/data/generate_eea_dataset.py \
  --output ~/pipeline_data/eea_airquality_5gb.csv \
  --size-gb 5 \
  --seed 42
```

## Shared Configuration

- Kafka partitions: 8
- Kafka producer acknowledgments: `acks=1`
- Kafka replication factor: 1
- Spark Kafka producer batch size: 16 KB
- Spark Kafka producer linger: 10 ms
- Number of runs per approach: 3
- Consumer idle timeout in scripts: 60 s after the last record

## Approach-Specific Configuration

### A1 Cloud-Side Processing

- Transfer raw 5 GB CSV to `us-east-1` using `scp`.
- Run Spark filtering and Kafka production on the EC2 instance.
- Destination consumer measures first-record latency and consumer total time.

### A2 Direct Producer

- Run Spark filtering on HPC.
- Produce filtered records directly to the `us-east-1` Kafka broker over WAN.

### A3 HPC-Side Processing

- Run Spark filtering on HPC.
- Write filtered output to HPC disk.
- Transfer filtered output using `scp`.
- Ingest files into destination Kafka on `us-east-1`.

### A4 MirrorMaker 2

- Produce filtered records from HPC to the regional Kafka broker in `eu-central-1`.
- Run MM2 from Frankfurt to replicate to `us-east-1`.
- MM2 settings: 8 tasks, 1 MB batch, 100 ms linger, 10 in-flight requests, `acks=1`.

### A5 SkyHOST

- Produce filtered records from HPC to the regional Kafka broker in `eu-central-1`.
- SkyHOST consumes from Frankfurt, aggregates into 32 MB chunks, and transfers to `us-east-1`.
- SkyHOST settings: 8 readers, 8 writers, 32 MB chunks, 8 parallel TCP connections.

## Final 5 GB Results

| # | Approach | Type | E2E (s) | FRL (s) | Throughput Note |
|---|---|---|---:|---:|---|
| 1 | Cloud-Side Processing | Batch | 368 | 339 | raw `scp` dominates |
| 2 | Direct Producer | Stream | 670 | 36 | 0.91 MB/s producer throughput |
| 3 | HPC-Side Processing | Batch | 183 | 103 | 41 s filtered `scp`, 82 s ingest |
| 4 | MM2 Replication | Stream | 219 | 35 | 8.54 MB/s consumer throughput |
| 5 | SkyHOST | Stream | 186 | 35 | 10.43 MB/s consumer throughput |

## Raw Result Files

- `experiments/results/5gb/approach1_cloud_side_processing.csv`
- `experiments/results/5gb/approach2_direct_producer.csv`
- `experiments/results/5gb/approach3_hpc_side_processing.csv`
- `experiments/results/5gb/approach4_mm2_replication_tuned.csv`
- `experiments/results/5gb/approach5_skyhost_transfer.csv`

These CSVs are the source of truth for the paper's pipeline table and Gantt figure.
