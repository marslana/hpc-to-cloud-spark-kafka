# Experiment Details

This document records the final 5 GB experiment configuration used by the paper.

## Infrastructure

### HPC Source

- Cluster: Aion, University of Luxembourg HPC
- Scheduler: SLURM with dedicated allocations
- Container runtime: Singularity/Apptainer
- Container image: `hsk.sif` 
- HPC software inside container: Spark 3.4, Kafka 2.8, ZooKeeper 3.7
- Spark allocation for pipeline runs: 2 workers, 4 cores per worker, 6 GB executor memory

### Cloud Destination and Regional Broker

- Region 1: AWS `us-east-1`, EC2 `m5.8xlarge`
- Region 2: AWS `eu-central-1`, EC2 `m5.8xlarge`
- EC2 setup script: `hpc-to-cloud-pipeline/scripts/setup_ec2_kafka.sh`
- EC2 Kafka: single Kafka 3.7 broker in Docker KRaft mode
- EC2 Spark: Spark 3.4 for cloud-side processing

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
python3 hpc-to-cloud-pipeline/scripts/generate_eea_dataset.py \
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
- Consumer idle timeout in scripts: 60 s after the last record (used only by the orchestrator to declare the run finished, not part of `e2e_s`)
- E2E metric reported in the paper: `e2e_s` averaged over 3 runs for all five approaches, measured by a consumer script on the destination broker. The consumer is launched a few seconds before the pipeline kickoff so its clock covers the full HPC processing, intermediate transit, and final delivery. The consumer's post-completion idle-timeout window is excluded from both `e2e_s` and `consumer_first_record_s`.

## Approach-Specific Configuration

### A1 Cloud-Side Processing

- Transfer raw 5 GB CSV to `us-east-1` using `scp`.
- Run Spark filtering and Kafka production on the EC2 instance.
- Destination consumer measures first-record latency and consumer total time.

### A2 Direct Producer

- Run Spark filtering on HPC.
- Produce filtered records directly to the `us-east-1` Kafka broker over WAN.
- E2E and FRL are the 3-run mean of consumer-side measurements in `approach2_direct_producer.csv`, captured during the same campaign as the other approaches.

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

3-run mean (standard deviation in parentheses).

| # | Approach | Type | E2E (s) | FRL (s) | TP (MB/s) |
|---|---|---|---:|---:|---:|
| 1 | Cloud-Side Processing | Batch  | 368 | 339   | —    |
| 2 | Direct Producer       | Stream | 676 (0.6) | 35.8 (0.4) | 2.45 |
| 3 | HPC-Side Processing   | Batch  | 187 (1.2) | 103.2 (1.2) | 6.90 |
| 4 | MM2 Replication       | Stream | 234 (4.6) | 35.6 (1.4) | 7.94 |
| 5 | SkyHOST               | Stream | 210 (6.8) | 36.2 (0.6) | 9.04 |

Inter-batch latency at the destination consumer (gap between successive
1,000-record samples) for the three streaming approaches:

| Approach | P50 | P90 | P95 | P99 |
|---|---:|---:|---:|---:|
| Direct Producer | 84.57 |  95.05 | 108.79 | 127.59 |
| MM2 Replication |  1.52 |  64.97 | 131.92 | 314.43 |
| SkyHOST         |  1.78 |  83.53 |  89.04 | 131.63 |

## Network Characterization

See `network_characterization.md` for `iperf3` single-stream bandwidth and
TCP-handshake RTT measurements on the three relevant links
(Aion -> eu-central-1, Aion -> us-east-1, eu-central-1 -> us-east-1).

## SkyHOST Inter-Gateway TCP Verification

See `raw_logs/skyhost_inter_gw_tcp_snapshot.txt` for a snapshot of the
established TCP connections between the EU and US SkyHOST gateway VMs during a
live A5 run, confirming 8 parallel data connections (matching
`--send-connections 8`) plus the control RPC channel on port 8081.

## Raw Result Files

- `hpc-to-cloud-pipeline/results/approach1_cloud_side_processing.csv`
- `hpc-to-cloud-pipeline/results/approach2_direct_producer.csv`
- `hpc-to-cloud-pipeline/results/approach3_hpc_side_processing.csv`
- `hpc-to-cloud-pipeline/results/approach4_mm2_replication_tuned.csv`
- `hpc-to-cloud-pipeline/results/approach5_skyhost_transfer.csv`
- `hpc-to-cloud-pipeline/results/raw_logs/<approach>_run*_timestamps.csv` (per-record arrival samples used for tail-latency analysis)

