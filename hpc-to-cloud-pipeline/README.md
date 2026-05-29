# HPC-to-Cloud Pipeline (5 GB)

This part of the repository covers Section 5 of the paper. Five end-to-end approaches deliver a filtered subset of a 5 GB synthetic EEA air-quality dataset from an HPC cluster to a destination Kafka broker in AWS `us-east-1` over WAN.

## Approaches

| #  | Approach              | Type   | Path                                                                |
| -- | --------------------- | ------ | ------------------------------------------------------------------- |
| A1 | Cloud-Side Processing | Batch  | `scp` raw to EC2 US-East -> Spark filter + Kafka produce on EC2     |
| A2 | Direct Producer       | Stream | HPC Spark -> us-east-1 Kafka over WAN                               |
| A3 | HPC-Side Processing   | Batch  | HPC Spark filter -> `scp` -> Kafka ingest on EC2                    |
| A4 | MM2 Replication       | Stream | HPC Spark -> eu-central-1 Kafka -> MirrorMaker 2 -> us-east-1 Kafka |
| A5 | SkyHOST               | Stream | HPC Spark -> eu-central-1 Kafka -> SkyHOST gateways -> us-east-1    |

## Shared configuration

```
Dataset                    : 5 GB synthetic EEA air-quality CSV (83.9M rows)
Filter                     : country in {LU, DE, BE}, keeps 9,317,527 records (~11.1%)
Spark on HPC               : 2 workers x 4 cores x 6 GB executor
Kafka partitions           : 8, acks=1, replication factor 1
Spark Kafka producer batch : 16 KB
EC2 instances              : m5.8xlarge (eu-central-1 and us-east-1)
TCP tuning                 : BBR, 128 MB socket buffers (applied to MM2 and SkyHOST paths)
Runs per approach          : 3
```

Edit the placeholders in `configs/experiment.conf` before running.

## Final 5 GB results

3-run mean (standard deviation in parentheses).

| # | Approach              | Type   | E2E (s)    | FRL (s)     | TP (MB/s) |
| - | --------------------- | ------ | ---------- | ----------- | --------- |
| 1 | Cloud-Side Processing | Batch  | 368        | 339         | -         |
| 2 | Direct Producer       | Stream | 676        | 35.8 (0.4)  | 2.45      |
| 3 | HPC-Side Processing   | Batch  | 187 (1.2)  | 103.2 (1.2) | 6.90      |
| 4 | MM2 Replication       | Stream | 234 (4.6)  | 35.6 (1.4)  | 7.94      |
| 5 | SkyHOST               | Stream | 210 (6.8)  | 36.2 (0.6)  | 9.04      |

Inter-batch latency at the destination consumer (gap between successive 1,000-record samples, ms):

| Approach        | P90 | P95 | P99 |
| --------------- | --- | --- | --- |
| Direct Producer |  95 | 109 | 128 |
| MM2 Replication |  65 | 132 | 314 |
| SkyHOST         |  84 |  89 | 132 |

The full breakdown is in `results/FINAL_COMPARISON.md` and `results/EXPERIMENT_DETAILS.md`.

## How to reproduce

### 1. Generate the dataset on HPC

```bash
python3 scripts/generate_eea_dataset.py \
  --output ~/pipeline_data/eea_airquality_5gb.csv \
  --size-gb 5 \
  --seed 42
```

The generator uses a fixed seed, so the output matches the dataset used in the paper.

### 2. Launch and configure the EC2 instances

- `us-east-1` for the destination Kafka broker and consumer (all approaches)
- `eu-central-1` for the regional broker used by A4 and A5

Open TCP 22 (SSH) and 9092 (Kafka) between HPC and the EC2 instances, and between the two EC2 instances.

```bash
ssh -i <key>.pem ubuntu@<US_EAST_IP> bash setup_ec2_kafka.sh
ssh -i <key>.pem ubuntu@<FRANKFURT_IP> bash setup_ec2_kafka.sh
```

`scripts/setup_ec2_kafka.sh` deploys a single Kafka 3.7 broker in Docker (KRaft mode) plus Spark 3.4 and the Python dependencies used by the consumer.

### 3. Edit the shared configuration

In `configs/experiment.conf`, replace the placeholders:

```
EC2_US_EAST_IP="<YOUR_US_EAST_IP>"
EC2_FRANKFURT_IP="<YOUR_FRANKFURT_IP>"
```

### 4. Start the Spark cluster on HPC

```bash
sbatch scripts/deploy_spark.sh
cat ~/coordinatorNode
ssh "$(cat ~/coordinatorNode)"
```

### 5. Run the five approaches

From the coordinator node:

```bash
bash scripts/approach1_cloud_side.sh
bash scripts/approach2_direct_producer.sh
bash scripts/approach3_hpc_side.sh
bash scripts/approach4_mm2.sh
bash scripts/approach5_skyhost.sh
```

Approach 5 pauses and prints the exact SkyHOST/Skyplane command that must be run from the client machine.

For more detail on the Aion-side command sequence, see `HPC_COMMANDS.md`.

## Result columns per approach

| Approach   | Key columns                                                                                                                  |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Approach 1 | `scp_time_s`, `spark_read_s`, `spark_filter_s`, `kafka_produce_s`, `consumer_first_record_s`, `consumer_total_s`             |
| Approach 2 | `read_time_s`, `filter_time_s`, `kafka_produce_time_s`, `total_time_s`                                                       |
| Approach 3 | `spark_read_s`, `spark_filter_s`, `spark_write_s`, `scp_time_s`, `ingest_time_s`, `consumer_first_record_s`, `consumer_total_s` |
| Approach 4 | `hpc_read_s`, `hpc_filter_s`, `hpc_produce_s`, `consumer_first_record_s`, `consumer_active_s`, `consumer_total_s`            |
| Approach 5 | `hpc_read_s`, `hpc_filter_s`, `hpc_produce_s`, `consumer_first_record_s`, `consumer_active_s`, `consumer_total_s`            |

E2E reported in the paper is `consumer_total_s` for Approaches 1, 3, 4, 5 and `total_time_s` for Approach 2. FRL comes from `consumer_first_record_s` (Approaches 1, 3, 4, 5) and from the producer-side first-acknowledgement timestamp for Approach 2. The consumer process is launched a few seconds before the pipeline kickoff, so `consumer_total_s` covers the full HPC processing, intermediate transit, and final delivery to the destination broker.
