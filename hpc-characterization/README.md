# HPC Spark-Kafka Characterization

This part of the repository covers the HPC streaming benchmarks reported in Section 4 of the paper. Three sub-experiments run inside a Singularity container on a SLURM-managed multi-node cluster.

## Sub-experiments

| Sub-experiment            | Source data                                                                            | Configurations |
| ------------------------- | -------------------------------------------------------------------------------------- | -------------- |
| Native Kafka              | `data/native_kafka/kafka_perf_results.csv`                                             | 168 measurements (72 producer-only + 36 consumer-only + 24 e2e-producer + 36 e2e-consumer) |
| Spark Structured Streaming| `data/spark_kafka/spark_kafka_test_*/spark_producer_results.csv` (3 timestamped runs)  | 24 distinct configurations averaged over 3 runs |
| Broker scaling and RF     | `data/multi_broker/scaling_bench_{1,2,3}br_*.csv`                                      | 36 configurations at 10 KB (6 broker/RF combinations x 3 partitions x 2 acks) |

For native Kafka, `producer` and `consumer` rows measure each tool in isolation, while `e2e-producer` and `e2e-consumer` rows measure them running simultaneously. The paper's end-to-end throughput table uses the `e2e-*` rows.

## Sweep parameters

```
Partitions          : 1, 2, 4, 8 (1, 4, 8 for broker scaling)
Record sizes        : 100 B, 1 KB, 10 KB (10 KB only for broker scaling in the paper)
Throughput targets  : 100K, 500K, 1M records per second
Acks                : 1, all
Brokers             : 1, 2, 3 (RF <= number of brokers)
```

## How to reproduce

### Build and stage the container

The container recipe is at `container/hsk.def` at the repository root.

```bash
sudo singularity build hsk.sif ../container/hsk.def
scp hsk.sif <HPC_USER>@<HPC_LOGIN_HOST>:~/
```

### Deploy the cluster on SLURM

```bash
# Full cluster (Spark + Kafka + ZooKeeper)
sbatch scripts/deploy_cluster.sh

# Or one of the smaller deployments
sbatch scripts/deploy_kafka_only.sh
NUM_BROKERS=3 sbatch --nodes=4 scripts/deploy_kafka_N_brokers.sh
```

Each deployment writes the coordinator hostname to `~/coordinatorNode` and starts services inside Singularity instances on the allocated nodes.

### Native Kafka benchmarks

From the coordinator node:

```bash
bash scripts/kafka_producer_bench.sh
bash scripts/kafka_consumer_bench.sh
bash scripts/kafka_full_bench.sh            # producer + consumer + e2e
```

Or run the legacy wrappers:

```bash
bash scripts/kafka_perf_test.sh
bash scripts/kafka_consumer_perf_test.sh
```

### Spark Structured Streaming benchmarks

```bash
bash scripts/spark_producer_bench.sh
bash scripts/spark_consumer_bench.sh
```

### Broker scaling and replication

```bash
bash scripts/kafka_scaling_bench.sh 1
bash scripts/kafka_scaling_bench.sh 2
bash scripts/kafka_scaling_bench.sh 3
bash scripts/rf_rerun_10kb.sh
```

See `EXPERIMENT_GUIDE.md` for the step-by-step procedure used to produce the raw CSVs in `data/multi_broker/`.

## CSV column reference

### Native Kafka (`data/native_kafka/`)

```
test_type, partitions, message_size_bytes, producer_throughput, producer_acks,
consumer_groups, records_per_sec, mb_per_sec, avg_latency_ms, max_latency_ms,
test_duration_sec
```

`test_type` is one of `producer`, `consumer`, `e2e-producer`, `e2e-consumer`.

### Spark Structured Streaming (`data/spark_kafka/`)

```
partitions, record_size_bytes, batch_size, records_per_sec, mb_per_sec,
duration_sec, total_records
```

### Broker scaling (`data/multi_broker/`)

```
num_brokers, replication_factor, test_type, partitions, message_size_bytes,
producer_acks, consumer_groups, records_per_sec, mb_per_sec, avg_latency_ms,
max_latency_ms
```
