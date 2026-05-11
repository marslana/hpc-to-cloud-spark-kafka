# Broker Scaling and Replication Guide

This guide documents the procedure used to produce the broker-scaling and replication-factor CSVs in `data/multi_broker/`. The sweep covers 1 to 3 brokers and replication factors 1 to 3 at 10 KB records.

## Sweep grid

```
Brokers       : 1, 2, 3 (RF <= brokers)
Partitions    : 1, 4, 8
Message size  : 10 KB
Acks          : 1, all
Test types    : producer, consumer, end-to-end
```

This gives 6 broker/RF combinations x 3 partitions x 2 acks = 36 configurations.

## Step-by-step

### 1. Transfer scripts to HPC

```bash
scp hpc-characterization/scripts/deploy_kafka_N_brokers.sh <HPC_USER>@<HPC_LOGIN_HOST>:~/
scp hpc-characterization/scripts/kafka_scaling_bench.sh   <HPC_USER>@<HPC_LOGIN_HOST>:~/
```

### 2. Deploy and benchmark each broker count

```bash
# 1-broker cluster (2 nodes: 1 ZK + 1 broker)
NUM_BROKERS=1 sbatch --nodes=2 deploy_kafka_N_brokers.sh
ssh "$(cat ~/coordinatorNode)"
bash ~/kafka_scaling_bench.sh 1
# Results -> ~/kafka_bench/results/scaling_bench_1br_*.csv
scancel <jobid>

# 2-broker cluster
NUM_BROKERS=2 sbatch --nodes=3 deploy_kafka_N_brokers.sh
ssh "$(cat ~/coordinatorNode)"
bash ~/kafka_scaling_bench.sh 2
# Results -> ~/kafka_bench/results/scaling_bench_2br_*.csv
scancel <jobid>

# 3-broker cluster
NUM_BROKERS=3 sbatch --nodes=4 deploy_kafka_N_brokers.sh
ssh "$(cat ~/coordinatorNode)"
bash ~/kafka_scaling_bench.sh 3
# Results -> ~/kafka_bench/results/scaling_bench_3br_*.csv
scancel <jobid>
```

### 3. Optional RF rerun at 10 KB

```bash
bash ~/kafka_scaling_bench.sh rf-rerun
# Results -> ~/kafka_bench/results/rf_rerun_10kb_*.csv
```

### 4. Download results

```bash
scp <HPC_USER>@<HPC_LOGIN_HOST>:~/kafka_bench/results/scaling_bench_*.csv \
    hpc-characterization/data/multi_broker/
scp <HPC_USER>@<HPC_LOGIN_HOST>:~/kafka_bench/results/rf_rerun_*.csv \
    hpc-characterization/data/multi_broker/
```

## CSV output format

```
num_brokers, replication_factor, test_type, partitions, message_size_bytes,
producer_acks, consumer_groups, records_per_sec, mb_per_sec,
avg_latency_ms, max_latency_ms
```

Example row:

```
3,2,producer,4,10240,1,N/A,10500.5,102.54,178.3,259.0
```
