# Experimental Environment Specification

## HPC Infrastructure

### University of Luxembourg HPC Facility

**Aion Cluster**
- CPU: AMD EPYC Rome 7H12 (2x 64 cores per node)
- RAM: 256 GB DDR4 per node
- Network: 10 Gbps Ethernet (eno1) + 100 Gbps InfiniBand HDR (ib0)
- Storage: Shared GPFS filesystem
- OS: Rocky Linux 8

### SLURM Configuration

```bash
#SBATCH -N 5              # 5 nodes (3 minimum)
#SBATCH -n 9              # 9 tasks total
#SBATCH --ntasks-per-node=3
#SBATCH --cpus-per-task=8
#SBATCH --mem=16GB        # Per task
#SBATCH -c 16             # Cores per task
#SBATCH --time=0-03:00:00
#SBATCH -p batch
#SBATCH --qos=normal
```

### Node Allocation (Full Deployment)

| Node | Role | Services |
|------|------|----------|
| Node 1 (Coordinator) | Master | Spark Master, ZooKeeper |
| Node 2 | Worker | Spark Worker (14 GB, 16 cores), Kafka Broker 1 (port 9092) |
| Node 3 | Worker | Spark Worker (14 GB, 16 cores), Kafka Broker 2 (port 9093) |
| Node 4 (optional) | Worker | Spark Worker (14 GB, 16 cores) |
| Node 5 (optional) | Worker | Spark Worker (14 GB, 16 cores) |

## Software Stack

### Container Runtime

- Singularity 3.7+ (loaded via `module load tools/Singularity`)
- Container image: `hsk.sif`, built from `container/hsk.def`
- All services run inside Singularity instances with bind mounts

### Apache Spark 3.4.x

```properties
# spark-defaults.conf
spark.master                    spark://<coordinator>:7078
spark.driver.memory             4g
spark.executor.memory           12g
spark.executor.cores            4
spark.cores.max                 8
spark.executor.instances        4
spark.driver.host               <coordinator>
spark.rdd.compress              True
spark.serializer.objectStreamReset  100
spark.logConf                   true
```

### Apache Kafka 2.8.x

```properties
# server.properties (per broker)
broker.id=<1|2>
listeners=PLAINTEXT://<hostname>:<9092|9093>
log.dirs=/tmp/kafka-logs
zookeeper.connect=<coordinator>:2181
num.partitions=1                  # Default, overridden per test
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
message.max.bytes=11534336
replica.fetch.max.bytes=11534336
```

### Apache ZooKeeper 3.7.x

```properties
# zoo.cfg
tickTime=2000
dataDir=/tmp/zookeeper
clientPort=2181
```

### Python Environment

- Python 3.10+
- kafka-python 2.0.2
- pyspark 3.4.0 (with `spark-sql-kafka-0-10_2.12:3.4.0` package)

### Spark-Kafka Connector

```bash
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0
```

## Cloud Infrastructure

- AWS EC2 `m5.8xlarge` in `eu-central-1` (Frankfurt) and `us-east-1` (Virginia)
- EC2 Kafka: single Kafka 3.7 broker in Docker (KRaft mode)
- EC2 Spark: Spark 3.4 (used by Approach 1 for cloud-side filtering)

## Network Configuration

### Network Interfaces

| Interface | Speed | Usage |
|-----------|-------|-------|
| eno1 | 10 Gbps | Kafka broker communication |
| ib0 | 100 Gbps | InfiniBand (available but not used by Kafka TCP) |

### TCP Tuning (cross-region paths only)

```
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.ipv4.tcp_congestion_control = bbr
```

Applied identically to MM2 and SkyHOST endpoints on EC2.

## Singularity Bind Mounts

```bash
--bind $HOME/spark/conf:/opt/spark/conf
--bind $HOME/spark/logs:/opt/spark/logs
--bind $HOME/spark/work:/opt/spark/work
--bind $HOME/kafka/config:/opt/kafka/config
--bind $HOME/kafka/logs:/opt/kafka/logs
--bind $HOME/pipeline_data:/opt/dataset
--bind /dev/shm:/dev/shm
```

## Benchmark Tools

### Native Kafka

- `kafka-producer-perf-test.sh` (bundled with Kafka)
- `kafka-consumer-perf-test.sh` (bundled with Kafka)
- Custom wrapper scripts for automated matrix testing

### Spark-Kafka

- PySpark with Spark Structured Streaming API
- `spark-sql-kafka-0-10` connector for Kafka integration

## Service Startup Sequence

1. Cleanup existing processes, Singularity instances, temp directories
2. Start ZooKeeper on coordinator (wait for port 2181)
3. Start Spark Master on coordinator (port 7078)
4. Start Spark Workers on worker nodes (connect to master)
5. Start Kafka Brokers on worker nodes (connect to ZooKeeper)
6. Verify all services via health checks (ZK shell, Spark UI)
7. Run benchmarks from coordinator node
