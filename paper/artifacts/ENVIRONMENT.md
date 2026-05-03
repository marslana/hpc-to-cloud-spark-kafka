# Experimental Environment Specification

## HPC Infrastructure

### University of Luxembourg HPC Facility

**Aion Cluster (Primary)**
- CPU: AMD EPYC Rome 7H12 (2x 64 cores per node)
- RAM: 256 GB DDR4 per node (16 GB allocated per experiment task)
- Network: 10 Gbps Ethernet (eno1) + 100 Gbps InfiniBand HDR (ib0)
- Storage: Shared GPFS filesystem
- OS: CentOS 7 / Rocky Linux 8
- Nodes used: aion-0001, aion-0022, aion-0111, aion-0221, aion-0222 (varies per run)

**Iris Cluster (Secondary, used for chunked transfer tests)**
- CPU: Intel Xeon Gold 6132 (2x 14 cores per node)
- RAM: 128 GB DDR4 per node
- Network: 10 Gbps Ethernet + 100 Gbps InfiniBand EDR
- Nodes used: iris-001, iris-011, iris-082, iris-096, iris-098 (varies per run)

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
| Node 1 (Coordinator) | Master | Spark Master, HDFS NameNode, ZooKeeper |
| Node 2 | Worker | Spark Worker (14GB, 16 cores), Kafka Broker 1 (port 9092), HDFS DataNode |
| Node 3 | Worker | Spark Worker (14GB, 16 cores), Kafka Broker 2 (port 9093), HDFS DataNode |
| Node 4 (optional) | Worker | Spark Worker (14GB, 16 cores) |
| Node 5 (optional) | Worker | Spark Worker (14GB, 16 cores) |

## Software Stack

### Container Runtime
- **Singularity** 3.7+ (loaded via `module load tools/Singularity`)
- Container image: `sparkhdfs.sif` (also referenced as `hsk.sif` in some scripts)
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
num.partitions=1                          # Default, overridden per test
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
message.max.bytes=11534336                # 11MB for chunked transfers
replica.fetch.max.bytes=11534336
```

### Apache ZooKeeper 3.7.x
```properties
# zoo.cfg
tickTime=2000
dataDir=/tmp/zookeeper
clientPort=2181
```

### Apache Hadoop HDFS 3.3.x
```xml
<!-- hdfs-site.xml -->
<property>
  <name>dfs.replication</name>
  <value>1</value>
</property>
<property>
  <name>dfs.namenode.name.dir</name>
  <value>/tmp/hadoop/hdfs/name</value>
</property>
<property>
  <name>dfs.datanode.data.dir</name>
  <value>/tmp/hadoop/hdfs/data</value>
</property>

<!-- core-site.xml -->
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://<coordinator>:9000</value>
</property>
```

### Python Environment
- Python 3.10+ (via Miniconda inside container)
- kafka-python 2.0.2
- pyspark 3.4.0 (with spark-sql-kafka-0-10_2.12:3.4.0 package)

### Spark-Kafka Connector
```bash
# Used in spark-submit
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0
```

## Network Configuration

### TCP Buffer Sizes (System defaults)
```
net.core.rmem_max = 262144
net.core.wmem_max = 212992
```

### Network Interfaces
| Interface | Speed | Usage |
|-----------|-------|-------|
| eno1 | 10 Gbps | Kafka broker communication |
| ib0 | 100 Gbps | InfiniBand (available but not used by Kafka TCP) |

## Singularity Bind Mounts

All services use consistent bind mounts for configuration and log persistence:

```bash
--bind $HOME/hadoop/logs:/opt/hadoop/logs
--bind $HOME/hadoop/etc/hadoop:/opt/hadoop/etc/hadoop
--bind $HOME/spark/conf:/opt/spark/conf
--bind $HOME/spark/logs:/opt/spark/logs
--bind $HOME/spark/work:/opt/spark/work
--bind $HOME/kafka/config:/opt/kafka/config
--bind $HOME/kafka/logs:/opt/kafka/logs
--bind $HOME/data:/opt/shared_data
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
- Custom data generation via Python multiprocessing

### Chunked Transfer
- `kafka-python` library KafkaProducer
- Binary file chunking with configurable segment sizes
- Timing via Python `time.time()` with per-chunk and aggregate metrics

## Service Startup Sequence

1. **Cleanup** existing processes, Singularity instances, temp directories
2. **Format** HDFS NameNode (if fresh deployment)
3. **Start** ZooKeeper on coordinator (wait for port 2181)
4. **Start** HDFS NameNode on coordinator
5. **Start** Spark Master on coordinator (port 7078)
6. **Start** HDFS DataNodes on worker nodes (wait 30s)
7. **Start** Spark Workers on worker nodes (connect to master)
8. **Start** Kafka Brokers on worker nodes (connect to ZooKeeper)
9. **Verify** all services via health checks (ZK shell, Spark UI, HDFS report)
10. **Run** benchmarks from coordinator node

## Experiment Dates
- Native Kafka benchmarks: March 17, 2025
- Spark-Kafka streaming: March 18 + March 27, 2025
- Chunked transfers: March 20-27, 2025
- Cluster deployments: SLURM jobs 6858077, 6895289, 6932273, 6984107
