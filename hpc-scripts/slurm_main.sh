#!/usr/bin/bash -l
#SBATCH --job-name=SparkHDFS
#SBATCH --nodes=5
#SBATCH --ntasks=9          # Total tasks (3 master + 4 workers + 2 brokers)
#SBATCH --ntasks-per-node=3 # Maximum 3 tasks per node
#SBATCH --mem-per-cpu=2GB
#SBATCH --cpus-per-task=8   # Maximum cores per task
#SBATCH --time=0-07:59:00
#SBATCH --partition=batch
#SBATCH --qos=normal
#SBATCH --mail-user=first.lastname@uni.lu
#SBATCH --mail-type=BEGIN,END

module load tools/Singularity

# Create logs directory for better debugging
mkdir -p ${HOME}/sparkhdfs/logs

# Function for logging
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a ${HOME}/sparkhdfs/logs/sparkhdfs_main.log
}

# Clean up any existing processes and directories
log_message "Cleaning up any existing processes and directories..."

# Kill any existing Java processes (Spark/HDFS) from previous runs
pkill -u $USER java || true

# Clean up temporary directories
rm -rf /tmp/hadoop/hdfs/name/* || true
rm -rf /tmp/hadoop/hdfs/data/* || true
rm -rf /tmp/zookeeper/* || true
rm -rf /tmp/kafka-logs/* || true

# Clean up work directories
rm -rf ${HOME}/sparkhdfs/hadoop/logs/*
rm -rf ${HOME}/sparkhdfs/spark/logs/*
rm -rf ${HOME}/sparkhdfs/spark/work/*
rm -rf ${HOME}/sparkhdfs/kafka/logs/*

# Clean up any existing Singularity instances
singularity instance list | grep shinst_ | awk '{print $1}' | xargs -r singularity instance stop || true
# Create all necessary directories
mkdir -p ${HOME}/sparkhdfs/hadoop/logs
mkdir -p ${HOME}/sparkhdfs/hadoop/etc/hadoop
mkdir -p ${HOME}/sparkhdfs/spark/logs
mkdir -p ${HOME}/sparkhdfs/spark/conf
mkdir -p ${HOME}/sparkhdfs/spark/work
mkdir -p ${HOME}/sparkhdfs/kafka/logs
mkdir -p ${HOME}/sparkhdfs/kafka/config
mkdir -p ${HOME}/data
mkdir -p /tmp/zookeeper
mkdir -p /tmp/hadoop/hdfs/name
mkdir -p /tmp/hadoop/hdfs/data
mkdir -p /tmp/kafka-logs

# Set proper permissions
chmod 755 /tmp/hadoop/hdfs/name
chmod 755 /tmp/hadoop/hdfs/data
chmod 755 /tmp/zookeeper
chmod 755 /tmp/kafka-logs

hostName="`hostname`"
log_message "hostname=$hostName"

# Save it for future job refs
myhostname="`hostname`"
rm -f coordinatorNode
touch coordinatorNode
cat > coordinatorNode << EOF
$myhostname
EOF

# Create HDFS configs
log_message "Creating HDFS configuration files..."
HDFS_SITE=${HOME}/sparkhdfs/hadoop/etc/hadoop/hdfs-site.xml
cat > ${HDFS_SITE} << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
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
    <property>
        <name>dfs.namenode.handler.count</name>
        <value>100</value>
    </property>
</configuration>
EOF

HDFS_CORESITE=${HOME}/sparkhdfs/hadoop/etc/hadoop/core-site.xml
cat > ${HDFS_CORESITE} << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://$hostName:9000</value>
    </property>
    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
    </property>
</configuration>
EOF

# Create Spark configs
log_message "Creating Spark configuration files..."
SPARK_CONF=${HOME}/sparkhdfs/spark/conf/spark-defaults.conf
cat > ${SPARK_CONF} << EOF
# Master settings
spark.master spark://$hostName:7078

# Memory settings
spark.driver.memory 2g
spark.executor.memory 6g

# Cores settings
spark.executor.cores 4
spark.cores.max 16

# Executor settings
spark.executor.instances 4

# Network settings
spark.driver.host $hostName

# HDFS settings
spark.hadoop.fs.defaultFS hdfs://$hostName:9000

# Other settings
spark.logConf true
spark.rdd.compress true
spark.serializer.objectStreamReset 100
spark.network.timeout 800s
EOF

SPARK_ENVSH=${HOME}/sparkhdfs/spark/conf/spark-env.sh
cat > ${SPARK_ENVSH} << EOF
#!/usr/bin/env bash

SPARK_MASTER_HOST="$hostName"
SPARK_MASTER_PORT="7078"
SPARK_HOME="/opt/spark"
HADOOP_HOME="/opt/hadoop"
HADOOP_CONF_DIR="${HOME}/sparkhdfs/hadoop/etc/hadoop"
EOF
chmod +x ${SPARK_ENVSH}

# Create logging configuration
SPARK_L4J=${HOME}/sparkhdfs/spark/conf/log4j.properties
cat > ${SPARK_L4J} << EOF
# Set everything to be logged to the console
log4j.rootCategory=INFO, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Settings to quiet third party logs that are too verbose
log4j.logger.org.spark_project.jetty=WARN
log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
EOF
# Create ZooKeeper configuration
log_message "Creating ZooKeeper configuration..."
cat > ${HOME}/sparkhdfs/kafka/config/zookeeper.properties << EOF
dataDir=/tmp/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
tickTime=2000
initLimit=10
syncLimit=5
EOF

# Launcher script for Spark master, HDFS Namenode, and ZooKeeper
SPARKM_LAUNCHER=${HOME}/sparkhdfs/spark-start-master-${SLURM_JOBID}.sh
log_message "Creating SparkMaster, HDFSNamenode, and ZooKeeper launcher script '${SPARKM_LAUNCHER}'"
cat << 'EOF' > ${SPARKM_LAUNCHER}
#!/bin/bash

echo "Starting master services on host: $(hostname)"
exec 1> >(tee ${HOME}/sparkhdfs/logs/master_services.log)
exec 2>&1

# Cleanup any existing instances
singularity instance stop shinst_master 2>/dev/null || true

# Start a new instance
echo "Starting master Singularity instance..."
singularity instance start --bind $HOME/sparkhdfs/hadoop/logs:/opt/hadoop/logs,$HOME/sparkhdfs/hadoop/etc/hadoop:/opt/hadoop/etc/hadoop,$HOME/sparkhdfs/spark/conf:/opt/spark/conf,$HOME/sparkhdfs/spark/logs:/opt/spark/logs,$HOME/sparkhdfs/spark/work:/opt/spark/work,$HOME/sparkhdfs/kafka/config:/opt/kafka/config,$HOME/sparkhdfs/kafka/logs:/opt/kafka/logs,$HOME/sparkhdfs:$HOME,$HOME/data:/opt/shared_data \
 hsk.sif shinst_master || true

# Format HDFS if needed
if [ ! -f "/tmp/hadoop/hdfs/name/current/VERSION" ]; then
    echo "Formatting HDFS namenode..."
    singularity exec instance://shinst_master hdfs namenode -format -force -nonInteractive
fi

# Start HDFS namenode
echo "Starting HDFS namenode..."
singularity exec instance://shinst_master hdfs namenode &

# Wait for namenode to start
echo "Waiting for namenode to start..."
for i in {1..30}; do
    if singularity exec instance://shinst_master hdfs dfsadmin -report >/dev/null 2>&1; then
        echo "Namenode started successfully"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Namenode failed to start after 30 attempts"
        exit 1
    fi
    echo "Attempt $i: Waiting for namenode..."
    sleep 10
done

# Start ZooKeeper
echo "Starting ZooKeeper..."
singularity exec instance://shinst_master \
    /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties > ${HOME}/sparkhdfs/kafka/logs/zookeeper.log 2>&1 &

# Wait for ZooKeeper to start
echo "Waiting for ZooKeeper to start..."
for i in {1..30}; do
    if singularity exec instance://shinst_master /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls / &>/dev/null; then
        echo "ZooKeeper started successfully"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: ZooKeeper failed to start after 30 attempts"
        exit 1
    fi
    echo "Attempt $i: Waiting for ZooKeeper..."
    sleep 10
done

# Start Spark master with safeguards
echo "Starting Spark master..."
if singularity exec instance://shinst_master jps | grep -q Master; then
    echo "Spark Master is already running"
else
    singularity exec instance://shinst_master \
        spark-class org.apache.spark.deploy.master.Master \
        --host $(hostname) \
        --port 7078 \
        --webui-port 8080 \
        >> ${HOME}/sparkhdfs/spark/logs/master.log 2>&1 &

    # Wait for master to start
    for i in {1..30}; do
        if singularity exec instance://shinst_master jps | grep -q Master; then
            echo "Spark Master started successfully"
            break
        fi
        if [ $i -eq 30 ]; then
            echo "ERROR: Master failed to start after 30 attempts"
            exit 1
        fi
        echo "Attempt $i: Waiting for master..."
        sleep 2
    done
fi

# Verify only one master is running
MASTER_COUNT=$(singularity exec instance://shinst_master jps | grep -c Master)
if [ $MASTER_COUNT -gt 1 ]; then
    echo "ERROR: Found $MASTER_COUNT masters running. Cleaning up..."
    singularity exec instance://shinst_master pkill -f "spark.*Master"
    sleep 5
    # Start a single master
    singularity exec instance://shinst_master \
        spark-class org.apache.spark.deploy.master.Master \
        --host $(hostname) \
        --port 7078 \
        --webui-port 8080 \
        >> ${HOME}/sparkhdfs/spark/logs/master.log 2>&1 &
fi

# Keep the services running
while true; do
    sleep 60
    if ! singularity instance list | grep -q shinst_master; then
        echo "ERROR: Master instance stopped unexpectedly"
        exit 1
    fi
done
EOF
chmod +x ${SPARKM_LAUNCHER}
# Create Spark workers launcher script
SPARK_LAUNCHER=${HOME}/sparkhdfs/spark-start-workers-${SLURM_JOBID}.sh
log_message "Creating Spark workers launcher script '${SPARK_LAUNCHER}'"
cat << 'EOF' > ${SPARK_LAUNCHER}
#!/bin/bash

echo "Starting Spark worker on host: $(hostname)"
exec 1> >(tee ${HOME}/sparkhdfs/logs/worker_${SLURM_PROCID}.log)
exec 2>&1

# Cleanup any existing instances
singularity instance stop shinst_worker_${SLURM_PROCID} 2>/dev/null || true

# Start a new instance
echo "Starting worker Singularity instance..."
singularity instance start --bind $HOME/sparkhdfs/spark/conf:/opt/spark/conf,$HOME/sparkhdfs/spark/logs:/opt/spark/logs,$HOME/sparkhdfs/spark/work:/opt/spark/work,$HOME/sparkhdfs:$HOME,$HOME/data:/opt/shared_data \
 hsk.sif shinst_worker_${SLURM_PROCID} || true

# Wait for master to be available
echo "Waiting for Spark master to be available..."
for i in {1..30}; do
    if nc -z $(echo $SPARKMASTER | cut -d/ -f3 | cut -d: -f1) $(echo $SPARKMASTER | cut -d: -f3) 2>/dev/null; then
        echo "Spark master is available"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Could not connect to Spark master after 30 attempts"
        exit 1
    fi
    echo "Attempt $i: Waiting for Spark master..."
    sleep 10
done

# Start Spark worker
echo "Starting Spark worker..."
singularity run --bind $HOME/sparkhdfs/spark/conf:/opt/spark/conf,$HOME/sparkhdfs/spark/logs:/opt/spark/logs,$HOME/sparkhdfs/spark/work:/opt/spark/work,$HOME/sparkhdfs:$HOME,$HOME/data:/opt/shared_data instance://shinst_worker_${SLURM_PROCID} \
  sparkWorker $SPARKMASTER -c 4 -m 6G

# Keep the worker running
while true; do
    sleep 60
    if ! singularity instance list | grep -q shinst_worker_${SLURM_PROCID}; then
        echo "ERROR: Worker instance stopped unexpectedly"
        exit 1
    fi
done
EOF
chmod +x ${SPARK_LAUNCHER}

# Create HDFS datanode launcher script
HDFS_LAUNCHER=${HOME}/sparkhdfs/hdfs-start-datanodes-${SLURM_JOBID}.sh
log_message "Creating HDFS datanodes launcher script '${HDFS_LAUNCHER}'"
cat << 'EOF' > ${HDFS_LAUNCHER}
#!/bin/bash

echo "Starting HDFS datanode on host: $(hostname)"
exec 1> >(tee ${HOME}/sparkhdfs/logs/datanode_${SLURM_PROCID}.log)
exec 2>&1

# Create HDFS data directory
mkdir -p /tmp/hadoop/hdfs/data
chmod 755 /tmp/hadoop/hdfs/data

# Cleanup any existing instances
singularity instance stop shinst_datanode_${SLURM_PROCID} 2>/dev/null || true

# Start datanode instance
echo "Starting datanode Singularity instance..."
singularity instance start --bind $HOME/sparkhdfs/hadoop/logs:/opt/hadoop/logs,$HOME/sparkhdfs/hadoop/etc/hadoop:/opt/hadoop/etc/hadoop,$HOME/sparkhdfs:$HOME,$HOME/data:/opt/shared_data \
 hsk.sif shinst_datanode_${SLURM_PROCID} || true

# Wait for namenode to be available
echo "Waiting for HDFS namenode to be available..."
for i in {1..30}; do
    if singularity exec instance://shinst_datanode_${SLURM_PROCID} hdfs dfsadmin -report >/dev/null 2>&1; then
        echo "Namenode is available"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Could not connect to namenode after 30 attempts"
        exit 1
    fi
    echo "Attempt $i: Waiting for namenode..."
    sleep 10
done

# Start HDFS datanode
echo "Starting HDFS datanode..."
singularity run --bind $HOME/sparkhdfs/hadoop/logs:/opt/hadoop/logs,$HOME/sparkhdfs/hadoop/etc/hadoop:/opt/hadoop/etc/hadoop,$HOME/sparkhdfs:$HOME,$HOME/data:/opt/shared_data instance://shinst_datanode_${SLURM_PROCID} \
  sparkHDFSDatanode

# Keep the datanode running
while true; do
    sleep 60
    if ! singularity instance list | grep -q shinst_datanode_${SLURM_PROCID}; then
        echo "ERROR: Datanode instance stopped unexpectedly"
        exit 1
    fi
done
EOF
chmod +x ${HDFS_LAUNCHER}
# Create Kafka broker launcher script
KAFKA_LAUNCHER=${HOME}/sparkhdfs/kafka-start-brokers-${SLURM_JOBID}.sh
log_message "Creating Kafka brokers launcher script '${KAFKA_LAUNCHER}'"
cat << 'KAFKAEOF' > ${KAFKA_LAUNCHER}
#!/bin/bash

echo "Starting Kafka broker on host: $(hostname)"
exec 1> >(tee ${HOME}/sparkhdfs/logs/kafka_broker_${SLURM_PROCID}.log)
exec 2>&1

# Get ZooKeeper host from coordinatorNode file
ZOOKEEPER_HOST=$(cat $HOME/coordinatorNode)
echo "ZooKeeper host: ${ZOOKEEPER_HOST}"

# Cleanup any existing instances
singularity instance stop shinst_kafka_${SLURM_PROCID} 2>/dev/null || true

# Start a new Singularity instance
echo "Starting Kafka broker Singularity instance..."
singularity instance start \
    --bind $HOME/sparkhdfs/kafka/config:/opt/kafka/config,$HOME/sparkhdfs/kafka/logs:/opt/kafka/logs,$HOME/sparkhdfs:$HOME,$HOME/data:/opt/shared_data \
    hsk.sif shinst_kafka_${SLURM_PROCID}

# Create broker config
BROKER_ID=$((SLURM_PROCID+1))
KAFKA_PORT=$((9092+SLURM_PROCID))

echo "Creating Kafka broker config (ID: ${BROKER_ID}, Port: ${KAFKA_PORT})..."
cat > $HOME/sparkhdfs/kafka/config/server.properties${BROKER_ID} << EOF
broker.id=${BROKER_ID}
listeners=PLAINTEXT://0.0.0.0:${KAFKA_PORT}
advertised.listeners=PLAINTEXT://$(hostname):${KAFKA_PORT}
log.dirs=/tmp/kafka-logs
zookeeper.connect=${ZOOKEEPER_HOST}:2181
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
default.replication.factor=1
min.insync.replicas=1
num.partitions=1
log.retention.hours=1
log.retention.bytes=1073741824
message.max.bytes=11000000
replica.fetch.max.bytes=11000000
max.message.bytes=11000000
fetch.message.max.bytes=11000000
log.flush.interval.messages=100000
log.flush.interval.ms=600000
log.flush.scheduler.interval.ms=600000
log.segment.bytes=536870912
replica.lag.time.max.ms=2000
replica.fetch.wait.max.ms=500
replica.fetch.min.bytes=1
replica.fetch.max.wait.ms=500
min.insync.replicas=1
EOF

# Wait for ZooKeeper to be available
echo "Waiting for ZooKeeper to be available..."
for i in {1..30}; do
    if singularity exec instance://shinst_kafka_${SLURM_PROCID} /opt/kafka/bin/zookeeper-shell.sh ${ZOOKEEPER_HOST}:2181 ls / &>/dev/null; then
        echo "ZooKeeper is available"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Could not connect to ZooKeeper after 30 attempts"
        exit 1
    fi
    echo "Attempt $i: Waiting for ZooKeeper..."
    sleep 10
done

# Start Kafka broker
echo "Starting Kafka broker..."
singularity exec instance://shinst_kafka_${SLURM_PROCID} \
    /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties${BROKER_ID} &

# Wait for broker to start
echo "Waiting for Kafka broker to start..."
sleep 30

# Create test topic
echo "Creating test topic..."
singularity exec instance://shinst_kafka_${SLURM_PROCID} \
    /opt/kafka/bin/kafka-topics.sh --create --topic test${BROKER_ID} \
    --bootstrap-server localhost:${KAFKA_PORT} --partitions 1 --replication-factor 1

# Keep the broker running
while true; do
    sleep 60
    if ! singularity instance list | grep -q shinst_kafka_${SLURM_PROCID}; then
        echo "ERROR: Kafka broker instance stopped unexpectedly"
        exit 1
    fi
done
KAFKAEOF
chmod +x ${KAFKA_LAUNCHER}

# Function to check if ZooKeeper is running
check_zookeeper() {
    ZOOKEEPER_HOST=$(cat coordinatorNode)
    log_message "Checking ZooKeeper on $ZOOKEEPER_HOST:2181"
    for i in {1..5}; do
        if singularity exec instance://shinst_master /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls / &>/dev/null; then
            log_message "ZooKeeper is responding"
            return 0
        fi
        log_message "Attempt $i: ZooKeeper not responding, waiting..."
        sleep 10
    done
    log_message "ERROR: ZooKeeper is not responding after 5 attempts"
    return 1
}

# Function to verify HDFS status
check_hdfs() {
    log_message "Checking HDFS status..."
    if singularity exec instance://shinst_master hdfs dfsadmin -report; then
        log_message "HDFS is operational"
        return 0
    else
        log_message "ERROR: HDFS check failed"
        return 1
    fi
}

# Function to verify Spark status
check_spark() {
    log_message "Checking Spark cluster status..."
    if nc -z $hostName 7078; then
        log_message "Spark master is responding"
        return 0
    else
        log_message "ERROR: Spark master is not responding"
        return 1
    fi
}

# Launch master services
log_message "Launching master services..."
srun --exclusive --nodes=1 --ntasks=3 --ntasks-per-node=3 --cpus-per-task=4 \
     --label --output=$HOME/sparkhdfs/logs/MasterServices-%N-%j.out \
     ${SPARKM_LAUNCHER} &

# Wait for master services to start
log_message "Waiting for master services to initialize..."
sleep 60

export SPARKMASTER="spark://$hostName:7078"

# Launch Spark workers
log_message "Launching Spark workers..."
srun --exclusive --nodes=2 --ntasks=4 --ntasks-per-node=2 --cpus-per-task=4 \
     --label --output=$HOME/sparkhdfs/logs/SparkWorkers-%N-%j.out \
     ${SPARK_LAUNCHER} &

# Wait for workers to start
log_message "Waiting for Spark workers to initialize..."
sleep 30

# Launch either HDFS datanodes or Kafka brokers based on SERVICE_TYPE
if [ "${SERVICE_TYPE:-kafka}" = "hdfs" ]; then
    log_message "Starting HDFS datanodes..."
    srun --exclusive --nodes=2 --ntasks=2 --ntasks-per-node=1 --cpus-per-task=16 \
         --label --output=${HOME}/sparkhdfs/logs/HDFSDatanodes-%N-%j.out \
         ${HDFS_LAUNCHER} &

    # Wait for datanodes to start
    log_message "Waiting for HDFS datanodes to initialize..."
    sleep 60

    # Verify HDFS cluster status
    if ! check_hdfs; then
        log_message "ERROR: HDFS cluster failed to start properly"
        exit 1
    fi
else
    if check_zookeeper; then
        log_message "Starting Kafka brokers..."
        srun --exclusive --nodes=2 --ntasks=2 --ntasks-per-node=1 --cpus-per-task=16 \
             --label --output=${HOME}/sparkhdfs/logs/KafkaBrokers-%N-%j.out \
             ${KAFKA_LAUNCHER} &

        # Wait for brokers to start
        log_message "Waiting for Kafka brokers to initialize..."
        sleep 120

        # Check broker registration
        log_message "Checking Kafka broker registration..."
        if ! singularity exec instance://shinst_master /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids; then
            log_message "ERROR: Kafka brokers failed to register with ZooKeeper"
            exit 1
        fi
    else
        log_message "ERROR: ZooKeeper is not running. Cannot start Kafka brokers."
        exit 1
    fi
fi

# Add trap for cleanup
trap cleanup EXIT
cleanup() {
    log_message "Cleaning up..."
    singularity instance list | grep shinst_ | awk '{print $1}' | xargs -r singularity instance stop
}

# Main monitoring loop
log_message "All services started. Beginning monitoring..."
COUNTER=0
while true; do
    sleep 60
    COUNTER=$((COUNTER + 1))

    # Every 5 minutes, perform health checks
    if [ $((COUNTER % 5)) -eq 0 ]; then
        log_message "Performing periodic health check..."

        # Check Spark
        check_spark || log_message "WARNING: Spark health check failed"

        # Check specific service
        if [ "${SERVICE_TYPE:-kafka}" = "hdfs" ]; then
            check_hdfs || log_message "WARNING: HDFS health check failed"
        else
            check_zookeeper || log_message "WARNING: ZooKeeper health check failed"
        fi

        # Check running instances
        RUNNING_INSTANCES=$(singularity instance list | grep shinst_ | wc -l)
        log_message "Running instances: $RUNNING_INSTANCES"

        if [ $RUNNING_INSTANCES -eq 0 ]; then
            log_message "ERROR: No running instances found. Exiting..."
            exit 1
        fi
    fi
done

log_message "Script completed"
echo $HOME/sparkhdfs
