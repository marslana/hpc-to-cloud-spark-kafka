#!/usr/bin/bash -l
#SBATCH --job-name=KafkaCluster
#SBATCH --nodes=2
#SBATCH --ntasks=2          # Total tasks (1 ZooKeeper + 1 broker)
#SBATCH --ntasks-per-node=1 # 1 task per node
#SBATCH --mem-per-cpu=2GB
#SBATCH --cpus-per-task=8   # Cores per task
#SBATCH --time=0-01:59:00
#SBATCH --partition=batch
#SBATCH --qos=normal
#SBATCH --mail-user=first.lastname@uni.lu
#SBATCH --mail-type=BEGIN,END

module load tools/Singularity

# Create logs directory for better debugging
mkdir -p ${HOME}/kafka_cluster/logs

# Function for logging
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a ${HOME}/kafka_cluster/logs/kafka_main.log
}

# Clean up any existing processes and directories
log_message "Cleaning up any existing processes and directories..."

# Kill any existing Java processes from previous runs
pkill -u $USER java || true

# Clean up temporary directories
rm -rf /tmp/zookeeper/* || true
rm -rf /tmp/kafka-logs/* || true

# Clean up work directories
rm -rf ${HOME}/kafka_cluster/kafka/logs/*

# Clean up any existing Singularity instances
singularity instance list | grep shinst_ | awk '{print $1}' | xargs -r singularity instance stop || true

# Create all necessary directories
mkdir -p ${HOME}/kafka_cluster/kafka/logs
mkdir -p ${HOME}/kafka_cluster/kafka/config
mkdir -p ${HOME}/data
mkdir -p /tmp/zookeeper
mkdir -p /tmp/kafka-logs

# Set proper permissions
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

# Create ZooKeeper configuration
log_message "Creating ZooKeeper configuration..."
cat > ${HOME}/kafka_cluster/kafka/config/zookeeper.properties << EOF
dataDir=/tmp/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
tickTime=2000
initLimit=10
syncLimit=5
EOF

# Launcher script for ZooKeeper
ZOOKEEPER_LAUNCHER=${HOME}/kafka_cluster/zookeeper-start-${SLURM_JOBID}.sh
log_message "Creating ZooKeeper launcher script '${ZOOKEEPER_LAUNCHER}'"
cat << 'EOF' > ${ZOOKEEPER_LAUNCHER}
#!/bin/bash

echo "Starting ZooKeeper on host: $(hostname)"
exec 1> >(tee ${HOME}/kafka_cluster/logs/zookeeper_service.log)
exec 2>&1

# Cleanup any existing instances
singularity instance stop shinst_zookeeper 2>/dev/null || true

# Start a new instance
echo "Starting ZooKeeper Singularity instance..."
singularity instance start --bind $HOME/kafka_cluster/kafka/config:/opt/kafka/config,$HOME/kafka_cluster/kafka/logs:/opt/kafka/logs,$HOME/kafka_cluster:$HOME,$HOME/data:/opt/shared_data \
 hsk.sif shinst_zookeeper || true

# Start ZooKeeper
echo "Starting ZooKeeper..."
singularity exec instance://shinst_zookeeper \
    /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties > ${HOME}/kafka_cluster/kafka/logs/zookeeper.log 2>&1 &

# Wait for ZooKeeper to start
echo "Waiting for ZooKeeper to start..."
for i in {1..30}; do
    if singularity exec instance://shinst_zookeeper /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls / &>/dev/null; then
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

# Keep the services running
while true; do
    sleep 60
    if ! singularity instance list | grep -q shinst_zookeeper; then
        echo "ERROR: ZooKeeper instance stopped unexpectedly"
        exit 1
    fi
done
EOF
chmod +x ${ZOOKEEPER_LAUNCHER}

# Create Kafka broker launcher script
KAFKA_LAUNCHER=${HOME}/kafka_cluster/kafka-start-broker-${SLURM_JOBID}.sh
log_message "Creating Kafka broker launcher script '${KAFKA_LAUNCHER}'"
cat << 'KAFKAEOF' > ${KAFKA_LAUNCHER}
#!/bin/bash

echo "Starting Kafka broker on host: $(hostname)"
exec 1> >(tee ${HOME}/kafka_cluster/logs/kafka_broker.log)
exec 2>&1

# Get ZooKeeper host from coordinatorNode file
ZOOKEEPER_HOST=$(cat $HOME/coordinatorNode)
echo "ZooKeeper host: ${ZOOKEEPER_HOST}"

# Cleanup any existing instances
singularity instance stop shinst_kafka 2>/dev/null || true

# Start a new Singularity instance
echo "Starting Kafka broker Singularity instance..."
singularity instance start \
    --bind $HOME/kafka_cluster/kafka/config:/opt/kafka/config,$HOME/kafka_cluster/kafka/logs:/opt/kafka/logs,$HOME/kafka_cluster:$HOME,$HOME/data:/opt/shared_data \
    hsk.sif shinst_kafka

# Create broker config
BROKER_ID=1
KAFKA_PORT=9092

echo "Creating Kafka broker config (ID: ${BROKER_ID}, Port: ${KAFKA_PORT})..."
cat > $HOME/kafka_cluster/kafka/config/server.properties << EOF
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
EOF

# Wait for ZooKeeper to be available
echo "Waiting for ZooKeeper to be available..."
for i in {1..30}; do
    if singularity exec instance://shinst_kafka /opt/kafka/bin/zookeeper-shell.sh ${ZOOKEEPER_HOST}:2181 ls / &>/dev/null; then
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
singularity exec instance://shinst_kafka \
    /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &

# Wait for broker to start
echo "Waiting for Kafka broker to start..."
sleep 30

# Create test topic
echo "Creating test topic..."
singularity exec instance://shinst_kafka \
    /opt/kafka/bin/kafka-topics.sh --create --topic test-topic \
    --bootstrap-server localhost:${KAFKA_PORT} --partitions 1 --replication-factor 1

# Keep the broker running
while true; do
    sleep 60
    if ! singularity instance list | grep -q shinst_kafka; then
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
        if singularity exec instance://shinst_zookeeper /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls / &>/dev/null; then
            log_message "ZooKeeper is responding"
            return 0
        fi
        log_message "Attempt $i: ZooKeeper not responding, waiting..."
        sleep 10
    done
    log_message "ERROR: ZooKeeper is not responding after 5 attempts"
    return 1
}

# Launch ZooKeeper service on the first node
log_message "Launching ZooKeeper service..."
srun --exclusive --nodes=1 --ntasks=1 --ntasks-per-node=1 --cpus-per-task=8 \
     --nodelist=$(hostname) \
     --label --output=$HOME/kafka_cluster/logs/ZooKeeper-%N-%j.out \
     ${ZOOKEEPER_LAUNCHER} &

# Wait for ZooKeeper service to start
log_message "Waiting for ZooKeeper service to initialize..."
sleep 60

# Launch Kafka broker on the second node
if check_zookeeper; then
    log_message "Starting Kafka broker..."
    srun --exclusive --nodes=1 --ntasks=1 --ntasks-per-node=1 --cpus-per-task=8 \
         --exclude=$(hostname) \
         --label --output=${HOME}/kafka_cluster/logs/KafkaBroker-%N-%j.out \
         ${KAFKA_LAUNCHER} &

    # Wait for broker to start
    log_message "Waiting for Kafka broker to initialize..."
    sleep 120

    # Check broker registration
    log_message "Checking Kafka broker registration..."
    if ! singularity exec instance://shinst_zookeeper /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids; then
        log_message "ERROR: Kafka broker failed to register with ZooKeeper"
        exit 1
    fi
else
    log_message "ERROR: ZooKeeper is not running. Cannot start Kafka broker."
    exit 1
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

        # Check ZooKeeper
        check_zookeeper || log_message "WARNING: ZooKeeper health check failed"

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
echo $HOME/kafka_cluster
