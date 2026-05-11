#!/usr/bin/bash -l
#SBATCH --job-name=KafkaBM
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=4GB
#SBATCH --cpus-per-task=16
#SBATCH --time=0-03:00:00
#SBATCH --partition=batch

# Bare-metal Kafka deployment (NO Singularity)
# Purpose: baseline comparison against containerized deployment
#
# Usage:
#   sbatch deploy_kafka_bare_metal.sh
#
# This deploys ZooKeeper + 1 Kafka broker using native Java on Aion,
# identical topology to the 1-broker containerized setup.

WORK_DIR="${HOME}/kafka_bench_bm"
KAFKA_VERSION="2.8.0"
SCALA_VERSION="2.13"
KAFKA_DIR="${HOME}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"

log_msg() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a ${WORK_DIR}/logs/main.log; }

# ─── Load Java ──────────────────────────────────────────────
module purge 2>/dev/null || true
module load lang/Java 2>/dev/null || module load devel/Java 2>/dev/null || module load Java 2>/dev/null || {
    echo "ERROR: Cannot load Java module. Try: module spider Java"
    echo "Available modules with 'java' in name:"
    module avail 2>&1 | grep -i java || true
    exit 1
}
java -version 2>&1 | head -1
log_msg "Java loaded successfully"

# ─── Download Kafka if needed ───────────────────────────────
if [ ! -d "$KAFKA_DIR" ]; then
    log_msg "Downloading Kafka ${KAFKA_VERSION}..."
    cd ${HOME}
    wget -q "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
    tar xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
    rm -f "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
    log_msg "Kafka downloaded and extracted to ${KAFKA_DIR}"
else
    log_msg "Kafka already present at ${KAFKA_DIR}"
fi

# ─── Cleanup ────────────────────────────────────────────────
pkill -u $USER -f 'kafka\.Kafka' 2>/dev/null || true
pkill -u $USER -f 'QuorumPeerMain' 2>/dev/null || true
sleep 2
rm -rf /tmp/zookeeper/* /tmp/kafka-logs-bm/* 2>/dev/null || true

mkdir -p ${WORK_DIR}/{logs,config,results}
mkdir -p /tmp/zookeeper /tmp/kafka-logs-bm

NODES=($(scontrol show hostnames $SLURM_JOB_NODELIST))
COORDINATOR=${NODES[0]}
BROKER_NODE=${NODES[1]}

echo "$COORDINATOR" > ${HOME}/coordinatorNode_bm
log_msg "Coordinator (ZK): $COORDINATOR | Broker: $BROKER_NODE"

# ─── Save environment for benchmark script ──────────────────
cat > ${WORK_DIR}/cluster_env.sh << ENVEOF
export KAFKA_DIR="${KAFKA_DIR}"
export WORK_DIR="${WORK_DIR}"
export BOOTSTRAP_SERVERS="${BROKER_NODE}:9092"
export COORDINATOR="${COORDINATOR}"
export BROKER_NODE="${BROKER_NODE}"
ENVEOF

echo "${BROKER_NODE}:9092" > ${WORK_DIR}/bootstrap_servers

# ─── ZooKeeper config ───────────────────────────────────────
cat > ${WORK_DIR}/config/zookeeper.properties << EOF
dataDir=/tmp/zookeeper
clientPort=2181
maxClientCnxns=0
tickTime=2000
initLimit=10
syncLimit=5
EOF

# ─── Kafka broker config ───────────────────────────────────
cat > ${WORK_DIR}/config/server.properties << EOF
broker.id=0
listeners=PLAINTEXT://${BROKER_NODE}:9092
advertised.listeners=PLAINTEXT://${BROKER_NODE}:9092
log.dirs=/tmp/kafka-logs-bm
num.partitions=1
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=262144
socket.receive.buffer.bytes=262144
log.retention.hours=1
zookeeper.connect=${COORDINATOR}:2181
zookeeper.connection.timeout.ms=18000
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF

# ─── Start ZooKeeper on coordinator ─────────────────────────
log_msg "Starting ZooKeeper on $COORDINATOR (bare-metal)..."
export KAFKA_HEAP_OPTS="-Xmx512M -Xms512M"
nohup ${KAFKA_DIR}/bin/zookeeper-server-start.sh \
    ${WORK_DIR}/config/zookeeper.properties \
    > ${WORK_DIR}/logs/zookeeper.log 2>&1 &
ZK_PID=$!
log_msg "ZooKeeper PID: $ZK_PID"
sleep 10

# Verify ZK is running
if ! kill -0 $ZK_PID 2>/dev/null; then
    log_msg "ERROR: ZooKeeper failed to start. Check ${WORK_DIR}/logs/zookeeper.log"
    exit 1
fi
log_msg "ZooKeeper is running"

# ─── Start Kafka broker on broker node ──────────────────────
log_msg "Starting Kafka broker on $BROKER_NODE (bare-metal)..."
ssh ${BROKER_NODE} bash -l << 'BROKEREOF'
    module load lang/Java 2>/dev/null || module load devel/Java 2>/dev/null || module load Java 2>/dev/null || true
    source ${HOME}/kafka_bench_bm/cluster_env.sh
    mkdir -p /tmp/kafka-logs-bm
    export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
    nohup ${KAFKA_DIR}/bin/kafka-server-start.sh \
        ${HOME}/kafka_bench_bm/config/server.properties \
        > ${HOME}/kafka_bench_bm/logs/broker.log 2>&1 &
    echo $! > ${HOME}/kafka_bench_bm/broker.pid
    echo "Broker PID: $(cat ${HOME}/kafka_bench_bm/broker.pid)"
BROKEREOF

log_msg "Waiting for broker to register..."
sleep 15

# Verify broker is reachable
${KAFKA_DIR}/bin/kafka-topics.sh --bootstrap-server ${BROKER_NODE}:9092 --list > /dev/null 2>&1
if [ $? -eq 0 ]; then
    log_msg "Broker is reachable at ${BROKER_NODE}:9092"
else
    log_msg "WARNING: Broker not responding yet. Waiting 15 more seconds..."
    sleep 15
fi

# ─── Ready ──────────────────────────────────────────────────
log_msg "════════════════════════════════════════════════════"
log_msg "BARE-METAL CLUSTER READY"
log_msg "Bootstrap: ${BROKER_NODE}:9092"
log_msg "Job ID: ${SLURM_JOB_ID}"
log_msg "TO RUN BENCHMARKS:"
log_msg "  ssh ${COORDINATOR}"
log_msg "  bash ~/experiment_scripts/bare_metal_bench.sh"
log_msg "════════════════════════════════════════════════════"

# Keep job alive for benchmarks
while true; do
    if ! kill -0 $ZK_PID 2>/dev/null; then
        log_msg "ZooKeeper stopped. Exiting."
        break
    fi
    sleep 60
done
