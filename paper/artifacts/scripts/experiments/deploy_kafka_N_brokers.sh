#!/usr/bin/bash -l
#SBATCH --job-name=KafkaBench
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=4GB
#SBATCH --cpus-per-task=16
#SBATCH --time=0-05:59:00
#SBATCH --partition=batch

# ─── Configuration ───────────────────────────────────────────
# Usage:
#   NUM_BROKERS=1 CONTAINER=hsk.sif sbatch --nodes=2 deploy_kafka_N_brokers.sh
#   NUM_BROKERS=2 CONTAINER=hsk.sif sbatch --nodes=3 deploy_kafka_N_brokers.sh
#   NUM_BROKERS=3 CONTAINER=hsk.sif sbatch --nodes=4 deploy_kafka_N_brokers.sh
#
# CONTAINER: your .sif file name (must be in $HOME). Check: ls ~/hsk.sif ~/sparkhdfs.sif
NUM_BROKERS=${NUM_BROKERS:-3}
CONTAINER="${CONTAINER:-hsk.sif}"
WORK_DIR="${HOME}/kafka_bench"

# Load container runtime (Apptainer on newer ULHPC, Singularity on older)
module load tools/Apptainer 2>/dev/null || module load tools/Singularity 2>/dev/null || true

log_msg() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a ${WORK_DIR}/logs/main.log; }

# Clean up previous state
pkill -u $USER java || true
rm -rf /tmp/zookeeper/* /tmp/kafka-logs/* 2>/dev/null
singularity instance list 2>/dev/null | grep shinst_ | awk '{print $1}' | xargs -r singularity instance stop 2>/dev/null || true

mkdir -p ${WORK_DIR}/{logs,kafka/config,kafka/logs,results}
mkdir -p /tmp/zookeeper /tmp/kafka-logs
chmod 755 /tmp/zookeeper /tmp/kafka-logs

hostName="$(hostname)"
echo "$hostName" > ${HOME}/coordinatorNode

# Save config so launcher scripts and benchmark script can read it
cat > ${WORK_DIR}/cluster_env.sh << ENVEOF
export CONTAINER="${CONTAINER}"
export NUM_BROKERS=${NUM_BROKERS}
export WORK_DIR="${WORK_DIR}"
ENVEOF

log_msg "Coordinator: $hostName | Deploying $NUM_BROKERS broker(s) with container $CONTAINER"

# ─── ZooKeeper config ────────────────────────────────────────
cat > ${WORK_DIR}/kafka/config/zookeeper.properties << EOF
dataDir=/tmp/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
tickTime=2000
initLimit=10
syncLimit=5
EOF

# ─── ZooKeeper launcher ──────────────────────────────────────
cat << 'ZKEOF' > ${WORK_DIR}/zk_launcher.sh
#!/bin/bash
source ${HOME}/kafka_bench/cluster_env.sh
module load tools/Apptainer 2>/dev/null || module load tools/Singularity 2>/dev/null || true

echo "Starting ZooKeeper on $(hostname) with container ${CONTAINER}"

singularity instance stop shinst_zk 2>/dev/null || true
singularity instance start \
    --bind ${WORK_DIR}/kafka/config:/opt/kafka/config,${WORK_DIR}/kafka/logs:/opt/kafka/logs \
    ${HOME}/${CONTAINER} shinst_zk || true

singularity exec instance://shinst_zk \
    /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties \
    > ${WORK_DIR}/kafka/logs/zookeeper.log 2>&1 &

echo "Waiting for ZooKeeper..."
for i in {1..60}; do
    if singularity exec instance://shinst_zk /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls / &>/dev/null; then
        echo "ZooKeeper READY on $(hostname):2181"
        break
    fi
    [ $i -eq 60 ] && echo "ERROR: ZooKeeper failed to start after 5 minutes"
    sleep 5
done

while true; do sleep 60; done
ZKEOF
chmod +x ${WORK_DIR}/zk_launcher.sh

# ─── Kafka broker launcher ───────────────────────────────────
cat << 'BREOF' > ${WORK_DIR}/broker_launcher.sh
#!/bin/bash
source ${HOME}/kafka_bench/cluster_env.sh
module load tools/Apptainer 2>/dev/null || module load tools/Singularity 2>/dev/null || true

ZK_HOST=$(cat ${HOME}/coordinatorNode)
BROKER_ID=$((SLURM_PROCID + 1))
KAFKA_PORT=$((9092 + SLURM_PROCID))

echo "Starting Kafka broker ${BROKER_ID} on $(hostname):${KAFKA_PORT} with container ${CONTAINER}"

singularity instance stop shinst_broker_${BROKER_ID} 2>/dev/null || true
singularity instance start \
    --bind ${WORK_DIR}/kafka/config:/opt/kafka/config,${WORK_DIR}/kafka/logs:/opt/kafka/logs \
    ${HOME}/${CONTAINER} shinst_broker_${BROKER_ID}

cat > ${WORK_DIR}/kafka/config/server_${BROKER_ID}.properties << EOF
broker.id=${BROKER_ID}
listeners=PLAINTEXT://0.0.0.0:${KAFKA_PORT}
advertised.listeners=PLAINTEXT://$(hostname):${KAFKA_PORT}
log.dirs=/tmp/kafka-logs
zookeeper.connect=${ZK_HOST}:2181
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
num.io.threads=8
num.network.threads=3
EOF

echo "Waiting for ZooKeeper at ${ZK_HOST}:2181..."
for i in {1..60}; do
    if singularity exec instance://shinst_broker_${BROKER_ID} \
        /opt/kafka/bin/zookeeper-shell.sh ${ZK_HOST}:2181 ls / &>/dev/null; then
        echo "ZooKeeper available"
        break
    fi
    [ $i -eq 60 ] && echo "ERROR: ZooKeeper not reachable after 5 minutes"
    sleep 5
done

singularity exec instance://shinst_broker_${BROKER_ID} \
    /opt/kafka/bin/kafka-server-start.sh ${WORK_DIR}/kafka/config/server_${BROKER_ID}.properties &
sleep 30
echo "Broker ${BROKER_ID} READY on $(hostname):${KAFKA_PORT}"

while true; do sleep 60; done
BREOF
chmod +x ${WORK_DIR}/broker_launcher.sh

# ─── Launch ZooKeeper ────────────────────────────────────────
log_msg "Launching ZooKeeper on ${hostName}..."
srun --exclusive --nodes=1 --ntasks=1 --cpus-per-task=8 \
     --nodelist=${hostName} \
     --output=${WORK_DIR}/logs/ZK-%N-%j.out \
     ${WORK_DIR}/zk_launcher.sh &
sleep 60

# Verify ZooKeeper
ZK_OK=false
for i in {1..10}; do
    if singularity exec instance://shinst_zk /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls / &>/dev/null; then
        ZK_OK=true
        break
    fi
    sleep 10
done
if $ZK_OK; then
    log_msg "ZooKeeper verified OK"
else
    log_msg "ERROR: ZooKeeper not responding. Check ${WORK_DIR}/logs/ZK-*.out"
    exit 1
fi

# ─── Launch Kafka brokers ────────────────────────────────────
log_msg "Launching ${NUM_BROKERS} Kafka broker(s)..."
srun --exclusive --nodes=${NUM_BROKERS} --ntasks=${NUM_BROKERS} --ntasks-per-node=1 --cpus-per-task=16 \
     --exclude=${hostName} \
     --output=${WORK_DIR}/logs/Broker-%N-%j.out \
     ${WORK_DIR}/broker_launcher.sh &
sleep 120

# Verify brokers
log_msg "Checking broker registration..."
BROKER_IDS=$(singularity exec instance://shinst_zk \
    /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids 2>/dev/null | grep '\[')
log_msg "Registered brokers: ${BROKER_IDS}"

# Build bootstrap servers list
BOOTSTRAP=""
for BID in $(echo "$BROKER_IDS" | sed 's/\[//;s/\]//;s/,/ /g'); do
    BDATA=$(singularity exec instance://shinst_zk \
        /opt/kafka/bin/zookeeper-shell.sh localhost:2181 get /brokers/ids/$BID 2>/dev/null | grep -v "^$" | tail -1)
    BHOST=$(echo $BDATA | grep -o '"host":"[^"]*"' | cut -d'"' -f4)
    BPORT=$(echo $BDATA | grep -o '"port":[0-9]*' | cut -d':' -f2)
    [ -n "$BOOTSTRAP" ] && BOOTSTRAP="${BOOTSTRAP},"
    BOOTSTRAP="${BOOTSTRAP}${BHOST}:${BPORT}"
done
echo "$BOOTSTRAP" > ${WORK_DIR}/bootstrap_servers

# Save full cluster info
cat > ${WORK_DIR}/cluster_info.txt << INFOEOF
num_brokers=${NUM_BROKERS}
bootstrap_servers=${BOOTSTRAP}
zookeeper=${hostName}:2181
container=${CONTAINER}
deployed_at=$(date -Iseconds)
slurm_jobid=${SLURM_JOBID}
INFOEOF

log_msg "════════════════════════════════════════════════════"
log_msg " CLUSTER READY"
log_msg " Brokers:   ${NUM_BROKERS}"
log_msg " Bootstrap: ${BOOTSTRAP}"
log_msg " Container: ${CONTAINER}"
log_msg " Job ID:    ${SLURM_JOBID}"
log_msg ""
log_msg " TO RUN BENCHMARKS:"
log_msg "   ssh ${hostName}"
log_msg "   bash ~/experiment_scripts/kafka_scaling_bench.sh"
log_msg "════════════════════════════════════════════════════"

trap 'singularity instance list | grep shinst_ | awk "{print \$1}" | xargs -r singularity instance stop' EXIT

while true; do sleep 60; done
