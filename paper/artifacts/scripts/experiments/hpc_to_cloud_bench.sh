#!/bin/bash
# hpc_to_cloud_bench.sh — HPC to Cloud Kafka transfer benchmark
#
# Measures throughput from HPC Kafka producer to a remote AWS Kafka broker.
# Run this from an Aion compute node within a SLURM job.
#
# Usage:
#   ssh <compute_node>
#   module load tools/Apptainer
#   bash ~/experiment_scripts/hpc_to_cloud_bench.sh <AWS_KAFKA_IP> <REGION_LABEL>
#
# Examples:
#   bash ~/experiment_scripts/hpc_to_cloud_bench.sh 3.120.45.67 eu-central-1
#   bash ~/experiment_scripts/hpc_to_cloud_bench.sh 54.210.12.34 us-east-1

set -euo pipefail

AWS_KAFKA_IP=${1:?"Usage: $0 <AWS_KAFKA_BROKER_IP> <REGION_LABEL>"}
AWS_REGION=${2:-"unknown-region"}
AWS_BOOTSTRAP="${AWS_KAFKA_IP}:9092"

WORK_DIR="${HOME}/kafka_bench_cloud"
mkdir -p ${WORK_DIR}/{results,logs}

module load tools/Apptainer 2>/dev/null || module load tools/Singularity 2>/dev/null || true

SIF="${HOME}/hsk.sif"
if [ ! -f "$SIF" ]; then
    SIF="${HOME}/sparkhdfs.sif"
fi

echo "=== HPC-to-Cloud Kafka Benchmark ==="
echo "HPC node: $(hostname)"
echo "AWS Kafka: ${AWS_BOOTSTRAP} (${AWS_REGION})"
echo "Container: ${SIF}"
echo ""

# Verify connectivity
echo -n "Testing connectivity to ${AWS_BOOTSTRAP}... "
singularity exec $SIF /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$AWS_BOOTSTRAP" --list > /dev/null 2>&1 \
    && echo "OK" || { echo "FAILED — check security group and network"; exit 1; }

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="${WORK_DIR}/results/hpc_to_${AWS_REGION}_${TIMESTAMP}.csv"
LOG_DIR="${WORK_DIR}/logs/hpc_${AWS_REGION}_${TIMESTAMP}"
mkdir -p "$LOG_DIR"

echo "Results: ${RESULTS_FILE}"
echo ""

echo "segment,aws_region,test_type,partitions,message_size_bytes,producer_acks,records_per_sec,mb_per_sec,avg_latency_ms,max_latency_ms,p50_latency_ms,p99_latency_ms" > "$RESULTS_FILE"

NUM_MESSAGES=100000
PARTITIONS=(1 4 8)
MESSAGE_SIZES=(100 1024 10240)
ACKS=("1" "all")

topic_create() {
    local topic=$1 partitions=$2
    singularity exec $SIF /opt/kafka/bin/kafka-topics.sh --create \
        --topic "$topic" --bootstrap-server "$AWS_BOOTSTRAP" \
        --partitions "$partitions" --replication-factor 1 \
        --config retention.ms=3600000 > /dev/null 2>&1
}

topic_delete() {
    singularity exec $SIF /opt/kafka/bin/kafka-topics.sh --delete \
        --topic "$1" --bootstrap-server "$AWS_BOOTSTRAP" > /dev/null 2>&1 || true
}

run_hpc_to_cloud_producer() {
    local part=$1 msg=$2 acks=$3
    local topic="hpc-cloud-p${part}-m${msg}-a${acks}-$(date +%s)"
    local logf="${LOG_DIR}/hpc_cloud_prod_p${part}_m${msg}_a${acks}.log"

    echo -n "  HPC→Cloud Producer: part=$part msg=${msg}B acks=$acks ... "
    topic_create "$topic" "$part"

    singularity exec $SIF /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic "$topic" --num-records $NUM_MESSAGES --record-size "$msg" \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$AWS_BOOTSTRAP acks=$acks \
            buffer.memory=67108864 batch.size=65536 linger.ms=10 \
        --print-metrics > "$logf" 2>&1

    local rps=$(grep -oP '\d+\.\d+ records/sec' "$logf" | head -1 | awk '{print $1}')
    local mbs=$(grep -oP '\d+\.\d+ MB/sec' "$logf" | head -1 | awk '{print $1}')
    local avg=$(grep -oP '\d+\.\d+ ms avg latency' "$logf" | head -1 | awk '{print $1}')
    local max=$(grep -oP '\d+\.\d+ ms max latency' "$logf" | head -1 | awk '{print $1}')

    # Extract percentile latencies from the summary line
    local p50=$(grep -oP '\d+ ms 50th' "$logf" | head -1 | awk '{print $1}')
    local p99=$(grep -oP '\d+ ms 99th' "$logf" | head -1 | awk '{print $1}')

    echo "hpc_to_cloud,${AWS_REGION},producer,${part},${msg},${acks},${rps:-0},${mbs:-0},${avg:-0},${max:-0},${p50:-0},${p99:-0}" >> "$RESULTS_FILE"
    echo "${mbs:-0} MB/s (avg lat: ${avg:-?} ms, p99: ${p99:-?} ms)"

    topic_delete "$topic"
    sleep 3
}

# Also run within-HPC producer for direct comparison (same script, same node, same time)
run_within_hpc_producer() {
    local part=$1 msg=$2 acks=$3
    # Uses the LOCAL HPC Kafka broker (if running)
    local hpc_bootstrap
    if [ -f "${HOME}/kafka_bench/bootstrap_servers" ]; then
        hpc_bootstrap=$(cat ${HOME}/kafka_bench/bootstrap_servers)
    else
        echo "  [SKIP] No local HPC Kafka broker running — skipping within-HPC comparison"
        return
    fi

    local topic="hpc-local-p${part}-m${msg}-a${acks}-$(date +%s)"
    local logf="${LOG_DIR}/hpc_local_prod_p${part}_m${msg}_a${acks}.log"

    echo -n "  Within-HPC Producer: part=$part msg=${msg}B acks=$acks ... "

    singularity exec $SIF /opt/kafka/bin/kafka-topics.sh --create \
        --topic "$topic" --bootstrap-server "$hpc_bootstrap" \
        --partitions "$part" --replication-factor 1 \
        --config retention.ms=3600000 > /dev/null 2>&1

    singularity exec $SIF /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic "$topic" --num-records $NUM_MESSAGES --record-size "$msg" \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$hpc_bootstrap acks=$acks \
            buffer.memory=67108864 batch.size=65536 linger.ms=10 \
        --print-metrics > "$logf" 2>&1

    local rps=$(grep -oP '\d+\.\d+ records/sec' "$logf" | head -1 | awk '{print $1}')
    local mbs=$(grep -oP '\d+\.\d+ MB/sec' "$logf" | head -1 | awk '{print $1}')
    local avg=$(grep -oP '\d+\.\d+ ms avg latency' "$logf" | head -1 | awk '{print $1}')
    local max=$(grep -oP '\d+\.\d+ ms max latency' "$logf" | head -1 | awk '{print $1}')
    local p50=$(grep -oP '\d+ ms 50th' "$logf" | head -1 | awk '{print $1}')
    local p99=$(grep -oP '\d+ ms 99th' "$logf" | head -1 | awk '{print $1}')

    echo "within_hpc,local,producer,${part},${msg},${acks},${rps:-0},${mbs:-0},${avg:-0},${max:-0},${p50:-0},${p99:-0}" >> "$RESULTS_FILE"
    echo "${mbs:-0} MB/s (avg lat: ${avg:-?} ms)"

    singularity exec $SIF /opt/kafka/bin/kafka-topics.sh --delete \
        --topic "$topic" --bootstrap-server "$hpc_bootstrap" > /dev/null 2>&1 || true
    sleep 3
}

# ─── Main benchmark loop ─────────────────────────────────────
START_TIME=$(date +%s)

echo "════════════════════════════════════════════"
echo " Phase 1: HPC → Cloud (AWS ${AWS_REGION})"
echo "════════════════════════════════════════════"

for part in "${PARTITIONS[@]}"; do
    for msg in "${MESSAGE_SIZES[@]}"; do
        for acks in "${ACKS[@]}"; do
            run_hpc_to_cloud_producer "$part" "$msg" "$acks"
        done
    done
done

echo ""
echo "════════════════════════════════════════════"
echo " Phase 2: Within-HPC (local comparison)"
echo "════════════════════════════════════════════"

for part in "${PARTITIONS[@]}"; do
    for msg in "${MESSAGE_SIZES[@]}"; do
        for acks in "${ACKS[@]}"; do
            run_within_hpc_producer "$part" "$msg" "$acks"
        done
    done
done

END_TIME=$(date +%s)
ELAPSED=$(( (END_TIME - START_TIME) / 60 ))
echo ""
echo "═══════════════════════════════════════════════"
echo " COMPLETE"
echo " Duration: ${ELAPSED} minutes"
echo " Results:  ${RESULTS_FILE}"
echo "═══════════════════════════════════════════════"
