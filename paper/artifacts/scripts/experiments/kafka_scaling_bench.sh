#!/bin/bash
# kafka_scaling_bench.sh — Broker scaling + replication factor benchmark
#
# Usage (run from coordinator node inside the SLURM job):
#   ssh <coordinator_node>
#   bash ~/experiment_scripts/kafka_scaling_bench.sh [MAX_RF]
#
# MAX_RF defaults to the number of brokers (auto-detected).
# Results are written to ~/kafka_bench/results/

set -euo pipefail

WORK_DIR="${HOME}/kafka_bench"

# Load container runtime
module load tools/Apptainer 2>/dev/null || module load tools/Singularity 2>/dev/null || true

# We run Kafka CLI tools via "singularity exec <container>" (not instances)
# because instances are only visible within the srun process that created them.
source ${WORK_DIR}/cluster_env.sh
SIF="${HOME}/${CONTAINER}"

BOOTSTRAP_SERVERS=$(cat ${WORK_DIR}/bootstrap_servers)
NUM_BROKERS=$(source ${WORK_DIR}/cluster_info.txt && echo $num_brokers)
MAX_RF=${1:-$NUM_BROKERS}

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="${WORK_DIR}/results/scaling_bench_${NUM_BROKERS}br_${TIMESTAMP}.csv"
LOG_DIR="${WORK_DIR}/results/logs_${TIMESTAMP}"
mkdir -p "$LOG_DIR"

echo "=== Kafka Scaling Benchmark ==="
echo "Brokers: ${NUM_BROKERS} | Max RF: ${MAX_RF}"
echo "Bootstrap: ${BOOTSTRAP_SERVERS}"
echo "Results: ${RESULTS_FILE}"
echo ""

# CSV header
echo "num_brokers,replication_factor,test_type,partitions,message_size_bytes,producer_acks,consumer_groups,records_per_sec,mb_per_sec,avg_latency_ms,max_latency_ms" > "$RESULTS_FILE"

NUM_MESSAGES=100000
PARTITIONS=(1 4 8)
MESSAGE_SIZES=(100 1024 10240)
ACKS=("1" "all")

topic_create() {
    local topic=$1 partitions=$2 rf=$3
    singularity exec $SIF \
        /opt/kafka/bin/kafka-topics.sh --create --topic "$topic" \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --partitions "$partitions" --replication-factor "$rf" \
        --config retention.ms=3600000 > /dev/null 2>&1
}

topic_delete() {
    singularity exec $SIF \
        /opt/kafka/bin/kafka-topics.sh --delete --topic "$1" \
        --bootstrap-server "$BOOTSTRAP_SERVERS" > /dev/null 2>&1 || true
}

run_producer() {
    local rf=$1 part=$2 msg=$3 acks=$4
    local topic="bench-${NUM_BROKERS}br-rf${rf}-p${part}-m${msg}-a${acks}-$(date +%s)"
    local logf="${LOG_DIR}/prod_rf${rf}_p${part}_m${msg}_a${acks}.log"

    echo -n "  Producer: RF=$rf part=$part msg=${msg}B acks=$acks ... "
    topic_create "$topic" "$part" "$rf"

    singularity exec $SIF \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic "$topic" --num-records $NUM_MESSAGES --record-size "$msg" \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=$acks \
        --print-metrics > "$logf" 2>&1

    local rps=$(grep -oP '\d+\.\d+ records/sec' "$logf" | head -1 | awk '{print $1}')
    local mbs=$(grep -oP '\d+\.\d+ MB/sec' "$logf" | head -1 | awk '{print $1}')
    local avg=$(grep -oP '\d+\.\d+ ms avg latency' "$logf" | head -1 | awk '{print $1}')
    local max=$(grep -oP '\d+\.\d+ ms max latency' "$logf" | head -1 | awk '{print $1}')

    echo "${NUM_BROKERS},${rf},producer,${part},${msg},${acks},N/A,${rps:-0},${mbs:-0},${avg:-0},${max:-0}" >> "$RESULTS_FILE"
    echo "${mbs:-0} MB/s (lat: ${avg:-?} ms)"

    topic_delete "$topic"
    sleep 3
}

run_consumer() {
    local rf=$1 part=$2 msg=$3
    local topic="bench-cons-${NUM_BROKERS}br-rf${rf}-p${part}-m${msg}-$(date +%s)"
    local logf="${LOG_DIR}/cons_rf${rf}_p${part}_m${msg}.log"

    echo -n "  Consumer: RF=$rf part=$part msg=${msg}B ... "
    topic_create "$topic" "$part" "$rf"

    singularity exec $SIF \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic "$topic" --num-records $NUM_MESSAGES --record-size "$msg" \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=1 > /dev/null 2>&1

    local group="cons-bench-$(date +%s)"
    timeout 300 singularity exec $SIF \
        /opt/kafka/bin/kafka-consumer-perf-test.sh \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --topic "$topic" --messages $NUM_MESSAGES \
        --group "$group" --print-metrics > "$logf" 2>&1 || true

    local metrics=$(grep -A1 "start.time" "$logf" 2>/dev/null | tail -1)
    local mbs=$(echo "$metrics" | awk -F, '{print $4}' | tr -d ' ')
    local rps=$(echo "$metrics" | awk -F, '{print $6}' | tr -d ' ')

    echo "${NUM_BROKERS},${rf},consumer,${part},${msg},N/A,1,${rps:-0},${mbs:-0},N/A,N/A" >> "$RESULTS_FILE"
    echo "${mbs:-0} MB/s"

    topic_delete "$topic"
    sleep 3
}

run_e2e() {
    local rf=$1 part=$2 msg=$3
    local topic="bench-e2e-${NUM_BROKERS}br-rf${rf}-p${part}-m${msg}-$(date +%s)"
    local prodlog="${LOG_DIR}/e2e_prod_rf${rf}_p${part}_m${msg}.log"
    local conslog="${LOG_DIR}/e2e_cons_rf${rf}_p${part}_m${msg}.log"

    echo -n "  E2E: RF=$rf part=$part msg=${msg}B ... "
    topic_create "$topic" "$part" "$rf"

    local group="e2e-bench-$(date +%s)"
    timeout 300 singularity exec $SIF \
        /opt/kafka/bin/kafka-consumer-perf-test.sh \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --topic "$topic" --messages $NUM_MESSAGES \
        --group "$group" --print-metrics > "$conslog" 2>&1 &
    local cons_pid=$!
    sleep 5

    singularity exec $SIF \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic "$topic" --num-records $NUM_MESSAGES --record-size "$msg" \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=1 \
        --print-metrics > "$prodlog" 2>&1

    local prps=$(grep -oP '\d+\.\d+ records/sec' "$prodlog" | head -1 | awk '{print $1}')
    local pmbs=$(grep -oP '\d+\.\d+ MB/sec' "$prodlog" | head -1 | awk '{print $1}')
    local pavg=$(grep -oP '\d+\.\d+ ms avg latency' "$prodlog" | head -1 | awk '{print $1}')
    local pmax=$(grep -oP '\d+\.\d+ ms max latency' "$prodlog" | head -1 | awk '{print $1}')

    echo "${NUM_BROKERS},${rf},e2e-producer,${part},${msg},1,1,${prps:-0},${pmbs:-0},${pavg:-0},${pmax:-0}" >> "$RESULTS_FILE"

    wait $cons_pid 2>/dev/null || true
    local cmetrics=$(grep -A1 "start.time" "$conslog" 2>/dev/null | tail -1)
    local cmbs=$(echo "$cmetrics" | awk -F, '{print $4}' | tr -d ' ')
    local crps=$(echo "$cmetrics" | awk -F, '{print $6}' | tr -d ' ')

    echo "${NUM_BROKERS},${rf},e2e-consumer,${part},${msg},N/A,1,${crps:-0},${cmbs:-0},N/A,N/A" >> "$RESULTS_FILE"
    echo "prod=${pmbs:-0} cons=${cmbs:-0} MB/s"

    topic_delete "$topic"
    sleep 3
}

# ─── Main benchmark loop ─────────────────────────────────────
START_TIME=$(date +%s)

for RF in $(seq 1 $MAX_RF); do
    echo ""
    echo "════════════════════════════════════════════"
    echo " Replication Factor: $RF (${NUM_BROKERS} brokers)"
    echo "════════════════════════════════════════════"

    echo "--- Producer Tests ---"
    for part in "${PARTITIONS[@]}"; do
        for msg in "${MESSAGE_SIZES[@]}"; do
            for acks in "${ACKS[@]}"; do
                run_producer "$RF" "$part" "$msg" "$acks"
            done
        done
    done

    echo "--- Consumer Tests ---"
    for part in "${PARTITIONS[@]}"; do
        for msg in "${MESSAGE_SIZES[@]}"; do
            run_consumer "$RF" "$part" "$msg"
        done
    done

    echo "--- End-to-End Tests ---"
    for part in "${PARTITIONS[@]}"; do
        for msg in "${MESSAGE_SIZES[@]}"; do
            run_e2e "$RF" "$part" "$msg"
        done
    done
done

END_TIME=$(date +%s)
ELAPSED=$(( (END_TIME - START_TIME) / 60 ))
echo ""
echo "═══════════════════════════════════════════════"
echo " COMPLETE: ${NUM_BROKERS} brokers, RF 1-${MAX_RF}"
echo " Duration: ${ELAPSED} minutes"
echo " Results:  ${RESULTS_FILE}"
echo "═══════════════════════════════════════════════"
