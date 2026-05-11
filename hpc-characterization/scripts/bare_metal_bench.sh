#!/bin/bash
# bare_metal_bench.sh — Bare-metal Kafka benchmark (NO Singularity)
#
# Mirrors kafka_scaling_bench.sh but uses native Kafka binaries.
# Same configs, same CSV format — only difference is no container.
#
# Usage (run from coordinator node inside the SLURM job):
#   ssh <coordinator_node>     # shown in SLURM output
#   module load lang/Java      # or appropriate Java module
#   bash ~/experiment_scripts/bare_metal_bench.sh

set -euo pipefail

WORK_DIR="${HOME}/kafka_bench_bm"
source ${WORK_DIR}/cluster_env.sh

# Verify Java is available
java -version 2>&1 | head -1 || {
    echo "ERROR: Java not loaded. Run: module load lang/Java"
    exit 1
}

BOOTSTRAP_SERVERS=$(cat ${WORK_DIR}/bootstrap_servers)
BIN="${KAFKA_DIR}/bin"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="${WORK_DIR}/results/bare_metal_bench_${TIMESTAMP}.csv"
LOG_DIR="${WORK_DIR}/results/logs_bm_${TIMESTAMP}"
mkdir -p "$LOG_DIR"

echo "=== Bare-Metal Kafka Benchmark (No Singularity) ==="
echo "Bootstrap: ${BOOTSTRAP_SERVERS}"
echo "Kafka dir: ${KAFKA_DIR}"
echo "Results: ${RESULTS_FILE}"
echo ""

# CSV header — same format as containerized results
echo "num_brokers,replication_factor,test_type,partitions,message_size_bytes,producer_acks,consumer_groups,records_per_sec,mb_per_sec,avg_latency_ms,max_latency_ms" > "$RESULTS_FILE"

NUM_MESSAGES=100000
PARTITIONS=(1 4 8)
MESSAGE_SIZES=(100 1024 10240)
ACKS=("1" "all")
RF=1
NUM_BROKERS=1

topic_create() {
    local topic=$1 partitions=$2 rf=$3
    ${BIN}/kafka-topics.sh --create --topic "$topic" \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --partitions "$partitions" --replication-factor "$rf" \
        --config retention.ms=3600000 > /dev/null 2>&1
}

topic_delete() {
    ${BIN}/kafka-topics.sh --delete --topic "$1" \
        --bootstrap-server "$BOOTSTRAP_SERVERS" > /dev/null 2>&1 || true
}

run_producer() {
    local part=$1 msg=$2 acks=$3
    local topic="bm-prod-p${part}-m${msg}-a${acks}-$(date +%s)"
    local logf="${LOG_DIR}/prod_p${part}_m${msg}_a${acks}.log"

    echo -n "  Producer: part=$part msg=${msg}B acks=$acks ... "
    topic_create "$topic" "$part" "$RF"

    ${BIN}/kafka-producer-perf-test.sh \
        --topic "$topic" --num-records $NUM_MESSAGES --record-size "$msg" \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=$acks \
        --print-metrics > "$logf" 2>&1

    local rps=$(grep -oP '\d+\.\d+ records/sec' "$logf" | head -1 | awk '{print $1}')
    local mbs=$(grep -oP '\d+\.\d+ MB/sec' "$logf" | head -1 | awk '{print $1}')
    local avg=$(grep -oP '\d+\.\d+ ms avg latency' "$logf" | head -1 | awk '{print $1}')
    local max=$(grep -oP '\d+\.\d+ ms max latency' "$logf" | head -1 | awk '{print $1}')

    echo "${NUM_BROKERS},${RF},producer,${part},${msg},${acks},N/A,${rps:-0},${mbs:-0},${avg:-0},${max:-0}" >> "$RESULTS_FILE"
    echo "${mbs:-0} MB/s (lat: ${avg:-?} ms)"

    topic_delete "$topic"
    sleep 3
}

run_consumer() {
    local part=$1 msg=$2
    local topic="bm-cons-p${part}-m${msg}-$(date +%s)"
    local logf="${LOG_DIR}/cons_p${part}_m${msg}.log"

    echo -n "  Consumer: part=$part msg=${msg}B ... "
    topic_create "$topic" "$part" "$RF"

    ${BIN}/kafka-producer-perf-test.sh \
        --topic "$topic" --num-records $NUM_MESSAGES --record-size "$msg" \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=1 > /dev/null 2>&1

    local group="bm-cons-$(date +%s)"
    timeout 300 ${BIN}/kafka-consumer-perf-test.sh \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --topic "$topic" --messages $NUM_MESSAGES \
        --group "$group" --print-metrics > "$logf" 2>&1 || true

    local metrics=$(grep -A1 "start.time" "$logf" 2>/dev/null | tail -1)
    local mbs=$(echo "$metrics" | awk -F, '{print $4}' | tr -d ' ')
    local rps=$(echo "$metrics" | awk -F, '{print $6}' | tr -d ' ')

    echo "${NUM_BROKERS},${RF},consumer,${part},${msg},N/A,1,${rps:-0},${mbs:-0},N/A,N/A" >> "$RESULTS_FILE"
    echo "${mbs:-0} MB/s"

    topic_delete "$topic"
    sleep 3
}

run_e2e() {
    local part=$1 msg=$2
    local topic="bm-e2e-p${part}-m${msg}-$(date +%s)"
    local prodlog="${LOG_DIR}/e2e_prod_p${part}_m${msg}.log"
    local conslog="${LOG_DIR}/e2e_cons_p${part}_m${msg}.log"

    echo -n "  E2E: part=$part msg=${msg}B ... "
    topic_create "$topic" "$part" "$RF"

    local group="bm-e2e-$(date +%s)"
    timeout 300 ${BIN}/kafka-consumer-perf-test.sh \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --topic "$topic" --messages $NUM_MESSAGES \
        --group "$group" --print-metrics > "$conslog" 2>&1 &
    local cons_pid=$!
    sleep 5

    ${BIN}/kafka-producer-perf-test.sh \
        --topic "$topic" --num-records $NUM_MESSAGES --record-size "$msg" \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=1 \
        --print-metrics > "$prodlog" 2>&1

    local prps=$(grep -oP '\d+\.\d+ records/sec' "$prodlog" | head -1 | awk '{print $1}')
    local pmbs=$(grep -oP '\d+\.\d+ MB/sec' "$prodlog" | head -1 | awk '{print $1}')
    local pavg=$(grep -oP '\d+\.\d+ ms avg latency' "$prodlog" | head -1 | awk '{print $1}')
    local pmax=$(grep -oP '\d+\.\d+ ms max latency' "$prodlog" | head -1 | awk '{print $1}')

    echo "${NUM_BROKERS},${RF},e2e-producer,${part},${msg},1,1,${prps:-0},${pmbs:-0},${pavg:-0},${pmax:-0}" >> "$RESULTS_FILE"

    wait $cons_pid 2>/dev/null || true
    local cmetrics=$(grep -A1 "start.time" "$conslog" 2>/dev/null | tail -1)
    local cmbs=$(echo "$cmetrics" | awk -F, '{print $4}' | tr -d ' ')
    local crps=$(echo "$cmetrics" | awk -F, '{print $6}' | tr -d ' ')

    echo "${NUM_BROKERS},${RF},e2e-consumer,${part},${msg},N/A,1,${crps:-0},${cmbs:-0},N/A,N/A" >> "$RESULTS_FILE"
    echo "prod=${pmbs:-0} cons=${cmbs:-0} MB/s"

    topic_delete "$topic"
    sleep 3
}

# ─── Main benchmark loop ─────────────────────────────────────
START_TIME=$(date +%s)

echo "════════════════════════════════════════════"
echo " Bare-Metal: 1 broker, RF=1 (no Singularity)"
echo "════════════════════════════════════════════"

echo "--- Producer Tests ---"
for part in "${PARTITIONS[@]}"; do
    for msg in "${MESSAGE_SIZES[@]}"; do
        for acks in "${ACKS[@]}"; do
            run_producer "$part" "$msg" "$acks"
        done
    done
done

echo "--- Consumer Tests ---"
for part in "${PARTITIONS[@]}"; do
    for msg in "${MESSAGE_SIZES[@]}"; do
        run_consumer "$part" "$msg"
    done
done

echo "--- End-to-End Tests ---"
for part in "${PARTITIONS[@]}"; do
    for msg in "${MESSAGE_SIZES[@]}"; do
        run_e2e "$part" "$msg"
    done
done

END_TIME=$(date +%s)
ELAPSED=$(( (END_TIME - START_TIME) / 60 ))
echo ""
echo "═══════════════════════════════════════════════"
echo " COMPLETE: Bare-metal 1 broker, RF=1"
echo " Duration: ${ELAPSED} minutes"
echo " Results:  ${RESULTS_FILE}"
echo "═══════════════════════════════════════════════"
