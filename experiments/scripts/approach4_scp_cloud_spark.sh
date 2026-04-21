#!/bin/bash
# approach4_scp_cloud_spark.sh
# Approach 4: scp raw data to US-East-1, then Spark filter + Kafka produce on cloud
#
# Same as Approach 3 but adds cloud-side Spark filtering.
# Shows the cost of filtering on cloud instead of HPC.
# Measures: T_total = T_scp + T_spark_filter_produce
#
# Prerequisites:
#   - EC2 instance with Kafka + Spark (standalone) running
#   - spark_filter_produce.py deployed to EC2

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../configs/experiment.conf"

APPROACH="approach4"
RESULTS_DIR="${RESULTS_BASE}/${APPROACH}"
mkdir -p "$RESULTS_DIR"
RESULTS_CSV="${RESULTS_DIR}/results_$(timestamp).csv"

SSH_OPTS="-o StrictHostKeyChecking=no -i ${EC2_KEY_US}"
REMOTE_DIR="/home/${EC2_USER}/pipeline_data"

if [ -z "$EC2_US_EAST_IP" ]; then
    echo "ERROR: Set EC2_US_EAST_IP in configs/experiment.conf"
    exit 1
fi

log "═══════════════════════════════════════════════"
log " Approach 4: scp + Cloud Spark-Kafka (HPC → US-East-1)"
log "═══════════════════════════════════════════════"

DATASET_BYTES=$(stat -f%z "$DATASET_1GB" 2>/dev/null || stat -c%s "$DATASET_1GB")
log "Dataset: $DATASET_1GB"
log "Target:  $EC2_US_EAST_IP"
log "Runs:    $NUM_RUNS"

ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "mkdir -p ${REMOTE_DIR}"

echo "run_id,approach,dataset_size_bytes,scp_time_s,scp_throughput_mbps,spark_filter_produce_time_s,spark_throughput_mbps,total_time_s,total_throughput_mbps,filtered_records,filter_ratio" > "$RESULTS_CSV"

for run in $(seq 1 $NUM_RUNS); do
    RUN_ID="${APPROACH}_run${run}"
    TOPIC="pipeline-a4-run${run}-$(date +%s)"
    REMOTE_FILE="${REMOTE_DIR}/eea_raw_run${run}.csv"

    log "── Run $run/$NUM_RUNS ──"

    # ── Phase 1: scp transfer ────────────────────────────────
    log "Phase 1: scp transfer..."
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "rm -f ${REMOTE_FILE}" 2>/dev/null || true

    T_SCP_START=$(date +%s%N)
    scp $SSH_OPTS "$DATASET_1GB" "${EC2_USER}@${EC2_US_EAST_IP}:${REMOTE_FILE}"
    T_SCP_END=$(date +%s%N)

    SCP_NS=$((T_SCP_END - T_SCP_START))
    SCP_S=$(echo "scale=2; $SCP_NS / 1000000000" | bc)
    SCP_MBPS=$(echo "scale=2; $DATASET_BYTES / 1048576 / ($SCP_NS / 1000000000)" | bc)
    log "  scp complete: ${SCP_S}s (${SCP_MBPS} MB/s)"

    # ── Phase 2: Spark filter + produce on cloud ─────────────
    log "Phase 2: Spark filter → Kafka on EC2..."

    # Create topic
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "
        sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
            --delete --topic ${TOPIC} 2>/dev/null || true
        sleep 2
        sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
            --create --topic ${TOPIC} --partitions ${KAFKA_PARTITIONS} \
            --replication-factor 1
    "

    # Run Spark on EC2 (standalone mode)
    SPARK_OUTPUT=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "
        cd /home/${EC2_USER} && \
        spark-submit \
            --master local[*] \
            --driver-memory 4g \
            --conf spark.hadoop.fs.defaultFS=file:/// \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
            /home/${EC2_USER}/spark_filter_produce.py \
                --input ${REMOTE_FILE} \
                --bootstrap-servers localhost:9092 \
                --topic ${TOPIC} \
                --filter-countries ${FILTER_COUNTRIES} \
                --partitions ${KAFKA_PARTITIONS} \
                --batch-size ${SPARK_BATCH_SIZE} \
                --acks ${KAFKA_ACKS} \
                --run-id ${RUN_ID} \
        2>&1
    ")

    echo "$SPARK_OUTPUT" > "${RESULTS_DIR}/${RUN_ID}_spark.log"

    SPARK_TIME=$(echo "$SPARK_OUTPUT" | grep -oP 'total_time_s:\s*\K[0-9.]+' || echo "0")
    SPARK_MBPS=$(echo "$SPARK_OUTPUT" | grep -oP 'kafka_throughput_mbps:\s*\K[0-9.]+' || echo "0")
    FILTERED_RECORDS=$(echo "$SPARK_OUTPUT" | grep -oP 'filtered_records:\s*\K[0-9]+' || echo "0")
    FILTER_RATIO=$(echo "$SPARK_OUTPUT" | grep -oP 'filter_ratio:\s*\K[0-9.]+' || echo "0")

    log "  Spark complete: ${SPARK_TIME}s (${SPARK_MBPS} MB/s Kafka throughput)"

    # ── Total ────────────────────────────────────────────────
    TOTAL_S=$(echo "scale=2; $SCP_S + $SPARK_TIME" | bc)
    TOTAL_MBPS=$(echo "scale=2; $DATASET_BYTES / 1048576 / $TOTAL_S" | bc 2>/dev/null || echo "0")

    log "  Total: ${TOTAL_S}s (${TOTAL_MBPS} MB/s effective)"

    echo "${RUN_ID},${APPROACH},${DATASET_BYTES},${SCP_S},${SCP_MBPS},${SPARK_TIME},${SPARK_MBPS},${TOTAL_S},${TOTAL_MBPS},${FILTERED_RECORDS},${FILTER_RATIO}" >> "$RESULTS_CSV"

    # Cleanup
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "
        sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
            --delete --topic ${TOPIC} 2>/dev/null || true
        rm -f ${REMOTE_FILE}
    "

    log "Run $run complete."
    sleep 5
done

log "═══════════════════════════════════════════════"
log " Approach 4 complete. Results: $RESULTS_CSV"
log "═══════════════════════════════════════════════"
