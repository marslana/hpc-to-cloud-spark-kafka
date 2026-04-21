#!/bin/bash
# Approach 2: Direct Producer (Stream)
# HPC Spark filter → produce directly to US-East-1 Kafka over WAN
#
# Run from HPC coordinator node.
# Prerequisites:
#   - EC2 US-East running Kafka, first_record_consumer.py deployed
#   - Spark cluster running (deploy_spark_5gb.sh)
#   - Dataset at ~/pipeline_data/eea_airquality_5gb.csv
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../configs/experiment_5gb.conf"

APPROACH="approach2"
OUT_DIR="${RESULTS_BASE}/${APPROACH}"
mkdir -p "$OUT_DIR"
RESULTS_CSV="${OUT_DIR}/results_$(timestamp).csv"

echo "run_id,dataset_gb,spark_read_s,spark_filter_s,kafka_produce_s,spark_total_s,kafka_throughput_mbps,filtered_records,consumer_first_record_s,consumer_active_s,consumer_total_s,consumer_records,consumer_bytes_mb,e2e_s" > "$RESULTS_CSV"

COORDINATOR_NODE=$(cat $HOME/coordinatorNode 2>/dev/null || hostname)
SPARK_MASTER="spark://${COORDINATOR_NODE}:7078"

log "═══════════════════════════════════════════════════════"
log " Approach 2: Direct Producer (HPC Spark → US-East Kafka)"
log " Dataset: $DATASET_5GB"
log " Target:  $KAFKA_US_EAST (WAN ~90ms RTT)"
log " Spark:   $SPARK_MASTER"
log "═══════════════════════════════════════════════════════"

for run in $(seq 1 $NUM_RUNS); do
    RUN_ID="${APPROACH}_run${run}"
    TOPIC="a2-5gb-run${run}"

    log "── Run $run/$NUM_RUNS ──"

    # 1. Create topic on US-East
    ensure_topic_remote "$EC2_US_EAST_IP" "$TOPIC" "$KAFKA_PARTITIONS"

    # 2. Start consumer on EC2
    log "Starting consumer on EC2..."
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "
        nohup python3 ~/scripts/first_record_consumer.py \
            --brokers localhost:9092 \
            --topic $TOPIC \
            --target-records 0 \
            --idle-timeout-ms $CONSUMER_IDLE_TIMEOUT_MS \
            --output ~/results/${APPROACH}_consumer.csv \
            --run-id $RUN_ID \
            > ~/results/${RUN_ID}_consumer.log 2>&1 &
        echo \$!
    " > /tmp/consumer_pid_${run}
    CONSUMER_PID=$(cat /tmp/consumer_pid_${run} | tr -d '[:space:]')
    log "Consumer PID: $CONSUMER_PID"
    sleep 2

    # 3. E2E timer
    E2E_START=$(date +%s)

    # 4. HPC Spark filter + produce to US-East
    log "Phase 1: Spark filter + Kafka produce to US-East over WAN..."
    singularity exec \
        --bind $HOME/pipeline_data:/opt/dataset \
        --bind "${OUT_DIR}":/opt/results \
        --bind $HOME/experiments:/opt/experiments \
        $SIF \
        spark-submit \
            --master "$SPARK_MASTER" \
            --conf spark.hadoop.fs.defaultFS=file:/// \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
            /opt/experiments/scripts/spark_filter_produce.py \
                --input "/opt/dataset/eea_airquality_5gb.csv" \
                --bootstrap-servers "$KAFKA_US_EAST" \
                --topic "$TOPIC" \
                --filter-countries "$FILTER_COUNTRIES" \
                --partitions "$KAFKA_PARTITIONS" \
                --batch-size "$KAFKA_BATCH_SIZE" \
                --acks "$KAFKA_ACKS" \
                --output-csv "/opt/results/results.csv" \
                --run-id "$RUN_ID" \
        2>&1 | tee "${OUT_DIR}/${RUN_ID}_spark.log"

    SPARK_LOG="${OUT_DIR}/${RUN_ID}_spark.log"
    SPARK_READ=$(grep -oP 'read_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    SPARK_FILTER=$(grep -oP 'filter_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    KAFKA_PRODUCE=$(grep -oP 'kafka_produce_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    SPARK_TOTAL=$(grep -oP 'total_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    KAFKA_TP=$(grep -oP 'kafka_throughput_mbps: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    FILTERED=$(grep -oP 'filtered_records: \K[0-9]+' "$SPARK_LOG" || echo "0")

    # 5. Wait for consumer
    log "Waiting for consumer..."
    for i in $(seq 1 180); do
        ALIVE=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "kill -0 $CONSUMER_PID 2>/dev/null && echo yes || echo no")
        [ "$ALIVE" = "no" ] && break
        sleep 10
    done

    E2E_END=$(date +%s)
    E2E_TIME=$((E2E_END - E2E_START))

    # 6. Collect consumer results
    CONSUMER_LOG=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "cat ~/results/${RUN_ID}_consumer.log")
    echo "$CONSUMER_LOG" | tee "${OUT_DIR}/${RUN_ID}_consumer.log"

    CONS_FRL=$(echo "$CONSUMER_LOG" | grep -oP 'first_record_latency:\s+\K[0-9.-]+' || echo "-1")
    CONS_ACTIVE=$(echo "$CONSUMER_LOG" | grep -oP 'active_duration_s:\s+\K[0-9.]+' || echo "0")
    CONS_TOTAL=$(echo "$CONSUMER_LOG" | grep -oP 'consumer_total_s:\s+\K[0-9.]+' || echo "0")
    CONS_RECORDS=$(echo "$CONSUMER_LOG" | grep -oP 'records:\s+\K[0-9]+' || echo "0")
    CONS_MB=$(echo "$CONSUMER_LOG" | grep -oP 'bytes_mb:\s+\K[0-9.]+' || echo "0")

    echo "${RUN_ID},5,${SPARK_READ},${SPARK_FILTER},${KAFKA_PRODUCE},${SPARK_TOTAL},${KAFKA_TP},${FILTERED},${CONS_FRL},${CONS_ACTIVE},${CONS_TOTAL},${CONS_RECORDS},${CONS_MB},${E2E_TIME}" >> "$RESULTS_CSV"

    log "=== RUN $run SUMMARY ==="
    log "  Spark total:       ${SPARK_TOTAL}s (read=${SPARK_READ} filter=${SPARK_FILTER} produce=${KAFKA_PRODUCE})"
    log "  WAN throughput:    ${KAFKA_TP} MB/s"
    log "  Consumer FRL:      ${CONS_FRL}s"
    log "  Consumer records:  ${CONS_RECORDS}"
    log "  E2E:               ${E2E_TIME}s"

    delete_topic_remote "$EC2_US_EAST_IP" "$TOPIC"
    sleep 5
done

log "═══════════════════════════════════════════════════════"
log " Approach 2 complete. Results: $RESULTS_CSV"
log "═══════════════════════════════════════════════════════"
