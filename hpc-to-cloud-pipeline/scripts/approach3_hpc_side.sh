#!/bin/bash
# Approach 3: HPC-Side Processing (Batch)
# HPC Spark filter → write to disk → scp filtered → EC2 Kafka ingest
#
# Run from HPC coordinator node.
# Prerequisites:
#   - EC2 US-East running Kafka, first_record_consumer.py deployed
#   - Spark cluster running (deploy_spark_5gb.sh)
#   - Dataset at ~/pipeline_data/eea_airquality_5gb.csv
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../configs/experiment_5gb.conf"

APPROACH="approach3"
OUT_DIR="${RESULTS_BASE}/${APPROACH}"
mkdir -p "$OUT_DIR"
RESULTS_CSV="${OUT_DIR}/results_$(timestamp).csv"

echo "run_id,dataset_gb,spark_read_s,spark_filter_s,spark_write_s,spark_total_s,filtered_records,filtered_mb,scp_time_s,ingest_time_s,ingest_mbps,consumer_first_record_s,consumer_active_s,consumer_total_s,consumer_records,consumer_bytes_mb,e2e_s" > "$RESULTS_CSV"

COORDINATOR_NODE=$(cat $HOME/coordinatorNode 2>/dev/null || hostname)
SPARK_MASTER="spark://${COORDINATOR_NODE}:7078"

log "═══════════════════════════════════════════════════════"
log " Approach 3: HPC-Side Processing (filter → scp → ingest)"
log " Dataset: $DATASET_5GB"
log " Target:  EC2 US-East ($EC2_US_EAST_IP)"
log " Spark:   $SPARK_MASTER"
log "═══════════════════════════════════════════════════════"

for run in $(seq 1 $NUM_RUNS); do
    RUN_ID="${APPROACH}_run${run}"
    TOPIC="a3-5gb-run${run}"
    FILTERED_DIR="$HOME/pipeline_data/filtered_5gb_run${run}"
    rm -rf "$FILTERED_DIR"

    log "── Run $run/$NUM_RUNS ──"

    # 1. Create topic on US-East
    ensure_topic_remote "$EC2_US_EAST_IP" "$TOPIC" "$KAFKA_PARTITIONS"

    # 2. Start consumer on EC2 BEFORE pipeline starts
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

    # 4. Phase 1: Spark filter + write to disk
    log "Phase 1: Spark filter + write to disk..."
    singularity exec \
        --bind $HOME/pipeline_data:/opt/dataset \
        --bind "${OUT_DIR}":/opt/results \
        --bind $HOME/experiments:/opt/experiments \
        $SIF \
        spark-submit \
            --master "$SPARK_MASTER" \
            --conf spark.hadoop.fs.defaultFS=file:/// \
            /opt/experiments/scripts/spark_filter_save.py \
                --input "/opt/dataset/eea_airquality_5gb.csv" \
                --output "/opt/dataset/filtered_5gb_run${run}" \
                --filter-countries "$FILTER_COUNTRIES" \
                --run-id "$RUN_ID" \
        2>&1 | tee "${OUT_DIR}/${RUN_ID}_spark.log"

    SPARK_LOG="${OUT_DIR}/${RUN_ID}_spark.log"
    SPARK_READ=$(grep -oP 'read_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    SPARK_FILTER=$(grep -oP 'filter_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    SPARK_WRITE=$(grep -oP 'write_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    SPARK_TOTAL=$(grep -oP 'total_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    FILTERED=$(grep -oP 'filtered_records: \K[0-9]+' "$SPARK_LOG" || echo "0")
    FILTERED_MB=$(grep -oP 'output_size_mb: \K[0-9.]+' "$SPARK_LOG" || echo "0")

    FILTERED_DIR_SIZE=$(du -sh "$FILTERED_DIR" 2>/dev/null | awk '{print $1}' || echo "?")
    log "  Filtered dir size: $FILTERED_DIR_SIZE"

    # 5. Phase 2: scp filtered data to EC2
    log "Phase 2: scp filtered data..."
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "rm -rf ~/pipeline_data/filtered && mkdir -p ~/pipeline_data/filtered"
    SCP_START=$(date +%s)
    scp $SSH_OPTS -r "$FILTERED_DIR"/* ${EC2_USER}@${EC2_US_EAST_IP}:~/pipeline_data/filtered/
    SCP_END=$(date +%s)
    SCP_TIME=$((SCP_END - SCP_START))
    log "  scp filtered: ${SCP_TIME}s"

    # 6. Phase 3: Kafka ingest on EC2
    log "Phase 3: Kafka ingest..."
    INGEST_OUTPUT=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "
        bash ~/scripts/kafka_dir_ingest.sh ~/pipeline_data/filtered $TOPIC 65536 10
    ")
    echo "$INGEST_OUTPUT" | tee "${OUT_DIR}/${RUN_ID}_ingest.log"

    INGEST_TIME=$(echo "$INGEST_OUTPUT" | grep -oP 'duration_s: \K[0-9.]+' || echo "0")
    INGEST_MBPS=$(echo "$INGEST_OUTPUT" | grep -oP 'throughput_mbps: \K[0-9.]+' || echo "0")

    # 7. Wait for consumer
    log "Waiting for consumer..."
    for i in $(seq 1 60); do
        ALIVE=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "kill -0 $CONSUMER_PID 2>/dev/null && echo yes || echo no")
        [ "$ALIVE" = "no" ] && break
        sleep 5
    done

    E2E_END=$(date +%s)
    E2E_TIME=$((E2E_END - E2E_START))

    # 8. Collect consumer results
    CONSUMER_LOG=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "cat ~/results/${RUN_ID}_consumer.log")
    echo "$CONSUMER_LOG" | tee "${OUT_DIR}/${RUN_ID}_consumer.log"

    CONS_FRL=$(echo "$CONSUMER_LOG" | grep -oP 'first_record_latency:\s+\K[0-9.-]+' || echo "-1")
    CONS_ACTIVE=$(echo "$CONSUMER_LOG" | grep -oP 'active_duration_s:\s+\K[0-9.]+' || echo "0")
    CONS_TOTAL=$(echo "$CONSUMER_LOG" | grep -oP 'consumer_total_s:\s+\K[0-9.]+' || echo "0")
    CONS_RECORDS=$(echo "$CONSUMER_LOG" | grep -oP 'records:\s+\K[0-9]+' || echo "0")
    CONS_MB=$(echo "$CONSUMER_LOG" | grep -oP 'bytes_mb:\s+\K[0-9.]+' || echo "0")

    echo "${RUN_ID},5,${SPARK_READ},${SPARK_FILTER},${SPARK_WRITE},${SPARK_TOTAL},${FILTERED},${FILTERED_MB},${SCP_TIME},${INGEST_TIME},${INGEST_MBPS},${CONS_FRL},${CONS_ACTIVE},${CONS_TOTAL},${CONS_RECORDS},${CONS_MB},${E2E_TIME}" >> "$RESULTS_CSV"

    log "=== RUN $run SUMMARY ==="
    log "  Spark:             ${SPARK_TOTAL}s (read=${SPARK_READ} filter=${SPARK_FILTER} write=${SPARK_WRITE})"
    log "  scp filtered:      ${SCP_TIME}s"
    log "  Kafka ingest:      ${INGEST_TIME}s (${INGEST_MBPS} MB/s)"
    log "  Consumer FRL:      ${CONS_FRL}s"
    log "  Consumer records:  ${CONS_RECORDS}"
    log "  E2E:               ${E2E_TIME}s"
    log "  Pipeline delivery: ~$(($(echo "$SPARK_TOTAL" | cut -d. -f1) + SCP_TIME + $(echo "$INGEST_TIME" | cut -d. -f1)))s"

    # Cleanup
    rm -rf "$FILTERED_DIR"
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "rm -rf ~/pipeline_data/filtered" || true
    delete_topic_remote "$EC2_US_EAST_IP" "$TOPIC"
    sleep 5
done

log "═══════════════════════════════════════════════════════"
log " Approach 3 complete. Results: $RESULTS_CSV"
log "═══════════════════════════════════════════════════════"
