#!/bin/bash
# Approach 1: Cloud-Side Processing (Batch)
# scp raw 5GB → EC2 US-East → Spark filter + Kafka produce on EC2
#
# Run from HPC coordinator node (ssh into coordinator after SLURM job starts).
# Prerequisites:
#   - EC2 US-East running Kafka + Spark (setup_ec2_kafka.sh done)
#   - first_record_consumer.py deployed to EC2: ~/scripts/first_record_consumer.py
#   - Dataset at ~/pipeline_data/eea_airquality_5gb.csv
#   - spark_filter_produce.py deployed to EC2: ~/scripts/spark_filter_produce.py
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../configs/experiment_5gb.conf"

APPROACH="approach1"
OUT_DIR="${RESULTS_BASE}/${APPROACH}"
mkdir -p "$OUT_DIR"
RESULTS_CSV="${OUT_DIR}/results_$(timestamp).csv"

echo "run_id,dataset_gb,scp_time_s,spark_read_s,spark_filter_s,kafka_produce_s,spark_total_s,filtered_records,consumer_first_record_s,consumer_total_s,consumer_records,consumer_bytes_mb,e2e_s" > "$RESULTS_CSV"

log "═══════════════════════════════════════════════════════"
log " Approach 1: Cloud-Side Processing (scp raw → EC2 Spark)"
log " Dataset: $DATASET_5GB"
log " Target:  EC2 US-East ($EC2_US_EAST_IP)"
log "═══════════════════════════════════════════════════════"

for run in $(seq 1 $NUM_RUNS); do
    RUN_ID="${APPROACH}_run${run}"
    TOPIC="a1-5gb-run${run}"

    log "── Run $run/$NUM_RUNS ──"

    # 1. Create topic on US-East
    ensure_topic_remote "$EC2_US_EAST_IP" "$TOPIC" "$KAFKA_PARTITIONS"

    # 2. Start consumer on EC2 (background, will wait for records)
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
    log "Consumer started (PID: $CONSUMER_PID)"
    sleep 2

    # 3. E2E timer start
    E2E_START=$(date +%s)

    # 4. Phase 1: scp raw 5GB to EC2
    log "Phase 1: scp 5GB raw file..."
    SCP_START=$(date +%s)
    scp $SSH_OPTS "$DATASET_5GB" ${EC2_USER}@${EC2_US_EAST_IP}:~/pipeline_data/eea_raw_5gb.csv
    SCP_END=$(date +%s)
    SCP_TIME=$((SCP_END - SCP_START))
    log "  scp: ${SCP_TIME}s"

    # 5. Phase 2: Spark filter + Kafka produce on EC2
    log "Phase 2: Spark filter + Kafka produce on EC2..."
    SPARK_OUTPUT=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "
        export SPARK_HOME=/opt/spark
        export PATH=\$PATH:/opt/spark/bin
        spark-submit \
            --master 'local[8]' \
            --driver-memory 2g \
            --conf spark.executor.memory=6g \
            --conf spark.hadoop.fs.defaultFS=file:/// \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
            ~/scripts/spark_filter_produce.py \
                --input ~/pipeline_data/eea_raw_5gb.csv \
                --bootstrap-servers localhost:9092 \
                --topic $TOPIC \
                --filter-countries $FILTER_COUNTRIES \
                --partitions $KAFKA_PARTITIONS \
                --batch-size $KAFKA_BATCH_SIZE \
                --acks $KAFKA_ACKS \
                --run-id $RUN_ID \
            2>&1
    ")
    echo "$SPARK_OUTPUT" | tee "${OUT_DIR}/${RUN_ID}_spark.log"

    # Parse Spark timings
    SPARK_READ=$(echo "$SPARK_OUTPUT" | grep -oP 'read_time_s: \K[0-9.]+' || echo "0")
    SPARK_FILTER=$(echo "$SPARK_OUTPUT" | grep -oP 'filter_time_s: \K[0-9.]+' || echo "0")
    KAFKA_PRODUCE=$(echo "$SPARK_OUTPUT" | grep -oP 'kafka_produce_time_s: \K[0-9.]+' || echo "0")
    SPARK_TOTAL=$(echo "$SPARK_OUTPUT" | grep -oP 'total_time_s: \K[0-9.]+' || echo "0")
    FILTERED_RECORDS=$(echo "$SPARK_OUTPUT" | grep -oP 'filtered_records: \K[0-9]+' || echo "0")

    # 6. Wait for consumer to finish
    log "Waiting for consumer to finish (idle timeout ${CONSUMER_IDLE_TIMEOUT_MS}ms)..."
    sleep 5
    for i in $(seq 1 120); do
        ALIVE=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "kill -0 $CONSUMER_PID 2>/dev/null && echo yes || echo no")
        [ "$ALIVE" = "no" ] && break
        sleep 5
    done

    E2E_END=$(date +%s)
    E2E_TIME=$((E2E_END - E2E_START))

    # 7. Collect consumer results
    CONSUMER_LOG=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "cat ~/results/${RUN_ID}_consumer.log")
    echo "$CONSUMER_LOG" | tee "${OUT_DIR}/${RUN_ID}_consumer.log"

    CONS_FRL=$(echo "$CONSUMER_LOG" | grep -oP 'first_record_latency:\s+\K[0-9.-]+' || echo "-1")
    CONS_TOTAL=$(echo "$CONSUMER_LOG" | grep -oP 'consumer_total_s:\s+\K[0-9.]+' || echo "0")
    CONS_RECORDS=$(echo "$CONSUMER_LOG" | grep -oP 'records:\s+\K[0-9]+' || echo "0")
    CONS_MB=$(echo "$CONSUMER_LOG" | grep -oP 'bytes_mb:\s+\K[0-9.]+' || echo "0")

    # 8. Write results
    echo "${RUN_ID},5,${SCP_TIME},${SPARK_READ},${SPARK_FILTER},${KAFKA_PRODUCE},${SPARK_TOTAL},${FILTERED_RECORDS},${CONS_FRL},${CONS_TOTAL},${CONS_RECORDS},${CONS_MB},${E2E_TIME}" >> "$RESULTS_CSV"

    log "=== RUN $run SUMMARY ==="
    log "  scp raw:           ${SCP_TIME}s"
    log "  Spark total:       ${SPARK_TOTAL}s (read=${SPARK_READ} filter=${SPARK_FILTER} produce=${KAFKA_PRODUCE})"
    log "  Consumer FRL:      ${CONS_FRL}s"
    log "  Consumer records:  ${CONS_RECORDS}"
    log "  E2E:               ${E2E_TIME}s"

    # Cleanup
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "rm -f ~/pipeline_data/eea_raw_5gb.csv" || true
    delete_topic_remote "$EC2_US_EAST_IP" "$TOPIC"
    sleep 5
done

log "═══════════════════════════════════════════════════════"
log " Approach 1 complete. Results: $RESULTS_CSV"
log "═══════════════════════════════════════════════════════"
