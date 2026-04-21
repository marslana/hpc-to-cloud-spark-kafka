#!/bin/bash
# Approach 5: SkyHOST Transfer (Stream)
# HPC Spark → Frankfurt Kafka → SkyHOST → US-East Kafka
#
# Run from HPC coordinator node.
# Prerequisites:
#   - EC2 Frankfurt running Kafka + SkyHOST source gateway
#   - EC2 US-East running Kafka + SkyHOST dest gateway
#   - first_record_consumer.py deployed to EC2 US-East
#   - SkyHOST transfer provisioned (skyplane CLI on local machine)
#   - Spark cluster running (deploy_spark_5gb.sh)
#
# SkyHOST provisioning (run from your laptop BEFORE this script):
#   source skyplane-dev-venv/bin/activate
#   export SKYPLANE_DOCKER_IMAGE=ghcr.io/arslan866/skyplane-kafka:amd64-latest
#   skyplane skyhost transfer \
#       --src-kafka-broker <FRANKFURT_IP>:9092 \
#       --src-topic <SRC_TOPIC> \
#       --dst-kafka-broker <US_EAST_IP>:9092 \
#       --dst-topic <DST_TOPIC> \
#       --src-region aws:eu-central-1 \
#       --dst-region aws:us-east-1 \
#       --num-readers 8 --num-writers 8 \
#       --chunk-size-mb 32
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../configs/experiment_5gb.conf"

APPROACH="approach5_skyhost"
OUT_DIR="${RESULTS_BASE}/${APPROACH}"
mkdir -p "$OUT_DIR"
RESULTS_CSV="${OUT_DIR}/results_$(timestamp).csv"

echo "run_id,dataset_gb,hpc_read_s,hpc_filter_s,hpc_produce_s,hpc_total_s,hpc_produce_mbps,filtered_records,consumer_first_record_s,consumer_active_s,consumer_total_s,consumer_records,consumer_bytes_mb,consumer_throughput_mbps,hpc_wall_s,e2e_s" > "$RESULTS_CSV"

COORDINATOR_NODE=$(cat $HOME/coordinatorNode 2>/dev/null || hostname)
SPARK_MASTER="spark://${COORDINATOR_NODE}:7078"

log "═══════════════════════════════════════════════════════"
log " Approach 5: SkyHOST (HPC → Frankfurt → SkyHOST → US-East)"
log " Dataset:    $DATASET_5GB"
log " Frankfurt:  $KAFKA_FRANKFURT"
log " US-East:    $KAFKA_US_EAST"
log " Spark:      $SPARK_MASTER"
log "═══════════════════════════════════════════════════════"
log ""
log "IMPORTANT: Before each run, ensure SkyHOST transfer is active"
log "  (provisioned from your laptop with skyplane CLI)."
log ""

for run in $(seq 1 $NUM_RUNS); do
    RUN_ID="${APPROACH}_run${run}"
    SRC_TOPIC="a5-src-5gb-run${run}"
    DST_TOPIC="a5-dst-5gb-run${run}"

    log "── Run $run/$NUM_RUNS ──"
    log "SRC topic: $SRC_TOPIC  |  DST topic: $DST_TOPIC"

    # 1. Create source topic on Frankfurt
    ensure_topic_remote "$EC2_FRANKFURT_IP" "$SRC_TOPIC" "$KAFKA_PARTITIONS" "$SSH_OPTS_FRA"

    # 2. Create destination topic on US-East
    ensure_topic_remote "$EC2_US_EAST_IP" "$DST_TOPIC" "$KAFKA_PARTITIONS"

    # 3. PAUSE — user must start SkyHOST transfer from laptop
    log ""
    log "╔══════════════════════════════════════════════════════╗"
    log "║  START SkyHOST transfer from your laptop NOW:       ║"
    log "║                                                      ║"
    log "║  skyplane skyhost transfer \\                         ║"
    log "║    --src-kafka-broker ${EC2_FRANKFURT_IP}:9092 \\     ║"
    log "║    --src-topic ${SRC_TOPIC} \\                        ║"
    log "║    --dst-kafka-broker ${EC2_US_EAST_IP}:9092 \\      ║"
    log "║    --dst-topic ${DST_TOPIC} \\                        ║"
    log "║    --src-region aws:eu-central-1 \\                   ║"
    log "║    --dst-region aws:us-east-1 \\                      ║"
    log "║    --num-readers 8 --num-writers 8 \\                 ║"
    log "║    --chunk-size-mb 32                                ║"
    log "║                                                      ║"
    log "║  Press ENTER here when SkyHOST is running...         ║"
    log "╚══════════════════════════════════════════════════════╝"
    read -r

    # 4. Start consumer on US-East
    log "Starting consumer on US-East..."
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "
        nohup python3 ~/scripts/first_record_consumer.py \
            --brokers localhost:9092 \
            --topic $DST_TOPIC \
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

    # 5. E2E timer
    E2E_START=$(date +%s)

    # 6. HPC Spark filter + produce to Frankfurt Kafka
    log "Phase 1: HPC Spark filter + produce to Frankfurt..."
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
                --bootstrap-servers "$KAFKA_FRANKFURT" \
                --topic "$SRC_TOPIC" \
                --filter-countries "$FILTER_COUNTRIES" \
                --partitions "$KAFKA_PARTITIONS" \
                --batch-size "$KAFKA_BATCH_SIZE" \
                --acks "$KAFKA_ACKS" \
                --output-csv "/opt/results/results.csv" \
                --run-id "$RUN_ID" \
        2>&1 | tee "${OUT_DIR}/${RUN_ID}_spark.log"

    HPC_WALL_END=$(date +%s)
    HPC_WALL=$((HPC_WALL_END - E2E_START))
    log "  HPC → Frankfurt done in ${HPC_WALL}s"
    log "  SkyHOST is now transferring to US-East..."

    SPARK_LOG="${OUT_DIR}/${RUN_ID}_spark.log"
    HPC_READ=$(grep -oP 'read_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    HPC_FILTER=$(grep -oP 'filter_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    HPC_PRODUCE=$(grep -oP 'kafka_produce_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    HPC_TOTAL=$(grep -oP 'total_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    HPC_TP=$(grep -oP 'kafka_throughput_mbps: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    FILTERED=$(grep -oP 'filtered_records: \K[0-9]+' "$SPARK_LOG" || echo "0")

    # 7. Wait for consumer (SkyHOST batching + transfer + dest ingest)
    log "Waiting for consumer to finish (SkyHOST transfer in progress)..."
    for i in $(seq 1 180); do
        ALIVE=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "kill -0 $CONSUMER_PID 2>/dev/null && echo yes || echo no")
        [ "$ALIVE" = "no" ] && break
        [ $((i % 6)) -eq 0 ] && {
            CNT=$(count_topic_remote "$EC2_US_EAST_IP" "$DST_TOPIC" 2>/dev/null || echo "?")
            log "  ... consumer received: ${CNT} records"
        }
        sleep 10
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
    CONS_TP=$(echo "$CONSUMER_LOG" | grep -oP 'throughput_mbps:\s+\K[0-9.]+' || echo "0")

    echo "${RUN_ID},5,${HPC_READ},${HPC_FILTER},${HPC_PRODUCE},${HPC_TOTAL},${HPC_TP},${FILTERED},${CONS_FRL},${CONS_ACTIVE},${CONS_TOTAL},${CONS_RECORDS},${CONS_MB},${CONS_TP},${HPC_WALL},${E2E_TIME}" >> "$RESULTS_CSV"

    log "=== RUN $run SUMMARY ==="
    log "  HPC Spark:         ${HPC_TOTAL}s (read=${HPC_READ} filter=${HPC_FILTER} produce=${HPC_PRODUCE})"
    log "  HPC wall:          ${HPC_WALL}s"
    log "  Consumer FRL:      ${CONS_FRL}s  ← first-record latency"
    log "  Consumer active:   ${CONS_ACTIVE}s (throughput: ${CONS_TP} MB/s)"
    log "  Consumer records:  ${CONS_RECORDS}"
    log "  E2E:               ${E2E_TIME}s"

    # Cleanup
    delete_topic_remote "$EC2_FRANKFURT_IP" "$SRC_TOPIC" "$SSH_OPTS_FRA"
    delete_topic_remote "$EC2_US_EAST_IP" "$DST_TOPIC"
    sleep 10
done

log "═══════════════════════════════════════════════════════"
log " Approach 5 complete. Results: $RESULTS_CSV"
log "═══════════════════════════════════════════════════════"
