#!/bin/bash
# Approach 4: MirrorMaker2 Replication (Stream)
# HPC Spark → Frankfurt Kafka → MM2 → US-East Kafka
#
# Run from HPC coordinator node.
# Prerequisites:
#   - EC2 Frankfurt running Kafka + MM2 configured
#   - EC2 US-East running Kafka
#   - first_record_consumer.py deployed to EC2 US-East
#   - Spark cluster running (deploy_spark_5gb.sh)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../configs/experiment_5gb.conf"

APPROACH="approach4_mm2"
OUT_DIR="${RESULTS_BASE}/${APPROACH}"
mkdir -p "$OUT_DIR"
RESULTS_CSV="${OUT_DIR}/results_$(timestamp).csv"

echo "run_id,dataset_gb,hpc_read_s,hpc_filter_s,hpc_produce_s,hpc_total_s,hpc_produce_mbps,filtered_records,consumer_first_record_s,consumer_active_s,consumer_total_s,consumer_records,consumer_bytes_mb,consumer_throughput_mbps,e2e_s" > "$RESULTS_CSV"

COORDINATOR_NODE=$(cat $HOME/coordinatorNode 2>/dev/null || hostname)
SPARK_MASTER="spark://${COORDINATOR_NODE}:7078"

log "═══════════════════════════════════════════════════════"
log " Approach 4: MirrorMaker2 (HPC → Frankfurt → MM2 → US-East)"
log " Dataset:    $DATASET_5GB"
log " Frankfurt:  $KAFKA_FRANKFURT"
log " US-East:    $KAFKA_US_EAST"
log " Spark:      $SPARK_MASTER"
log "═══════════════════════════════════════════════════════"

for run in $(seq 1 $NUM_RUNS); do
    RUN_ID="${APPROACH}_run${run}"
    SRC_TOPIC="a4-src-5gb-run${run}"
    DST_TOPIC="source.${SRC_TOPIC}"   # MM2 prefixes with source cluster alias

    log "── Run $run/$NUM_RUNS ──"

    # 1. Create source topic on Frankfurt
    ensure_topic_remote "$EC2_FRANKFURT_IP" "$SRC_TOPIC" "$KAFKA_PARTITIONS" "$SSH_OPTS_FRA"

    # 2. Write MM2 config locally, scp to Frankfurt, then start MM2
    log "Configuring MM2 on Frankfurt..."
    MM2_CONF="/tmp/mm2_5gb_run${run}.properties"
    cat > "$MM2_CONF" << MMEOF
clusters = source, destination
source.bootstrap.servers = localhost:9092
destination.bootstrap.servers = ${EC2_US_EAST_IP}:9092
source->destination.enabled = true
destination->source.enabled = false
source->destination.topics = ${SRC_TOPIC}
tasks.max = ${MM2_TASKS}
replication.factor = 1
offset.storage.replication.factor = 1
config.storage.replication.factor = 1
status.storage.replication.factor = 1
offset.storage.topic = mm2-offsets.source.internal
config.storage.topic = mm2-configs.source.internal
status.storage.topic = mm2-status.source.internal
destination.producer.acks = ${MM2_ACKS}
destination.producer.batch.size = ${MM2_BATCH_SIZE}
destination.producer.linger.ms = ${MM2_LINGER_MS}
destination.producer.max.in.flight.requests.per.connection = ${MM2_INFLIGHT}
destination.producer.buffer.memory = 134217728
destination.producer.send.buffer.bytes = 1048576
destination.producer.max.request.size = 10485760
source.consumer.fetch.min.bytes = 1048576
source.consumer.max.partition.fetch.bytes = 10485760
emit.heartbeats.enabled = false
emit.checkpoints.enabled = false
sync.group.offsets.enabled = false
refresh.topics.interval.seconds = 10
MMEOF

    # Copy config to Frankfurt
    scp $SSH_OPTS_FRA "$MM2_CONF" ${EC2_USER}@${EC2_FRANKFURT_IP}:/tmp/mm2.properties

    # Stop any previous MM2, copy config into Docker container, start MM2
    ssh $SSH_OPTS_FRA ${EC2_USER}@${EC2_FRANKFURT_IP} \
        "sudo docker exec kafka pkill -f connect-mirror-maker 2>/dev/null; sleep 3; \
         sudo docker cp /tmp/mm2.properties kafka:/tmp/mm2.properties; \
         nohup sudo docker exec kafka /opt/kafka/bin/connect-mirror-maker.sh /tmp/mm2.properties > /tmp/mm2_run${run}.log 2>&1 & echo \$!"
    log "MM2 started, waiting 30s for initialization..."
    sleep 30

    # 3. Start consumer on US-East (topic = source.<src_topic>)
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

    # 4. E2E timer
    E2E_START=$(date +%s)

    # 5. HPC Spark filter + produce to Frankfurt
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
    log "  HPC wall time: ${HPC_WALL}s"

    SPARK_LOG="${OUT_DIR}/${RUN_ID}_spark.log"
    HPC_READ=$(grep -oP 'read_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    HPC_FILTER=$(grep -oP 'filter_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    HPC_PRODUCE=$(grep -oP 'kafka_produce_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    HPC_TOTAL=$(grep -oP 'total_time_s: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    HPC_TP=$(grep -oP 'kafka_throughput_mbps: \K[0-9.]+' "$SPARK_LOG" || echo "0")
    FILTERED=$(grep -oP 'filtered_records: \K[0-9]+' "$SPARK_LOG" || echo "0")

    # 6. Wait for MM2 to finish replicating + consumer to stop
    log "HPC done. Waiting for MM2 replication + consumer to finish..."
    for i in $(seq 1 360); do
        ALIVE=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "kill -0 $CONSUMER_PID 2>/dev/null && echo yes || echo no")
        [ "$ALIVE" = "no" ] && break
        [ $((i % 12)) -eq 0 ] && {
            CNT=$(count_topic_remote "$EC2_US_EAST_IP" "$DST_TOPIC" 2>/dev/null || echo "?")
            log "  ... MM2 replicated: ${CNT} records so far"
        }
        sleep 10
    done

    E2E_END=$(date +%s)
    E2E_TIME=$((E2E_END - E2E_START))

    # 7. Collect consumer results
    CONSUMER_LOG=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "cat ~/results/${RUN_ID}_consumer.log")
    echo "$CONSUMER_LOG" | tee "${OUT_DIR}/${RUN_ID}_consumer.log"

    CONS_FRL=$(echo "$CONSUMER_LOG" | grep -oP 'first_record_latency:\s+\K[0-9.-]+' || echo "-1")
    CONS_ACTIVE=$(echo "$CONSUMER_LOG" | grep -oP 'active_duration_s:\s+\K[0-9.]+' || echo "0")
    CONS_TOTAL=$(echo "$CONSUMER_LOG" | grep -oP 'consumer_total_s:\s+\K[0-9.]+' || echo "0")
    CONS_RECORDS=$(echo "$CONSUMER_LOG" | grep -oP 'records:\s+\K[0-9]+' || echo "0")
    CONS_MB=$(echo "$CONSUMER_LOG" | grep -oP 'bytes_mb:\s+\K[0-9.]+' || echo "0")
    CONS_TP=$(echo "$CONSUMER_LOG" | grep -oP 'throughput_mbps:\s+\K[0-9.]+' || echo "0")

    echo "${RUN_ID},5,${HPC_READ},${HPC_FILTER},${HPC_PRODUCE},${HPC_TOTAL},${HPC_TP},${FILTERED},${CONS_FRL},${CONS_ACTIVE},${CONS_TOTAL},${CONS_RECORDS},${CONS_MB},${CONS_TP},${E2E_TIME}" >> "$RESULTS_CSV"

    log "=== RUN $run SUMMARY ==="
    log "  HPC Spark:         ${HPC_TOTAL}s (read=${HPC_READ} filter=${HPC_FILTER} produce=${HPC_PRODUCE})"
    log "  HPC wall:          ${HPC_WALL}s"
    log "  Consumer FRL:      ${CONS_FRL}s"
    log "  Consumer active:   ${CONS_ACTIVE}s (throughput: ${CONS_TP} MB/s)"
    log "  Consumer records:  ${CONS_RECORDS}"
    log "  E2E:               ${E2E_TIME}s"

    # Cleanup: stop MM2, delete topics
    ssh $SSH_OPTS_FRA ${EC2_USER}@${EC2_FRANKFURT_IP} "pkill -f 'connect-mirror-maker' 2>/dev/null || true"
    delete_topic_remote "$EC2_FRANKFURT_IP" "$SRC_TOPIC" "$SSH_OPTS_FRA"
    delete_topic_remote "$EC2_US_EAST_IP" "$DST_TOPIC"
    sleep 10
done

log "═══════════════════════════════════════════════════════"
log " Approach 4 complete. Results: $RESULTS_CSV"
log "═══════════════════════════════════════════════════════"
