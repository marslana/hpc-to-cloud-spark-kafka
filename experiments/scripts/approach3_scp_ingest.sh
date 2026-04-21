#!/bin/bash
# approach3_scp_ingest.sh
# Approach 3: scp raw data from HPC to US-East-1, then Kafka ingest (no filtering)
#
# This is the traditional HPC data movement baseline.
# Measures: T_total = T_scp + T_ingest
#
# Prerequisites:
#   - EC2 instance in us-east-1 with Kafka running
#   - SSH key configured for passwordless access
#   - kafka_file_ingest.py deployed to EC2 instance
#
# Usage:
#   bash approach3_scp_ingest.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../configs/experiment.conf"

APPROACH="approach3"
RESULTS_DIR="${RESULTS_BASE}/${APPROACH}"
mkdir -p "$RESULTS_DIR"
RESULTS_CSV="${RESULTS_DIR}/results_$(timestamp).csv"

SSH_OPTS="-o StrictHostKeyChecking=no -i ${EC2_KEY_US}"
REMOTE_DIR="/home/${EC2_USER}/pipeline_data"

# ── Validate ─────────────────────────────────────────────────
if [ -z "$EC2_US_EAST_IP" ]; then
    echo "ERROR: Set EC2_US_EAST_IP in configs/experiment.conf"
    exit 1
fi
if [ ! -f "$DATASET_1GB" ]; then
    echo "ERROR: Dataset not found: $DATASET_1GB"
    exit 1
fi

log "═══════════════════════════════════════════════"
log " Approach 3: scp + Kafka Ingest (HPC → US-East-1)"
log "═══════════════════════════════════════════════"

DATASET_SIZE=$(du -h "$DATASET_1GB" | cut -f1)
DATASET_BYTES=$(stat -f%z "$DATASET_1GB" 2>/dev/null || stat -c%s "$DATASET_1GB")
log "Dataset: $DATASET_1GB ($DATASET_SIZE)"
log "Target:  $EC2_US_EAST_IP"
log "Runs:    $NUM_RUNS"

# Ensure remote directory exists
ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "mkdir -p ${REMOTE_DIR}"

# Write CSV header
echo "run_id,approach,dataset_size_bytes,scp_time_s,scp_throughput_mbps,ingest_time_s,ingest_throughput_mbps,total_time_s,total_throughput_mbps" > "$RESULTS_CSV"

for run in $(seq 1 $NUM_RUNS); do
    RUN_ID="${APPROACH}_run${run}"
    TOPIC="pipeline-a3-run${run}-$(date +%s)"
    REMOTE_FILE="${REMOTE_DIR}/eea_raw_run${run}.csv"

    log "── Run $run/$NUM_RUNS ──"

    # ── Phase 1: scp transfer ────────────────────────────────
    log "Phase 1: scp transfer..."
    # Remove old file on remote if exists
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "rm -f ${REMOTE_FILE}" 2>/dev/null || true

    T_SCP_START=$(date +%s%N)
    scp $SSH_OPTS "$DATASET_1GB" "${EC2_USER}@${EC2_US_EAST_IP}:${REMOTE_FILE}"
    T_SCP_END=$(date +%s%N)

    SCP_NS=$((T_SCP_END - T_SCP_START))
    SCP_S=$(echo "scale=2; $SCP_NS / 1000000000" | bc)
    SCP_MBPS=$(echo "scale=2; $DATASET_BYTES / 1048576 / ($SCP_NS / 1000000000)" | bc)

    log "  scp complete: ${SCP_S}s (${SCP_MBPS} MB/s)"

    # ── Phase 2: Kafka ingest on remote ──────────────────────
    log "Phase 2: Kafka ingest on EC2..."

    # Create topic on remote Kafka
    ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "
        sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
            --delete --topic ${TOPIC} 2>/dev/null || true
        sleep 2
        sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
            --create --topic ${TOPIC} --partitions ${KAFKA_PARTITIONS} \
            --replication-factor 1
    "

    # Run ingest and capture timing
    INGEST_OUTPUT=$(ssh $SSH_OPTS ${EC2_USER}@${EC2_US_EAST_IP} "
        python3 /home/${EC2_USER}/kafka_file_ingest.py \
            --input ${REMOTE_FILE} \
            --bootstrap-servers localhost:9092 \
            --topic ${TOPIC} \
            --batch-size ${KAFKA_BATCH_SIZE} \
            --acks ${KAFKA_ACKS}
    ")

    INGEST_S=$(echo "$INGEST_OUTPUT" | grep -oP 'duration_s:\s*\K[0-9.]+' || echo "0")
    INGEST_MBPS=$(echo "$INGEST_OUTPUT" | grep -oP 'throughput_mbps:\s*\K[0-9.]+' || echo "0")
    log "  Ingest complete: ${INGEST_S}s (${INGEST_MBPS} MB/s)"

    # ── Total ────────────────────────────────────────────────
    TOTAL_S=$(echo "scale=2; $SCP_S + $INGEST_S" | bc)
    TOTAL_MBPS=$(echo "scale=2; $DATASET_BYTES / 1048576 / $TOTAL_S" | bc 2>/dev/null || echo "0")

    log "  Total: ${TOTAL_S}s (${TOTAL_MBPS} MB/s effective)"

    echo "${RUN_ID},${APPROACH},${DATASET_BYTES},${SCP_S},${SCP_MBPS},${INGEST_S},${INGEST_MBPS},${TOTAL_S},${TOTAL_MBPS}" >> "$RESULTS_CSV"

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
log " Approach 3 complete. Results: $RESULTS_CSV"
log "═══════════════════════════════════════════════"
