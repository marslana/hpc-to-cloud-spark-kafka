#!/bin/bash
# approach1_direct_streaming.sh
# Approach 1: Spark on HPC filters data → produces directly to US-East-1 Kafka
#
# Prerequisites:
#   - HPC cluster deployed (deploy_cluster.sh running with Spark + Kafka)
#   - EC2 Kafka broker running in us-east-1
#   - Dataset generated at $DATASET_1GB
#   - Connectivity verified (HPC can reach EC2_US_EAST_IP:9092)
#
# Usage (from an HPC compute node within a SLURM job):
#   bash approach1_direct_streaming.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../configs/experiment.conf"

APPROACH="approach1"
RESULTS_DIR="${RESULTS_BASE}/${APPROACH}"
mkdir -p "$RESULTS_DIR"
RESULTS_CSV="${RESULTS_DIR}/results_$(timestamp).csv"

# ── Validate ─────────────────────────────────────────────────
if [ -z "$EC2_US_EAST_IP" ]; then
    echo "ERROR: Set EC2_US_EAST_IP in configs/experiment.conf"
    exit 1
fi

log "═══════════════════════════════════════════════"
log " Approach 1: Direct Kafka Streaming (HPC → US-East-1)"
log "═══════════════════════════════════════════════"
log "Dataset: $DATASET_1GB"
log "Target:  $KAFKA_US_EAST"
log "Runs:    $NUM_RUNS"

check_connectivity "$EC2_US_EAST_IP" 9092 || exit 1

# Detect Spark master
COORDINATOR_NODE=$(cat $HOME/coordinatorNode 2>/dev/null || hostname)
SPARK_MASTER="spark://${COORDINATOR_NODE}:7078"
log "Spark master: $SPARK_MASTER"

# ── Run experiments ──────────────────────────────────────────
for run in $(seq 1 $NUM_RUNS); do
    TOPIC="pipeline-a1-run${run}-$(date +%s)"
    RUN_ID="${APPROACH}_run${run}"

    log "── Run $run/$NUM_RUNS (topic: $TOPIC) ──"

    # Create topic on remote broker
    ensure_topic "$KAFKA_US_EAST" "$TOPIC" "$KAFKA_PARTITIONS"

    # Run Spark filter + produce
    singularity exec \
        --bind $HOME/data:/opt/shared_data \
        --bind "$DATASET_DIR":/opt/dataset \
        --bind "$RESULTS_DIR":/opt/results \
        --bind "${SCRIPT_DIR}":/opt/scripts \
        $SIF \
        spark-submit \
            --master "$SPARK_MASTER" \
            --conf spark.hadoop.fs.defaultFS=file:/// \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
            /opt/scripts/spark_filter_produce.py \
                --input "/opt/dataset/$(basename $DATASET_1GB)" \
                --bootstrap-servers "$KAFKA_US_EAST" \
                --topic "$TOPIC" \
                --filter-countries "$FILTER_COUNTRIES" \
                --partitions "$KAFKA_PARTITIONS" \
                --batch-size "$SPARK_BATCH_SIZE" \
                --acks "$KAFKA_ACKS" \
                --output-csv "/opt/results/$(basename $RESULTS_CSV)" \
                --run-id "$RUN_ID" \
        2>&1 | tee "${RESULTS_DIR}/${RUN_ID}.log"

    # Cleanup topic
    delete_topic "$KAFKA_US_EAST" "$TOPIC"

    log "Run $run complete."
    sleep 5
done

log "═══════════════════════════════════════════════"
log " Approach 1 complete. Results: $RESULTS_CSV"
log "═══════════════════════════════════════════════"
