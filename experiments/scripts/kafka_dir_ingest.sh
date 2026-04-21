#!/bin/bash
# kafka_dir_ingest.sh — Ingest a directory of CSV part-files into Kafka
# Handles Spark's output format (multiple part-*.csv files)
#
# Usage:
#   bash kafka_dir_ingest.sh <input_dir> <topic> [batch_size] [linger_ms]

set -euo pipefail

INPUT_DIR="$1"
TOPIC="$2"
BATCH_SIZE="${3:-65536}"
LINGER_MS="${4:-10}"

TOTAL_SIZE=0
TOTAL_RECORDS=0

for f in "$INPUT_DIR"/part-*.csv; do
    [ -f "$f" ] || continue
    TOTAL_SIZE=$((TOTAL_SIZE + $(stat -c%s "$f")))
    TOTAL_RECORDS=$((TOTAL_RECORDS + $(wc -l < "$f") - 1))  # subtract header per file
done

TOTAL_SIZE_MB=$(echo "scale=2; $TOTAL_SIZE / 1048576" | bc)
echo "Ingesting $TOTAL_RECORDS records ($TOTAL_SIZE_MB MB) from $INPUT_DIR → $TOPIC"

T_START=$(date +%s%N)

for f in "$INPUT_DIR"/part-*.csv; do
    [ -f "$f" ] || continue
    tail -n +2 "$f" | sudo docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$TOPIC" \
        --producer-property batch.size="$BATCH_SIZE" \
        --producer-property linger.ms="$LINGER_MS" \
        --producer-property buffer.memory=67108864 \
        --producer-property acks=1
done

T_END=$(date +%s%N)

DURATION_NS=$((T_END - T_START))
DURATION_S=$(echo "scale=2; $DURATION_NS / 1000000000" | bc)
THROUGHPUT_MBPS=$(echo "scale=2; $TOTAL_SIZE_MB / ($DURATION_NS / 1000000000)" | bc)

echo "=== INGEST RESULTS ==="
echo "duration_s: $DURATION_S"
echo "throughput_mbps: $THROUGHPUT_MBPS"
echo "records: $TOTAL_RECORDS"
echo "file_size_mb: $TOTAL_SIZE_MB"
