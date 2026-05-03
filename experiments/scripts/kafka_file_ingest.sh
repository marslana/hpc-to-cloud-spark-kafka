#!/bin/bash
# kafka_file_ingest.sh — Ingest a CSV file into Kafka using kafka-console-producer
# Deploy this to the EC2 instance. Much more reliable than kafka-python.
#
# Usage:
#   bash kafka_file_ingest.sh <input_file> <topic> [batch_size] [linger_ms]

set -euo pipefail

INPUT="$1"
TOPIC="$2"
BATCH_SIZE="${3:-65536}"
LINGER_MS="${4:-10}"

if [ ! -f "$INPUT" ]; then
    echo "ERROR: File not found: $INPUT"
    exit 1
fi

FILE_SIZE=$(stat -c%s "$INPUT")
FILE_SIZE_MB=$(echo "scale=2; $FILE_SIZE / 1048576" | bc)
RECORD_COUNT=$(wc -l < "$INPUT")
RECORD_COUNT=$((RECORD_COUNT - 1))  # subtract header

echo "Ingesting $RECORD_COUNT records ($FILE_SIZE_MB MB) from $INPUT → $TOPIC"

T_START=$(date +%s%N)

# Skip header line, pipe rest to kafka-console-producer
tail -n +2 "$INPUT" | sudo docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" \
    --producer-property batch.size="$BATCH_SIZE" \
    --producer-property linger.ms="$LINGER_MS" \
    --producer-property buffer.memory=67108864 \
    --producer-property acks=1

T_END=$(date +%s%N)

DURATION_NS=$((T_END - T_START))
DURATION_S=$(echo "scale=2; $DURATION_NS / 1000000000" | bc)
THROUGHPUT_MBPS=$(echo "scale=2; $FILE_SIZE_MB / ($DURATION_NS / 1000000000)" | bc)
RECORDS_PER_SEC=$(echo "scale=0; $RECORD_COUNT / ($DURATION_NS / 1000000000)" | bc)

echo "=== INGEST RESULTS ==="
echo "duration_s: $DURATION_S"
echo "throughput_mbps: $THROUGHPUT_MBPS"
echo "records_per_sec: $RECORDS_PER_SEC"
echo "records: $RECORD_COUNT"
echo "file_size_mb: $FILE_SIZE_MB"
