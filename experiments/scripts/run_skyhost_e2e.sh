#!/usr/bin/env bash
set -euo pipefail

# === Configuration ===
FRANKFURT_IP="3.122.226.202"
USEAST_IP="3.84.68.196"

FRANKFURT_KEY="$HOME/.ssh/pipeline-frankfurt-key.pem"
USEAST_KEY="$HOME/.ssh/pipeline-useast-key.pem"

SSH_F="-o StrictHostKeyChecking=no -i $FRANKFURT_KEY"
SSH_U="-o StrictHostKeyChecking=no -i $USEAST_KEY"

SRC_BROKER="${FRANKFURT_IP}:9092"
DST_BROKER="${USEAST_IP}:9092"

SRC_TOPIC="skyhost_source"
DST_TOPIC="skyhost_dest"

PARTITIONS=8
TARGET_RECORDS=1865767
RUNS=3
PROVISION_WAIT=150

RESULT_DIR="$HOME/Desktop/HPC data/experiments/results/approach4_skyhost"
mkdir -p "$RESULT_DIR"

# === Activate SkyHOST venv ===
cd "$HOME/mock skyplane/skyplane"
source skyplane-dev-venv/bin/activate
export SKYPLANE_DOCKER_IMAGE=ghcr.io/arslan866/skyplane-kafka:amd64-latest

skyplane deprovision 2>/dev/null || true

for run in $(seq 1 $RUNS); do
  echo "============================================"
  echo "=== SKYHOST RUN $run / $RUNS ==="
  echo "============================================"

  # Fresh topics
  echo "Creating topics..."
  ssh $SSH_F ubuntu@${FRANKFURT_IP} "
    sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic ${SRC_TOPIC} 2>/dev/null || true
    sleep 2
    sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --topic ${SRC_TOPIC} \
      --partitions ${PARTITIONS} --replication-factor 1
  "
  ssh $SSH_U ubuntu@${USEAST_IP} "
    sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic ${DST_TOPIC} 2>/dev/null || true
    sleep 2
    sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --topic ${DST_TOPIC} \
      --partitions ${PARTITIONS} --replication-factor 1
  "

  # Start SkyHOST stream (provisions gateways)
  echo "Starting SkyHOST stream (Frankfurt → US-East)..."
  skyplane stream "kafka://${SRC_BROKER}/${SRC_TOPIC}" "kafka://${DST_BROKER}/${DST_TOPIC}" \
    --src-region aws:eu-central-1 --dst-region aws:us-east-1 \
    --reader-processes 8 --writer-processes 8 \
    --batch-size-mb 32 --preserve-partitions \
    --dst-acks 1 --send-connections 8 --dst-max-in-flight 5 \
    -y > "${RESULT_DIR}/stream_run${run}.log" 2>&1 &
  STREAM_PID=$!

  echo "Waiting ${PROVISION_WAIT}s for gateways to provision..."
  sleep ${PROVISION_WAIT}

  # Verify stream is still running
  if ! kill -0 $STREAM_PID 2>/dev/null; then
    echo "ERROR: skyplane stream died. Check ${RESULT_DIR}/stream_run${run}.log"
    cat "${RESULT_DIR}/stream_run${run}.log" | tail -30
    continue
  fi
  echo "Stream is running (PID=$STREAM_PID)"

  # Start consumer on US-East (background)
  echo "Starting consumer on US-East..."
  ssh $SSH_U ubuntu@${USEAST_IP} "
    python3 ~/skyhost_consumer.py \
      --brokers 127.0.0.1:9092 \
      --topic ${DST_TOPIC} \
      --target-records ${TARGET_RECORDS} \
      --timeout-ms 60000 \
      --save ~/skyhost_results.csv
  " > "${RESULT_DIR}/consumer_run${run}.log" 2>&1 &
  CONSUMER_PID=$!

  sleep 5

  # Produce on Frankfurt
  echo "Producing ${TARGET_RECORDS} records to Frankfurt Kafka..."
  PRODUCE_START=$(date +%s)
  ssh $SSH_F ubuntu@${FRANKFURT_IP} "
    python3 ~/skyhost_producer.py \
      --brokers 127.0.0.1:9092 \
      --topic ${SRC_TOPIC} \
      --records ${TARGET_RECORDS} \
      --batch-size 16384 \
      --acks 1
  " | tee "${RESULT_DIR}/producer_run${run}.log"
  PRODUCE_END=$(date +%s)

  echo "Producer done in $((PRODUCE_END - PRODUCE_START))s. Waiting for consumer..."

  # Wait for consumer
  wait $CONSUMER_PID || true
  echo "Consumer done."
  cat "${RESULT_DIR}/consumer_run${run}.log"

  # Cleanup: stop stream and deprovision
  kill $STREAM_PID 2>/dev/null || true
  sleep 2
  skyplane deprovision 2>/dev/null || true

  # Cleanup topics
  ssh $SSH_F ubuntu@${FRANKFURT_IP} "
    sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic ${SRC_TOPIC} 2>/dev/null || true
  "
  ssh $SSH_U ubuntu@${USEAST_IP} "
    sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic ${DST_TOPIC} 2>/dev/null || true
  "

  echo "=== Run $run complete ==="
  echo ""
  sleep 5
done

echo "=== ALL SKYHOST RUNS COMPLETE ==="
echo "Results in: ${RESULT_DIR}/"
