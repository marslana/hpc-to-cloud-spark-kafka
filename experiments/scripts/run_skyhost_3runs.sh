#!/usr/bin/env bash
set -euo pipefail

FRANKFURT_IP="3.122.226.202"
USEAST_IP="3.84.68.196"
FRANKFURT_KEY="$HOME/.ssh/pipeline-frankfurt-key.pem"
USEAST_KEY="$HOME/.ssh/pipeline-useast-key.pem"
SSH_F="-o StrictHostKeyChecking=no -i $FRANKFURT_KEY"
SSH_U="-o StrictHostKeyChecking=no -i $USEAST_KEY"

SRC_TOPIC="skyhost_src"
DST_TOPIC="skyhost_dst"
PARTITIONS=8
TARGET=1865767
RUNS=3

RESULT_DIR="$HOME/Desktop/HPC data/experiments/results/approach4_skyhost"
mkdir -p "$RESULT_DIR"

cd "$HOME/mock skyplane/skyplane"
source skyplane-dev-venv/bin/activate
export SKYPLANE_DOCKER_IMAGE=ghcr.io/arslan866/skyplane-kafka:amd64-latest

skyplane deprovision 2>/dev/null || true

create_topics() {
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
}

for run in $(seq 1 $RUNS); do
  echo "============================================"
  echo "=== SKYHOST RUN $run / $RUNS ==="
  echo "============================================"

  create_topics

  echo "Starting SkyHOST stream..."
  skyplane stream "kafka://${FRANKFURT_IP}:9092/${SRC_TOPIC}" "kafka://${USEAST_IP}:9092/${DST_TOPIC}" \
    --src-region aws:eu-central-1 --dst-region aws:us-east-1 \
    --reader-processes 8 --writer-processes 8 \
    --batch-size-mb 32 --preserve-partitions \
    --dst-acks 1 --send-connections 8 --dst-max-in-flight 5 \
    -y > "${RESULT_DIR}/stream_run${run}.log" 2>&1 &
  STREAM_PID=$!

  echo "Waiting 150s for gateway provisioning..."
  sleep 150

  if ! kill -0 $STREAM_PID 2>/dev/null; then
    echo "ERROR: stream died. Log:"
    tail -20 "${RESULT_DIR}/stream_run${run}.log"
    continue
  fi
  echo "Stream running (PID=$STREAM_PID)"

  echo "Starting consumer..."
  ssh $SSH_U ubuntu@${USEAST_IP} "
    python3 ~/skyhost_consumer.py \
      --brokers 127.0.0.1:9092 \
      --topic ${DST_TOPIC} \
      --target-records ${TARGET} \
      --timeout-ms 120000 \
      --save ~/skyhost_results.csv
  " > "${RESULT_DIR}/consumer_run${run}.log" 2>&1 &
  CONSUMER_PID=$!

  sleep 5

  echo "Producing ${TARGET} records..."
  ssh $SSH_F ubuntu@${FRANKFURT_IP} "
    python3 ~/skyhost_producer.py \
      --brokers 127.0.0.1:9092 \
      --topic ${SRC_TOPIC} \
      --records ${TARGET} \
      --batch-size 16384 \
      --acks 1
  " | tee "${RESULT_DIR}/producer_run${run}.log"

  echo "Waiting for consumer..."
  wait $CONSUMER_PID || true
  echo ""
  echo "=== RUN $run CONSUMER RESULTS ==="
  cat "${RESULT_DIR}/consumer_run${run}.log"

  kill $STREAM_PID 2>/dev/null || true
  sleep 2
  skyplane deprovision 2>/dev/null || true

  echo "=== Run $run complete ==="
  echo ""
  sleep 10
done

echo "=== ALL 3 SKYHOST RUNS DONE ==="
