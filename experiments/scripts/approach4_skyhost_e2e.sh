#!/usr/bin/env bash
# Approach 4: Full E2E — HPC Spark filter → Frankfurt Kafka → SkyHOST → US-East Kafka
#
# Run INSIDE the Singularity instance on the HPC coordinator node:
#   singularity exec instance://shinst_master bash ~/experiments/scripts/approach4_skyhost_e2e.sh
#
# Or from outside, just run spark-submit directly (see bottom of script).

set -euo pipefail

FRANKFURT_IP="${FRANKFURT_IP:-35.159.122.89}"
SRC_TOPIC="${SRC_TOPIC:-a4-src}"
PARTITIONS=8
DATASET="${DATASET:-/opt/dataset/eea_airquality_1gb.csv}"
RESULT_DIR="${RESULT_DIR:-/opt/results/approach4_skyhost}"
RUNS="${RUNS:-1}"

mkdir -p "$RESULT_DIR" 2>/dev/null || true

SPARK_MASTER=$(cat $HOME/coordinatorNode)

for run in $(seq 1 $RUNS); do
  echo "============================================"
  echo "=== APPROACH 4 (SkyHOST) — RUN $run ==="
  echo "============================================"

  echo "Spark filter + produce to Frankfurt Kafka (individual records)..."
  echo "Target: ${FRANKFURT_IP}:9092 / ${SRC_TOPIC}"
  E2E_START=$(date +%s)

  spark-submit \
    --master spark://${SPARK_MASTER}:7078 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    /opt/scripts/spark_filter_produce.py \
      --input ${DATASET} \
      --bootstrap-servers ${FRANKFURT_IP}:9092 \
      --topic ${SRC_TOPIC} \
      --partitions ${PARTITIONS} \
      --batch-size 16384 \
      --acks 1 \
      --output-csv ${RESULT_DIR}/results.csv \
      --run-id approach4_run${run}

  HPC_END=$(date +%s)
  HPC_TIME=$((HPC_END - E2E_START))
  echo ""
  echo "=== HPC → Frankfurt done in ${HPC_TIME}s ==="
  echo "SkyHOST is now transferring to US-East..."
  echo "Check consumer output on local machine."
  echo ""
  sleep 5
done

echo "=== ALL RUNS DONE ==="
