#!/usr/bin/env bash
# Approach 4 Clean E2E: 3 runs with full measurement
# Run from LOCAL machine. Requires: SkyHOST venv, HPC SLURM job running, EC2 Kafka ready.
set -euo pipefail

FRA="${FRA_IP:?Set FRA_IP}"
USE="${USE_IP:?Set USE_IP}"
FK="$HOME/.ssh/pipeline-frankfurt-key.pem"
UK="$HOME/.ssh/pipeline-useast-key.pem"
SF="-o StrictHostKeyChecking=no -i $FK"
SU="-o StrictHostKeyChecking=no -i $UK"
HPC_SSH="-o StrictHostKeyChecking=no -p 8022"
HPC_HOST="mtariq@access-aion.uni.lu"

RUNS=3
PARTITIONS=8
TARGET_RECORDS=1865767

RESULT_DIR="$HOME/Desktop/HPC data/experiments/results/approach4_skyhost_clean"
mkdir -p "$RESULT_DIR"

cd "$HOME/mock skyplane/skyplane"
source skyplane-dev-venv/bin/activate
export SKYPLANE_DOCKER_IMAGE=ghcr.io/arslan866/skyplane-kafka:amd64-latest

# Get HPC coordinator node
HPC_COORD=$(ssh $HPC_SSH $HPC_HOST "cat ~/coordinatorNode 2>/dev/null" || echo "")
if [ -z "$HPC_COORD" ]; then
  echo "ERROR: No HPC coordinator node found. Is the SLURM job running?"
  exit 1
fi
echo "HPC coordinator: $HPC_COORD"

skyplane deprovision 2>/dev/null || true

for run in $(seq 1 $RUNS); do
  SRC="a4clean-src-r${run}"
  DST="a4clean-dst-r${run}"
  
  echo ""
  echo "================================================================"
  echo "  APPROACH 4 CLEAN RUN $run / $RUNS"
  echo "  Source topic: $SRC (Frankfurt $FRA)"
  echo "  Dest topic:   $DST (US-East $USE)"
  echo "================================================================"

  # ── Step 1: Create fresh topics ──
  echo "[Step 1] Creating topics..."
  ssh $SF ubuntu@$FRA "
    sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --topic $SRC \
      --partitions $PARTITIONS --replication-factor 1 2>&1
  "
  ssh $SU ubuntu@$USE "
    sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --topic $DST \
      --partitions $PARTITIONS --replication-factor 1 2>&1
  "

  # ── Step 2: Start SkyHOST stream ──
  echo "[Step 2] Starting SkyHOST stream..."
  skyplane stream "kafka://${FRA}:9092/${SRC}" "kafka://${USE}:9092/${DST}" \
    --src-region aws:eu-central-1 --dst-region aws:us-east-1 \
    --reader-processes 8 --writer-processes 8 \
    --batch-size-mb 32 --preserve-partitions \
    --dst-acks 1 --send-connections 8 --dst-max-in-flight 5 \
    -y > "${RESULT_DIR}/stream_r${run}.log" 2>&1 &
  STREAM_PID=$!
  echo "  SkyHOST PID=$STREAM_PID, waiting 150s for provisioning..."
  sleep 150

  if ! kill -0 $STREAM_PID 2>/dev/null; then
    echo "  ERROR: SkyHOST died. Log:"
    tail -20 "${RESULT_DIR}/stream_r${run}.log"
    continue
  fi
  echo "  SkyHOST stream READY"

  # ── Step 3: Start consumer on US-East (foreground via SSH, captures all output) ──
  echo "[Step 3] Starting consumer on US-East..."
  ssh $SU ubuntu@$USE "
    python3 ~/skyhost_consumer.py \
      --brokers 127.0.0.1:9092 \
      --topic $DST \
      --target-records $TARGET_RECORDS \
      --timeout-ms 120000 \
      --save ~/clean_results_r${run}.csv
  " > "${RESULT_DIR}/consumer_r${run}.log" 2>&1 &
  CONSUMER_PID=$!
  sleep 5

  # ── Step 4: Run HPC produce via SSH ──
  echo "[Step 4] Running HPC Spark filter + produce to Frankfurt..."
  E2E_START=$(date +%s)

  ssh $HPC_SSH $HPC_HOST "
    ssh $HPC_COORD 'module load env/development/2024a && module load tools/Apptainer/1.4.1 && \
    singularity exec instance://shinst_master \
      spark-submit \
        --master spark://\$(cat ~/coordinatorNode):7078 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
        /opt/scripts/spark_filter_produce.py \
          --input /opt/dataset/eea_airquality_1gb.csv \
          --bootstrap-servers ${FRA}:9092 \
          --topic ${SRC} \
          --partitions ${PARTITIONS} \
          --batch-size 16384 \
          --acks 1 \
          --output-csv /opt/results/approach4_skyhost/clean_r${run}.csv \
          --run-id clean_run${run}'
  " 2>&1 | tee "${RESULT_DIR}/hpc_r${run}.log"

  HPC_END=$(date +%s)
  echo "  HPC phase wall clock: $((HPC_END - E2E_START))s"

  # ── Step 5: Wait for consumer to finish ──
  echo "[Step 5] Waiting for consumer to receive all records..."
  wait $CONSUMER_PID || true
  CONSUMER_END=$(date +%s)
  FULL_E2E=$((CONSUMER_END - E2E_START))

  echo ""
  echo "=== RUN $run RESULTS ==="
  echo "HPC wall: $((HPC_END - E2E_START))s"
  echo "Full E2E (HPC start → consumer done): ${FULL_E2E}s"
  echo ""
  echo "--- HPC Output ---"
  grep -E "read_time|filter_time|kafka_produce|total_time|throughput" "${RESULT_DIR}/hpc_r${run}.log" || true
  echo ""
  echo "--- Consumer Output ---"
  cat "${RESULT_DIR}/consumer_r${run}.log"
  echo ""

  # Save E2E timing
  echo "run${run},hpc_wall=$((HPC_END - E2E_START)),full_e2e=${FULL_E2E}" >> "${RESULT_DIR}/e2e_timing.csv"

  # ── Step 6: Verify record count ──
  echo "[Step 6] Verifying record count on US-East..."
  ssh $SU ubuntu@$USE "
    sudo docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic $DST 2>/dev/null
  " | tee "${RESULT_DIR}/offsets_r${run}.txt"

  # ── Cleanup: stop stream and deprovision ──
  kill $STREAM_PID 2>/dev/null || true
  sleep 2
  skyplane deprovision 2>/dev/null || true

  echo "=== Run $run complete ==="
  sleep 10
done

echo ""
echo "================================================================"
echo "  ALL 3 RUNS COMPLETE"
echo "  Results in: $RESULT_DIR/"
echo "================================================================"
cat "${RESULT_DIR}/e2e_timing.csv"
