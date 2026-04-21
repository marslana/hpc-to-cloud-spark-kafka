#!/bin/bash
# rf_rerun_10kb.sh — Re-run ONLY the RF table values (10KB, RF=1 and RF=3)
#                     with 3 runs each for statistical confidence.
#
# Prerequisites:
#   1. Deploy 3-broker cluster:
#      NUM_BROKERS=3 CONTAINER=hsk.sif sbatch --nodes=4 deploy_kafka_N_brokers.sh
#   2. Wait for CLUSTER READY message in ~/kafka_bench/logs/main.log
#   3. SSH to coordinator:  ssh $(cat ~/coordinatorNode)
#   4. Run:  bash ~/paper/artifacts/scripts/experiments/rf_rerun_10kb.sh
#
set -euo pipefail

WORK_DIR="${HOME}/kafka_bench"
source ${WORK_DIR}/cluster_env.sh
module load env/development/2024a 2>/dev/null || true
module load tools/Apptainer/1.4.1 2>/dev/null || module load tools/Apptainer 2>/dev/null || true

SIF="${HOME}/${CONTAINER}"
BOOTSTRAP_SERVERS=$(cat ${WORK_DIR}/bootstrap_servers)

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="${WORK_DIR}/results/rf_rerun_10kb_${TIMESTAMP}.csv"
LOG_DIR="${WORK_DIR}/results/rf_rerun_logs_${TIMESTAMP}"
mkdir -p "$LOG_DIR"

echo "=== RF Rerun: 10KB messages, RF=1 & RF=3, 3 runs each ==="
echo "Bootstrap: ${BOOTSTRAP_SERVERS}"
echo "Results:   ${RESULTS_FILE}"
echo ""

echo "rf,partitions,acks,run,records_per_sec,mb_per_sec,avg_latency_ms,max_latency_ms" > "$RESULTS_FILE"

NUM_MESSAGES=100000
MSG_SIZE=10240

topic_create() {
    local topic=$1 partitions=$2 rf=$3
    singularity exec $SIF \
        /opt/kafka/bin/kafka-topics.sh --create --topic "$topic" \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --partitions "$partitions" --replication-factor "$rf" \
        --config retention.ms=3600000 > /dev/null 2>&1
}

topic_delete() {
    singularity exec $SIF \
        /opt/kafka/bin/kafka-topics.sh --delete --topic "$1" \
        --bootstrap-server "$BOOTSTRAP_SERVERS" > /dev/null 2>&1 || true
    sleep 2
}

run_test() {
    local rf=$1 part=$2 acks=$3 run=$4
    local topic="rf-rerun-rf${rf}-p${part}-a${acks}-r${run}-$(date +%s)"
    local logf="${LOG_DIR}/rf${rf}_p${part}_a${acks}_r${run}.log"

    printf "  RF=%s part=%s acks=%-3s run=%s ... " "$rf" "$part" "$acks" "$run"
    topic_create "$topic" "$part" "$rf"
    sleep 3

    singularity exec $SIF \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic "$topic" --num-records $NUM_MESSAGES --record-size $MSG_SIZE \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=$acks \
        --print-metrics > "$logf" 2>&1

    local rps=$(grep -oP '\d+\.\d+ records/sec' "$logf" | head -1 | awk '{print $1}')
    local mbs=$(grep -oP '\d+\.\d+ MB/sec' "$logf" | head -1 | awk '{print $1}')
    local avg=$(grep -oP '\d+\.\d+ ms avg latency' "$logf" | head -1 | awk '{print $1}')
    local max=$(grep -oP '\d+\.\d+ ms max latency' "$logf" | head -1 | awk '{print $1}')

    echo "${rf},${part},${acks},${run},${rps:-0},${mbs:-0},${avg:-0},${max:-0}" >> "$RESULTS_FILE"
    echo "${mbs:-0} MB/s  (avg lat: ${avg:-?} ms)"

    topic_delete "$topic"
}

PARTITIONS=(1 4 8)
ACKS_MODES=("1" "all")
RUNS=3

START_TIME=$(date +%s)

for RF in 1 3; do
    echo ""
    echo "════════════════════════════════════════"
    echo "  RF=$RF  (3 brokers, 10KB messages)"
    echo "════════════════════════════════════════"
    for part in "${PARTITIONS[@]}"; do
        for acks in "${ACKS_MODES[@]}"; do
            for run in $(seq 1 $RUNS); do
                run_test "$RF" "$part" "$acks" "$run"
            done
        done
    done
done

END_TIME=$(date +%s)
ELAPSED=$(( END_TIME - START_TIME ))

echo ""
echo "════════════════════════════════════════"
echo "  DONE in ${ELAPSED}s"
echo "  Results: ${RESULTS_FILE}"
echo ""
echo "  Quick summary (averages):"
echo "════════════════════════════════════════"

# Print averages per config
python3 -c "
import csv, collections
data = collections.defaultdict(list)
with open('${RESULTS_FILE}') as f:
    for row in csv.DictReader(f):
        key = (row['rf'], row['partitions'], row['acks'])
        data[key].append(float(row['mb_per_sec']))
print(f'  {\"RF\":>3s} {\"Part\":>5s} {\"Acks\":>5s}  {\"Avg MB/s\":>10s}  {\"Min\":>8s}  {\"Max\":>8s}  {\"CV%\":>6s}')
print(f'  {\"---\":>3s} {\"-----\":>5s} {\"-----\":>5s}  {\"----------\":>10s}  {\"--------\":>8s}  {\"--------\":>8s}  {\"------\":>6s}')
for (rf, p, a) in sorted(data.keys()):
    vals = data[(rf, p, a)]
    avg = sum(vals)/len(vals)
    mn, mx = min(vals), max(vals)
    cv = (((sum((v-avg)**2 for v in vals)/len(vals))**0.5)/avg*100) if avg > 0 else 0
    print(f'  {rf:>3s} {p:>5s} {a:>5s}  {avg:>10.2f}  {mn:>8.2f}  {mx:>8.2f}  {cv:>5.1f}%')
" 2>/dev/null || echo "(install python3 to see summary)"
