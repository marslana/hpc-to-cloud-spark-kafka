#!/usr/bin/bash -l
#SBATCH --job-name=Pipeline
#SBATCH --nodes=2
#SBATCH --ntasks=3
#SBATCH --ntasks-per-node=2
#SBATCH --mem-per-cpu=4GB
#SBATCH --cpus-per-task=8
#SBATCH --time=0-02:00:00
#SBATCH --partition=batch
#SBATCH --qos=normal

# Pipeline experiment deployment:
#   Node 0: Spark master (1 task)
#   Node 1: Spark workers (2 tasks)
#
# Uses configs from HPC characterization:
#   - 2 executors × 4 cores × 6g memory (fair comparison with EC2 local[8])
#   - No HDFS (data on local disk)
#   - No HPC Kafka broker (produce directly to EC2)

module load env/development/2024a
module load tools/Apptainer/1.4.1

SIF="$HOME/hsk.sif"
[ ! -f "$SIF" ] && SIF="$HOME/sparkhdfs.sif"

mkdir -p ${HOME}/sparkhdfs/logs
mkdir -p ${HOME}/sparkhdfs/spark/{logs,conf,work}
mkdir -p ${HOME}/pipeline_results/{approach1,approach2}
mkdir -p ${HOME}/pipeline_data

log_msg() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a ${HOME}/sparkhdfs/logs/pipeline_main.log; }

# Kill stale instances
singularity instance list 2>/dev/null | grep shinst_ | awk '{print $1}' | xargs -r singularity instance stop 2>/dev/null || true

hostName="$(hostname)"
echo "$hostName" > ${HOME}/coordinatorNode
log_msg "Coordinator: $hostName"

# Spark config matching characterization
cat > ${HOME}/sparkhdfs/spark/conf/spark-defaults.conf << EOF
spark.master spark://$hostName:7078
spark.driver.memory 2g
spark.executor.memory 6g
spark.executor.cores 4
spark.cores.max 8
spark.executor.instances 2
spark.driver.host $hostName
spark.hadoop.fs.defaultFS file:///
spark.logConf true
spark.network.timeout 800s
spark.sql.shuffle.partitions 16
spark.sql.adaptive.enabled true
EOF

cat > ${HOME}/sparkhdfs/spark/conf/spark-env.sh << EOF
#!/usr/bin/env bash
SPARK_MASTER_HOST="$hostName"
SPARK_MASTER_PORT="7078"
SPARK_HOME="/opt/spark"
EOF
chmod +x ${HOME}/sparkhdfs/spark/conf/spark-env.sh

# ── Master launcher (single task, no race) ───────────────────
MASTER_SCRIPT=${HOME}/sparkhdfs/pipeline-master-${SLURM_JOBID}.sh
cat << 'MASTEREOF' > ${MASTER_SCRIPT}
#!/bin/bash
echo "[$(date)] Starting Spark master on $(hostname)"

singularity instance stop shinst_master 2>/dev/null || true
sleep 2

singularity instance start \
  --bind $HOME/sparkhdfs/spark/conf:/opt/spark/conf \
  --bind $HOME/sparkhdfs/spark/logs:/opt/spark/logs \
  --bind $HOME/sparkhdfs/spark/work:/opt/spark/work \
  --bind $HOME/pipeline_data:/opt/dataset \
  --bind $HOME/pipeline_results:/opt/results \
  --bind $HOME/experiments/scripts:/opt/scripts \
  --bind $HOME/data:/opt/shared_data \
  $HOME/hsk.sif shinst_master

echo "[$(date)] Starting Spark master process..."
singularity exec instance://shinst_master \
  spark-class org.apache.spark.deploy.master.Master \
    --host $(hostname) --port 7078 --webui-port 8080 \
    >> ${HOME}/sparkhdfs/spark/logs/master.log 2>&1 &

for i in $(seq 1 30); do
  if singularity exec instance://shinst_master jps 2>/dev/null | grep -q Master; then
    echo "[$(date)] Spark master READY"
    break
  fi
  [ $i -eq 30 ] && echo "ERROR: Spark master failed to start"
  sleep 5
done

while true; do
  sleep 60
  singularity instance list | grep -q shinst_master || { echo "Master instance gone"; exit 1; }
done
MASTEREOF
chmod +x ${MASTER_SCRIPT}

# ── Worker launcher ──────────────────────────────────────────
WORKER_SCRIPT=${HOME}/sparkhdfs/pipeline-worker-${SLURM_JOBID}.sh
cat << 'WORKEREOF' > ${WORKER_SCRIPT}
#!/bin/bash
WORKER_ID=${SLURM_PROCID}
echo "[$(date)] Starting Spark worker $WORKER_ID on $(hostname)"

singularity instance stop shinst_worker_${WORKER_ID} 2>/dev/null || true
sleep 2

singularity instance start \
  --bind $HOME/sparkhdfs/spark/conf:/opt/spark/conf \
  --bind $HOME/sparkhdfs/spark/logs:/opt/spark/logs \
  --bind $HOME/sparkhdfs/spark/work:/opt/spark/work \
  --bind $HOME/pipeline_data:/opt/dataset \
  --bind $HOME/pipeline_results:/opt/results \
  --bind $HOME/experiments/scripts:/opt/scripts \
  --bind $HOME/data:/opt/shared_data \
  $HOME/hsk.sif shinst_worker_${WORKER_ID}

# Wait for master
MASTER_HOST=$(cat $HOME/coordinatorNode)
SPARKMASTER="spark://${MASTER_HOST}:7078"
echo "Connecting to $SPARKMASTER"
for i in $(seq 1 30); do
  nc -z ${MASTER_HOST} 7078 2>/dev/null && break
  [ $i -eq 30 ] && { echo "ERROR: Cannot reach Spark master"; exit 1; }
  sleep 5
done

singularity exec instance://shinst_worker_${WORKER_ID} \
  spark-class org.apache.spark.deploy.worker.Worker \
    ${SPARKMASTER} -c 4 -m 6G \
    >> ${HOME}/sparkhdfs/spark/logs/worker_${WORKER_ID}.log 2>&1 &

echo "[$(date)] Worker $WORKER_ID started"

while true; do
  sleep 60
  singularity instance list | grep -q shinst_worker_${WORKER_ID} || { echo "Worker gone"; exit 1; }
done
WORKEREOF
chmod +x ${WORKER_SCRIPT}

# ── Launch ───────────────────────────────────────────────────
log_msg "Launching Spark master (1 task on coordinator)..."
srun --exclusive --nodes=1 --ntasks=1 --cpus-per-task=8 \
     --label --output=$HOME/sparkhdfs/logs/Master-%N-%j.out \
     ${MASTER_SCRIPT} &

sleep 45
log_msg "Launching Spark workers (2 tasks on 1 node)..."

export SPARKMASTER="spark://$hostName:7078"
srun --exclusive --nodes=1 --ntasks=2 --ntasks-per-node=2 --cpus-per-task=8 \
     --label --output=$HOME/sparkhdfs/logs/Workers-%N-%j.out \
     ${WORKER_SCRIPT} &

sleep 30

log_msg "Verifying Spark cluster..."
if singularity exec instance://shinst_master jps 2>/dev/null | grep -q Master; then
    log_msg "Spark master: OK"
else
    log_msg "ERROR: Spark master not running"
    exit 1
fi

log_msg "============================================"
log_msg " Pipeline cluster READY"
log_msg " Coordinator: $hostName"
log_msg " Spark master: spark://$hostName:7078"
log_msg " SSH to coordinator: ssh $hostName"
log_msg " Then run: bash ~/experiments/scripts/approach1_direct_streaming.sh"
log_msg "============================================"

trap 'singularity instance list 2>/dev/null | grep shinst_ | awk "{print \$1}" | xargs -r singularity instance stop' EXIT

while true; do
    sleep 60
done
