#!/usr/bin/bash -l
#SBATCH --job-name=Pipe5GB
#SBATCH --nodes=2
#SBATCH --ntasks=3
#SBATCH --ntasks-per-node=2
#SBATCH --mem-per-cpu=4GB
#SBATCH --cpus-per-task=8
#SBATCH --time=0-06:00:00
#SBATCH --partition=batch
#SBATCH --qos=normal

# Spark cluster for 5GB pipeline experiments:
#   Node 0: Spark master (1 task)
#   Node 1: 2 Spark workers × 4 cores × 6g memory
# Same Spark config as 1GB experiments for fair comparison.

module load env/development/2024a
module load tools/Apptainer/1.4.1

SIF="$HOME/hsk.sif"
[ ! -f "$SIF" ] && SIF="$HOME/sparkhdfs.sif"

mkdir -p ${HOME}/sparkhdfs/logs
mkdir -p ${HOME}/sparkhdfs/spark/{logs,conf,work}
mkdir -p ${HOME}/5gb_results/{approach1,approach2,approach3,approach4_mm2,approach5_skyhost}
mkdir -p ${HOME}/pipeline_data

singularity instance list 2>/dev/null | grep shinst_ | awk '{print $1}' | xargs -r singularity instance stop 2>/dev/null || true

hostName="$(hostname)"
echo "$hostName" > ${HOME}/coordinatorNode
echo "[$(date)] Coordinator: $hostName"

cat > ${HOME}/sparkhdfs/spark/conf/spark-defaults.conf << EOF
spark.master spark://$hostName:7078
spark.driver.memory 2g
spark.executor.memory 6g
spark.executor.cores 4
spark.cores.max 8
spark.executor.instances 2
spark.driver.host $hostName
spark.hadoop.fs.defaultFS file:///
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

MASTER_SCRIPT=${HOME}/sparkhdfs/5gb-master-${SLURM_JOBID}.sh
cat << 'MASTEREOF' > ${MASTER_SCRIPT}
#!/bin/bash
singularity instance stop shinst_master 2>/dev/null || true
sleep 2
singularity instance start \
  --bind $HOME/sparkhdfs/spark/conf:/opt/spark/conf \
  --bind $HOME/sparkhdfs/spark/logs:/opt/spark/logs \
  --bind $HOME/sparkhdfs/spark/work:/opt/spark/work \
  --bind $HOME/pipeline_data:/opt/dataset \
  --bind $HOME/5gb_results:/opt/results \
  --bind $HOME/experiments:/opt/experiments \
  $HOME/hsk.sif shinst_master

singularity exec instance://shinst_master \
  spark-class org.apache.spark.deploy.master.Master \
    --host $(hostname) --port 7078 --webui-port 8080 \
    >> ${HOME}/sparkhdfs/spark/logs/master.log 2>&1 &

for i in $(seq 1 30); do
  singularity exec instance://shinst_master jps 2>/dev/null | grep -q Master && break
  [ $i -eq 30 ] && { echo "ERROR: Spark master failed"; exit 1; }
  sleep 5
done
echo "[$(date)] Spark master READY"
while true; do sleep 60; done
MASTEREOF
chmod +x ${MASTER_SCRIPT}

WORKER_SCRIPT=${HOME}/sparkhdfs/5gb-worker-${SLURM_JOBID}.sh
cat << 'WORKEREOF' > ${WORKER_SCRIPT}
#!/bin/bash
WID=${SLURM_PROCID}
singularity instance stop shinst_worker_${WID} 2>/dev/null || true
sleep 2
singularity instance start \
  --bind $HOME/sparkhdfs/spark/conf:/opt/spark/conf \
  --bind $HOME/sparkhdfs/spark/logs:/opt/spark/logs \
  --bind $HOME/sparkhdfs/spark/work:/opt/spark/work \
  --bind $HOME/pipeline_data:/opt/dataset \
  --bind $HOME/5gb_results:/opt/results \
  --bind $HOME/experiments:/opt/experiments \
  $HOME/hsk.sif shinst_worker_${WID}

MASTER_HOST=$(cat $HOME/coordinatorNode)
for i in $(seq 1 30); do
  nc -z ${MASTER_HOST} 7078 2>/dev/null && break
  [ $i -eq 30 ] && { echo "ERROR: Cannot reach master"; exit 1; }
  sleep 5
done

singularity exec instance://shinst_worker_${WID} \
  spark-class org.apache.spark.deploy.worker.Worker \
    spark://${MASTER_HOST}:7078 -c 4 -m 6G \
    >> ${HOME}/sparkhdfs/spark/logs/worker_${WID}.log 2>&1 &

echo "[$(date)] Worker $WID started"
while true; do sleep 60; done
WORKEREOF
chmod +x ${WORKER_SCRIPT}

echo "[$(date)] Launching Spark master..."
srun --exclusive --nodes=1 --ntasks=1 --cpus-per-task=8 \
     --output=$HOME/sparkhdfs/logs/Master-%N-%j.out \
     ${MASTER_SCRIPT} &
sleep 45

echo "[$(date)] Launching 2 Spark workers..."
srun --exclusive --nodes=1 --ntasks=2 --ntasks-per-node=2 --cpus-per-task=8 \
     --output=$HOME/sparkhdfs/logs/Workers-%N-%j.out \
     ${WORKER_SCRIPT} &
sleep 30

if singularity exec instance://shinst_master jps 2>/dev/null | grep -q Master; then
    echo "[$(date)] Spark master: OK"
else
    echo "ERROR: Spark master not running"; exit 1
fi

echo "============================================"
echo " 5GB Pipeline cluster READY"
echo " Coordinator: $hostName"
echo " Spark master: spark://$hostName:7078"
echo " SSH to coordinator: ssh $hostName"
echo "============================================"

trap 'singularity instance list 2>/dev/null | grep shinst_ | awk "{print \$1}" | xargs -r singularity instance stop' EXIT
while true; do sleep 60; done
