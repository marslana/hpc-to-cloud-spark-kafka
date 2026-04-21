# HPC Commands — Run These on Aion

## EC2 Instance Info
- **IP**: `100.30.231.167`
- **Kafka**: `100.30.231.167:9092`
- **User**: `ubuntu`
- **Key**: `~/.ssh/pipeline-useast-key.pem` (you need to copy this to HPC)

---

## Step 1: Copy files to HPC

Run these from **your laptop**:

```bash
AION="mtariq@access-aion.uni.lu"
AION_SSH="ssh -p 8022 $AION"
AION_SCP="scp -P 8022"

# Copy the SSH key (needed for scp-based approaches)
$AION_SCP ~/.ssh/pipeline-useast-key.pem ${AION}:~/.ssh/
$AION_SSH "chmod 600 ~/.ssh/pipeline-useast-key.pem"

# Create directories on HPC
$AION_SSH "mkdir -p ~/pipeline_data ~/experiments ~/pipeline_results/{approach1,approach3,approach4}"

# Copy the dataset
$AION_SCP ~/pipeline_data/eea_airquality_1gb.csv ${AION}:~/pipeline_data/

# Copy the experiment scripts
$AION_SCP -r "/Users/arslan.tariq/Desktop/HPC data/experiments/scripts" ${AION}:~/experiments/
$AION_SCP -r "/Users/arslan.tariq/Desktop/HPC data/experiments/configs" ${AION}:~/experiments/
```

## Step 2: Verify connectivity from HPC

SSH into Aion and test:

```bash
ssh -p 8022 mtariq@access-aion.uni.lu

# Test SSH to EC2
ssh -o StrictHostKeyChecking=no -i ~/.ssh/pipeline-useast-key.pem ubuntu@100.30.231.167 "echo OK"

# Test Kafka connectivity (from a compute node via SLURM)
salloc -p batch -N 1 -t 00:10:00
module load env/development/2024a
module load tools/Apptainer/1.4.1
singularity exec ~/hsk.sif /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 100.30.231.167:9092 --list
```

If the Kafka test fails, the HPC external firewall may block port 9092. In that case, only scp-based approaches will work directly; for Approach 1, we'd need to tunnel.

---

## Step 3: Run Approach 3 — scp + Kafka Ingest (no SLURM needed)

This runs from a **login node** (no Spark/Singularity needed):

```bash
cd ~/experiments

# Quick single-run test first
EC2_IP="100.30.231.167"
KEY="$HOME/.ssh/pipeline-useast-key.pem"
SSH_OPTS="-o StrictHostKeyChecking=no -i $KEY"
DATASET="$HOME/pipeline_data/eea_airquality_1gb.csv"

mkdir -p ~/pipeline_results/approach3

# ── Phase 1: Time the scp ──
echo "Starting scp..."
T_START=$(date +%s)
scp $SSH_OPTS "$DATASET" ubuntu@${EC2_IP}:~/pipeline_data/eea_raw.csv
T_END=$(date +%s)
echo "scp took $((T_END - T_START)) seconds"

# ── Phase 2: Ingest on EC2 ──
# Create topic
ssh $SSH_OPTS ubuntu@${EC2_IP} "
  sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic approach3-test \
    --partitions 8 --replication-factor 1
"

# Run ingest
ssh $SSH_OPTS ubuntu@${EC2_IP} "
  python3 ~/kafka_file_ingest.py \
    --input ~/pipeline_data/eea_raw.csv \
    --bootstrap-servers localhost:9092 \
    --topic approach3-test \
    --batch-size 65536 --acks 1
"

# Cleanup
ssh $SSH_OPTS ubuntu@${EC2_IP} "
  sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic approach3-test
  rm -f ~/pipeline_data/eea_raw.csv
"
```

For the full 3-run experiment, use the script:
```bash
bash ~/experiments/scripts/approach3_scp_ingest.sh
```

---

## Step 4: Run Approach 4 — scp + Cloud Spark-Kafka

Same as above but Spark does filtering on cloud side:

```bash
cd ~/experiments

EC2_IP="100.30.231.167"
KEY="$HOME/.ssh/pipeline-useast-key.pem"
SSH_OPTS="-o StrictHostKeyChecking=no -i $KEY"
DATASET="$HOME/pipeline_data/eea_airquality_1gb.csv"

mkdir -p ~/pipeline_results/approach4

# ── Phase 1: scp ──
echo "Starting scp..."
T_START=$(date +%s)
scp $SSH_OPTS "$DATASET" ubuntu@${EC2_IP}:~/pipeline_data/eea_raw.csv
T_END=$(date +%s)
echo "scp took $((T_END - T_START)) seconds"

# ── Phase 2: Spark filter + produce on cloud ──
ssh $SSH_OPTS ubuntu@${EC2_IP} "
  sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic approach4-test \
    --partitions 8 --replication-factor 1
"

ssh $SSH_OPTS ubuntu@${EC2_IP} "
  export SPARK_HOME=/opt/spark
  export PATH=\$PATH:/opt/spark/bin
  spark-submit \
    --master 'local[*]' \
    --driver-memory 4g \
    --conf spark.hadoop.fs.defaultFS=file:/// \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    ~/spark_filter_produce.py \
      --input ~/pipeline_data/eea_raw.csv \
      --bootstrap-servers localhost:9092 \
      --topic approach4-test \
      --filter-countries LU,DE,BE \
      --partitions 8 \
      --batch-size 16384 \
      --acks 1 \
      --run-id approach4_test
"

# Cleanup
ssh $SSH_OPTS ubuntu@${EC2_IP} "
  sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic approach4-test
  rm -f ~/pipeline_data/eea_raw.csv
"
```

---

## Step 5: Run Approach 1 — Direct Kafka Streaming (needs SLURM)

This requires the HPC cluster with Spark + Kafka running.

### 5a. Deploy the HPC cluster

```bash
cd ~/experiments
# Make sure hsk.sif is in your home directory
ls ~/hsk.sif || ls ~/sparkhdfs.sif

# Submit the cluster job (adjust path to your existing deploy script)
sbatch ~/paper/artifacts/scripts/deployment/deploy_cluster.sh
# Wait ~3 minutes for cluster to start
# Check it's running:
squeue -u $USER
```

### 5b. SSH to the coordinator node and run the experiment

```bash
# Get coordinator hostname
COORD=$(cat ~/coordinatorNode)
echo "Coordinator: $COORD"

# SSH to the coordinator
ssh $COORD
```

Then on the coordinator node:
```bash
cd ~/experiments

# Verify HPC Kafka is running
singularity exec instance://shinst_master \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server $(hostname):9092 --list

# Verify remote Kafka is reachable
singularity exec instance://shinst_master \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server 100.30.231.167:9092 --list

# Get Spark master
SPARK_MASTER="spark://$(hostname):7078"

# ── Create topic on US-East EC2 ──
singularity exec ~/hsk.sif /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 100.30.231.167:9092 --create \
  --topic approach1-test --partitions 8 --replication-factor 1

# ── Run Spark filter + produce to US-East ──
singularity exec \
  --bind ~/pipeline_data:/opt/dataset \
  --bind ~/pipeline_results:/opt/results \
  --bind ~/experiments/scripts:/opt/scripts \
  ~/hsk.sif \
  spark-submit \
    --master "$SPARK_MASTER" \
    --conf spark.hadoop.fs.defaultFS=file:/// \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    /opt/scripts/spark_filter_produce.py \
      --input /opt/dataset/eea_airquality_1gb.csv \
      --bootstrap-servers 100.30.231.167:9092 \
      --topic approach1-test \
      --filter-countries LU,DE,BE \
      --partitions 8 \
      --batch-size 16384 \
      --acks 1 \
      --output-csv /opt/results/approach1/results.csv \
      --run-id approach1_run1

# ── Cleanup ──
singularity exec ~/hsk.sif /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 100.30.231.167:9092 --delete --topic approach1-test
```

Repeat 3 times changing `--run-id approach1_run2` and `--run-id approach1_run3`.

---

## Collecting Results

After all experiments, copy results back to your laptop:
```bash
# From your laptop
scp -P 8022 -r mtariq@access-aion.uni.lu:~/pipeline_results/ "/Users/arslan.tariq/Desktop/HPC data/experiments/results/"
```

---

## Important Notes

- **Approach 3 and 4** can run from the HPC login node (no SLURM needed, just scp + SSH)
- **Approach 1** requires the SLURM cluster to be running (Spark + Kafka on HPC)
- The EC2 instance is **m5.8xlarge** ($1.536/hr) — terminate when done
- Terminate command: `aws ec2 terminate-instances --instance-ids i-07a33fe16d0a9d9b3 --region us-east-1`
