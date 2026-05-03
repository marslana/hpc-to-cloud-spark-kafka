# 5GB Pipeline Experiments — Step-by-Step Guide

All 5 approaches use the same 5GB EEA dataset with identical Spark/Kafka configs.
Every approach measures: per-phase subprocess timings + first-record latency + E2E time.

## Folder Structure

```
5gb/
├── configs/experiment_5gb.conf    ← shared config (UPDATE EC2 IPs here)
├── scripts/
│   ├── first_record_consumer.py   ← deploy to EC2 US-East
│   ├── deploy_spark_5gb.sh        ← SLURM job
│   ├── approach1_cloud_side.sh
│   ├── approach2_direct_producer.sh
│   ├── approach3_hpc_side.sh
│   ├── approach4_mm2.sh
│   └── approach5_skyhost.sh
└── results/                       ← auto-created by scripts
```

---

## Step 0: Generate 5GB Dataset on HPC

```bash
# From HPC login node
salloc -p batch -N 1 -t 01:00:00
module load env/development/2024a
module load tools/Apptainer/1.4.1

cd ~/pipeline_data
python3 ~/experiments/data/generate_eea_dataset.py \
    --output ~/pipeline_data/eea_airquality_5gb.csv \
    --size-gb 5 \
    --seed 42

# Verify
ls -lh ~/pipeline_data/eea_airquality_5gb.csv
wc -l ~/pipeline_data/eea_airquality_5gb.csv
# Expected: ~5.0GB, ~83.9M lines (including header)
```

## Step 1: Launch EC2 Instances

**US-East-1 (Virginia)** — needed for ALL approaches:
- Instance: m5.8xlarge
- AMI: Ubuntu 22.04
- Security group: allow inbound 9092 (Kafka), 22 (SSH) from HPC IP range
- Storage: 50GB gp3

**EU-Central-1 (Frankfurt)** — needed for Approaches 4 & 5:
- Instance: m5.8xlarge
- Same AMI/security group config
- Security group: allow inbound 9092 from HPC and US-East

## Step 2: Setup EC2 Instances

```bash
# SSH to each instance and run setup
ssh -i ~/.ssh/ec2-key.pem ubuntu@<US_EAST_IP>
bash ~/setup_ec2_kafka.sh    # reuse existing script

ssh -i ~/.ssh/ec2-key.pem ubuntu@<FRANKFURT_IP>
bash ~/setup_ec2_kafka.sh
```

## Step 3: Deploy Scripts to EC2

```bash
# From your laptop
scp -i ~/.ssh/ec2-key.pem \
    experiments/5gb/scripts/first_record_consumer.py \
    experiments/scripts/spark_filter_produce.py \
    experiments/scripts/kafka_dir_ingest.sh \
    ubuntu@<US_EAST_IP>:~/scripts/

# Also install confluent-kafka on EC2
ssh -i ~/.ssh/ec2-key.pem ubuntu@<US_EAST_IP> \
    "pip3 install confluent-kafka"

# Create results dir on EC2
ssh -i ~/.ssh/ec2-key.pem ubuntu@<US_EAST_IP> \
    "mkdir -p ~/results ~/pipeline_data"
```

## Step 4: Update Config

Edit `experiments/5gb/configs/experiment_5gb.conf` on HPC:
```bash
EC2_US_EAST_IP="<actual US-East public IP>"
EC2_FRANKFURT_IP="<actual Frankfurt public IP>"
```

Copy the 5gb experiment folder to HPC:
```bash
# From your laptop
scp -P 8022 -r "experiments/5gb" <HPC_USER>@<HPC_LOGIN_HOST>:~/experiments/5gb
```

## Step 5: Deploy Spark Cluster on HPC

```bash
# From HPC login node
sbatch ~/experiments/5gb/scripts/deploy_spark_5gb.sh

# Wait for cluster, then check
cat ~/sparkhdfs/logs/pipeline_main.log
# Look for: "5GB Pipeline cluster READY"

# Note the coordinator node
cat ~/coordinatorNode
# SSH to it
ssh <coordinator_node>
```

## Step 6: Run Approaches 1-3 (US-East only)

From the HPC coordinator node:

```bash
# Approach 1: Cloud-Side Processing (scp raw → EC2 Spark)
# Estimated time: ~25 min (3 runs × ~8 min each)
bash ~/experiments/5gb/scripts/approach1_cloud_side.sh

# Approach 2: Direct Producer (HPC Spark → US-East over WAN)
# Estimated time: ~50 min (3 runs × ~16 min each)
bash ~/experiments/5gb/scripts/approach2_direct_producer.sh

# Approach 3: HPC-Side Processing (filter → scp → ingest)
# Estimated time: ~15 min (3 runs × ~5 min each)
bash ~/experiments/5gb/scripts/approach3_hpc_side.sh
```

## Step 7: Run Approach 4 (MM2 via Frankfurt)

Requires Frankfurt EC2 running. From HPC coordinator:

```bash
# Estimated time: ~1.5 hours (3 runs × ~30 min each)
bash ~/experiments/5gb/scripts/approach4_mm2.sh
```

## Step 8: Run Approach 5 (SkyHOST via Frankfurt)

Requires Frankfurt EC2 + SkyHOST gateways. From your **laptop**:

```bash
# Terminal 1 (laptop): activate SkyHOST
cd /path/to/skyplane
source skyplane-dev-venv/bin/activate
export SKYPLANE_DOCKER_IMAGE=<SKYHOST_DOCKER_IMAGE>
```

From HPC coordinator:
```bash
# Start the approach script - it will PAUSE and prompt you
# to start SkyHOST from your laptop for each run
bash ~/experiments/5gb/scripts/approach5_skyhost.sh
```

When prompted, start SkyHOST from your laptop (Terminal 1):
```bash
skyplane skyhost transfer \
    --src-kafka-broker <FRANKFURT_IP>:9092 \
    --src-topic <TOPIC_SHOWN_IN_PROMPT> \
    --dst-kafka-broker <US_EAST_IP>:9092 \
    --dst-topic <TOPIC_SHOWN_IN_PROMPT> \
    --src-region aws:eu-central-1 \
    --dst-region aws:us-east-1 \
    --num-readers 8 --num-writers 8 \
    --chunk-size-mb 32
```

Then press ENTER on HPC to continue.

## Step 9: Collect Results

Results are stored in `~/5gb_results/` on HPC:
```
~/5gb_results/
├── approach1/results_*.csv
├── approach2/results_*.csv
├── approach3/results_*.csv
├── approach4_mm2/results_*.csv
└── approach5_skyhost/results_*.csv
```

Copy to laptop:
```bash
scp -P 8022 -r <HPC_USER>@<HPC_LOGIN_HOST>:~/5gb_results/ \
    ./experiments/results/5gb/
```

## Final Results

| # | Approach | E2E | First Record |
|---|----------|----------|-------------------|
| 1 | Cloud-Side Processing | 368s | 339s |
| 2 | Direct Producer | 670s | 36s |
| 3 | HPC-Side Processing | 183s | 103s |
| 4 | MM2 Replication | 219s | 35s |
| 5 | SkyHOST Transfer | 186s | 35s |

## CSV Columns (per approach)

All CSVs include per-phase subprocess timings:

- **Approach 1**: scp_time, spark_read, spark_filter, kafka_produce, consumer_first_record, e2e
- **Approach 2**: spark_read, spark_filter, kafka_produce (WAN), producer-side total time
- **Approach 3**: spark_read, spark_filter, spark_write, scp_time, ingest_time, consumer_first_record, e2e
- **Approach 4**: hpc_read, hpc_filter, hpc_produce (to Frankfurt), consumer_first_record, consumer_active (MM2 replication), e2e
- **Approach 5**: hpc_read, hpc_filter, hpc_produce (to Frankfurt), consumer_first_record, consumer_active (SkyHOST transfer), hpc_wall, e2e

## Cloud Cost Estimate

- US-East m5.8xlarge: ~$1.54/hr × ~3 hrs = ~$4.62
- Frankfurt m5.8xlarge: ~$1.71/hr × ~2 hrs = ~$3.42
- SkyHOST gateways (auto-provisioned): ~$1-2
- Data transfer: ~$0.50
- **Total estimate: ~$6-7**
