# Pipeline Experiments: HPC-to-Cloud Transfer Evaluation

## Overview

Four approaches for delivering EEA environmental data from HPC to US-East-1 cloud:

| # | Approach | Transfer | Filter | Path |
|---|---------|----------|--------|------|
| 1 | Direct Kafka Streaming | Spark→Kafka (stream) | HPC-side | HPC → US-East-1 |
| 2 | Regional + SkyHOST | Spark→Kafka (stream) | HPC-side | HPC → Frankfurt → US-East-1 |
| 3 | scp + Kafka Ingest | scp (file) | None | HPC → US-East-1 → Kafka ingest |
| 4 | scp + Cloud Spark-Kafka | scp (file) | Cloud-side | HPC → US-East-1 → Spark → Kafka |

## Folder Structure

```
experiments/
├── configs/experiment.conf         # ← Base config (edit EC2 IPs, keys)
├── data/generate_eea_dataset.py    # Deterministic synthetic EEA dataset generator
├── scripts/                        # Shared experiment scripts
│   ├── spark_filter_produce.py     # Core PySpark: filter + produce to Kafka
│   ├── kafka_file_ingest.py        # CSV → Kafka ingester (cloud-side)
│   ├── setup_ec2_kafka.sh          # EC2 Kafka + Spark setup
│   └── ...
├── 5gb/                            # 5 GB experiment (primary)
│   ├── configs/experiment_5gb.conf # 5 GB-specific configuration
│   └── scripts/approach[1-5]*.sh   # Per-approach automation
└── results/
    ├── 5gb/approach[1-5]*.csv      # Raw per-run result CSVs
    ├── EXPERIMENT_DETAILS.md
    ├── FINAL_COMPARISON.md
    └── tcp_tuning_applied.md
```

## Step-by-Step Instructions

### Phase 0: Generate Dataset (on your laptop or HPC)

```bash
# Generate 1 GB dataset (~8.3M records, ~10% pass filter)
python3 data/generate_eea_dataset.py --output ~/pipeline_data/eea_airquality_1gb.csv --size-gb 1

# Optional: 10 GB version
python3 data/generate_eea_dataset.py --output ~/pipeline_data/eea_airquality_10gb.csv --size-gb 10
```

Upload to HPC if generated locally:
```bash
scp ~/pipeline_data/eea_airquality_1gb.csv aion:~/pipeline_data/
```

### Phase 1: Launch EC2 Instances

```bash
# Launch m5.8xlarge in us-east-1 (Virginia)
aws ec2 run-instances \
  --image-id ami-0c7217cdde317cfec \
  --instance-type m5.8xlarge \
  --key-name <your-key> \
  --security-group-ids <sg-id> \
  --region us-east-1 \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=pipeline-useast}]'

# Launch m5.8xlarge in eu-central-1 (Frankfurt) — for Approach 2 later
aws ec2 run-instances \
  --image-id ami-0faab6bdbac9486fb \
  --instance-type m5.8xlarge \
  --key-name <your-key> \
  --security-group-ids <sg-id> \
  --region eu-central-1 \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=pipeline-frankfurt}]'
```

**Security groups must allow**:
- Port 22 (SSH) from your IP + HPC external IP
- Port 9092 (Kafka) from 0.0.0.0/0 (or restrict to HPC + other EC2)

### Phase 2: Set Up EC2 Instances

```bash
# SSH into US-East instance
ssh -i key.pem ubuntu@<US_EAST_IP>
bash setup_ec2_kafka.sh

# Deploy cloud-side scripts
scp scripts/kafka_file_ingest.py ubuntu@<US_EAST_IP>:~/
scp scripts/spark_filter_produce.py ubuntu@<US_EAST_IP>:~/
```

### Phase 3: Configure & Verify

Edit `configs/experiment.conf`:
```bash
EC2_US_EAST_IP="<your-us-east-ip>"
EC2_FRANKFURT_IP="<your-frankfurt-ip>"
EC2_KEY_US="$HOME/.ssh/ec2-us-east.pem"
```

On HPC, verify connectivity:
```bash
# From an Aion compute node
module load tools/Apptainer
singularity exec ~/hsk.sif /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server <US_EAST_IP>:9092 --list
```

### Phase 4: Run Experiments (in order)

**Day 1 — Easy baselines (Approaches 3 & 4):**
```bash
# From HPC login node (scp doesn't need SLURM)
bash scripts/approach3_scp_ingest.sh
bash scripts/approach4_scp_cloud_spark.sh
```

**Day 2 — Direct streaming (Approach 1):**
```bash
# First deploy HPC cluster (existing SLURM job)
sbatch paper/artifacts/scripts/deployment/deploy_cluster.sh

# Then SSH to the coordinator node and run:
bash scripts/approach1_direct_streaming.sh
```

**Later — SkyHOST (Approach 2):**
Run after setting up SkyHOST with Frankfurt instance.

### Phase 5: Collect Results

All results are saved to `~/pipeline_results/<approach>/results_<timestamp>.csv`.

## Consistent Configuration

All approaches use these settings (from `experiment.conf`):
- Kafka: 8 partitions, acks=1, 64KB batch, 10ms linger
- Spark: 16KB batch, 6GB executor memory
- Dataset: 1GB EEA air quality CSV, 10% filter ratio
- EC2: m5.8xlarge (matching SkyHOST evaluation)
- Repetitions: 3 runs per approach
