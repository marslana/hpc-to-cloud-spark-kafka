# Pipeline Experiments

This directory contains the reproducibility material for the 5 GB HPC-to-cloud pipeline evaluation reported in the paper.

## Scope

The paper uses one primary dataset scale, a 5 GB synthetic EEA-style air-quality CSV.

## Structure

```text
experiments/
├── 5gb/
│   ├── configs/experiment_5gb.conf
│   └── scripts/approach[1-5]*.sh
├── configs/experiment.conf
├── data/generate_eea_dataset.py
├── scripts/
│   ├── setup_ec2_kafka.sh
│   ├── spark_filter_produce.py
│   ├── spark_filter_save.py
│   ├── kafka_dir_ingest.sh
│   └── first-record and SkyHOST support scripts
└── results/
    ├── 5gb/approach[1-5]*.csv
    ├── EXPERIMENT_DETAILS.md
    ├── FINAL_COMPARISON.md
    └── tcp_tuning_applied.md
```

## Reproduction Steps

1. Generate the dataset.

```bash
python3 experiments/data/generate_eea_dataset.py \
  --output ~/pipeline_data/eea_airquality_5gb.csv \
  --size-gb 5 \
  --seed 42
```

2. Launch two AWS EC2 `m5.8xlarge` instances.

- `us-east-1` for the destination Kafka broker and consumer.
- `eu-central-1` for Approaches 4 and 5.
- Open SSH and Kafka port `9092` between HPC and the EC2 instances, and between the two EC2 instances.

3. Configure EC2.

```bash
bash experiments/scripts/setup_ec2_kafka.sh
```

The EC2 setup script starts a single Kafka 3.7 broker in Docker KRaft mode and installs Spark 3.4 plus the Python dependencies used by the consumer scripts.

4. Update `experiments/5gb/configs/experiment_5gb.conf`.

```bash
EC2_US_EAST_IP="<YOUR_US_EAST_IP>"
EC2_FRANKFURT_IP="<YOUR_FRANKFURT_IP>"
```

5. Start the HPC Spark/Kafka environment using the SLURM deployment script.

```bash
sbatch experiments/5gb/scripts/deploy_spark_5gb.sh
```

6. Run the five approaches from the HPC coordinator node.

```bash
bash experiments/5gb/scripts/approach1_cloud_side.sh
bash experiments/5gb/scripts/approach2_direct_producer.sh
bash experiments/5gb/scripts/approach3_hpc_side.sh
bash experiments/5gb/scripts/approach4_mm2.sh
bash experiments/5gb/scripts/approach5_skyhost.sh
```

Approach 5 requires SkyHOST/Skyplane to be started from the client machine when the script prompts for it.

## Result Traceability

The raw result CSVs used for the paper are in `experiments/results/5gb/`.

| Approach | Source CSV |
|---|---|
| Cloud-Side Processing | `approach1_cloud_side_processing.csv` |
| Direct Producer | `approach2_direct_producer.csv` |
| HPC-Side Processing | `approach3_hpc_side_processing.csv` |
| MM2 Replication | `approach4_mm2_replication_tuned.csv` |
| SkyHOST | `approach5_skyhost_transfer.csv` |

The paper uses the consumer-side `consumer_total_s` as E2E for Approaches 1, 3, 4, and 5.
