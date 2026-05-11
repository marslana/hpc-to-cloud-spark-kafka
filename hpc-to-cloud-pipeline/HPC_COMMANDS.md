# HPC Commands

This file gives the Aion-side command sequence for the pipeline experiments. All paths assume you transfer the `hpc-to-cloud-pipeline/` folder to your HPC home as `~/hpc-to-cloud-pipeline/`.

## Copy Experiment Material to Aion

Run from the local machine.

```bash
AION="<HPC_USER>@<HPC_LOGIN_HOST>"
AION_SCP="scp -P 8022"

$AION_SCP -r hpc-to-cloud-pipeline ${AION}:~/
```

## Generate the 5 GB Dataset

Run on Aion.

```bash
salloc -p batch -N 1 -t 01:00:00
module load env/development/2024a

mkdir -p ~/pipeline_data
python3 ~/hpc-to-cloud-pipeline/scripts/generate_eea_dataset.py \
  --output ~/pipeline_data/eea_airquality_5gb.csv \
  --size-gb 5 \
  --seed 42
```

## Configure Cloud Endpoints

Edit `~/hpc-to-cloud-pipeline/configs/experiment.conf` on Aion.

```bash
EC2_US_EAST_IP="<YOUR_US_EAST_IP>"
EC2_FRANKFURT_IP="<YOUR_FRANKFURT_IP>"
SSH_KEY_US="$HOME/.ssh/pipeline-useast-key.pem"
SSH_KEY_FRA="$HOME/.ssh/pipeline-frankfurt-key.pem"
```

## Start the Spark Cluster

```bash
sbatch ~/hpc-to-cloud-pipeline/scripts/deploy_spark.sh
squeue -u "$USER"
cat ~/coordinatorNode
ssh "$(cat ~/coordinatorNode)"
```

## Run the Five Approaches

Run from the coordinator node.

```bash
bash ~/hpc-to-cloud-pipeline/scripts/approach1_cloud_side.sh
bash ~/hpc-to-cloud-pipeline/scripts/approach2_direct_producer.sh
bash ~/hpc-to-cloud-pipeline/scripts/approach3_hpc_side.sh
bash ~/hpc-to-cloud-pipeline/scripts/approach4_mm2.sh
bash ~/hpc-to-cloud-pipeline/scripts/approach5_skyhost.sh
```

Approach 5 pauses and prints the exact SkyHOST command that must be run from the client machine.

## Collect Results

```bash
scp -P 8022 -r <HPC_USER>@<HPC_LOGIN_HOST>:~/pipeline_results/ \
  ./hpc-to-cloud-pipeline/results/
```

The cleaned result CSVs used by the paper are stored in `hpc-to-cloud-pipeline/results/`.
