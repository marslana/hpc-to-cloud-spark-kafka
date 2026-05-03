# HPC Commands

This file gives the Aion-side command sequence for the final 5 GB pipeline experiments.

## Copy Experiment Material to Aion

Run from the local machine.

```bash
AION="<HPC_USER>@<HPC_LOGIN_HOST>"
AION_SCP="scp -P 8022"

$AION_SCP -r experiments/5gb ${AION}:~/experiments/
$AION_SCP -r experiments/scripts ${AION}:~/experiments/
$AION_SCP -r experiments/data ${AION}:~/experiments/
$AION_SCP experiments/configs/experiment.conf ${AION}:~/experiments/configs/
```

## Generate the 5 GB Dataset

Run on Aion.

```bash
salloc -p batch -N 1 -t 01:00:00
module load env/development/2024a

mkdir -p ~/pipeline_data
python3 ~/experiments/data/generate_eea_dataset.py \
  --output ~/pipeline_data/eea_airquality_5gb.csv \
  --size-gb 5 \
  --seed 42
```

## Configure Cloud Endpoints

Edit `~/experiments/5gb/configs/experiment_5gb.conf` on Aion.

```bash
EC2_US_EAST_IP="<YOUR_US_EAST_IP>"
EC2_FRANKFURT_IP="<YOUR_FRANKFURT_IP>"
SSH_KEY_US="$HOME/.ssh/pipeline-useast-key.pem"
SSH_KEY_FRA="$HOME/.ssh/pipeline-frankfurt-key.pem"
```

## Start the 5 GB Spark Cluster

```bash
sbatch ~/experiments/5gb/scripts/deploy_spark_5gb.sh
squeue -u "$USER"
cat ~/coordinatorNode
ssh "$(cat ~/coordinatorNode)"
```

## Run the Five Approaches

Run from the coordinator node.

```bash
bash ~/experiments/5gb/scripts/approach1_cloud_side.sh
bash ~/experiments/5gb/scripts/approach2_direct_producer.sh
bash ~/experiments/5gb/scripts/approach3_hpc_side.sh
bash ~/experiments/5gb/scripts/approach4_mm2.sh
bash ~/experiments/5gb/scripts/approach5_skyhost.sh
```

Approach 5 pauses and prints the exact SkyHOST command that must be run from the client machine.

## Collect Results

```bash
scp -P 8022 -r <HPC_USER>@<HPC_LOGIN_HOST>:~/5gb_results/ \
  ./experiments/results/5gb/
```

The cleaned result CSVs used by the paper are stored in `experiments/results/5gb/`.
