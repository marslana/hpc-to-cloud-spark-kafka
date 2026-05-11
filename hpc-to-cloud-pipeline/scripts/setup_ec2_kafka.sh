#!/bin/bash
# setup_ec2_kafka.sh — Set up Kafka + Spark on an EC2 m5.8xlarge instance
#
# Run this ON the EC2 instance after SSH'ing in.
# Works for both us-east-1 and eu-central-1 instances.
#
# Usage:
#   ssh -i key.pem ubuntu@<EC2_IP>
#   bash setup_ec2_kafka.sh

set -euo pipefail

echo "=== EC2 Kafka + Spark Setup ==="
echo "Instance: $(curl -s http://169.254.169.254/latest/meta-data/instance-type)"
echo "Region:   $(curl -s http://169.254.169.254/latest/meta-data/placement/region)"
echo ""

# ── Docker ───────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    echo "Installing Docker..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq docker.io
    sudo usermod -aG docker $USER
    echo "Docker installed. You may need to re-login for group changes."
fi
sudo systemctl start docker

# ── Kafka via Docker ─────────────────────────────────────────
echo "Starting Kafka (KRaft mode, single broker)..."

PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)
PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)

# Stop existing container if running
docker rm -f kafka 2>/dev/null || true

docker run -d --name kafka \
    --network host \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" \
    -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${PUBLIC_IP}:9092" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093" \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_LOG_RETENTION_HOURS=2 \
    -e KAFKA_MESSAGE_MAX_BYTES=11000000 \
    -e KAFKA_REPLICA_FETCH_MAX_BYTES=11000000 \
    -e CLUSTER_ID="$(docker run --rm apache/kafka:3.7.0 kafka-storage.sh random-uuid)" \
    apache/kafka:3.7.0

echo "Waiting for Kafka to start..."
for i in $(seq 1 30); do
    if docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list &>/dev/null; then
        echo "Kafka is ready."
        break
    fi
    [ $i -eq 30 ] && { echo "ERROR: Kafka failed to start"; exit 1; }
    sleep 5
done

# ── Java + Spark (for Approach 4) ────────────────────────────
if ! command -v java &>/dev/null; then
    echo "Installing Java 11..."
    sudo apt-get install -y -qq openjdk-11-jdk-headless
fi

if [ ! -d /opt/spark ]; then
    echo "Installing Spark 3.4..."
    cd /tmp
    wget -q https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
    sudo tar -xzf spark-3.4.0-bin-hadoop3.tgz -C /opt/
    sudo ln -sf /opt/spark-3.4.0-bin-hadoop3 /opt/spark
    echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
    echo 'export PATH=$PATH:/opt/spark/bin' >> ~/.bashrc
fi
export SPARK_HOME=/opt/spark
export PATH=$PATH:/opt/spark/bin

# ── Python dependencies ───────────────────────────────────────
pip3 install kafka-python confluent-kafka 2>/dev/null || {
    sudo apt-get install -y python3-pip
    pip3 install kafka-python confluent-kafka
}

echo ""
echo "=== Setup Complete ==="
echo "Kafka broker:  ${PUBLIC_IP}:9092"
echo "Spark:         $(spark-submit --version 2>&1 | head -1 || echo 'installed')"
echo ""
echo "Test with:"
echo "  docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list"
