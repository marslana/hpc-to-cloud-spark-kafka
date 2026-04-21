#!/bin/bash
# kafka_perf_test.sh - Minimal Kafka producer throughput testing script

# Enable error handling
set -e  # Exit on error
set -u  # Exit on undefined variable
set -o pipefail  # Exit on pipe failure

# Function to handle errors
handle_error() {
    local line=$1
    local command=$2
    local code=$3
    echo "ERROR: Command '$command' failed with exit code $code at line $line"
}

trap 'handle_error ${LINENO} "$BASH_COMMAND" $?' ERR

# Configuration parameters
COORDINATOR_NODE=$(cat $HOME/coordinatorNode)
echo "Coordinator node: $COORDINATOR_NODE"

# Get broker information from ZooKeeper
echo "Retrieving broker information from ZooKeeper..."
BROKER_INFO=$(singularity exec instance://shinst_master /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids | grep '\[' | sed 's/\[//' | sed 's/\]//' | sed 's/,/ /g')
echo "Broker IDs: $BROKER_INFO"

# Build bootstrap servers string
BOOTSTRAP_SERVERS=""
for BROKER_ID in $BROKER_INFO; do
    BROKER_DATA=$(singularity exec instance://shinst_master /opt/kafka/bin/zookeeper-shell.sh localhost:2181 get /brokers/ids/$BROKER_ID | grep -v "^$" | tail -1)
    BROKER_HOST=$(echo $BROKER_DATA | grep -o '"host":"[^"]*"' | cut -d'"' -f4)
    BROKER_PORT=$(echo $BROKER_DATA | grep -o '"port":[0-9]*' | cut -d':' -f2)
    
    if [ -n "$BOOTSTRAP_SERVERS" ]; then
        BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVERS,$BROKER_HOST:$BROKER_PORT"
    else
        BOOTSTRAP_SERVERS="$BROKER_HOST:$BROKER_PORT"
    fi
done

echo "Bootstrap servers: $BOOTSTRAP_SERVERS"

# Test configuration
NUM_PARTITIONS=(1 2 4 8)
MESSAGE_SIZES=(100 1000 10000)
PRODUCER_ACKS=("1" "all")

# Define number of records based on message size
declare -A NUM_MESSAGES
NUM_MESSAGES[100]=10000000  # 10M for 100 bytes
NUM_MESSAGES[1000]=1000000  # 1M for 1000 bytes
NUM_MESSAGES[10000]=100000  # 100K for 10000 bytes

# Results file
RESULTS_DIR="$HOME/kafka_perf_results/kafka_producer_18_0109"
mkdir -p $RESULTS_DIR
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$RESULTS_DIR/kafka_max_throughput_results_$TIMESTAMP.csv"

# Create results file header
echo "partitions,message_size_bytes,producer_acks,records_per_sec,mb_per_sec,avg_latency_ms,max_latency_ms" > $RESULTS_FILE

# Function to run maximum throughput test
run_max_throughput_test() {
    local partitions=$1
    local msg_size=$2
    local acks=$3
    local num_records=${NUM_MESSAGES[$msg_size]}
    
    echo "Running max throughput test: partitions=$partitions, msg_size=$msg_size bytes, acks=$acks, records=$num_records"
    
    # Create a unique topic for this test
    local unique_topic="perf-test-${msg_size}b-${partitions}p-$(date +%s)"
    
    # Create topic with specified partitions
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --create --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS --partitions $partitions --replication-factor 1 \
        --config retention.ms=3600000 > /dev/null 2>&1
    
    echo "Created topic $unique_topic."
    
    # Wait for topic creation to complete
    sleep 2
    
    # Run producer performance test with throughput -1 (max throughput)
    local producer_output_file="$RESULTS_DIR/producer_${partitions}p_${msg_size}b_${acks}_${TIMESTAMP}.log"
    echo "Starting test, output will be saved to $producer_output_file"
    
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic $unique_topic \
        --num-records $num_records \
        --record-size $msg_size \
        --throughput -1 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS \
                         buffer.memory=67108864 \
                         batch.size=16384 \
                         linger.ms=0 \
                         compression.type=none \
                         acks=$acks \
        --print-metrics > "$producer_output_file" 2>&1
    
    # Extract metrics from the last few lines (more reliable)
    local records_per_sec=$(grep -oP '\d+\.\d+ records/sec' "$producer_output_file" | tail -5 | awk '{sum+=$1} END {print sum/NR}')
    local mb_per_sec=$(grep -oP '\d+\.\d+ MB/sec' "$producer_output_file" | tail -5 | awk '{sum+=$1} END {print sum/NR}')
    local avg_latency=$(grep -oP '\d+\.\d+ ms avg latency' "$producer_output_file" | tail -5 | awk '{sum+=$1} END {print sum/NR}')
    local max_latency=$(grep -oP '\d+\.\d+ ms max latency' "$producer_output_file" | tail -5 | awk '{max=0; for(i=1;i<=NR;i++){if($i>max){max=$i}}} END {print max}')
    
    # Verify we have valid metrics
    if [ -z "$records_per_sec" ] || [ -z "$mb_per_sec" ]; then
        echo "WARNING: Could not extract metrics from producer output. Here's the log content:"
        cat "$producer_output_file"
        records_per_sec="0"
        mb_per_sec="0"
        avg_latency="0"
        max_latency="0"
    fi
    
    # Save results to CSV
    echo "$partitions,$msg_size,$acks,$records_per_sec,$mb_per_sec,$avg_latency,$max_latency" >> "$RESULTS_FILE"
    
    echo "Results: $records_per_sec records/sec, $mb_per_sec MB/sec, $avg_latency ms avg latency"
    
    # Delete topic after test
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --delete --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS > /dev/null 2>&1
    
    # Wait a bit before next test
    sleep 5
}

# Run maximum throughput tests
echo "Running maximum throughput tests..."
for partitions in "${NUM_PARTITIONS[@]}"; do
    for msg_size in "${MESSAGE_SIZES[@]}"; do
        for acks in "${PRODUCER_ACKS[@]}"; do
            run_max_throughput_test $partitions $msg_size $acks
        done
    done
done

echo "All Kafka maximum throughput tests completed. Results saved to $RESULTS_FILE"

# Generate a summary report
echo "Generating summary report..."
SUMMARY_FILE="$RESULTS_DIR/kafka_max_throughput_summary_$TIMESTAMP.txt"

{
    echo "=== Kafka Maximum Throughput Test Summary ==="
    echo "Date: $(date)"
    echo "Bootstrap Servers: $BOOTSTRAP_SERVERS"
    echo ""
    echo "=== Throughput by Message Size and Partition Count ==="
    
    for msg_size in "${MESSAGE_SIZES[@]}"; do
        echo "Message Size: $msg_size bytes"
        echo "----------------------------------------"
        echo "Partitions | Acks | Records/sec | MB/sec | Avg Latency (ms)"
        echo "----------------------------------------"
        grep "^[0-9]*,$msg_size," "$RESULTS_FILE" | sort -t, -k1,1n -k3,3 | 
        awk -F, '{printf "%-10s | %-4s | %-11s | %-6s | %-15s\n", $1, $3, $4, $5, $6}'
        echo ""
    done
    
    echo "=== Best Configurations ==="
    echo "Highest Throughput (records/sec): $(sort -t, -k4,4nr "$RESULTS_FILE" | head -1 | awk -F, '{print $4 " records/sec with partitions=" $1 ", msg_size=" $2 "b, acks=" $3}')"
    echo "Highest Throughput (MB/sec): $(sort -t, -k5,5nr "$RESULTS_FILE" | head -1 | awk -F, '{print $5 " MB/sec with partitions=" $1 ", msg_size=" $2 "b, acks=" $3}')"
    echo "Lowest Latency: $(sort -t, -k6,6n "$RESULTS_FILE" | head -1 | awk -F, '{print $6 " ms with partitions=" $1 ", msg_size=" $2 "b, acks=" $3}')"
    echo ""
    echo "Full results available in: $RESULTS_FILE"
} > "$SUMMARY_FILE"

echo "Summary report saved to $SUMMARY_FILE"
