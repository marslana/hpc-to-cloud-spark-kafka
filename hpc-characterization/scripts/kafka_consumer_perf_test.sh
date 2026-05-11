#!/bin/bash
# kafka_consumer_perf_test.sh - Minimal Kafka consumer throughput testing script

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

# Test configuration - same as producer tests
NUM_PARTITIONS=(1 2 4 8)
MESSAGE_SIZES=(100 1000 10000)

# Define number of records to consume based on message size
declare -A NUM_MESSAGES
NUM_MESSAGES[100]=10000000  # 10M for 100 bytes
NUM_MESSAGES[1000]=1000000  # 1M for 1000 bytes
NUM_MESSAGES[10000]=100000  # 100K for 10000 bytes

# Results file
RESULTS_DIR="$HOME/kafka_perf_results/kafka_consumer_18_0109"
mkdir -p $RESULTS_DIR
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$RESULTS_DIR/kafka_consumer_throughput_results_$TIMESTAMP.csv"

# Create results file header
echo "partitions,message_size_bytes,records_per_sec,mb_per_sec,rebalance_time_ms,fetch_time_ms" > $RESULTS_FILE

# Function to run consumer throughput test
run_consumer_throughput_test() {
    local partitions=$1
    local msg_size=$2
    local num_records=${NUM_MESSAGES[$msg_size]}
    
    echo "Running consumer throughput test: partitions=$partitions, msg_size=$msg_size bytes"
    
    # Create a unique topic for this test
    local unique_topic="consumer-test-${msg_size}b-${partitions}p-$(date +%s)"
    
    # Create topic with specified partitions
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --create --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS --partitions $partitions --replication-factor 1 \
        --config retention.ms=3600000 > /dev/null 2>&1
    
    echo "Created topic $unique_topic."
    
    # Wait for topic creation to complete
    sleep 2
    
    # Produce data to the topic first
    echo "Producing data to topic $unique_topic..."
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic $unique_topic \
        --num-records $num_records \
        --record-size $msg_size \
        --throughput 100000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=1 > /dev/null 2>&1
    
    # Create a unique consumer group
    local consumer_group="perf-consumer-group-${TIMESTAMP}-${partitions}p-${msg_size}b"
    
    # Run consumer performance test
    local consumer_output_file="$RESULTS_DIR/consumer_${partitions}p_${msg_size}b_${TIMESTAMP}.log"
    echo "Starting consumer test, output will be saved to $consumer_output_file"
    
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-consumer-perf-test.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --topic $unique_topic \
        --group $consumer_group \
        --messages $num_records \
        --show-detailed-stats \
        --reporting-interval 1000 \
        > "$consumer_output_file" 2>&1
    
    # Extract metrics from the output - using the last line for final results
    # The format is: time, threadId, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
    local last_line=$(grep -v "^time" "$consumer_output_file" | tail -1)
    
    if [ -n "$last_line" ]; then
        # Parse the last line using awk to extract the metrics
        local mb_per_sec=$(echo "$last_line" | awk -F, '{print $4}' | tr -d ' ')
        local records_per_sec=$(echo "$last_line" | awk -F, '{print $6}' | tr -d ' ')
        local rebalance_time=$(echo "$last_line" | awk -F, '{print $7}' | tr -d ' ')
        local fetch_time=$(echo "$last_line" | awk -F, '{print $8}' | tr -d ' ')
        
        # Save results to CSV
        echo "$partitions,$msg_size,$records_per_sec,$mb_per_sec,$rebalance_time,$fetch_time" >> "$RESULTS_FILE"
        
        echo "Results: $records_per_sec records/sec, $mb_per_sec MB/sec, rebalance time: $rebalance_time ms, fetch time: $fetch_time ms"
    else
        echo "WARNING: Could not extract metrics from consumer output. Here's the log content:"
        cat "$consumer_output_file"
        echo "$partitions,$msg_size,0,0,0,0" >> "$RESULTS_FILE"
    fi
    
    # Delete topic after test
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --delete --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS > /dev/null 2>&1
    
    # Wait a bit before next test
    sleep 5
}

# Run consumer throughput tests
echo "Running consumer throughput tests..."
for partitions in "${NUM_PARTITIONS[@]}"; do
    for msg_size in "${MESSAGE_SIZES[@]}"; do
        run_consumer_throughput_test $partitions $msg_size
    done
done

echo "All Kafka consumer throughput tests completed. Results saved to $RESULTS_FILE"

# Generate a summary report
echo "Generating summary report..."
SUMMARY_FILE="$RESULTS_DIR/kafka_consumer_throughput_summary_$TIMESTAMP.txt"

{
    echo "=== Kafka Consumer Throughput Test Summary ==="
    echo "Date: $(date)"
    echo "Bootstrap Servers: $BOOTSTRAP_SERVERS"
    echo ""
    echo "=== Throughput by Message Size and Partition Count ==="
    
    for msg_size in "${MESSAGE_SIZES[@]}"; do
        echo "Message Size: $msg_size bytes"
        echo "----------------------------------------"
        echo "Partitions | Records/sec | MB/sec | Rebalance Time (ms) | Fetch Time (ms)"
        echo "----------------------------------------"
        grep "^[0-9]*,$msg_size," "$RESULTS_FILE" | sort -t, -k1,1n | 
        awk -F, '{printf "%-10s | %-11s | %-6s | %-18s | %-14s\n", $1, $3, $4, $5, $6}'
        echo ""
    done
    
    echo "=== Best Configurations ==="
    echo "Highest Throughput (records/sec): $(sort -t, -k3,3nr "$RESULTS_FILE" | head -1 | awk -F, '{print $3 " records/sec with partitions=" $1 ", msg_size=" $2 "b"}')"
    echo "Highest Throughput (MB/sec): $(sort -t, -k4,4nr "$RESULTS_FILE" | head -1 | awk -F, '{print $4 " MB/sec with partitions=" $1 ", msg_size=" $2 "b"}')"
    echo ""
    echo "Full results available in: $RESULTS_FILE"
} > "$SUMMARY_FILE"

echo "Summary report saved to $SUMMARY_FILE"
