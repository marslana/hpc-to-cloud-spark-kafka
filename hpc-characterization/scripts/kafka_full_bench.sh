#!/bin/bash
# kafka_perf_test.sh - Comprehensive Kafka performance testing script

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
TEST_TOPIC="perf-test-$(date +%s)"
NUM_PARTITIONS=(1 2 4 8)
MESSAGE_SIZES=(100 1024 10240)  # bytes
NUM_MESSAGES=100000  # Messages per test
PRODUCER_THROUGHPUT=(100000 500000 1000000)  # Messages per second
PRODUCER_ACKS=("1" "all")
CONSUMER_GROUPS=(1 2)

# Debug mode - set to true to run a single test
# DEBUG_MODE=true

# Results file
RESULTS_DIR="$HOME/kafka_perf_results"
mkdir -p $RESULTS_DIR
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$RESULTS_DIR/kafka_perf_results_$TIMESTAMP.csv"

# Create results file header
echo "test_type,partitions,message_size_bytes,producer_throughput,producer_acks,consumer_groups,records_per_sec,mb_per_sec,avg_latency_ms,max_latency_ms,test_duration_sec" > $RESULTS_FILE

# Function to verify data flow in Kafka using consumer group lag
verify_kafka_data_flow() {
    local topic=$1
    local expected_count=$2
    
    # Check if topic exists
    local topic_exists=$(singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --list \
        --bootstrap-server $BOOTSTRAP_SERVERS | grep -c "^$topic$")
    
    if [ "$topic_exists" -eq 0 ]; then
        echo "ERROR: Topic $topic does not exist!"
        return 1
    fi
    
    # Use kafka-consumer-groups.sh to check if messages are available
    # First create a temporary consumer group
    local temp_group="temp-verify-group-$(date +%s)"
    
    # Consume a few messages to register the consumer group
    echo "Consuming a few messages to verify topic has data..."
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --topic $topic \
        --group $temp_group \
        --from-beginning \
        --max-messages 5 > /dev/null 2>&1 || true
    
    # Check consumer group lag
    echo "Checking consumer group lag to verify message count..."
    local has_data=$(singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --describe --group $temp_group 2>/dev/null | grep -c $topic || echo "0")
    
    if [ "$has_data" -gt 0 ]; then
        echo "  ✓ Topic $topic has data"
        return 0
    else
        echo "  ✗ Topic $topic has no data"
        return 1
    fi
}

# Function to run producer performance test
run_producer_test() {
    local partitions=$1
    local msg_size=$2
    local throughput=$3
    local acks=$4
    
    echo "Running producer test: partitions=$partitions, msg_size=$msg_size bytes, throughput=$throughput, acks=$acks"
    
    # Create a unique topic for this test
    local unique_topic="${TEST_TOPIC}-prod-${partitions}-${msg_size}-${throughput}-${acks}"
    
    # Check if topic already exists
    local topic_exists=$(singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --list \
        --bootstrap-server $BOOTSTRAP_SERVERS | grep -c "^$unique_topic$")
    
    if [ "$topic_exists" -eq 0 ]; then
        # Create topic with specified partitions (redirect output to avoid duplication)
        singularity exec instance://shinst_master \
            /opt/kafka/bin/kafka-topics.sh --create --topic $unique_topic \
            --bootstrap-server $BOOTSTRAP_SERVERS --partitions $partitions --replication-factor 1 \
            --config retention.ms=3600000 > /dev/null 2>&1
        
        echo "Created topic $unique_topic."
    else
        echo "Topic $unique_topic already exists."
    fi
    
    # Run producer performance test
    local producer_output_file="$RESULTS_DIR/producer_${partitions}_${msg_size}_${throughput}_${acks}.log"
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic $unique_topic \
        --num-records $NUM_MESSAGES \
        --record-size $msg_size \
        --throughput $throughput \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=$acks \
        --print-metrics > "$producer_output_file" 2>&1
    
    # Extract metrics more carefully
    local records_per_sec=$(grep -oP '\d+\.\d+ records/sec' "$producer_output_file" | head -1 | awk '{print $1}')
    local mb_per_sec=$(grep -oP '\d+\.\d+ MB/sec' "$producer_output_file" | head -1 | awk '{print $1}')
    local avg_latency=$(grep -oP '\d+\.\d+ ms avg latency' "$producer_output_file" | head -1 | awk '{print $1}')
    local max_latency=$(grep -oP '\d+\.\d+ ms max latency' "$producer_output_file" | head -1 | awk '{print $1}')
    
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
    echo "producer,$partitions,$msg_size,$throughput,$acks,N/A,$records_per_sec,$mb_per_sec,$avg_latency,$max_latency," >> "$RESULTS_FILE"
    
    # Verify data was produced
    verify_kafka_data_flow "$unique_topic" "$NUM_MESSAGES"
    
    # Delete topic after test
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --delete --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS > /dev/null 2>&1
    
    # Wait a bit before next test
    sleep 5
}

# Function to run consumer performance test
run_consumer_test() {
    local partitions=$1
    local msg_size=$2
    local consumer_groups=$3
    
    echo "Running consumer test: partitions=$partitions, msg_size=$msg_size bytes, consumer_groups=$consumer_groups"
    
    # Create a unique topic for this test
    local unique_topic="${TEST_TOPIC}-cons-${partitions}-${msg_size}-${consumer_groups}"
    
    # Check if topic already exists
    local topic_exists=$(singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --list \
        --bootstrap-server $BOOTSTRAP_SERVERS | grep -c "^$unique_topic$")
    
    if [ "$topic_exists" -eq 0 ]; then
        # Create topic with specified partitions
        singularity exec instance://shinst_master \
            /opt/kafka/bin/kafka-topics.sh --create --topic $unique_topic \
            --bootstrap-server $BOOTSTRAP_SERVERS --partitions $partitions --replication-factor 1 \
            --config retention.ms=3600000 > /dev/null 2>&1
        
        echo "Created topic $unique_topic."
    else
        echo "Topic $unique_topic already exists."
    fi
    
    # Produce messages first
    echo "Producing messages for consumer test..."
    local producer_output=$(singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic $unique_topic \
        --num-records $NUM_MESSAGES \
        --record-size $msg_size \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=1)
    
    echo "Producer output: $producer_output"
    
    # Verify data was produced
    verify_kafka_data_flow "$unique_topic" "$NUM_MESSAGES"
    
    # Run consumer performance test for each consumer group
    for ((i=1; i<=consumer_groups; i++)); do
        local group_id="perf-consumer-group-${i}-$(date +%s)"
        echo "Running consumer test for group $group_id..."
        
        # Run consumer test with timeout and save output to file
        local consumer_log="$RESULTS_DIR/consumer_${partitions}_${msg_size}_${i}.log"
        timeout 300 singularity exec instance://shinst_master \
            /opt/kafka/bin/kafka-consumer-perf-test.sh \
            --bootstrap-server $BOOTSTRAP_SERVERS \
            --topic $unique_topic \
            --messages $NUM_MESSAGES \
            --group $group_id \
            --print-metrics > "$consumer_log" 2>&1
        
        local status=$?
        
        if [ $status -eq 0 ]; then
            echo "Consumer test completed successfully"
            
            # Extract metrics from the first line of output (after the header)
            local metrics=$(grep -A 1 "start.time" "$consumer_log" | tail -1)
            
            if [ -n "$metrics" ]; then
                # Parse metrics
                local mb_sec=$(echo "$metrics" | awk -F, '{print $4}' | tr -d ' ')
                local records_sec=$(echo "$metrics" | awk -F, '{print $6}' | tr -d ' ')
                
                # Save results to CSV
                echo "consumer,$partitions,$msg_size,N/A,N/A,$i,$records_sec,$mb_sec,N/A,N/A,N/A" >> "$RESULTS_FILE"
                
                echo "Consumer test results: $records_sec records/sec, $mb_sec MB/sec"
            else
                echo "WARNING: Could not extract metrics from consumer output"
                echo "consumer,$partitions,$msg_size,N/A,N/A,$i,0,0,N/A,N/A,N/A" >> "$RESULTS_FILE"
            fi
        else
            echo "Consumer test failed with status $status"
            echo "consumer,$partitions,$msg_size,N/A,N/A,$i,0,0,N/A,N/A,N/A" >> "$RESULTS_FILE"
            echo "Consumer log:"
            cat "$consumer_log"
        fi
    done
    
    # Delete topic after test
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --delete --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS > /dev/null 2>&1
    
    # Wait a bit before next test
    sleep 5
}

# Function to run end-to-end performance test
run_e2e_test() {
    local partitions=$1
    local msg_size=$2
    local consumer_groups=$3
    
    echo "Running end-to-end test: partitions=$partitions, msg_size=$msg_size bytes, consumer_groups=$consumer_groups"
    
    # Create a unique topic for this test
    local unique_topic="${TEST_TOPIC}-e2e-${partitions}-${msg_size}-${consumer_groups}"
    
    # Check if topic already exists
    local topic_exists=$(singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --list \
        --bootstrap-server $BOOTSTRAP_SERVERS | grep -c "^$unique_topic$")
    
    if [ "$topic_exists" -eq 0 ]; then
        # Create topic with specified partitions
        singularity exec instance://shinst_master \
            /opt/kafka/bin/kafka-topics.sh --create --topic $unique_topic \
            --bootstrap-server $BOOTSTRAP_SERVERS --partitions $partitions --replication-factor 1 \
            --config retention.ms=3600000 > /dev/null 2>&1
        
        echo "Created topic $unique_topic."
    else
        echo "Topic $unique_topic already exists."
    fi
    
    # Start consumer processes in background
    local consumer_pids=()
    local consumer_logs=()
    
    for ((i=1; i<=consumer_groups; i++)); do
        local group_id="perf-e2e-consumer-group-${i}-$(date +%s)"
        local consumer_log="$RESULTS_DIR/e2e_consumer_${partitions}_${i}_${msg_size}.log"
        consumer_logs+=("$consumer_log")
        
        echo "Starting consumer $i with group $group_id..."
        
        # Start consumer in background
        singularity exec instance://shinst_master \
            /opt/kafka/bin/kafka-consumer-perf-test.sh \
            --bootstrap-server $BOOTSTRAP_SERVERS \
            --topic $unique_topic \
            --messages $NUM_MESSAGES \
            --group $group_id \
            --print-metrics > "$consumer_log" 2>&1 &
        
        consumer_pids+=($!)
        echo "Started consumer $i with PID ${consumer_pids[-1]}"
    done
    
    # Wait a bit for consumers to start
    sleep 5
    
    # Run producer performance test
    echo "Running producer for end-to-end test..."
    local producer_output_file="$RESULTS_DIR/e2e_producer_${partitions}_${consumer_groups}_${msg_size}.log"
    
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic $unique_topic \
        --num-records $NUM_MESSAGES \
        --record-size $msg_size \
        --throughput 1000000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=1 \
        --print-metrics > "$producer_output_file" 2>&1
    
    # Extract producer metrics
    local records_per_sec=$(grep -oP '\d+\.\d+ records/sec' "$producer_output_file" | head -1 | awk '{print $1}')
    local mb_per_sec=$(grep -oP '\d+\.\d+ MB/sec' "$producer_output_file" | head -1 | awk '{print $1}')
    local avg_latency=$(grep -oP '\d+\.\d+ ms avg latency' "$producer_output_file" | head -1 | awk '{print $1}')
    local max_latency=$(grep -oP '\d+\.\d+ ms max latency' "$producer_output_file" | head -1 | awk '{print $1}')
    
    # Save producer results
    echo "e2e-producer,$partitions,$msg_size,1000000,1,$consumer_groups,$records_per_sec,$mb_per_sec,$avg_latency,$max_latency," >> "$RESULTS_FILE"
    
    # Wait for consumers to finish (with timeout)
    echo "Waiting for consumers to finish..."
    local timeout=300
    local start_time=$(date +%s)
    local all_finished=false
    
    while [ $(($(date +%s) - start_time)) -lt $timeout ]; do
        all_finished=true
        for pid in "${consumer_pids[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                all_finished=false
                break
            fi
        done
        
        if $all_finished; then
            break
        fi
        
        sleep 5
    done
    
    # Kill any remaining consumer processes
    for pid in "${consumer_pids[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            echo "Killing consumer process $pid..."
            kill $pid
        fi
    done
    
    # Process consumer results
    for ((i=0; i<${#consumer_logs[@]}; i++)); do
        local consumer_log="${consumer_logs[$i]}"
        local consumer_num=$((i+1))
        
        echo "Processing consumer $consumer_num results..."
        
        # Extract metrics from the first line of output (after the header)
        local metrics=$(grep -A 1 "start.time" "$consumer_log" | tail -1)
        
        if [ -n "$metrics" ]; then
            # Parse metrics
            local mb_sec=$(echo "$metrics" | awk -F, '{print $4}' | tr -d ' ')
            local records_sec=$(echo "$metrics" | awk -F, '{print $6}' | tr -d ' ')
            
            # Save results to CSV
            echo "e2e-consumer,$partitions,$msg_size,N/A,N/A,$consumer_num,$records_sec,$mb_sec,N/A,N/A,N/A" >> "$RESULTS_FILE"
            
            echo "Consumer $consumer_num test results: $records_sec records/sec, $mb_sec MB/sec"
        else
            echo "WARNING: Could not extract metrics from consumer $consumer_num output"
            echo "e2e-consumer,$partitions,$msg_size,N/A,N/A,$consumer_num,0,0,N/A,N/A,N/A" >> "$RESULTS_FILE"
        fi
    done
    
    # Delete topic after test
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --delete --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS > /dev/null 2>&1
    
    # Wait a bit before next test
    sleep 5
}

# Debug mode - run a single test
if [ "${DEBUG_MODE:-false}" = true ]; then
    echo "=== RUNNING IN DEBUG MODE ==="
    
    # Create a debug topic
    DEBUG_TOPIC="kafka-debug-test-$(date +%s)"
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --create --topic $DEBUG_TOPIC \
        --bootstrap-server $BOOTSTRAP_SERVERS --partitions 1 --replication-factor 1 > /dev/null 2>&1
    
    echo "Created topic $DEBUG_TOPIC."
    
    # Run a simple producer test
    echo "Producing 1000 messages to $DEBUG_TOPIC..."
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic $DEBUG_TOPIC \
        --num-records 1000 \
        --record-size 100 \
        --throughput 1000 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS acks=1
    
    # Verify data flow
    verify_kafka_data_flow "$DEBUG_TOPIC" 1000
    
    # Debug consumer issues
    echo "=== DEBUGGING CONSUMER ISSUES ==="
    
    # Check if topic exists and has data
    echo "1. Checking topic $DEBUG_TOPIC..."
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --describe \
        --bootstrap-server $BOOTSTRAP_SERVERS --topic $DEBUG_TOPIC
    
    # Try consuming a few messages manually
    echo "2. Attempting to consume 5 messages manually..."
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --topic $DEBUG_TOPIC \
        --from-beginning \
        --max-messages 5
    
    # Check consumer groups
    echo "3. Checking consumer groups..."
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --list
    
    # Run a simple consumer test with verbose output
    echo "4. Running a simple consumer test with verbose output..."
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-consumer-perf-test.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --topic $DEBUG_TOPIC \
        --messages 500 \
        --print-metrics
    
    echo "=== END DEBUGGING ==="
    
    echo "Debug test completed. Exiting."
    exit 0
fi

# Run producer tests
echo "Running producer tests..."
for partitions in "${NUM_PARTITIONS[@]}"; do
    for msg_size in "${MESSAGE_SIZES[@]}"; do
        for throughput in "${PRODUCER_THROUGHPUT[@]}"; do
            for acks in "${PRODUCER_ACKS[@]}"; do
                run_producer_test $partitions $msg_size $throughput $acks
            done
        done
    done
done

# Run consumer tests
echo "Running consumer tests..."
for partitions in "${NUM_PARTITIONS[@]}"; do
    for msg_size in "${MESSAGE_SIZES[@]}"; do
        for consumer_groups in "${CONSUMER_GROUPS[@]}"; do
            run_consumer_test $partitions $msg_size $consumer_groups
        done
    done
done

# Run end-to-end tests
echo "Running end-to-end tests..."
for partitions in "${NUM_PARTITIONS[@]}"; do
    for msg_size in "${MESSAGE_SIZES[@]}"; do
        for consumer_groups in "${CONSUMER_GROUPS[@]}"; do
            run_e2e_test $partitions $msg_size $consumer_groups
        done
    done
done

echo "All Kafka performance tests completed. Results saved to $RESULTS_FILE"

# Fix any potential CSV formatting issues
echo "Fixing CSV formatting..."
cat "$RESULTS_FILE" | tr -d '\n' | sed 's/producer,/\nproducer,/g' | sed 's/consumer,/\nconsumer,/g' | sed 's/e2e-producer,/\ne2e-producer,/g' | sed 's/e2e-consumer,/\ne2e-consumer,/g' | sed '1s/^/test_type,partitions,message_size_bytes,producer_throughput,producer_acks,consumer_groups,records_per_sec,mb_per_sec,avg_latency_ms,max_latency_ms,test_duration_sec\n/' > "${RESULTS_FILE}.fixed"

echo "Fixed CSV file saved to ${RESULTS_FILE}.fixed"
