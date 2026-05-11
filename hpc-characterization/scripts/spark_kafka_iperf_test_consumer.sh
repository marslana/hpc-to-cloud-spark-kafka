#!/bin/bash
# spark_kafka_iperf_test_consumer.sh - Test Spark as consumer from Kafka (without HDFS)

# Enable error handling
set -e
set -u
set -o pipefail

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
SPARK_MASTER="spark://$COORDINATOR_NODE:7078"
echo "Spark master: $SPARK_MASTER"

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

# Test configuration - full test suite
NUM_PARTITIONS=(1 2 4 8)
RECORD_SIZES=(100 1000 10000)
BATCH_SIZES=("0" "16384")  # No batching and 16K

# Define number of records based on record size - SAME AS PRODUCER
declare -A NUM_RECORDS
NUM_RECORDS[100]=10000000  # 10M for 100 bytes
NUM_RECORDS[1000]=1000000  # 1M for 1000 bytes
NUM_RECORDS[10000]=100000  # 100K for 10000 bytes

# Base directory for results - use home directory which should be accessible
SHARED_DATA_DIR="$HOME/data"
mkdir -p $SHARED_DATA_DIR

RESULTS_DIR="$SHARED_DATA_DIR/spark_kafka_test_$(date +%Y%m%d_%H%M)"
mkdir -p $RESULTS_DIR
RESULTS_FILE="$RESULTS_DIR/spark_consumer_results.csv"

# Create results file header
echo "partitions,record_size_bytes,batch_size,records_per_sec,mb_per_sec,duration_sec,total_records" > $RESULTS_FILE

# Function to generate test data directly - same as producer
generate_test_data() {
    local output_file=$1
    local num_records=$2
    local record_size=$3
    
    echo "Generating $num_records records of approximately $record_size bytes each..."
    
    # Create a Python script in the shared data directory
    local data_gen_script="$RESULTS_DIR/generate_test_data.py"
    cat > "$data_gen_script" << 'EOF'
#!/usr/bin/env python3
import sys
import random
import string
import json
import time
import os
import multiprocessing

def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def generate_record(record_size):
    # Create a record with random data of approximately record_size bytes
    record = {
        'id': random.randint(1, 1000000),
        'timestamp': int(time.time()),
        'data': generate_random_string(record_size - 60)  # Adjust for JSON overhead
    }
    return record

def worker_function(args):
    start_idx, end_idx, record_size, output_file = args
    records = []
    for i in range(start_idx, end_idx):
        records.append(generate_record(record_size))
    
    with open(f"{output_file}.part{start_idx}", 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')
    
    return end_idx - start_idx

def generate_test_data(file_path, num_records, record_size_bytes):
    # Use multiprocessing to speed up data generation
    cpu_count = multiprocessing.cpu_count()
    chunk_size = max(1, num_records // cpu_count)
    
    args_list = []
    for i in range(0, num_records, chunk_size):
        end_idx = min(i + chunk_size, num_records)
        args_list.append((i, end_idx, record_size_bytes, file_path))
    
    with multiprocessing.Pool(processes=cpu_count) as pool:
        results = []
        for i, result in enumerate(pool.imap_unordered(worker_function, args_list)):
            results.append(result)
            if (i+1) % max(1, len(args_list)//10) == 0:
                print(f"Generated {sum(results)} records of {num_records}...")
    
    # Combine all part files
    with open(file_path, 'w') as outfile:
        for i in range(0, num_records, chunk_size):
            part_file = f"{file_path}.part{i}"
            if os.path.exists(part_file):
                with open(part_file, 'r') as infile:
                    outfile.write(infile.read())
                os.remove(part_file)
    
    print(f"Generated {num_records} records, each approximately {record_size_bytes} bytes")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: generate_test_data.py <output_file> <num_records> <record_size_bytes>")
        sys.exit(1)
    
    output_file = sys.argv[1]
    num_records = int(sys.argv[2])
    record_size_bytes = int(sys.argv[3])
    
    generate_test_data(output_file, num_records, record_size_bytes)
EOF
    
    chmod +x "$data_gen_script"
    
    # Run the data generation script
    python3 "$data_gen_script" "$output_file" "$num_records" "$record_size"
}

# Function to run a Spark consumer test
run_spark_consumer_test() {
    local partitions=$1
    local record_size=$2
    local batch_size=$3
    local num_records=${NUM_RECORDS[$record_size]}
    
    # Format batch size for display
    local batch_display
    if [ "$batch_size" == "0" ]; then
        batch_display="nobatch"
    else
        batch_display="${batch_size}b"
    fi
    
    echo "Running Spark consumer test: partitions=$partitions, record_size=$record_size bytes, batch_size=$batch_display, records=$num_records"
    
    # Create a unique topic name for this test
    local unique_topic="spark-perf-test-${record_size}b-${partitions}p-$(date +%s)"
    
    # Create the topic with the specified number of partitions
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --create --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS --partitions $partitions --replication-factor 1
    
    # Verify topic creation
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --describe --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS
    
    # Generate test data
    local test_data_file="$RESULTS_DIR/test_data_${record_size}b_${num_records}.json"
    if [ ! -f "$test_data_file" ]; then
        generate_test_data "$test_data_file" "$num_records" "$record_size"
    else
        echo "Using existing test data file: $test_data_file"
    fi
    
    # Produce data to Kafka using the Kafka producer tool
    echo "Producing data to Kafka topic $unique_topic..."
    cat "$test_data_file" | singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-console-producer.sh --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS > /dev/null 2>&1 &
    
    # Store the producer process ID
    PRODUCER_PID=$!
    
    # Wait for producer to complete with a timeout (10 minutes)
    echo "Waiting for producer to complete (timeout: 600 seconds)..."
    TIMEOUT=600
    START_TIME=$(date +%s)
    while kill -0 $PRODUCER_PID 2>/dev/null; do
        CURRENT_TIME=$(date +%s)
        ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
        if [ $ELAPSED_TIME -gt $TIMEOUT ]; then
            echo "Producer timeout reached. Killing producer process."
            kill $PRODUCER_PID
            break
        fi
        sleep 5
    done
    
    # Verify data was produced - using a different approach
    echo "Verifying data was produced to topic $unique_topic..."
    # Create a temporary consumer group to check message count
    CONSUMER_GROUP="temp-consumer-group-$(date +%s)"
    
    # Use kafka-consumer-groups to get lag information
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP_SERVERS \
        --group $CONSUMER_GROUP --describe --new-consumer > /dev/null 2>&1 || true
    
    # Wait a bit for the topic to be fully populated
    echo "Waiting for topic to be fully populated..."
    sleep 10
    
    # Create Spark script for Kafka consumer test
    local spark_script="$RESULTS_DIR/spark_kafka_consumer.py"
    local consumer_output_file="$RESULTS_DIR/spark_consumer_${partitions}p_${record_size}b_${batch_display}.log"
    
    echo "Starting Spark consumer test, output will be saved to $consumer_output_file"
    
    cat > "$spark_script" << 'EOF'
#!/usr/bin/env python3
import os
import sys
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr

def run_kafka_consumer_test(topic, bootstrap_servers, batch_size, expected_records):
    # Start timing
    start_time = time.time()
    
    # Create Spark session with Kafka
    builder = SparkSession.builder \
        .appName("SparkKafkaConsumer") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.default.parallelism", "16") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "2g")
    
    # Only set batch size if it's not zero (no batching)
    if batch_size != "0":
        builder = builder.config("spark.kafka.consumer.fetchOffset.numRetries", "5") \
                         .config("spark.kafka.consumer.cache.maxCapacity", "64") \
                         .config("spark.kafka.consumer.fetchOffset.retryIntervalMs", "100")
    
    spark = builder.getOrCreate()
    
    # First, verify that data is in the topic
    print(f"Checking for data in topic {topic}...")
    
    # Get offsets information to verify data is available
    offsets_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load() \
        .selectExpr("partition", "CAST(offset AS LONG) as end_offset")
    
    # Calculate total messages
    total_messages = offsets_df.agg({"end_offset": "sum"}).collect()[0][0]
    
    print(f"Topic {topic} contains {total_messages} messages")
    
    if total_messages == 0:
        print("ERROR: No data found in topic. Waiting 30 seconds for data to arrive...")
        time.sleep(30)
        
        # Check again
        total_messages = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load() \
            .selectExpr("partition", "CAST(offset AS LONG) as end_offset") \
            .agg({"end_offset": "sum"}).collect()[0][0]
            
        if total_messages == 0:
            print("ERROR: Still no data in topic. Exiting.")
            return
    
    # Read from Kafka using batch mode (not streaming)
    kafka_read_start = time.time()
    
    print(f"Starting batch consumption from topic {topic}...")
    
    # Read all data from Kafka in batch mode
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse the JSON value
    parsed_df = df.select(
        expr("CAST(value AS STRING)").alias("json_value")
    )
    
    # Count records and force execution
    record_count = parsed_df.count()
    
    kafka_read_end = time.time()
    
    # End timing
    end_time = time.time()
    total_duration = end_time - start_time
    kafka_duration = kafka_read_end - kafka_read_start
    
    # Calculate size (estimate based on record size)
    # Assuming average record size is 1.1x the specified size due to JSON overhead
    record_size_bytes = int(topic.split("-")[2].replace("b", ""))
    estimated_size_bytes = record_count * record_size_bytes * 1.1
    estimated_size_mb = estimated_size_bytes / (1024 * 1024)
    
    # Calculate throughput
    records_per_second = record_count / kafka_duration if kafka_duration > 0 else 0
    mb_per_second = estimated_size_mb / kafka_duration if kafka_duration > 0 else 0
    
    # Print results
    print("RESULTS_START")
    print(json.dumps({
        "records": record_count,
        "expected_records": expected_records,
        "size_mb": estimated_size_mb,
        "duration_seconds": kafka_duration,
        "total_duration_seconds": total_duration,
        "throughput_records_per_second": records_per_second,
        "throughput_mb_per_second": mb_per_second
    }, indent=2))
    print("RESULTS_END")
    
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: spark_kafka_consumer.py <topic> <bootstrap_servers> <batch_size> <expected_records>")
        sys.exit(1)
    
    topic = sys.argv[1]
    bootstrap_servers = sys.argv[2]
    batch_size = sys.argv[3]
    expected_records = int(sys.argv[4])
    
    run_kafka_consumer_test(topic, bootstrap_servers, batch_size, expected_records)
EOF
    
    chmod +x "$spark_script"
    
    # Run the Kafka consumer test
    echo "Running Spark Kafka consumer test..."
    singularity exec \
        --bind $SHARED_DATA_DIR:/opt/shared_data \
        instance://shinst_master \
        spark-submit \
        --master $SPARK_MASTER \
        --conf spark.hadoop.fs.defaultFS=file:/// \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
        "/opt/shared_data/$(basename $RESULTS_DIR)/spark_kafka_consumer.py" \
        "$unique_topic" \
        "$BOOTSTRAP_SERVERS" \
        "$batch_size" \
        "$num_records" > "$consumer_output_file" 2>&1
    
    # Check if the job ran successfully
    if grep -q "RESULTS_START" "$consumer_output_file"; then
        echo "Spark Kafka consumer test completed successfully."
        
        # Extract metrics
        results=$(sed -n '/RESULTS_START/,/RESULTS_END/p' "$consumer_output_file" | grep -v "RESULTS_")
        
        local records_per_sec=$(echo "$results" | grep -o '"throughput_records_per_second": [0-9.]*' | awk '{print $2}' | tr -d ',')
        local mb_per_sec=$(echo "$results" | grep -o '"throughput_mb_per_second": [0-9.]*' | awk '{print $2}' | tr -d ',')
        local duration=$(echo "$results" | grep -o '"duration_seconds": [0-9.]*' | awk '{print $2}' | tr -d ',')
        local total_records=$(echo "$results" | grep -o '"records": [0-9]*' | awk '{print $2}' | tr -d ',')
        
        # Save results to CSV
        echo "$partitions,$record_size,$batch_display,$records_per_sec,$mb_per_sec,$duration,$total_records" >> "$RESULTS_FILE"
        
        echo "Results: $records_per_sec records/sec, $mb_per_sec MB/sec, duration: $duration sec"
    else
        echo "ERROR: Spark Kafka consumer test failed. Check the log: $consumer_output_file"
        cat "$consumer_output_file"
        echo "$partitions,$record_size,$batch_display,0,0,0,0" >> "$RESULTS_FILE"
    fi
    
    # Delete topic after test
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --delete --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS > /dev/null 2>&1
    
    # Wait a bit before next test
    sleep 5
}

# Run Spark consumer tests
echo "Running Spark consumer tests..."
for partitions in "${NUM_PARTITIONS[@]}"; do
    for record_size in "${RECORD_SIZES[@]}"; do
        for batch_size in "${BATCH_SIZES[@]}"; do
            run_spark_consumer_test $partitions $record_size $batch_size
        done
    done
done

echo "All Spark consumer tests completed. Results saved to $RESULTS_FILE"
