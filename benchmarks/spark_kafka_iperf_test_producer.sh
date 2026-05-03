#!/bin/bash
# spark_producer_test.sh - Test Spark as producer to Kafka (without HDFS)

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

# Define number of records based on record size - UPDATED PER REQUEST
declare -A NUM_RECORDS
NUM_RECORDS[100]=10000000  # 10M for 100 bytes
NUM_RECORDS[1000]=1000000  # 1M for 1000 bytes
NUM_RECORDS[10000]=100000  # 100K for 10000 bytes (unchanged)

# Base directory for results - use home directory which should be accessible
SHARED_DATA_DIR="$HOME/data"
mkdir -p $SHARED_DATA_DIR

RESULTS_DIR="$SHARED_DATA_DIR/spark_kafka_test_$(date +%Y%m%d_%H%M)"
mkdir -p $RESULTS_DIR
RESULTS_FILE="$RESULTS_DIR/spark_producer_results.csv"

# Create results file header
echo "partitions,record_size_bytes,batch_size,records_per_sec,mb_per_sec,duration_sec,total_records" > $RESULTS_FILE

# Function to generate test data directly
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

# Function to run a Spark producer test
run_spark_producer_test() {
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
    
    echo "Running Spark producer test: partitions=$partitions, record_size=$record_size bytes, batch_size=$batch_display, records=$num_records"
    
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
    
    # Create Spark script for Kafka producer test
    local spark_script="$RESULTS_DIR/spark_kafka_producer.py"
    local producer_output_file="$RESULTS_DIR/spark_producer_${partitions}p_${record_size}b_${batch_display}.log"
    
    echo "Starting Spark producer test, output will be saved to $producer_output_file"
    
    cat > "$spark_script" << 'EOF'
#!/usr/bin/env python3
import os
import sys
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

def run_kafka_producer_test(input_file, topic, bootstrap_servers, batch_size):
    # Start timing
    start_time = time.time()
    
    # Create Spark session with Kafka
    builder = SparkSession.builder \
        .appName("SparkKafkaProducer") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.default.parallelism", "16") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.kafka.producer.linger.ms", "5") \
        .config("spark.kafka.producer.acks", "1")
    
    # Only set batch size if it's not zero (no batching)
    if batch_size != "0":
        builder = builder.config("spark.kafka.producer.batchSize", batch_size) \
                         .config("spark.kafka.producer.bufferMemory", "67108864")
    
    spark = builder.getOrCreate()
    
    # Read the input file
    df = spark.read.json(input_file)
    record_count = df.count()
    
    print(f"Read {record_count} records from {input_file}")
    
    # Convert DataFrame to Kafka format and write to Kafka
    kafka_write_start = time.time()
    
    # Convert to string to ensure proper serialization
    df_string = df.select(
        expr("CAST(id AS STRING) AS key"),
        expr("to_json(struct(*)) AS value")
    )
    
    # Write to Kafka
    df_string.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", topic) \
        .save()
    
    kafka_write_end = time.time()
    
    # End timing
    end_time = time.time()
    total_duration = end_time - start_time
    kafka_duration = kafka_write_end - kafka_write_start
    
    # Calculate size
    file_size_bytes = os.path.getsize(input_file)
    file_size_mb = file_size_bytes / (1024 * 1024)
    
    # Calculate throughput
    records_per_second = record_count / kafka_duration
    mb_per_second = file_size_mb / kafka_duration
    
    # Print results
    print("RESULTS_START")
    print(json.dumps({
        "records": record_count,
        "size_mb": file_size_mb,
        "duration_seconds": kafka_duration,
        "total_duration_seconds": total_duration,
        "throughput_records_per_second": records_per_second,
        "throughput_mb_per_second": mb_per_second
    }, indent=2))
    print("RESULTS_END")
    
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: spark_kafka_producer.py <input_file> <topic> <bootstrap_servers> <batch_size>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    topic = sys.argv[2]
    bootstrap_servers = sys.argv[3]
    batch_size = sys.argv[4]
    
    run_kafka_producer_test(input_file, topic, bootstrap_servers, batch_size)
EOF
    
    chmod +x "$spark_script"
    
    # Run the Kafka producer test
    echo "Running Spark Kafka producer test..."
    singularity exec \
        --bind $SHARED_DATA_DIR:/opt/shared_data \
        instance://shinst_master \
        spark-submit \
        --master $SPARK_MASTER \
        --conf spark.hadoop.fs.defaultFS=file:/// \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
        "/opt/shared_data/$(basename $RESULTS_DIR)/spark_kafka_producer.py" \
        "/opt/shared_data/$(basename $RESULTS_DIR)/$(basename $test_data_file)" \
        "$unique_topic" \
        "$BOOTSTRAP_SERVERS" \
        "$batch_size" > "$producer_output_file" 2>&1
    
    # Check if the job ran successfully
    if grep -q "RESULTS_START" "$producer_output_file"; then
        echo "Spark Kafka producer test completed successfully."
        
        # Extract metrics
        results=$(sed -n '/RESULTS_START/,/RESULTS_END/p' "$producer_output_file" | grep -v "RESULTS_")
        
        local records_per_sec=$(echo "$results" | grep -o '"throughput_records_per_second": [0-9.]*' | awk '{print $2}' | tr -d ',')
        local mb_per_sec=$(echo "$results" | grep -o '"throughput_mb_per_second": [0-9.]*' | awk '{print $2}' | tr -d ',')
        local duration=$(echo "$results" | grep -o '"duration_seconds": [0-9.]*' | awk '{print $2}' | tr -d ',')
        local total_records=$(echo "$results" | grep -o '"records": [0-9]*' | awk '{print $2}' | tr -d ',')
        
        # Save results to CSV
        echo "$partitions,$record_size,$batch_display,$records_per_sec,$mb_per_sec,$duration,$total_records" >> "$RESULTS_FILE"
        
        echo "Results: $records_per_sec records/sec, $mb_per_sec MB/sec, duration: $duration sec"
    else
        echo "ERROR: Spark Kafka producer test failed. Check the log: $producer_output_file"
        cat "$producer_output_file"
        echo "$partitions,$record_size,$batch_display,0,0,0,0" >> "$RESULTS_FILE"
    fi
    
    # Delete topic after test
    singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --delete --topic $unique_topic \
        --bootstrap-server $BOOTSTRAP_SERVERS > /dev/null 2>&1
    
    # Wait a bit before next test
    sleep 5
}

# Run Spark producer tests
echo "Running Spark producer tests..."
for partitions in "${NUM_PARTITIONS[@]}"; do
    for record_size in "${RECORD_SIZES[@]}"; do
        for batch_size in "${BATCH_SIZES[@]}"; do
            run_spark_producer_test $partitions $record_size $batch_size
        done
    done
done

echo "All Spark producer tests completed. Results saved to $RESULTS_FILE"
