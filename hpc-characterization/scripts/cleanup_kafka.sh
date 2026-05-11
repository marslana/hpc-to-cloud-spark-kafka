#!/bin/bash
# kafka_cleanup.sh - Script to clean up Kafka topics and consumer groups

# Set your bootstrap servers
BOOTSTRAP_SERVERS="aion-0221:9092,aion-0222:9093"

echo "=== KAFKA CLEANUP SCRIPT ==="
echo "Bootstrap servers: $BOOTSTRAP_SERVERS"

# Function to delete all consumer groups
delete_consumer_groups() {
    echo "Listing all consumer groups..."
    GROUPS=$(singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-consumer-groups.sh --list \
        --bootstrap-server $BOOTSTRAP_SERVERS)
    
    if [ -z "$GROUPS" ]; then
        echo "No consumer groups found."
        return
    fi
    
    echo "Found consumer groups: $GROUPS"
    
    for group in $GROUPS; do
        echo "Deleting consumer group: $group"
        singularity exec instance://shinst_master \
            /opt/kafka/bin/kafka-consumer-groups.sh --delete \
            --group $group \
            --bootstrap-server $BOOTSTRAP_SERVERS
    done
    
    echo "All consumer groups deleted."
}

# Function to delete all topics
delete_topics() {
    echo "Listing all topics..."
    TOPICS=$(singularity exec instance://shinst_master \
        /opt/kafka/bin/kafka-topics.sh --list \
        --bootstrap-server $BOOTSTRAP_SERVERS)
    
    if [ -z "$TOPICS" ]; then
        echo "No topics found."
        return
    fi
    
    echo "Found topics: $TOPICS"
    
    for topic in $TOPICS; do
        # Skip internal topics that start with __
        if [[ $topic != __* ]]; then
            echo "Deleting topic: $topic"
            singularity exec instance://shinst_master \
                /opt/kafka/bin/kafka-topics.sh --delete \
                --topic $topic \
                --bootstrap-server $BOOTSTRAP_SERVERS
        fi
    done
    
    echo "All user topics deleted."
}

# First delete consumer groups, then topics
echo "Step 1: Deleting consumer groups..."
delete_consumer_groups

echo "Step 2: Deleting topics..."
delete_topics

echo "Kafka cleanup completed."
