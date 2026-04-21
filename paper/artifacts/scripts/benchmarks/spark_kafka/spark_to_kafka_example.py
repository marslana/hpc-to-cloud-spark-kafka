from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col
from kafka import KafkaProducer, KafkaConsumer
import json

# Kafka configuration
kafka_brokers = ['iris-121:9092', 'iris-122:9093']
kafka_topic = 'spark-kafka-test'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkToKafka") \
    .master("spark://iris-114:7078") \
    .getOrCreate()

# Create a sample DataFrame directly
df = spark.createDataFrame([
    (1, "Alice", 30),
    (2, "Bob", 35),
    (3, "Charlie", 40)
], ["id", "name", "age"])

# Convert the DataFrame to a list of dictionaries
data = df.rdd.map(lambda row: row.asDict()).collect()

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_brokers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send each record to Kafka
for record in data:
    producer.send(kafka_topic, value=record)

# Ensure all messages are sent
producer.flush()

print(f"Sent {len(data)} messages to Kafka topic '{kafka_topic}'")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_brokers,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Consume messages
print(f"Consuming messages from Kafka topic '{kafka_topic}':")
message_count = 0
for message in consumer:
    print(message.value)
    message_count += 1
    if message_count == len(data):
        break

consumer.close()
spark.stop()
