#!/usr/bin/env python3
"""
Simple Kafka file ingester — reads a CSV and produces every row to Kafka.
Deploy this to the EC2 instance for Approach 3 (scp + ingest).

No filtering — every row is produced as-is.

Usage:
  python3 kafka_file_ingest.py \
    --input /path/to/data.csv \
    --bootstrap-servers localhost:9092 \
    --topic pipeline-test \
    --batch-size 65536 \
    --acks 1
"""

import argparse
import csv
import json
import os
import time
from kafka import KafkaProducer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--bootstrap-servers", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--batch-size", type=int, default=65536)
    ap.add_argument("--linger-ms", type=int, default=10)
    ap.add_argument("--acks", default="1")
    ap.add_argument("--buffer-memory", type=int, default=67108864)
    args = ap.parse_args()

    file_size = os.path.getsize(args.input)
    file_size_mb = file_size / (1024 * 1024)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks=int(args.acks) if args.acks != "all" else "all",
        batch_size=args.batch_size,
        linger_ms=args.linger_ms,
        buffer_memory=args.buffer_memory,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    record_count = 0
    t_start = time.time()

    with open(args.input, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get("station_id", None)
            producer.send(args.topic, key=key, value=row)
            record_count += 1

            if record_count % 500000 == 0:
                elapsed = time.time() - t_start
                rate = record_count / elapsed
                print(f"  Ingested {record_count:,} records ({rate:,.0f} rec/s)")

    producer.flush()
    producer.close()

    t_end = time.time()
    duration = t_end - t_start
    throughput = file_size_mb / duration if duration > 0 else 0

    print(f"duration_s: {duration:.2f}")
    print(f"throughput_mbps: {throughput:.2f}")
    print(f"records: {record_count}")
    print(f"file_size_mb: {file_size_mb:.1f}")


if __name__ == "__main__":
    main()
