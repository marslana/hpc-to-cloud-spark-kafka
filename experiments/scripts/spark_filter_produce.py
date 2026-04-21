#!/usr/bin/env python3
"""
PySpark job: Read CSV from disk → filter → produce to Kafka.

Used by Approach 1 (HPC → US-East Kafka) and Approach 2 (HPC → Frankfurt Kafka).
The only difference is the --bootstrap-servers target.

Usage (inside Singularity via spark-submit):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    spark_filter_produce.py \
    --input /path/to/eea_airquality_1gb.csv \
    --bootstrap-servers <KAFKA_IP>:9092 \
    --topic pipeline-test \
    --filter-countries LU,DE,BE \
    --partitions 8 \
    --batch-size 16384 \
    --output-csv /path/to/results.csv \
    --run-id approach1_run1
"""

import argparse
import json
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, lit


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to input CSV file")
    ap.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap servers")
    ap.add_argument("--topic", required=True, help="Kafka topic to produce to")
    ap.add_argument("--filter-countries", default="LU,DE,BE",
                    help="Comma-separated country codes to keep")
    ap.add_argument("--partitions", type=int, default=8, help="Kafka topic partitions")
    ap.add_argument("--batch-size", default="16384", help="Kafka producer batch.size")
    ap.add_argument("--acks", default="1", help="Kafka acks setting")
    ap.add_argument("--output-csv", default=None, help="Append results to this CSV")
    ap.add_argument("--run-id", default="run", help="Identifier for this run")
    args = ap.parse_args()

    countries = [c.strip() for c in args.filter_countries.split(",")]

    spark = (SparkSession.builder
             .appName(f"PipelineFilter_{args.run_id}")
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .config("spark.sql.shuffle.partitions", str(args.partitions))
             .config("spark.executor.memory", "6g")
             .config("spark.driver.memory", "2g")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # ── Phase 1: Read raw data ───────────────────────────────
    t_start = time.time()
    raw_df = spark.read.option("header", "true").csv(args.input)
    raw_count = raw_df.count()
    t_read = time.time()

    input_size_bytes = os.path.getsize(args.input)
    input_size_mb = input_size_bytes / (1024 * 1024)

    print(f"Read {raw_count:,} records ({input_size_mb:.1f} MB) in {t_read - t_start:.1f}s")

    # ── Phase 2: Filter ──────────────────────────────────────
    t_filter_start = time.time()
    filtered_df = raw_df.filter(col("country").isin(countries))
    filtered_count = filtered_df.count()
    t_filter = time.time()

    filter_ratio = filtered_count / raw_count if raw_count > 0 else 0
    print(f"Filtered to {filtered_count:,} records ({filter_ratio*100:.1f}%) "
          f"in {t_filter - t_filter_start:.1f}s")

    # ── Phase 3: Produce to Kafka ────────────────────────────
    kafka_df = filtered_df.select(
        col("station_id").alias("key"),
        to_json(struct("*")).alias("value")
    )

    t_kafka_start = time.time()
    (kafka_df.write
     .format("kafka")
     .option("kafka.bootstrap.servers", args.bootstrap_servers)
     .option("topic", args.topic)
     .option("kafka.acks", args.acks)
     .option("kafka.batch.size", args.batch_size)
     .option("kafka.linger.ms", "10")
     .option("kafka.buffer.memory", "67108864")
     .save())
    t_kafka_end = time.time()

    # ── Results ──────────────────────────────────────────────
    kafka_duration = t_kafka_end - t_kafka_start
    total_duration = t_kafka_end - t_start
    estimated_filtered_mb = input_size_mb * filter_ratio
    kafka_throughput = estimated_filtered_mb / kafka_duration if kafka_duration > 0 else 0

    results = {
        "run_id": args.run_id,
        "input_file": os.path.basename(args.input),
        "input_size_mb": round(input_size_mb, 1),
        "raw_records": raw_count,
        "filtered_records": filtered_count,
        "filter_ratio": round(filter_ratio, 4),
        "estimated_filtered_mb": round(estimated_filtered_mb, 1),
        "bootstrap_servers": args.bootstrap_servers,
        "topic": args.topic,
        "partitions": args.partitions,
        "acks": args.acks,
        "batch_size": args.batch_size,
        "read_time_s": round(t_read - t_start, 2),
        "filter_time_s": round(t_filter - t_filter_start, 2),
        "kafka_produce_time_s": round(kafka_duration, 2),
        "total_time_s": round(total_duration, 2),
        "kafka_throughput_mbps": round(kafka_throughput, 2),
    }

    print("\n" + "=" * 60)
    print("PIPELINE RESULTS")
    print("=" * 60)
    for k, v in results.items():
        print(f"  {k}: {v}")
    print("=" * 60)

    # Append to CSV if requested
    if args.output_csv:
        write_header = not os.path.exists(args.output_csv)
        with open(args.output_csv, "a") as f:
            if write_header:
                f.write(",".join(results.keys()) + "\n")
            f.write(",".join(str(v) for v in results.values()) + "\n")
        print(f"Results appended to {args.output_csv}")

    spark.stop()


if __name__ == "__main__":
    main()
