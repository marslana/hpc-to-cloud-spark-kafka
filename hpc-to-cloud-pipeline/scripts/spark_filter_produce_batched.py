#!/usr/bin/env python3
"""
PySpark job: Read CSV → filter → batch records → produce to Kafka.

Batches multiple small EEA records (~70 bytes each) into larger Kafka messages
(1KB or 10KB) to reduce per-message overhead and improve streaming throughput.

Usage:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    spark_filter_produce_batched.py \
    --input /path/to/eea_airquality_5gb.csv \
    --bootstrap-servers <KAFKA_IP>:9092 \
    --topic pipeline-test \
    --batch-kb 10 \
    --run-id approach4_run1
"""

import argparse
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--bootstrap-servers", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--filter-countries", default="LU,DE,BE")
    ap.add_argument("--partitions", type=int, default=8)
    ap.add_argument("--batch-kb", type=int, default=10,
                    help="Target Kafka message size in KB (1 or 10)")
    ap.add_argument("--acks", default="1")
    ap.add_argument("--output-csv", default=None)
    ap.add_argument("--run-id", default="run")
    args = ap.parse_args()

    avg_record_bytes = 70
    target_bytes = args.batch_kb * 1024
    records_per_batch = max(1, target_bytes // avg_record_bytes)

    countries = [c.strip() for c in args.filter_countries.split(",")]

    kafka_batch_size = str(max(16384, target_bytes * 16))

    spark = (SparkSession.builder
             .appName(f"PipelineBatched_{args.run_id}")
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .config("spark.sql.shuffle.partitions", str(args.partitions))
             .config("spark.executor.memory", "6g")
             .config("spark.driver.memory", "2g")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # Phase 1: Read
    t_start = time.time()
    raw_df = spark.read.option("header", "true").csv(args.input)
    raw_count = raw_df.count()
    t_read = time.time()

    input_size_mb = os.path.getsize(args.input) / (1024 * 1024)
    print(f"Read {raw_count:,} records ({input_size_mb:.1f} MB) in {t_read - t_start:.1f}s")

    # Phase 2: Filter
    t_filter_start = time.time()
    filtered_df = raw_df.filter(F.col("country").isin(countries))
    filtered_count = filtered_df.count()
    t_filter = time.time()

    filter_ratio = filtered_count / raw_count if raw_count > 0 else 0
    print(f"Filtered to {filtered_count:,} records ({filter_ratio*100:.1f}%) "
          f"in {t_filter - t_filter_start:.1f}s")

    # Phase 3: Batch records into larger messages
    t_batch_start = time.time()

    data_cols = [c for c in filtered_df.columns]
    row_str = F.concat_ws(",", *[F.col(c) for c in data_cols])
    filtered_with_str = filtered_df.withColumn("_row_str", row_str)

    filtered_with_str = filtered_with_str.withColumn(
        "_row_num", F.monotonically_increasing_id()
    )
    filtered_with_str = filtered_with_str.withColumn(
        "_batch_id", (F.col("_row_num") / F.lit(records_per_batch)).cast("long")
    )

    batched_df = (filtered_with_str
                  .groupBy("_batch_id")
                  .agg(F.concat_ws("\n", F.collect_list("_row_str")).alias("value")))

    batched_count = batched_df.count()
    t_batch = time.time()

    print(f"Batched {filtered_count:,} records into {batched_count:,} messages "
          f"(~{args.batch_kb}KB each, {records_per_batch} rec/msg) "
          f"in {t_batch - t_batch_start:.1f}s")

    # Phase 4: Produce to Kafka
    t_kafka_start = time.time()
    (batched_df
     .select(F.col("value"))
     .write
     .format("kafka")
     .option("kafka.bootstrap.servers", args.bootstrap_servers)
     .option("topic", args.topic)
     .option("kafka.acks", args.acks)
     .option("kafka.batch.size", kafka_batch_size)
     .option("kafka.linger.ms", "10")
     .option("kafka.buffer.memory", "67108864")
     .save())
    t_kafka_end = time.time()

    kafka_duration = t_kafka_end - t_kafka_start
    total_duration = t_kafka_end - t_start
    estimated_filtered_mb = input_size_mb * filter_ratio
    kafka_throughput = estimated_filtered_mb / kafka_duration if kafka_duration > 0 else 0

    results = {
        "run_id": args.run_id,
        "input_size_mb": round(input_size_mb, 1),
        "raw_records": raw_count,
        "filtered_records": filtered_count,
        "filter_ratio": round(filter_ratio, 4),
        "estimated_filtered_mb": round(estimated_filtered_mb, 1),
        "batch_kb": args.batch_kb,
        "records_per_batch": records_per_batch,
        "kafka_messages": batched_count,
        "bootstrap_servers": args.bootstrap_servers,
        "topic": args.topic,
        "partitions": args.partitions,
        "acks": args.acks,
        "read_time_s": round(t_read - t_start, 2),
        "filter_time_s": round(t_filter - t_filter_start, 2),
        "batch_time_s": round(t_batch - t_batch_start, 2),
        "kafka_produce_time_s": round(kafka_duration, 2),
        "total_time_s": round(total_duration, 2),
        "kafka_throughput_mbps": round(kafka_throughput, 2),
    }

    print("\n" + "=" * 60)
    print("BATCHED PIPELINE RESULTS")
    print("=" * 60)
    for k, v in results.items():
        print(f"  {k}: {v}")
    print("=" * 60)

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
