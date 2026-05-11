#!/usr/bin/env python3
"""
PySpark job: Read CSV from disk → filter → write filtered CSV to disk.

Used by Approach 3 (HPC Spark filter → scp filtered file → EC2 Kafka ingest).

Usage:
  spark-submit spark_filter_save.py \
    --input /path/to/eea_airquality_5gb.csv \
    --output /path/to/eea_filtered.csv \
    --filter-countries LU,DE,BE \
    --run-id approach3_run1
"""

import argparse
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True, help="Output filtered CSV path")
    ap.add_argument("--filter-countries", default="LU,DE,BE")
    ap.add_argument("--run-id", default="run")
    args = ap.parse_args()

    countries = [c.strip() for c in args.filter_countries.split(",")]

    spark = (SparkSession.builder
             .appName(f"PipelineFilterSave_{args.run_id}")
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .config("spark.executor.memory", "6g")
             .config("spark.driver.memory", "2g")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    t_start = time.time()
    raw_df = spark.read.option("header", "true").csv(args.input)
    raw_count = raw_df.count()
    t_read = time.time()

    input_size_bytes = os.path.getsize(args.input)
    input_size_mb = input_size_bytes / (1024 * 1024)

    print(f"Read {raw_count:,} records ({input_size_mb:.1f} MB) in {t_read - t_start:.1f}s")

    t_filter_start = time.time()
    filtered_df = raw_df.filter(col("country").isin(countries))
    filtered_count = filtered_df.count()
    t_filter = time.time()

    filter_ratio = filtered_count / raw_count if raw_count > 0 else 0
    print(f"Filtered to {filtered_count:,} records ({filter_ratio*100:.1f}%) "
          f"in {t_filter - t_filter_start:.1f}s")

    t_write_start = time.time()
    filtered_df.write.option("header", "true").mode("overwrite").csv(args.output)
    t_write = time.time()

    # Find the actual output file (Spark writes to a directory)
    output_size = 0
    if os.path.isdir(args.output):
        for f in os.listdir(args.output):
            fp = os.path.join(args.output, f)
            if os.path.isfile(fp):
                output_size += os.path.getsize(fp)

    output_size_mb = output_size / (1024 * 1024)
    total_time = t_write - t_start

    print(f"\n{'='*60}")
    print("FILTER-SAVE RESULTS")
    print(f"{'='*60}")
    print(f"  run_id: {args.run_id}")
    print(f"  input_size_mb: {input_size_mb:.1f}")
    print(f"  raw_records: {raw_count}")
    print(f"  filtered_records: {filtered_count}")
    print(f"  filter_ratio: {filter_ratio:.4f}")
    print(f"  output_size_mb: {output_size_mb:.1f}")
    print(f"  read_time_s: {t_read - t_start:.2f}")
    print(f"  filter_time_s: {t_filter - t_filter_start:.2f}")
    print(f"  write_time_s: {t_write - t_write_start:.2f}")
    print(f"  total_time_s: {total_time:.2f}")
    print(f"{'='*60}")

    spark.stop()


if __name__ == "__main__":
    main()
