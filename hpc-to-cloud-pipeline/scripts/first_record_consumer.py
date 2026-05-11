#!/usr/bin/env python3
"""
Kafka consumer that measures:
  - first_record_latency: wall-clock time from consumer start to first record
  - last_record_time: when the last record arrived
  - total records, bytes, throughput

Runs on EC2 (US-East-1) destination. Start BEFORE the pipeline begins.

Usage:
  pip3 install confluent-kafka
  python3 first_record_consumer.py \
      --brokers localhost:9092 \
      --topic <topic_name> \
      --target-records 9328880 \
      --idle-timeout-ms 60000 \
      --output results.csv
"""

import argparse
import csv
import json
import os
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--target-records", type=int, default=0,
                    help="Stop after this many records (0=rely on idle timeout)")
    ap.add_argument("--idle-timeout-ms", type=int, default=60000,
                    help="Stop after this many ms with no new records")
    ap.add_argument("--output", default=None, help="Append CSV results here")
    ap.add_argument("--run-id", default="run", help="Label for this run")
    args = ap.parse_args()

    from confluent_kafka import Consumer

    c = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": f"frc-{args.run_id}-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 500,
        "max.partition.fetch.bytes": 10485760,
    })
    c.subscribe([args.topic])

    consumer_start = time.time()
    first_record_wall = None
    last_record_wall = None
    count = 0
    total_bytes = 0
    idle_ms = 0

    print(f"[{time.strftime('%H:%M:%S')}] Consumer started, waiting for records on '{args.topic}'...")

    try:
        while True:
            msg = c.poll(0.5)
            if msg is None or msg.error():
                if count > 0:
                    idle_ms += 500
                    if idle_ms >= args.idle_timeout_ms:
                        print(f"[{time.strftime('%H:%M:%S')}] Idle timeout after {idle_ms}ms, stopping.")
                        break
                continue

            idle_ms = 0
            now = time.time()

            if first_record_wall is None:
                first_record_wall = now
                first_record_latency = now - consumer_start
                print(f"[{time.strftime('%H:%M:%S')}] FIRST RECORD received! "
                      f"latency={first_record_latency:.2f}s after consumer start")

            last_record_wall = now
            v = msg.value()
            if v:
                total_bytes += len(v)
            count += 1

            if count % 500000 == 0:
                elapsed = now - first_record_wall
                mb = total_bytes / 1048576
                print(f"  [{time.strftime('%H:%M:%S')}] {count:,} records "
                      f"({mb:.1f} MB, {elapsed:.1f}s)")

            if args.target_records > 0 and count >= args.target_records:
                print(f"[{time.strftime('%H:%M:%S')}] Target {args.target_records} reached.")
                break
    finally:
        c.close()

    total_mb = total_bytes / 1048576
    consumer_total_s = (last_record_wall or time.time()) - consumer_start
    active_s = (last_record_wall - first_record_wall) if (first_record_wall and last_record_wall) else 0
    frl = (first_record_wall - consumer_start) if first_record_wall else -1
    throughput = total_mb / active_s if active_s > 0 else 0

    print(f"\n{'='*60}")
    print(f"CONSUMER RESULTS")
    print(f"{'='*60}")
    print(f"  run_id:                {args.run_id}")
    print(f"  records:               {count}")
    print(f"  bytes_mb:              {total_mb:.2f}")
    print(f"  first_record_latency:  {frl:.2f}s")
    print(f"  active_duration_s:     {active_s:.2f}")
    print(f"  consumer_total_s:      {consumer_total_s:.2f}")
    print(f"  throughput_mbps:       {throughput:.2f}")
    print(f"{'='*60}")

    # Machine-readable summary line for easy parsing
    print(f"CONSUMER_CSV:{args.run_id},{count},{total_mb:.2f},{frl:.2f},"
          f"{active_s:.2f},{consumer_total_s:.2f},{throughput:.2f}")

    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        exists = os.path.exists(args.output)
        with open(args.output, "a") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["run_id", "records", "bytes_mb", "first_record_latency_s",
                            "active_duration_s", "consumer_total_s", "throughput_mbps"])
            w.writerow([args.run_id, count, f"{total_mb:.2f}", f"{frl:.2f}",
                        f"{active_s:.2f}", f"{consumer_total_s:.2f}", f"{throughput:.2f}"])
        print(f"Results appended to {args.output}")


if __name__ == "__main__":
    main()
