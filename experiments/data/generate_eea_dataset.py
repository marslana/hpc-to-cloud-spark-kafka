#!/usr/bin/env python3
"""
Generate synthetic EEA Air Quality monitoring dataset.
Mimics the structure of real EEA Air Quality e-Reporting data:
  - station_id, timestamp, country, city, lat, lon, pollutant, value, unit

Records are roughly 64 bytes each in CSV for this synthetic schema.
  5 GB ≈ 83.9M records

Usage:
  python3 generate_eea_dataset.py --output /path/to/output.csv --size-gb 5
"""

import argparse
import csv
import io
import os
import random
import sys
import time

COUNTRIES = {
    "LU": ["Luxembourg", "Esch-sur-Alzette", "Differdange"],
    "DE": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Stuttgart", "Cologne",
           "Dortmund", "Essen", "Leipzig", "Dresden"],
    "BE": ["Brussels", "Antwerp", "Ghent", "Liege"],
    "FR": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Bordeaux",
           "Strasbourg", "Lille", "Nantes", "Montpellier"],
    "NL": ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"],
    "IT": ["Rome", "Milan", "Naples", "Turin", "Florence", "Bologna"],
    "ES": ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"],
    "PL": ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"],
    "CZ": ["Prague", "Brno", "Ostrava"],
    "AT": ["Vienna", "Graz", "Linz", "Salzburg"],
}

FILTER_COUNTRIES = {"LU", "DE", "BE"}

COORDS = {
    "LU": (49.6, 6.1), "DE": (51.2, 10.4), "BE": (50.8, 4.4),
    "FR": (46.6, 2.2), "NL": (52.1, 5.3), "IT": (42.5, 12.5),
    "ES": (40.4, -3.7), "PL": (52.0, 19.0), "CZ": (49.8, 15.5),
    "AT": (47.5, 13.6),
}

POLLUTANTS = [
    ("PM10", "µg/m³", 5, 120),
    ("PM2.5", "µg/m³", 2, 80),
    ("NO2", "µg/m³", 5, 200),
    ("O3", "µg/m³", 10, 250),
    ("SO2", "µg/m³", 1, 100),
    ("CO", "mg/m³", 0.1, 10.0),
]

# Weight countries so FILTER_COUNTRIES get ~11.1% combined
# LU + DE + BE have weight 10 out of total weight 90.
COUNTRY_WEIGHTS = {
    "LU": 2, "DE": 5, "BE": 3,
    "FR": 15, "NL": 10, "IT": 15, "ES": 15, "PL": 13, "CZ": 7, "AT": 5,
}
WEIGHTED_COUNTRIES = []
for c, w in COUNTRY_WEIGHTS.items():
    WEIGHTED_COUNTRIES.extend([c] * w)


def estimate_records_for_gb(target_gb):
    sample_row = "STA-FR-0042,1711497600,FR,Paris,46.632,2.215,PM10,42.37,µg/m³\n"
    bytes_per_record = len(sample_row.encode("utf-8"))
    return int(target_gb * 1024 * 1024 * 1024 / bytes_per_record)


def generate_dataset(output_path, target_gb, seed=42):
    random.seed(seed)
    num_records = estimate_records_for_gb(target_gb)

    print(f"Generating {num_records:,} records (~{target_gb} GB) → {output_path}")
    print(f"Filter countries {FILTER_COUNTRIES} will be ~10% of records")

    base_ts = 1704067200  # 2024-01-01 00:00:00 UTC
    ts_step = 3600        # 1 hour between readings

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    start = time.time()
    filter_count = 0

    with open(output_path, "w", newline="", buffering=8 * 1024 * 1024) as f:
        writer = csv.writer(f)
        writer.writerow(["station_id", "timestamp", "country", "city",
                         "latitude", "longitude", "pollutant", "value", "unit"])

        for i in range(num_records):
            country = random.choice(WEIGHTED_COUNTRIES)
            city = random.choice(COUNTRIES[country])
            base_lat, base_lon = COORDS[country]
            lat = round(base_lat + random.uniform(-2, 2), 3)
            lon = round(base_lon + random.uniform(-2, 2), 3)

            pollutant, unit, vmin, vmax = random.choice(POLLUTANTS)
            value = round(random.uniform(vmin, vmax), 2)

            station_id = f"STA-{country}-{random.randint(1, 500):04d}"
            ts = base_ts + (i % 8760) * ts_step  # wrap around 1 year

            writer.writerow([station_id, ts, country, city, lat, lon,
                             pollutant, value, unit])

            if country in FILTER_COUNTRIES:
                filter_count += 1

            if (i + 1) % 5_000_000 == 0:
                elapsed = time.time() - start
                pct = (i + 1) / num_records * 100
                rate = (i + 1) / elapsed
                print(f"  {pct:.0f}% ({i+1:,} records, {rate:,.0f} rec/s)")

    elapsed = time.time() - start
    file_size = os.path.getsize(output_path)
    actual_filter_pct = filter_count / num_records * 100

    print(f"\nDone in {elapsed:.1f}s")
    print(f"  File size: {file_size / 1024**3:.2f} GB")
    print(f"  Records:   {num_records:,}")
    print(f"  Filter %:  {actual_filter_pct:.1f}% ({filter_count:,} records pass)")
    print(f"  Filtered size estimate: {file_size * actual_filter_pct / 100 / 1024**2:.0f} MB")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate synthetic EEA air quality dataset")
    ap.add_argument("--output", required=True, help="Output CSV path")
    ap.add_argument("--size-gb", type=float, default=1.0, help="Target size in GB (default: 1)")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = ap.parse_args()
    generate_dataset(args.output, args.size_gb, args.seed)
