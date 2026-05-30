# Maritime Traffic and Environmental Pressure Analysis

This project analyzes large-scale AIS vessel tracking data using Apache Spark, Spark SQL and Parquet. The goal is to identify maritime traffic hotspots, vessel type patterns, speed behavior, possible congestion indicators and relationships between AIS activity and EU MRV CO₂ emissions.

## Technologies
- Apache Spark
- Spark SQL
- Parquet
- Tableau / Databricks / Kibana / Superset
- GitHub

## Local Spark Setup

This repository uses Docker Compose to run a local Apache Spark standalone setup. Docker keeps the Spark version and runtime environment consistent across development machines.

Prerequisites:

- Docker Desktop
- Docker Compose

Start Spark:

```bash
docker compose up -d
```

The raw data directory is configured in `.env`:

```text
RAW_DATA_DIR=./data/raw
```

By default, files in `data/raw/` are available inside Spark at:

```text
/workspace/data/raw
```

To use raw data from an external drive instead, edit `.env`:

```text
RAW_DATA_DIR=/Volumes/YourDrive/path/to/raw-data
```

The external folder is mounted read-only at the same in-container path:

```text
/workspace/data/raw
```

On macOS, make sure Docker Desktop is allowed to access the external drive path in Docker Desktop's file sharing settings.

Open the Spark master UI:

```text
http://localhost:8080
```

Run Spark's built-in Python example as a smoke test:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/examples/src/main/python/pi.py 10
```

Stop Spark:

```bash
docker compose down
```

## Load AIS Data

Prepare AIS CSV files manually before running the Spark loader. If the source files are ZIP archives, extract them first so the raw data folder contains CSV files.

Local raw data example:

```text
data/raw/
  AIS_2025_01.csv
  AIS_2025_02.csv
  ...
```

When the CSV files are on an external drive, set `RAW_DATA_DIR` in `.env` before starting Spark:

```text
RAW_DATA_DIR=/Volumes/YourDrive/path/to/ais-csvs
```

Run the AIS CSV-to-Parquet loader:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/load_ais.py
```

The default input path is `/workspace/data/raw`. The default output path is `/workspace/data/processed/ais_parquet`.

To use custom paths:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/load_ais.py --input /workspace/data/raw --output /workspace/data/processed/ais_parquet
```

## Data Policy

Large source and processed datasets must stay local and must not be committed to git.

Recommended local data layout once datasets are added:

```text
data/
  raw/          # original local source files, not committed
  processed/    # generated local data, not committed
  sample/       # small committed sample files only
```

## Datasets
- NOAA Marine Cadastre AIS Vessel Tracking Data, currently tested with 2023 data
- EU MRV Ship CO₂ Emissions Dataset 2024

## Research Questions
1. Which maritime regions show the highest vessel traffic intensity?
2. Which vessel types contribute most to maritime traffic intensity?
3. Can AIS data reveal congestion or waiting behavior?
4. How does vessel speed vary across vessel types and regions?
5. How are AIS activity indicators related to reported EU MRV CO₂ emissions?
