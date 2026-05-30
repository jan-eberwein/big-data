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
- NOAA Marine Cadastre AIS Vessel Tracking Data 2025
- EU MRV Ship CO₂ Emissions Dataset 2024

## Research Questions
1. Which maritime regions show the highest vessel traffic intensity?
2. Which vessel types contribute most to maritime traffic intensity?
3. Can AIS data reveal congestion or waiting behavior?
4. How does vessel speed vary across vessel types and regions?
5. How are AIS activity indicators related to reported EU MRV CO₂ emissions?
