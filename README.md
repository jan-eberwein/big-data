# Maritime Traffic and Environmental Pressure Analysis

This project processes AIS vessel tracking data with Apache Spark and stores cleaned and aggregated outputs as Parquet files.

## Tools
- Apache Spark
- Spark SQL
- Parquet
- Docker Compose

## Spark Setup

Start Spark:

```bash
docker compose up -d
```

Open the Spark master UI:

```text
http://localhost:8080
```

Stop Spark:

```bash
docker compose down
```

Raw AIS CSV files are mounted from the path configured in `.env`:

```text
RAW_DATA_DIR=./data/raw
```

Inside the Spark containers, this path is available as:

```text
/workspace/data/raw
```

## Load AIS Data

Expected local input layout:

```text
data/raw/
  ais-2025-01-01.csv
  ais-2025-01-02.csv
  ...
```

Run the CSV-to-Parquet loader:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/load_ais.py
```

Default paths:

```text
input:  /workspace/data/raw
output: /workspace/data/processed/ais_parquet
```

The loader removes rows with missing position fields, invalid coordinates, speeds outside `0-60` knots, and duplicate vessel positions.

Optional sample output:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/load_ais.py --sample-output /workspace/data/sample/ais_sample --sample-limit 10000
```

## Aggregate AIS Data

Run the aggregation job:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/aggregate_hotspots.py
```

Default paths:

```text
input:  /workspace/data/processed/ais_parquet
output: /workspace/data/processed/aggregations
```

Generated Parquet folders:

```text
data/processed/aggregations/
  traffic_hotspots_by_grid/
  traffic_by_vessel_type_grid/
  waiting_by_grid/
  speed_by_vessel_type_grid/
```

Defaults:

```text
grid size:                  0.1 degrees
waiting speed threshold:    1.0 knot
waiting minimum duration:   30 minutes
```

Smoke test:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/aggregate_hotspots.py --row-limit 10000 --shuffle-partitions 16 --output /workspace/data/processed/aggregations_smoke
```

## Data Policy

Large source and processed datasets stay local and are not committed.

```text
data/
  raw/
  processed/
  sample/
```

## Datasets
- NOAA Marine Cadastre AIS Vessel Tracking Data from 2025
- EU MRV Ship CO2 Emissions Dataset 2024

## Research Questions
1. Which maritime regions show the highest vessel traffic intensity?
2. Which vessel types contribute most to maritime traffic intensity?
3. Can AIS data reveal congestion or waiting behavior?
4. How does vessel speed vary across vessel types and regions?
5. How are AIS activity indicators related to reported EU MRV CO2 emissions?
