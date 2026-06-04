# Maritime Traffic and Environmental Pressure Analysis

This project is a two-person Big Data analytics pipeline that processes global AIS vessel tracking telemetry and integrates it with reported EU MRV ship CO₂ emissions. The pipeline runs on Apache Spark (Spark SQL, PySpark) deployed via Docker, saving aggregated results in column-oriented Parquet files.

---

## 1. Project Overview & Research Questions

The core objective is to analyze shipping movements and environmental pressure. We address the following five research questions:
1. **RQ1:** Which maritime regions show the highest vessel traffic intensity in 2025?
2. **RQ2:** Which vessel types contribute most to maritime traffic intensity?
3. **RQ3:** Can AIS data reveal inefficient vessel behavior, such as congestion or long waiting times?
4. **RQ4:** How does vessel speed vary across vessel types and regions?
5. **RQ5:** For matching vessels, how are AIS-based activity indicators from 2025 related to reported EU ship CO₂ emissions from 2024?

---

## 2. Technology Stack & Architecture

* **Data Processing:** Apache Spark 4.1.2 (PySpark, Spark SQL)
* **Storage Format:** Parquet (column-oriented, compressed)
* **Environment Setup:** Docker Compose (local Spark Master & Worker containers)
* **Visualizations:** Tableau, Databricks SQL Dashboards, or Apache Superset
* **Programming Language:** Python 3 (pyspark library)

---

## 3. Team Roles & Responsibilities

* **Person A (Jonas):**
  * Spark cluster configuration (Docker)
  * Raw AIS dataset ingestion and schema mapping (`src/ais_schema.py`)
  * Data filtering and position cleaning (`src/clean_ais.py`, `src/load_ais.py`)
  * Spatial gridding and hotspot/waiting-time aggregation (`src/aggregate_hotspots.py`)

* **Person B (Jan):**
  * EU MRV CO₂ emissions dataset preparation (`spark/08_prepare_mrv_emissions.py`)
  * IMO number cleaning, standardization, and dataset matching (`spark/09_match_ais_mrv.py`)
  * Spark SQL analytical query development (`sql/`)
  * BI Dashboard design and visualization guidelines (`visualization/`)
  * Presentation slides, outline, script, and documentation (`presentation/`, `docs/`)

---

## 4. Local Data Structure & Setup

Due to size limits, raw datasets and processed outputs are **never** committed to GitHub. They are ignored using `.gitignore`.

### Expected Local Directory Layout
Place raw files manually according to the following layout:
```text
data/
  raw/
    ais/
      AIS_2025_01_01.csv  <-- Downloaded raw NOAA AIS files
      ...
    mrv/
      eu_mrv_2024.csv     <-- Downloaded raw EU MRV emissions CSV
  processed/
    ais_parquet/          <-- Cleaned AIS outputs in Parquet
    aggregations/         <-- Aggregated grids in Parquet
    mrv_emissions_clean.parquet
    ais_mrv_matched.parquet
  sample/
    mrv_emissions_sample.csv
    ais_activity_by_imo_sample.csv
```

### Dataset Download Locations
1. **NOAA AIS 2025 Data:** Download daily CSVs from the [NOAA Marine Cadastre Portal](https://marinecadastre.gov/data/) or directly via HTTPS server: `https://coast.noaa.gov/htdata/AIS/2025/`.
2. **EU MRV 2024 Data:** Download the annual Excel/CSV spreadsheet from the [EMSA THETIS-MRV Public Portal](https://mrv.emsa.europa.eu/#public/licence) (Public Information -> Emissions).

---

## 5. Running the Pipeline

### Start the Spark Cluster
Initialize the Docker Spark container nodes:
```bash
docker compose up -d
```
You can view the Spark Master Web UI at `http://localhost:8080` and Worker UI at `http://localhost:8081`.

---

### Step A: Run Person A Ingestion & Aggregation (Jonas)

1. **Load and Clean AIS CSVs:**
   Reads raw CSVs from `/workspace/data/raw`, applies cleaning filters, and saves cleaned Parquet to `/workspace/data/processed/ais_parquet`.
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/load_ais.py
   ```
   *To run a quick test with row limits and output a sample:*
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/load_ais.py --sample-output /workspace/data/sample/ais_sample --sample-limit 10000
   ```

2. **Aggregate Traffic Hotspots & Congestion:**
   Processes the cleaned Parquet files to generate spatial grid aggregates (hotspots, speed statistics, and port waiting times).
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/aggregate_hotspots.py
   ```
   *Smoke test with limited rows:*
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/aggregate_hotspots.py --row-limit 10000 --shuffle-partitions 16 --output /workspace/data/processed/aggregations_smoke
   ```

---

### Step B: Run Person B Emissions & Matching (Jan)

1. **Prepare and Clean MRV Emissions Data:**
   Reads the raw emissions CSV, standardizes and cleans the IMO number field, formats emission counts, and exports clean Parquet.
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/spark/08_prepare_mrv_emissions.py
   ```
   *Local test using sample files (runs outside docker if PySpark is installed):*
   ```bash
   python3 spark/08_prepare_mrv_emissions.py --input data/sample/mrv_emissions_sample.csv --output data/processed/mrv_emissions_clean.parquet --sample-csv output/tables/mrv_emissions_clean_sample.csv
   ```

2. **Match AIS Activity with MRV Emissions:**
   Performs an inner join between AIS activity summaries (by IMO) and the cleaned MRV emissions dataset. Logs matching success and outputs matched Parquet.
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/spark/09_match_ais_mrv.py
   ```
   *Local test using sample files:*
   ```bash
   python3 spark/09_match_ais_mrv.py --ais-input data/sample/ais_activity_by_imo_sample.csv --mrv-input data/processed/mrv_emissions_clean.parquet --output data/processed/ais_mrv_matched.parquet
   ```

---

## 6. Running SQL Queries

Spark SQL queries addressing the five research questions are saved under the `sql/` directory:
* `sql/hotspot_queries.sql` (RQ1 & RQ2)
* `sql/vessel_type_queries.sql` (RQ2)
* `sql/speed_queries.sql` (RQ4)
* `sql/congestion_queries.sql` (RQ3)
* `sql/emissions_queries.sql` (RQ5)

These queries can be copy-pasted directly into a Spark SQL shell, Databricks Query Editor, or loaded dynamically inside a Jupyter Notebook.

---

## 7. BI Dashboard & Visualization

Refer to [instructions.md](file:///Users/jan/Library/CloudStorage/Dropbox/STUDIUM/MASTER/4. Semester/Big Data/big-data/visualization/instructions.md) inside the `visualization/` directory for detail on loading the final aggregated Spark tables into **Tableau** or **Databricks SQL Dashboards** to create density maps, fleet speed histograms, and emissions correlation scatter plots.

---

## 8. Presentation Outline & Script

Slide notes and division of labor for the 10-minute final presentation are located in the `presentation/` directory:
* [outline.md](file:///Users/jan/Library/CloudStorage/Dropbox/STUDIUM/MASTER/4. Semester/Big Data/big-data/presentation/outline.md): Speaker time allocations.
* [slide_content.md](file:///Users/jan/Library/CloudStorage/Dropbox/STUDIUM/MASTER/4. Semester/Big Data/big-data/presentation/slide_content.md): Layout and text definitions.
* [script.md](file:///Users/jan/Library/CloudStorage/Dropbox/STUDIUM/MASTER/4. Semester/Big Data/big-data/presentation/script.md): Word-for-word spoken transcript.

---

## 9. Key Project Limitations

1. **Year Mismatch:** We match AIS tracking from 2025 with emissions data from 2024.
2. **Gross Tonnage Threshold:** EU MRV only captures vessels > 5,000 GT, excluding small vessels (tugs, fishing boats, pleasure crafts).
3. **Congestion Proxy:** Idle speeds ($\le$ 1 knot for 30+ mins) are used as a proxy for congestion, which can also capture standard anchorages.
4. **Local Hardware Constraints:** Single machine execution is limited by memory; production requires cloud Spark clusters. Detailed descriptions of limits are in [limitations.md](file:///Users/jan/Library/CloudStorage/Dropbox/STUDIUM/MASTER/4. Semester/Big Data/big-data/docs/limitations.md).
