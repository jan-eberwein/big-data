# Maritime Traffic and Environmental Pressure Analysis

This project is a collaborative Big Data analytics pipeline that processes U.S. coastal AIS vessel tracking telemetry (NOAA Marine Cadastre, full-year 2025) and integrates it with reported EU MRV ship CO₂ emissions. The pipeline runs on Apache Spark (Spark SQL, PySpark) deployed via Docker, saving aggregated results in column-oriented Parquet files.

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

* **Jonas:**
  * Spark cluster configuration (Docker)
  * Raw AIS dataset ingestion and schema mapping (`src/ais_schema.py`)
  * Data filtering and position cleaning (`src/clean_ais.py`, `src/01_load_ais.py`)
  * Spatial gridding and hotspot/waiting-time aggregation (`src/02_aggregate_hotspots.py`)

* **Jan:**
  * EU MRV CO₂ emissions dataset preparation (`src/04_prepare_mrv_emissions.py`)
  * IMO number cleaning, standardization, and dataset matching (`src/05_match_ais_mrv.py`)
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
      ais-2025-01-01.csv.zst  <-- Downloaded raw NOAA AIS files (zstd-compressed)
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
1. **NOAA AIS 2025 Data:** Daily zstd-compressed CSVs (`ais-2025-MM-DD.csv.zst`) from the Marine Cadastre Azure
   blob: `https://noaaocm.blob.core.windows.net/ais/csv2/csv2025/`. The repo includes a resumable parallel
   downloader — `./download_ais_2025.sh` fetches the full year (~85 GB) to the configured `RAW_DATA_DIR`. Spark
   reads `.csv.zst` natively, so no decompression step is needed.
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

### Step A: Run Ingestion & Aggregation (Jonas)

1. **Load and Clean AIS CSVs:**
   Reads raw CSVs from `/workspace/data/raw`, applies cleaning filters, and saves cleaned Parquet to `/workspace/data/processed/ais_parquet`.
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/01_load_ais.py
   ```
   *To run a quick test with row limits and output a sample:*
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/01_load_ais.py --sample-output /workspace/data/sample/ais_sample --sample-limit 10000
   ```

2. **Aggregate Traffic Hotspots & Congestion:**
   Processes the cleaned Parquet files to generate spatial grid aggregates (hotspots, speed statistics, and port waiting times).
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/02_aggregate_hotspots.py
   ```
   *Smoke test with limited rows:*
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/02_aggregate_hotspots.py --row-limit 10000 --shuffle-partitions 16 --output /workspace/data/processed/aggregations_smoke
   ```

3. **Aggregate AIS Activity by IMO:**
   Rolls the cleaned position Parquet up to one row per vessel (IMO) — ping counts, distinct grid cells visited, average speed, vessel type. This per-vessel activity table is what the emissions match (step 05) joins against, so it must run before Step B.
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/03_prepare_ais_activity.py
   ```
   *Full-year runs need a real executor heap (the default 1 GB OOMs); pass Spark resource flags before the script:*
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 28g --executor-cores 12 --total-executor-cores 12 /workspace/src/03_prepare_ais_activity.py --shuffle-partitions 2000
   ```

---

### Step B: Run Emissions & Matching (Jan)

1. **Prepare and Clean MRV Emissions Data:**
   Reads the raw emissions CSV, standardizes and cleans the IMO number field, formats emission counts, and exports clean Parquet.
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/04_prepare_mrv_emissions.py
   ```
   *Local test using sample files (runs outside docker if PySpark is installed):*
   ```bash
   python3 src/04_prepare_mrv_emissions.py --input data/sample/mrv_emissions_sample.csv --output data/processed/mrv_emissions_clean.parquet --sample-csv output/tables/mrv_emissions_clean_sample.csv
   ```

2. **Match AIS Activity with MRV Emissions:**
   Performs an inner join between AIS activity summaries (by IMO) and the cleaned MRV emissions dataset. Logs matching success and outputs matched Parquet.
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/05_match_ais_mrv.py
   ```
   *Local test using sample files:*
   ```bash
   python3 src/05_match_ais_mrv.py --ais-input data/sample/ais_activity_by_imo_sample.csv --mrv-input data/processed/mrv_emissions_clean.parquet --output data/processed/ais_mrv_matched.parquet
   ```

3. **Export Dashboard Assets:**
   Writes the JSON files the interactive dashboard reads into `dashboard/assets/`. Step 06 exports the
   emissions/matched datasets (+ KPI `summary.json`); step 07 exports the hotspot map grid (coarsened to
   0.5° cells), traffic-by-type, speed-by-type, speed-by-region, and congestion datasets from the step-02
   aggregations.
   Run both after the steps above:
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/06_export_dashboard_data.py
   docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/07_export_hotspot_assets.py
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

### Interactive dashboard (`dashboard/index.html`)

The dashboard is a self-contained page (Chart.js + Leaflet) that **loads its data at runtime** from
`dashboard/assets/*.json`. Those files are produced by pipeline steps **06 and 07** (see §5.B.3), so the whole
dashboard — KPIs, hotspot map (RQ1), traffic-by-type (RQ2), congestion (RQ3), speed by type & region (RQ4), and
the activity-vs-emissions scatter / top emitters (RQ5) — regenerates from one pipeline run. To refresh: re-run
steps 06 + 07, then reload the page.

Because it uses `fetch()`, it **must be served over HTTP** (browsers block `fetch` from `file://`):
```bash
python3 -m http.server 8000 --directory dashboard
# open http://localhost:8000
```

**Deploying to a web server / VPS:** upload **`index.html` and the `assets/` folder together** at the same path
(the fetch paths are relative — `assets/<name>.json`). Same origin means no CORS config is needed. Re-deploying
after a new run = upload the updated `assets/*.json` (and `index.html` if it changed).

### BI tools
Refer to [instructions.md](visualization/instructions.md) inside the `visualization/` directory for loading the
aggregated Spark tables into **Tableau** or **Databricks SQL Dashboards**.

---

## 8. Presentation Outline & Script

Slide notes and division of labor for the 10-minute final presentation are located in the `presentation/` directory:
* [outline.md](presentation/outline.md): Speaker time allocations.
* [slide_content.md](presentation/slide_content.md): Layout and text definitions.
* [script.md](presentation/script.md): Word-for-word spoken transcript.

---

## 9. Key Project Limitations

1. **Year Mismatch:** We match AIS tracking from 2025 with emissions data from 2024.
2. **Geographic Coverage:** NOAA Marine Cadastre AIS covers U.S. coastal waters, while EU MRV reports ships calling EEA ports. The overlap is inherently limited, which bounds the IMO match rate. Coverage within the 2025 feed is also uneven — e.g. Alaska has negligible data (~43k points above 50°N vs. ~1 billion in the Gulf of Mexico), so it is omitted from the per-region speed view.
3. **Gross Tonnage Threshold:** EU MRV only captures vessels > 5,000 GT, excluding small vessels (tugs, fishing boats, pleasure crafts).
4. **Congestion Proxy:** Idle speeds ($\le$ 1 knot for 30+ mins) are used as a proxy for congestion, which can also capture standard anchorages.
5. **Local Hardware Constraints:** Single machine execution is limited by memory; production requires cloud Spark clusters. Detailed descriptions of limits are in [limitations.md](docs/limitations.md).
