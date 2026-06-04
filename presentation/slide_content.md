# Presentation Slide Content

This document details the text, layout, and visual suggestions for each of the 10 slides in the presentation.

---

### Slide 1: Title
* **Header:** Maritime Traffic & Environmental Pressure Analysis
* **Sub-header:** Big Data Processing of NOAA AIS Tracking and EU MRV Emissions
* **Authors:** 
  * Jonas (Data Pipelines & Spark Infrastructure)
  * Jan (ETL Cleaning, SQL Analysis & Emissions Matching)
* **Visuals:** A professional background graphic of a global container terminal or shipping routes map.

---

### Slide 2: Motivation & Research Questions
* **Header:** Motivation & Research Questions
* **Bullet Points:**
  * **Global Logistics:** 90% of global trade is carried by sea; tracking traffic is key to optimizing routes.
  * **Environmental Policy:** Shipping is a major greenhouse gas emitter; data-driven environmental regulation requires ship-level profiling.
  * **Core Research Questions:**
    1. Where are the highest density traffic hotspots?
    2. Which ship types contribute most to maritime traffic?
    3. Can AIS data reveal port bottlenecks and inefficiencies?
    4. How does velocity vary by ship category and region?
    5. How does physical AIS activity correlate with reported annual CO₂?
* **Visuals:** Clean, structured list of the 5 Research Questions with highlighted key terms.

---

### Slide 3: Datasets & Ingest Challenge
* **Header:** Datasets & Big Data Challenge
* **Bullet Points:**
  * **Primary Dataset: NOAA Marine Cadastre AIS 2025**
    * Daily tracking broadcasts (MMSI, position, speed, IMO, type).
    * **Scale:** ~81.5 GB compressed CSV. Too large for Git.
  * **Companion Dataset: EU MRV CO₂ Emissions 2024**
    * Annual reported ship fuel and CO₂ emissions (from THETIS-MRV).
    * **Scope:** Ships > 5,000 gross tonnage visiting EEA ports.
  * **Git Data Policy:** Local processing only. Raw/processed directories are strictly ignored via `.gitignore` to keep git clean.
* **Visuals:** Icons comparing AIS (high frequency, spatial, 81GB) and MRV (annual, tabular, 10MB) datasets.

---

### Slide 4: Spark & Docker Architecture
* **Header:** Local Spark Ingestion Pipeline
* **Bullet Points:**
  * **Infrastructure:** Multi-container Docker Compose cluster (`spark-master` and `spark-worker`).
  * **Resource Limits:** Configured worker container memory (8 GB) and CPU cores (4 cores) to fit local MacBook limits.
  * **Ingestion:** Spark CSV Reader parses the raw data stream using a predefined schema (`ais_schema.py`).
  * **Dynamic Directory Mounts:** Docker volumes mount host `RAW_DATA_DIR` into `/workspace/data/raw`.
* **Visuals:** Simple diagram showing Docker Containers, local host volume mount, and the Spark master/worker connection.

---

### Slide 5: Schema Cleaning & Parquet Conversion
* **Header:** AIS Schema Cleaning & Parquet Ingestion
* **Bullet Points:**
  * **Standard cleaning filters (`clean_ais.py`):**
    * Drop records missing essential location/time indicators.
    * Limit coordinates to valid ranges: Lat [-90, 90], Lon [-180, 180].
    * Filter speed over ground to standard vessel limits (0 to 60 knots).
    * Normalize empty fields to SQL NULL and drop duplicate points.
  * **Parquet File Format:**
    * Converts raw row-based CSVs to column-oriented Parquet.
    * Drastic reduction in disk space, optimized for downstream spatial queries.
* **Visuals:** Flowchart showing: Raw CSV $\rightarrow$ Clean Filters $\rightarrow$ Parquet Output.

---

### Slide 6: Traffic Hotspots & Spatial Density (RQ1 & RQ2)
* **Header:** Traffic Hotspots & Fleet Contributions
* **Bullet Points:**
  * **Spatial Gridding:** AIS points are aggregated into a 0.1-degree latitude/longitude grid (approx. 11 km cells).
  * **Hotspots:** Identified in major shipping lanes (English Channel, Dover Strait, Gibraltar) and port approaches.
  * **Fleet Shares:** Cargo vessels (container/bulk) represent the largest share of overall AIS points.
  * **Transit vs Hubs:** Chokepoints show high broadcast counts with moderate vessel variety, whereas ports show high counts of distinct vessels.
* **Visuals:** Screenshot of Tab 1 Dashboard (hotspot map with colored grid density).

---

### Slide 7: Vessel Speed Profile Analysis (RQ4)
* **Header:** Speed Profiles by Vessel Class
* **Bullet Points:**
  * **Speed Statistics:** PySpark calculated average, median, min, max, and p90 speeds.
  * **Fleet Comparisons:**
    * *Container Ships:* High average speed (15–20 knots), steep energy curve.
    * *Bulk Carriers / Tankers:* Slower transits (10–13 knots), optimized for slow-steaming fuel savings.
  * **Spatial Speed Limits:** Speed reductions clearly visible in port entry lanes and protected environmental channels.
* **Visuals:** Boxplot chart showing speed ranges by vessel type (e.g. Container vs. Bulker).

---

### Slide 8: Port Congestion & Idle Time Proxies (RQ3)
* **Header:** Congestion & Bottleneck Detection
* **Bullet Points:**
  * **Proxy Metric:** A vessel is flagged in "waiting status" when Speed Over Ground (SOG) is $\le$ 1 knot for $\ge$ 30 minutes inside a grid cell.
  * **Grid Aggregation:** Total idle hours summed by grid block over the period.
  * **Backlogs:** High cumulative waiting hours mapped directly outside major ports (e.g., Rotterdam, LA/Long Beach, Singapore).
  * **Utility:** Port operators can use this to measure port turnaround efficiency and schedule optimization.
* **Visuals:** Heatmap showing geographic clusters of waiting times near port approaches.

---

### Slide 9: CO₂ Emissions Matching Results (RQ5)
* **Header:** Matching Activity with Reported Emissions
* **Bullet Points:**
  * **Join Key Standardization:**
    * IMO numbers cleaned by stripping characters/spaces and verifying exactly 7 digits.
    * Inner join on IMO link AIS activity and MRV emissions.
  * **Matching Stats:** Matches large merchant ships (cargo, tanker, passenger, LNG), while excluding small vessels (tugs, fishing).
  * **Correlation:** Shows a strong linear trend between AIS point count (hours active) and annual reported CO₂ emissions.
* **Visuals:** Scatter plot comparing AIS Point Count (X-axis) and Total CO₂ Emissions (Y-axis), colored by ship type.

---

### Slide 10: Conclusion & Project Limitations
* **Header:** Conclusion & Project Limitations
* **Bullet Points:**
  * **Summary:** Successfully leveraged Apache Spark to process large-scale AIS data, identifying hubs, speed profiles, and linking emissions.
  * **Key Project Limitations:**
    * **Year Mismatch:** AIS data (2025) and MRV emissions (2024) are offset.
    * **Emissions Reporting Threshold:** Only ships > 5,000 GT are in MRV; smaller vessels are unrepresented.
    * **Congestion Proxy:** Idle speeds are a proxy, not direct confirmation of port inefficiency.
    * **Hardware:** Local MacBook runs require downscaling and sample datasets.
* **Visuals:** Balanced list showing project contributions on one side, and key caveats on the other.
