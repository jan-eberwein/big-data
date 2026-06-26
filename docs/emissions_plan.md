# Emissions & SQL Analysis Plan - Jan

This document outlines the specific responsibilities, workflow, and remaining deliverables for Jan in the Maritime Traffic and Environmental Pressure Analysis project.

## Responsibilities Overview

As part of this collaborative effort, Jan is responsible for integrating environmental metrics with spatial/activity indicators from AIS, performing the downstream SQL analysis to address research questions, designing the presentation structure, and formulating BI guidelines.

### Summary of Tasks

1. **EU MRV CO₂ Emissions Preparation:**
   * Ingest THETIS-MRV 2024 emissions CSV.
   * Clean and standardise the raw fields (especially the IMO number).
   * Write output to clean Parquet for Spark joining.

2. **AIS and MRV Joining:**
   * Join Jonas' AIS summaries (grouped by IMO number) with the cleaned emissions dataset.
   * Log matching success rate, and output the matched table to Parquet.

3. **Spark SQL Queries:**
   * Create standard SQL files targeting traffic intensity, speed, congestion, and emissions.
   * Ensure queries are optimised for large-scale execution using Spark SQL structures.

4. **BI & Dashboards:**
   * Document how to load aggregated Parquets into BI platforms (Tableau, Databricks SQL Dashboards, Apache Superset, or Kibana).
   * Design dashboard layout and metrics.

5. **Presentation & Documentation:**
   * Detail limitations of the datasets and proxies.
   * Prepare a slide outline, slides, and scripts for a joint 10-minute presentation.

---

## Technical Deliverables

| Deliverable Path | Description | Status |
|---|---|---|
| `src/04_prepare_mrv_emissions.py` | PySpark ETL to clean raw MRV emissions CSV | Complete |
| `src/05_match_ais_mrv.py` | PySpark script to join AIS and MRV datasets | Complete |
| `sql/hotspot_queries.sql` | Spark SQL queries for traffic hotspots | Complete |
| `sql/vessel_type_queries.sql` | Spark SQL queries for vessel type analysis | Complete |
| `sql/speed_queries.sql` | Spark SQL queries for speed statistics | Complete |
| `sql/congestion_queries.sql` | Spark SQL queries for congestion proxies | Complete |
| `sql/emissions_queries.sql` | Spark SQL queries for AIS vs CO₂ emissions | Complete |
| `visualization/instructions.md` | BI Dashboard setup instructions | Complete |
| `presentation/outline.md` | Joint presentation division outline | Complete |
| `presentation/slide_content.md` | Detailed text and layout for slides 1–10 | Complete |
| `presentation/script.md` | 10-minute presentation script | Complete |

---

## Workflow & Collaboration

To keep work separated and clean, all emissions and SQL code is developed on the `feature/emissions-analysis` branch.
Outputs from Jonas' AIS pipeline are expected in `data/processed/ais_activity_by_imo.parquet`. During development, sample placeholders in `data/sample/` are used to run smoke tests locally.
Once both the AIS and emissions pipelines are ready and tested on samples, they can be merged and run against the full datasets on the Docker Spark cluster.
