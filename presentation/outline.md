# Presentation Outline: Maritime Traffic & Environmental Pressure Analysis

This document divides the 10-minute presentation slides and responsibilities between **Person A (Jonas)** and **Person B (Jan)**.

---

## Presentation Details
* **Total Time:** 10 minutes (approx. 1 minute per slide)
* **Structure:** 10 slides
* **Speakers:** Person A (Jonas) & Person B (Jan)

---

## Division of Slides

### Part 1: Pipeline, Ingestion, and Big Data Architecture (Presented by Person A - Jonas)
* **Slide 1: Title & Overview**
  * Introduce the team, roles, and project goal.
* **Slide 2: Motivation & Research Questions**
  * The growth of global shipping, maritime corridors, and the need for data-driven environmental policies.
* **Slide 3: Datasets & Ingest Challenge**
  * Describe NOAA AIS 2025 (~81.5 GB compressed CSV) and EU MRV 2024 emissions datasets. Explain why raw data isn't committed.
* **Slide 4: Spark & Docker Architecture**
  * Explain the local Spark cluster setup in Docker (Master/Worker allocation) and the schema parsing pipeline.
* **Slide 5: AIS Schema Cleaning & Parquet Ingestion**
  * Detail coordinate filtering, velocity limits, blank value normalization, deduplication, and file size benefits of Parquet format.

### Part 2: SQL Analysis, Emissions Matching, and Findings (Presented by Person B - Jan)
* **Slide 6: Traffic Hotspots & Spatial Distribution (RQ1 & RQ2)**
  * Show findings from Spark SQL hotspot queries (English Channel, ports, etc.).
* **Slide 7: Vessel Speed Profile Analysis (RQ4)**
  * Speed distributions by vessel type and region. Show how container ships compare to tankers.
* **Slide 8: Port Congestion and Bottleneck Detection (RQ3)**
  * Present waiting time proxies (low speed $\le$ 1 knot for $\ge$ 30 minutes) at ports.
* **Slide 9: CO₂ Emissions Matching Results (RQ5)**
  * Explain the IMO standardization/cleaning logic, join success statistics, and the relationship between AIS points and CO₂ emissions.
* **Slide 10: Conclusion & Project Limitations**
  * Summary of answers, dataset year mismatches, reporting thresholds, and local hardware constraints.
