# Project Limitations and Assumptions

Every data science project has limitations. In this maritime big data analysis, several biases and constraints arise from our datasets, technical setups, and choices of proxy metrics.

---

## 1. Year Mismatch Between Datasets
* **Temporal Gap:** The main vessel activity dataset (NOAA AIS) is from **2025**, while the environmental emissions dataset (EU MRV) is from **2024**.
* **Assumption:** We assume that a ship's operational profile and annual emissions remain relatively stable year-over-year.
* **Limitations:** 
  * Vessels scrapped at the end of 2024 will show up in the MRV dataset but will have no 2025 AIS records.
  * Vessels launched in 2025 will appear in the AIS tracking but will be absent from the 2024 emissions baseline.
  * Fleet changes, retrofits (e.g. installation of sails or alternate fuel systems), or changes in ship operators between 2024 and 2025 will introduce discrepancies.

---

## 2. EU MRV Dataset Coverage & Scope
* **Reporting Threshold:** Under Regulation (EU) 2015/757, only vessels above **5,000 gross tonnage** are required to report emissions.
* **Biases:**
  * Smaller vessels (tugs, fishing vessels, local ferries, yachts, small cargo ships) are entirely absent from the emissions dataset, despite representing a substantial portion of AIS points and local coastal traffic.
  * Geographic bias: Only ships performing voyages that start or end at an EEA port are included. A vessel operating exclusively in Asian or trans-Pacific routes will not match the MRV database.

---

## 3. IMO Number Quality
* **Parsing Challenges:** In raw AIS files, the `imo` field is frequently missing, contains placeholder values (e.g., `0`, `999999999`), or is malformed. Similarly, in the MRV dataset, the column contains text prefixes like `"IMO "` or trailing spaces.
* **Implication:** The cleaning step filters out rows with invalid or missing IMO numbers. This drops a significant portion of matched activity data, introducing potential selection bias (under-representing ships with poorly configured AIS transceivers).

---

## 4. Congestion Proxy Validity
* **Definition:** We define a congestion event as a vessel staying within a 0.1-degree grid cell with a Speed Over Ground (SOG) $\le$ 1.0 knot for 30 minutes or longer.
* **Limitations:**
  * Low speed does not automatically equate to congestion. A ship drifting in an ocean swell, fishing, performing survey operations, or waiting in a dedicated anchorage zone (without queuing inefficiency) will be counted as "congested".
  * Anchorages are standard operational zones, not necessarily evidence of "inefficiency." Without port log integration, we cannot verify if the waiting time was due to port capacity bottlenecks or normal scheduling.

---

## 5. Local Hardware & Scale Limitations
* **MacBook Constraints:** Developing and running large-scale Spark jobs on a single local machine is limited by CPU cores, thermal throttling, and RAM.
* **Scale Downs:** 
  * For development, we rely on small CSV samples (`data/sample/`).
  * Running the full 81.5 GB dataset on local hardware will result in Out-Of-Memory (OOM) errors or excessive runtimes. The pipeline is designed for a distributed Spark cluster (e.g., Databricks), and local execution is only meant for testing script validity.
  * HyperLogLog (Approximate Distinct Counts) is used for distinct vessel tallies to reduce shuffle memory overhead. This introduces a small margin of error (typically $\le 2\%$) in count outputs.
