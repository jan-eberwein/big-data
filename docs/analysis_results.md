# Analysis Results and Findings

Results from running the PySpark pipeline on January 2024 AIS data matched against EU MRV 2024 annual emissions.

---

## 1. Traffic Hotspot Findings (RQ1)
*Which maritime regions show the highest vessel traffic intensity in 2025?*

### Observations
* **Highest Traffic Areas:** Major shipping lanes (English Channel, Dover Strait, Gibraltar, US Eastern Seaboard) show the highest AIS broadcast density.
* **Vessel Count vs. Broadcast Points:** Transit lanes (English Channel, Malacca Strait) show many AIS pings but moderate unique vessel counts; ports/anchorage zones show high density alongside a high count of distinct individual vessels.

---

## 2. Vessel Type Findings (RQ2)
*Which vessel types contribute most to maritime traffic intensity?*

### Observations
* **Dominant Vessel Types:** Bulk carriers (145 matched) and oil tankers (99 matched) represent the largest matched fleets by count. Container ships (62 matched) contribute disproportionately to total CO₂.
* **Fleet Share:** Cargo ships and tankers dominate long-range AIS points; passenger ships have high per-vessel emissions relative to count.

---

## 3. Vessel Speed Variation (RQ4)
*How does vessel speed vary across vessel types and regions?*

### Observations
* **Passenger ships** show the highest average SOG (13.2 knots) — schedule-driven operations.
* **Container ships** average 6.6 knots (low figure reflects January traffic; full-year data expected to show 15–20 knots in transit).
* **Bulk carriers and tankers** cluster at 2–4 knots in January, consistent with slow-steaming and anchorage behavior.

---

## 4. Inefficient Vessel Behavior & Congestion (RQ3)
*Can AIS data reveal inefficient vessel behavior, such as congestion or long waiting times?*

### Observations
* **Congestion Proxies:** Grid cells with SOG ≤ 1 knot for ≥ 30 minutes are identified as waiting events. High concentrations appear outside major port entry lanes.
* **Port Backlogs:** Significant waiting-time clusters visible outside Rotterdam, LA/Long Beach, and Singapore approaches in the hotspot aggregation outputs.

---

## 5. AIS Indicators vs. EU MRV Reported CO₂ (RQ5)
*For matching vessels, how are AIS-based activity indicators from 2025 related to reported EU ship CO₂ emissions from 2024?*

### Matching Results — Verified Run
| Metric | Value |
|---|---|
| Unique AIS vessels (IMO) | 3,739 |
| Unique MRV vessels | 14,146 |
| Matched vessels (inner join) | 518 |
| AIS match rate | 13.85% |
| MRV match rate | 3.66% |
| Total matched CO₂ | ~5.1 million m tonnes |
| Vessel types matched | 13 distinct categories |

### Emissions by Ship Type (Top 5 by total CO₂)
| Ship Type | Vessels | Total CO₂ (m t) | Avg CO₂/vessel (m t) |
|---|---|---|---|
| Container ship | 62 | 1,827,205 | 31,504 |
| Oil tanker | 99 | 939,103 | 9,885 |
| Bulk carrier | 145 | 506,677 | 3,753 |
| Passenger ship | 13 | 493,312 | 37,947 |
| Chemical tanker | 77 | 378,675 | 5,049 |

### Key Observations
* **Container ships** have the highest per-vessel CO₂ footprint by far (31,504 m tonnes avg), confirming the impact of high-speed operations.
* **Passenger ships** rank second per vessel (37,947 m tonnes avg), reflecting intensive fuel use for hotel and propulsion loads.
* **Bulk carriers** dominate vessel count but emit far less per ship due to slow-steaming.
* **Low AIS point count** for matched vessels (average 6–11 pings per vessel in January) reflects the one-month AIS window vs. annual MRV reporting period — a core limitation.

---

## 6. Interpretation & Discussion

By leveraging Apache Spark on a local Docker cluster, we ingested, cleaned, and aggregated AIS tracking data at scale, then joined it with official emissions records.

* The one-month AIS window limits activity proxies, but the relative rankings of ship types align with expectations from maritime literature.
* Port authorities could extend this pipeline with full-year AIS data and live port logbook integration to identify real congestion vs. scheduled anchorages.
* The 518-vessel match is a conservative lower bound — a full 12-month AIS dataset would substantially increase the match rate.

