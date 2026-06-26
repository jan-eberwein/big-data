# Analysis Results and Findings

Results from running the PySpark pipeline on full-year 2025 NOAA Marine Cadastre AIS data
(U.S. coastal waters) matched against EU MRV 2024 annual emissions. All figures are produced by
the pipeline (steps 01–07) and exported to `dashboard/assets/*.json`.

> **Coverage note:** NOAA AIS covers U.S. coastal and inland waters only, so every traffic and
> congestion finding below refers to U.S. ports and lanes — not global shipping routes.

---

## 1. Traffic Hotspot Findings (RQ1)
*Which maritime regions show the highest vessel traffic intensity in 2025?*

### Observations
* **Highest-traffic cells (0.5° grid, by AIS broadcast density):**
  1. Houston Ship Channel / Galveston Bay, TX (~104 M points)
  2. Puget Sound / Seattle, WA (~101 M)
  3. Fort Lauderdale / Port Everglades, FL (~88 M)
  4. Mississippi River delta / New Orleans, LA (~84 M)
  5. San Pedro Bay — LA / Long Beach, CA (~65 M)
  6. Galveston, TX (~61 M)
* **Gulf, Pacific Northwest, and Southeast Florida dominate.** The Texas Gulf coast (Houston–Galveston)
  and the Mississippi delta reflect dense tanker/cargo and tug-and-tow industrial traffic; Puget Sound
  and SE Florida are driven by large recreational and passenger fleets.
* **Broadcast density vs. unique vessels:** industrial port channels show very high ping counts from a
  comparatively small set of commercial vessels dwelling in the area, whereas recreational hotspots
  (Puget Sound, SE Florida) combine high density with large distinct-vessel counts.

---

## 2. Vessel Type Findings (RQ2)
*Which vessel types contribute most to maritime traffic intensity?*

### Observations (full fleet, by total AIS position reports)
| Vessel type group | AIS points | Share |
|---|---|---|
| Pleasure craft / sailing | ~920 M | highest |
| Other / non-classified | ~749 M | |
| Tug & tow | ~547 M | |
| Cargo | ~234 M | |
| Fishing | ~215 M | |
| Passenger | ~208 M | |
| Tanker | ~106 M | |

* **Recreational and tug-and-tow traffic dominate raw broadcast volume**, because these vessels operate
  intensively in coastal and harbour waters where AIS reporting is dense.
* **Commercial cargo and tanker traffic** is a smaller share of total points but accounts for nearly all
  of the matched CO₂ emissions (see RQ5) — the vessels that matter environmentally are not the ones that
  dominate the ping count.

---

## 3. Vessel Speed Variation (RQ4)
*How does vessel speed vary across vessel types and regions?*

### By vessel type (full-year average SOG, knots)
* **Tankers (5.66) and cargo ships (5.51)** show the highest average speeds — they spend more of the year
  underway in transit.
* **Passenger vessels** average 3.99 kn but have the widest spread (P90 ≈ 9.6 kn), mixing slow harbour
  manoeuvres with scheduled transits.
* **Pleasure craft (1.03), tug & tow (1.59), and fishing (1.82)** sit lowest — consistent with harbour,
  anchorage, and station-keeping behaviour.
* Absolute averages are low across the board because they are computed over the **entire year**, including
  large amounts of moored/anchored idle time; the *relative ordering* is the meaningful result.

### By maritime region (average SOG, knots)
* **Caribbean & Puerto Rico (3.20) and Great Lakes (2.85)** are fastest — more open-water transit.
* **Gulf of Mexico (1.80) and US West Coast (1.87)** are slowest — congested industrial port complexes
  and dense recreational traffic.
* **Alaska is omitted** — the 2025 feed contains negligible data above 50°N (~43 k points vs. ~1 B in the Gulf).

---

## 4. Inefficient Vessel Behavior & Congestion (RQ3)
*Can AIS data reveal inefficient vessel behavior, such as congestion or long waiting times?*

### Observations
* **Congestion proxy:** grid cells where vessels hold SOG ≤ 1 knot for ≥ 30 minutes are flagged as waiting
  events and ranked by total waiting time.
* **Most congested U.S. areas:**
  1. Puget Sound / Seattle, WA
  2. Fort Lauderdale / Port Everglades, FL
  3. West Palm Beach, FL
  4. San Diego, CA
* These coincide with major commercial-port approaches and large anchorage/marina zones — the proxy
  captures both genuine port backlog and routine anchoring, which is a known limitation.

---

## 5. AIS Indicators vs. EU MRV Reported CO₂ (RQ5)
*For matching vessels, how are AIS-based activity indicators from 2025 related to reported EU ship CO₂ emissions from 2024?*

### Matching Results — Full-Year Run
| Metric | Value |
|---|---|
| Unique AIS vessels (IMO) | 20,421 |
| Unique MRV vessels | 14,149 |
| Matched vessels (inner join) | 6,763 |
| AIS match rate | 33.12% |
| MRV match rate | 47.80% |
| Total matched CO₂ | ~59.76 million tonnes |
| Vessel types matched | 17 distinct categories |

### Emissions by Ship Type (Top 5 by total CO₂)
| Ship Type | Vessels | Total CO₂ (t) | Avg CO₂/vessel (t) |
|---|---|---|---|
| Container ship | 825 | 20,801,211 | 25,808 |
| Oil tanker | 997 | 9,377,915 | 9,698 |
| Bulk carrier | 2,389 | 8,221,904 | 3,638 |
| Passenger ship | 138 | 4,558,474 | 33,032 |
| Chemical tanker | 755 | 4,027,539 | 5,517 |

### Top Individual Emitters
The highest-CO₂ matched ships are dominated by cruise and large passenger vessels — e.g. *Ventura*,
*Norwegian Epic*, *MSC Seaside*, *Oasis of the Seas* — alongside LNG carriers such as
*Ribera del Duero Knutsen*, each reporting ~90,000–107,000 tonnes CO₂.

### Key Observations
* **Container ships dominate total CO₂** (20.8 M tonnes across 825 vessels) — the single largest contributor.
* **Passenger and cruise ships have the highest per-vessel footprint** (~33,000–36,000 tonnes avg),
  reflecting intensive hotel and propulsion loads.
* **Bulk carriers are the most numerous matched type** (2,389 vessels) but emit far less per ship,
  consistent with slow-steaming.
* **Activity vs. emissions:** higher AIS activity (more pings, more active days) broadly tracks higher
  reported CO₂, but the relationship is moderated by ship type — a high-speed container ship emits far
  more per active day than a slow bulk carrier with comparable tracking.

---

## 6. Interpretation & Discussion

Using Apache Spark on a local Docker cluster, we ingested, cleaned, and aggregated a full year of AIS
tracking data (~3 billion position reports) and joined it with official EU emissions records.

* The **33% AIS / 48% MRV match rate** is bounded by geography: NOAA AIS covers U.S. waters while EU MRV
  reports ships calling EEA ports, so only the globally-trading vessels that appear in both are matched.
* The relative rankings of ship types by emissions align with expectations from maritime literature, even
  though the two datasets cover different years (AIS 2025 vs. MRV 2024).
* The congestion proxy and per-region speed breakdown surface plausible U.S. port-level patterns, but
  cannot by themselves distinguish scheduled anchorage from genuine backlog — integrating port logbook
  data would resolve this.
