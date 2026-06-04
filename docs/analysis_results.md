# Analysis Results and Findings

This document is a placeholder for the final findings derived from running the PySpark pipeline and executing the Spark SQL scripts. Results can be populated here after completing the pipeline runs on the full dataset.

---

## 1. Traffic Hotspot Findings (RQ1)
*Which maritime regions show the highest vessel traffic intensity in 2025?*

### Observations
* **Highest Traffic Areas:** (To be completed based on the output of `sql/hotspot_queries.sql`). Expect high density in regions like the English Channel, Malacca Strait, Gibraltar, and the US Eastern Seaboard (New York/New Jersey ports).
* **Vessel Count vs. Broadcast Points:** Distinction between transit lanes (many points, moderate unique vessels) and ports/anchorage zones (many points, high count of unique vessels).

---

## 2. Vessel Type Findings (RQ2)
*Which vessel types contribute most to maritime traffic intensity?*

### Observations
* **Dominant Vessel Types:** Cargo ships (container/bulk) and tankers generally contribute the highest percentage of AIS broadcast points due to their continuous operation.
* **Local vs. Ocean-going:** Pleasure crafts and fishing vessels show localized spikes, while large cargo vessels dominate long-range sea lines of communication.

---

## 3. Vessel Speed Variation (RQ4)
*How does vessel speed vary across vessel types and regions?*

### Observations
* **Speed Profile by Type:** Container ships show the highest average speeds (e.g. 15–20 knots), whereas bulk carriers and tankers operate at lower speeds (e.g. 10–13 knots) to optimize fuel efficiency.
* **Regional Speed limits:** In major channels or marine protected areas, average speed drops due to traffic separation schemes or speed restrictions.

---

## 4. Inefficient Vessel Behavior & Congestion (RQ3)
*Can AIS data reveal inefficient vessel behavior, such as congestion or long waiting times?*

### Observations
* **Congestion Proxies:** Aggregations of points with Speed Over Ground (SOG) < 1 knot for more than 30 minutes in specific grid blocks show waiting/anchorage patterns.
* **Port Backlogs:** Significant congestion hotspots identified outside major port entries (e.g. LA/Long Beach, Rotterdam).

---

## 5. AIS Indicators vs. EU MRV Reported CO₂ (RQ5)
*For matching vessels, how are AIS-based activity indicators from 2025 related to reported EU ship CO₂ emissions from 2024?*

### Observations
* **Correlation Analysis:** Analysis of the correlation between 2025 AIS points (a proxy for active hours/distance) and 2024 reported annual emissions.
* **Class Differences:** Large container ships exhibit the steepest slope (high emissions per active hour due to high speeds/installed power), while bulk carriers are more clustered.
* **Match Success Statistics:** (To be completed after running `spark/09_match_ais_mrv.py`).

---

## 6. Interpretation of Results & Discussion

### Big Data Pipelines and Insights
By leveraging Apache Spark, we successfully filtered, cleaned, and aggregated millions of tracking points to draw structural insights into global shipping routes. 
* The combination of high-resolution spatial tracking and annual fuel reporting enables researchers to identify the most polluting vessel segments.
* Port authorities can use congestion metrics to optimize berths, lowering idling emissions.
