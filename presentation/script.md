# Presentation Script (10 Minutes)

This script provides spoken notes and time allocations for each slide. Jonas presents the data pipeline and aggregation, while Jan presents the SQL analysis, emissions matching, and dashboard design.

---

### [0:00 - 1:00] Slide 1: Title & Overview (Presenter: Jonas)
* **Jonas:** "Good morning everyone. Welcome to our presentation on Maritime Traffic and Environmental Pressure Analysis. My name is Jonas, and here with me is my teammate Jan. For this project, we divided our roles based on a typical data engineering and analytics split: I managed the Spark cluster, raw AIS data ingestion, and cleaning pipeline. Jan was responsible for the environmental emissions data ingestion, matching the datasets by IMO number, running the SQL research queries, and designing the BI dashboards. Our goal is to extract spatial and environmental patterns from shipping tracking data."

---

### [1:00 - 2:00] Slide 2: Motivation & Research Questions (Presenter: Jonas)
* **Jonas:** "Maritime transport is the backbone of global trade, carrying around 90% of global goods. However, this sector is also a major source of global greenhouse gas emissions. Traditional reporting is annual and aggregated. By combining high-frequency spatial tracking data from AIS with official annual emissions reports, we can build a much more detailed, ship-level environmental profile. We formulated five research questions, moving from general traffic hotspot mapping to vessel speeds, port congestion, and finally, correlating AIS physical activity with reported carbon footprints."

---

### [2:00 - 3:00] Slide 3: Datasets & Ingest Challenge (Presenter: Jonas)
* **Jonas:** "To address these questions, we leveraged two datasets. Our primary dataset is the NOAA Marine Cadastre AIS Vessel Tracking Data for 2025. This contains daily vessel positions, speeds, and vessel identifiers. The raw dataset is massive, totaling around 81.5 gigabytes of compressed CSV files, which presents a significant big data storage and processing challenge. Because of this scale, we established a strict local data policy, ignoring all raw and processed files in git using `.gitignore` to keep our codebase clean. Our companion dataset is the EU MRV Ship CO₂ Emissions database for 2024, which contains official annual emissions reports for ships exceeding 5,000 gross tonnage visiting European ports."

---

### [3:00 - 4:00] Slide 4: Spark & Docker Architecture (Presenter: Jonas)
* **Jonas:** "Processing an 80-gigabyte dataset on a single local laptop requires careful infrastructure configuration. We set up a local Spark cluster using Docker Compose, creating a master node and a worker node. To prevent local hardware crashes, we limited the worker to 4 CPU cores and 8 gigabytes of RAM. The raw data directories on the host machine are mounted as read-only volumes inside the Docker containers at `/workspace/data/raw`. This enables Spark to stream and read the CSV files concurrently directly from the host filesystem without copying files into the containers."

---

### [4:00 - 5:00] Slide 5: Schema Cleaning & Parquet Ingestion (Presenter: Jonas)
* **Jonas:** "In our ingestion script, `load_ais.py`, we defined a strict Spark StructType schema to read the CSV rows without expensive type inference. We applied several cleaning filters to handle raw data noise: dropping rows with missing locations, restricting coordinates to valid global ranges, filtering speeds between 0 and 60 knots, and removing duplicate transponder broadcasts. Finally, we wrote the cleaned records out as columnar Parquet files. This reduced the data footprint on disk, and optimized downstream spatial queries. I will now hand over to Jan to present the SQL analysis and emissions matching."

---

### [5:00 - 6:00] Slide 6: Traffic Hotspots & Spatial Density (Presenter: Jan)
* **Jan:** "Thank you, Jonas. Once the clean Parquet dataset was ready, we registered temporary Spark SQL views to perform our analyses. For Research Question 1 and 2, we mapped traffic intensity by dividing coordinates into a grid of 0.1-degree cells. Our SQL query ranked these cells by overall point density and unique vessel count. Not surprisingly, major chokepoints like the English Channel and Dover Strait emerged with the highest broadcast counts. We also noted that transit lanes show high point counts but a moderate count of unique vessels, whereas ports show high density alongside a very high count of unique, diverse ship profiles."

---

### [6:00 - 7:00] Slide 7: Vessel Speed Profile Analysis (Presenter: Jan)
* **Jan:** "For Research Question 4, we analyzed vessel speed profiles. Shipping speed is a critical variable because fuel consumption increases cubically with vessel velocity. Using Spark SQL, we aggregated average and percentile speeds by ship class. Our findings show that Container ships maintain the highest average transit speeds, often between 15 and 20 knots, reflecting the time-sensitive nature of consumer goods. In contrast, bulk carriers and tankers cluster at much lower speeds of 10 to 13 knots, showing widespread adoption of 'slow-steaming' to cut fuel costs. We can also visually identify port boundaries where speeds drop below 3 knots."

---

### [7:00 - 8:00] Slide 8: Port Congestion & Idle Time Proxies (Presenter: Jan)
* **Jan:** "Research Question 3 asks if AIS can reveal port congestion. To model this, we defined a 'waiting event' proxy: when a ship stays inside a grid cell with a Speed Over Ground of 1 knot or less for at least 30 minutes. We then aggregated the total waiting hours per grid block. When we rank these grid cells, we see massive waiting times concentrated directly outside major ports like Rotterdam and LA/Long Beach. This demonstrates that spatial tracking can identify shipping inefficiencies and port bottlenecks without requiring private port logbooks, which is valuable for logistics planning."

---

### [8:00 - 9:00] Slide 9: CO₂ Emissions Matching Results (Presenter: Jan)
* **Jan:** "For Research Question 5, we integrated our tracking data with the EU emissions database. The key challenge was standardizing the join key. The raw MRV data had text prefixes like 'IMO ' or trailing spaces in the IMO field. We wrote a PySpark script to strip all non-digits, pad the strings to exactly 7 digits, cast them to integers, and join them. The resulting match showed a strong correlation between AIS points—our proxy for active hours—and annual reported CO₂ emissions. We can also extract carbon intensity curves showing that container ships release significantly more CO₂ per active hour than bulk carriers of a similar size due to their higher operating speeds."

---

### [9:00 - 10:00] Slide 10: Conclusion & Project Limitations (Presenter: Jan)
* **Jan:** "In conclusion, this project demonstrates how Apache Spark and Spark SQL can ingest and analyze large spatial datasets to answer critical logistical and environmental questions. However, we must note several limitations: there is a one-year mismatch between our AIS and emissions data, the MRV dataset only includes ships over 5,000 gross tonnage visiting European ports, and our congestion indicators are proxies. Furthermore, running these pipelines locally requires testing on sample data before scaling up. This concludes our presentation, and we are happy to take any questions. Thank you."
