# Dataset Documentation

This project processes and combines two distinct maritime datasets: vessel activity tracking (AIS) and annual reported greenhouse gas emissions (EU MRV).

---

## 1. NOAA Marine Cadastre AIS Vessel Tracking Data 2025

### Source & Description
The primary dataset contains Automatic Identification System (AIS) vessel tracking broadcasts compiled by the US Coast Guard and NOAA. It captures high-frequency vessel locations, speeds, headings, and identification numbers in US waters and global shipping corridors.
* **Download Portal:** [NOAA Marine Cadastre AIS Data](https://marinecadastre.gov/data/)
* **Direct File Server:** `https://coast.noaa.gov/htdata/AIS/2025/`
* **File Format:** Zipped daily CSV files.
* **Size:** ~81.5 GB compressed CSV. Due to this size, raw AIS data must **never** be committed to GitHub.

### Expected Local Placement
* **Directory:** `data/raw/ais/`
* **Filenames:** `AIS_2025_01_01.csv`, `AIS_2025_01_02.csv`, etc.

### Key Schema Columns
* `mmsi` (Long): Maritime Mobile Service Identity.
* `base_date_time` (Timestamp): Date and time of the broadcast (UTC).
* `latitude` / `longitude` (Double): Vessel coordinate position.
* `sog` (Double): Speed Over Ground in knots.
* `cog` (Double): Course Over Ground.
* `heading` (Double): Ship heading.
* `vessel_name` (String): Registered name of the vessel.
* `imo` (String): International Maritime Organization number (matching key).
* `vessel_type` (Integer): Numeric code representing the ship category (e.g. 70-79 Cargo, 80-89 Tanker).

---

## 2. EU MRV Ship CO₂ Emissions Dataset 2024

### Source & Description
The companion dataset contains annual greenhouse gas emissions reported by ships over 5,000 gross tonnage performing voyages to or from European Economic Area (EEA) ports. It is regulated under EU Regulation 2015/757 and published by the European Maritime Safety Agency (EMSA).
* **Download Portal:** [EMSA THETIS-MRV Public Information](https://mrv.emsa.europa.eu/#public/licence)
* **File Format:** Excel or CSV spreadsheet.
* **Size:** ~5–10 MB. Since it is small but contains sensitive information, it is ignored by git (`data/raw/mrv/` is in `.gitignore`) to keep the repository lean and focus commits purely on code and aggregated outputs.

### Expected Local Placement
* **Directory:** `data/raw/mrv/`
* **Filename:** `eu_mrv_2024.csv`

### Key Schema Columns
* `IMO Number` (String): Unique vessel ID (parsed and standardized).
* `Name` (String): Registered vessel name.
* `Ship type` (String): Vessel category name (e.g. Bulk carrier, Container ship).
* `Reporting Period` (Integer): Reporting year (2024).
* `Total CO₂ emissions [m tonnes]` (Double): Total annual greenhouse gas emissions in metric tonnes.
* `Technical efficiency` (String): Ship design efficiency values (e.g. EEDI, EEXI).
* `Total time spent at sea [hours]` (Double): Annual hours active.

---

## 3. Data Integration & Linking Key

The datasets are matched using the **IMO (International Maritime Organization) number**. 

* **Why IMO and not MMSI?** MMSI numbers are assigned by flag states and can change when a vessel is sold or reflagged. The IMO number is assigned to the hull upon construction and remains permanently linked to that vessel throughout its service life, making it the most robust key for long-term emissions tracking.
* **Inconsistencies:** The raw AIS and MRV datasets contain formatting inconsistencies in the IMO columns. The MRV dataset often lists IMO numbers as strings with text prefixes (e.g., `IMO 9481300`), spaces, or missing values.
* **Cleaning Approach:** In `spark/08_prepare_mrv_emissions.py`, the IMO string is cleaned by stripping non-numeric characters, padding/checking for exactly 7 digits, casting to integer, and filtering out invalid entries.

---

## 4. Year Mismatch and Limitations

> [!WARNING]
> **Year Mismatch:** The main AIS activity tracking is from the year **2025**, whereas the latest available EU MRV emissions dataset is from **2024**. 
> * **Implication:** The MRV dataset represents a historical baseline. When matching, we assume a vessel's annual emissions profile from 2024 remains a strong proxy for its size, engine configuration, and fuel use during 2025. 
> * **Exclusion:** Vessels built or commissioned in 2025 will have AIS activity records but will not exist in the 2024 MRV dataset.
> * **Thresholding:** MRV only includes ships above 5,000 gross tonnage. Smaller ships tracked in AIS (tugs, fishing boats, yachts) will not match.
