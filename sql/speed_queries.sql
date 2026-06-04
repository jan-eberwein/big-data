-- =====================================================================
-- Research Question 4: Vessel Speed Variation
-- Spark SQL queries for evaluating velocity patterns across vessel groups and geography.
--
-- Before running, register the Parquet files as temp views in Spark:
-- spark.read.parquet("data/processed/aggregations/speed_by_vessel_type_grid").createOrReplaceTempView("speed_by_vessel_type")
-- =====================================================================

-- 1. Speed Summary Statistics by Vessel Type Group
-- Calculates average, typical median, and p90 speed profiles globally.
SELECT 
    vessel_type_group,
    ROUND(AVG(avg_sog), 2) AS overall_avg_speed,
    ROUND(AVG(median_sog), 2) AS avg_median_speed,
    ROUND(MAX(max_sog), 2) AS absolute_max_speed,
    ROUND(AVG(p90_sog), 2) AS avg_p90_speed,
    SUM(point_count) AS total_speed_datapoints
FROM 
    speed_by_vessel_type
GROUP BY 
    vessel_type_group
ORDER BY 
    overall_avg_speed DESC;


-- 2. Average Speed by Region/Grid Block
-- Identifies geographical grid cells characterized by slow operations (ports) or fast transits (high seas).
SELECT 
    grid_id,
    grid_lat_center,
    grid_lon_center,
    ROUND(AVG(avg_sog), 2) AS regional_avg_speed,
    SUM(point_count) AS speed_records,
    ROUND(MIN(min_sog), 2) AS absolute_min_speed
FROM 
    speed_by_vessel_type
GROUP BY 
    grid_id, grid_lat_center, grid_lon_center
HAVING 
    speed_records > 100
ORDER BY 
    regional_avg_speed ASC
LIMIT 50;


-- 3. Speed Statistics inside Specific Speed Restriction Zones
-- This helps analyze compliance or spatial behavior in grids (e.g. hypothetical speed limits).
SELECT 
    vessel_type_group,
    grid_id,
    ROUND(avg_sog, 2) AS avg_speed,
    ROUND(p90_sog, 2) AS p90_speed,
    point_count
FROM 
    speed_by_vessel_type
WHERE 
    avg_sog > 15.0  -- High-speed transits
ORDER BY 
    avg_speed DESC
LIMIT 50;
