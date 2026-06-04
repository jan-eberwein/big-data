-- =====================================================================
-- Research Question 1: Maritime Hotspots & Traffic Density
-- Spark SQL queries for identifying maritime grid cell hotspots.
--
-- Before running, register the Parquet files as temp views in Spark:
-- spark.read.parquet("data/processed/aggregations/traffic_hotspots_by_grid").createOrReplaceTempView("traffic_hotspots")
-- spark.read.parquet("data/processed/aggregations/traffic_by_vessel_type_grid").createOrReplaceTempView("traffic_by_vessel_type")
-- =====================================================================

-- 1. Top Traffic Grid Cells by AIS Point Count
-- Identifies grids with the highest overall broadcast density (high activity lanes or anchorages).
SELECT 
    grid_id,
    grid_lat_center,
    grid_lon_center,
    point_count,
    distinct_vessels,
    active_hours
FROM 
    traffic_hotspots
ORDER BY 
    point_count DESC
LIMIT 50;


-- 2. Unique Vessels per Grid Cell
-- Identifies key choke points or transit hubs where the largest count of individual vessels was tracked.
SELECT 
    grid_id,
    grid_lat_center,
    grid_lon_center,
    distinct_vessels,
    point_count
FROM 
    traffic_hotspots
ORDER BY 
    distinct_vessels DESC
LIMIT 50;


-- 3. Average Speed per High-Traffic Grid Cell
-- Analyzes traffic flows. Low speeds in high-density grids indicate choke points, ports, or canals.
SELECT 
    grid_id,
    grid_lat_center,
    grid_lon_center,
    AVG(avg_sog) AS avg_speed_knots,
    SUM(point_count) AS total_points,
    SUM(distinct_vessels) AS total_vessel_crossings
FROM 
    traffic_by_vessel_type
GROUP BY 
    grid_id, grid_lat_center, grid_lon_center
ORDER BY 
    total_points DESC
LIMIT 50;


-- 4. Possible Hotspot Index Ranking
-- Combines point count and unique vessel counts to score grids that are both busy and traversed by diverse fleets.
SELECT 
    grid_id,
    grid_lat_center,
    grid_lon_center,
    point_count,
    distinct_vessels,
    (point_count * distinct_vessels) AS hotspot_intensity_score
FROM 
    traffic_hotspots
ORDER BY 
    hotspot_intensity_score DESC
LIMIT 50;
