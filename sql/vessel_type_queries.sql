-- =====================================================================
-- Research Question 2: Traffic contribution by Vessel Type
-- Spark SQL queries for analyzing traffic metrics across ship categories.
--
-- Before running, register the Parquet files as temp views in Spark:
-- spark.read.parquet("data/processed/aggregations/traffic_by_vessel_type_grid").createOrReplaceTempView("traffic_by_vessel_type")
-- =====================================================================

-- 1. Traffic Intensity by Vessel Type Group
-- Aggregates overall point count and estimated vessel crossings by vessel group.
SELECT 
    vessel_type_group,
    SUM(point_count) AS total_point_count,
    SUM(distinct_vessels) AS total_vessel_crossings,
    ROUND(AVG(avg_sog), 2) AS average_speed_knots
FROM 
    traffic_by_vessel_type
GROUP BY 
    vessel_type_group
ORDER BY 
    total_point_count DESC;


-- 2. Unique Vessel Count by Vessel Type and Grid
-- Shows which vessel types dominate specific corridors or routes.
SELECT 
    vessel_type_group,
    grid_id,
    grid_lat_center,
    grid_lon_center,
    distinct_vessels,
    point_count
FROM 
    traffic_by_vessel_type
WHERE 
    distinct_vessels > 5
ORDER BY 
    distinct_vessels DESC
LIMIT 50;


-- 3. Top Vessel Types by AIS Broadcast Density inside Hotspots
-- Identifies the vessel types responsible for creating density in the top 10% highest-traffic grids.
WITH top_grids AS (
    SELECT grid_id
    FROM traffic_by_vessel_type
    GROUP BY grid_id
    ORDER BY SUM(point_count) DESC
    LIMIT 20
)
SELECT 
    vessel_type_group,
    SUM(point_count) AS hotspot_points,
    SUM(distinct_vessels) AS hotspot_vessels,
    ROUND(AVG(avg_sog), 2) AS hotspot_avg_speed
FROM 
    traffic_by_vessel_type
WHERE 
    grid_id IN (SELECT grid_id FROM top_grids)
GROUP BY 
    vessel_type_group
ORDER BY 
    hotspot_points DESC;
