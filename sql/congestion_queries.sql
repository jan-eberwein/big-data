-- =====================================================================
-- Research Question 3: Port Congestion and Waiting Behavior
-- Spark SQL queries for evaluating congestion using waiting time proxies.
--
-- Before running, register the Parquet files as temp views in Spark:
-- spark.read.parquet("data/processed/aggregations/waiting_by_grid").createOrReplaceTempView("waiting_by_grid")
-- =====================================================================

-- 1. Congestion Proxy Ranking (Top Congested Grids)
-- Ranks grids by total cumulative waiting hours. Represents port backlogs and queue bottlenecks.
SELECT 
    grid_id,
    grid_lat_center,
    grid_lon_center,
    waiting_event_count,
    distinct_waiting_vessels,
    ROUND(total_waiting_minutes / 60.0, 1) AS total_waiting_hours,
    ROUND(avg_waiting_minutes, 1) AS avg_waiting_minutes_per_event,
    ROUND(max_waiting_minutes / 60.0, 1) AS max_waiting_hours_single_event
FROM 
    waiting_by_grid
ORDER BY 
    total_waiting_hours DESC
LIMIT 50;


-- 2. Affected Vessels by Grid Region
-- Ranks regions based on the count of unique ships affected by waiting times.
SELECT 
    grid_id,
    grid_lat_center,
    grid_lon_center,
    distinct_waiting_vessels,
    waiting_event_count,
    ROUND(total_waiting_minutes / 60.0, 1) AS total_waiting_hours
FROM 
    waiting_by_grid
ORDER BY 
    distinct_waiting_vessels DESC
LIMIT 50;


-- 3. Ports/Channels with Longest Average Waiting Duration
-- Focuses on locations where the *average* waiting delay is highest (severe bottleneck zones).
SELECT 
    grid_id,
    grid_lat_center,
    grid_lon_center,
    waiting_event_count,
    ROUND(avg_waiting_minutes / 60.0, 2) AS avg_waiting_hours,
    ROUND(max_waiting_minutes / 60.0, 1) AS max_waiting_hours,
    ROUND(avg_waiting_sog, 2) AS avg_waiting_sog_knots
FROM 
    waiting_by_grid
WHERE 
    waiting_event_count >= 5  -- Filters out grids with sparse outlier data
ORDER BY 
    avg_waiting_hours DESC
LIMIT 50;
