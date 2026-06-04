-- =====================================================================
-- Research Question 5: AIS Activity vs. Reported CO2 Emissions
-- Spark SQL queries for evaluating the relationship between 2025 activity and 2024 emissions.
--
-- Before running, register the Parquet files as temp views in Spark:
-- spark.read.parquet("data/processed/ais_mrv_matched").createOrReplaceTempView("ais_mrv_matched")
-- =====================================================================

-- 1. General Matching Summary
-- Overview of matched ships, total annual emissions, and activity indices.
SELECT 
    COUNT(DISTINCT imo_number) AS total_matched_ships,
    SUM(total_co2_emissions) AS total_reported_co2_m_tonnes,
    AVG(total_co2_emissions) AS avg_reported_co2_per_ship,
    SUM(ais_point_count) AS total_ais_tracking_points,
    AVG(ais_point_count) AS avg_ais_points_per_ship
FROM 
    ais_mrv_matched;


-- 2. Average Emissions and Activity by Ship Type
-- Aggregates CO2 emissions, times at sea, and tracking density by ship category.
SELECT 
    ship_type,
    COUNT(DISTINCT imo_number) AS ship_count,
    ROUND(AVG(total_co2_emissions), 1) AS avg_annual_co2_m_tonnes,
    ROUND(AVG(total_time_at_sea), 1) AS avg_reported_hours_at_sea,
    ROUND(AVG(ais_point_count), 0) AS avg_ais_points_2025,
    ROUND(AVG(avg_speed), 2) AS avg_ais_speed_knots
FROM 
    ais_mrv_matched
GROUP BY 
    ship_type
ORDER BY 
    avg_annual_co2_m_tonnes DESC;


-- 3. Top CO2 Emitters and their AIS Activity Profile
-- Identifies the top 20 polluting vessels in the matched fleet and correlates with AIS activity.
SELECT 
    imo_number,
    ship_name,
    ship_type,
    total_co2_emissions AS co2_emissions_2024,
    ais_point_count AS ais_points_2025,
    ROUND(avg_speed, 2) AS avg_speed_2025,
    slow_movement_count AS slow_move_points_2025,
    unique_days_active AS days_active_2025
FROM 
    ais_mrv_matched
ORDER BY 
    total_co2_emissions DESC
LIMIT 20;


-- 4. Activity Efficiency Proxy: Emissions per AIS Data Point
-- Investigates the average environmental footprint normalized by activity intensity.
-- A high ratio indicates ships that are heavy emitters relative to their relative time spent active (e.g. very large bulkers/tankers).
SELECT 
    ship_type,
    COUNT(DISTINCT imo_number) AS vessel_count,
    ROUND(SUM(total_co2_emissions) / SUM(ais_point_count), 4) AS co2_emissions_per_ais_point,
    ROUND(AVG(avg_speed), 2) AS average_speed_knots
FROM 
    ais_mrv_matched
GROUP BY 
    ship_type
HAVING 
    vessel_count >= 2
ORDER BY 
    co2_emissions_per_ais_point DESC;
