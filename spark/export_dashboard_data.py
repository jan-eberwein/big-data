from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("export-dashboard-csvs")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

matched = spark.read.parquet("data/processed/ais_mrv_matched.parquet")

# Emissions totals by ship type
by_type = (
    matched.groupBy("ship_type")
    .agg(
        F.count("imo_number").alias("vessel_count"),
        F.round(F.avg("total_co2_emissions"), 1).alias("avg_co2_m_tonnes"),
        F.round(F.sum("total_co2_emissions"), 1).alias("total_co2_m_tonnes"),
        F.round(F.avg("avg_speed"), 2).alias("avg_speed_knots"),
        F.round(F.avg("ais_point_count"), 0).alias("avg_ais_points"),
    )
    .filter("vessel_count >= 2")
    .orderBy(F.col("total_co2_m_tonnes").desc())
)
by_type.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "output/dashboard/emissions_by_type.csv"
)

# All matched vessels for scatter plot
scatter = matched.select(
    "imo_number",
    "ship_name",
    "ship_type",
    F.round("total_co2_emissions", 2).alias("total_co2_emissions"),
    "ais_point_count",
    F.round("avg_speed", 2).alias("avg_speed"),
    "unique_days_active",
).orderBy("ship_type")
scatter.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "output/dashboard/matched_scatter.csv"
)

# Top 50 emitters table
top50 = matched.select(
    "imo_number",
    "ship_name",
    "ship_type",
    F.round("total_co2_emissions", 2).alias("total_co2_emissions"),
    "ais_point_count",
    F.round("avg_speed", 2).alias("avg_speed"),
    "unique_days_active",
).orderBy(F.col("total_co2_emissions").desc()).limit(50)
top50.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "output/dashboard/top_emitters.csv"
)

print("All dashboard CSV exports done.")
spark.stop()
