import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_INPUT = "data/processed/ais_mrv_matched.parquet"
DEFAULT_STATS = "output/tables/matching_stats_summary.csv"
DEFAULT_ASSETS_DIR = "dashboard/assets"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export matched AIS/MRV emissions data as JSON for the dashboard."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help=f"Matched Parquet. Default {DEFAULT_INPUT}.")
    parser.add_argument("--stats", default=DEFAULT_STATS, help=f"Matching stats CSV (from step 05). Default {DEFAULT_STATS}.")
    parser.add_argument("--assets-dir", default=DEFAULT_ASSETS_DIR, help=f"Output dir for dashboard JSON. Default {DEFAULT_ASSETS_DIR}.")
    return parser.parse_args()


def write_json(df, assets_dir: str, name: str) -> None:
    """Collect a small aggregate to the driver and write it as a JSON array.

    Uses collect()+json (not toPandas) so we don't depend on pandas being
    installed in the Spark image. Only ever called on tiny, already-aggregated
    DataFrames, so collecting to the driver is safe.
    """
    rows = [row.asDict() for row in df.collect()]
    path = os.path.join(assets_dir, f"{name}.json")
    with open(path, "w") as handle:
        json.dump(rows, handle)
    print(f"Wrote {len(rows)} records -> {path}")


def read_match_stats(spark, stats_path: str) -> dict:
    """Read the metric/value stats CSV written by step 05 (a Spark CSV dir).

    Returns {} if the path is absent so the summary still exports gracefully.
    """
    if not os.path.exists(stats_path):
        print(f"Match stats not found at {stats_path}; summary will omit AIS/MRV totals.")
        return {}
    stats_rows = spark.read.option("header", "true").csv(stats_path).collect()
    return {r["metric"]: float(r["value"]) for r in stats_rows}


def main() -> None:
    args = parse_args()
    os.makedirs(args.assets_dir, exist_ok=True)

    spark = (
        SparkSession.builder.appName("export-dashboard-data")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    matched = spark.read.parquet(args.input)

    # Emissions totals by ship type (MRV ship_type taxonomy) -- RQ5 + emissions-by-type
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
    write_json(by_type, args.assets_dir, "emissions_by_type")

    # All matched vessels for the activity-vs-emissions scatter -- RQ5
    scatter = matched.select(
        "imo_number",
        "ship_name",
        "ship_type",
        F.round("total_co2_emissions", 2).alias("total_co2_emissions"),
        "ais_point_count",
        F.round("avg_speed", 2).alias("avg_speed"),
        "unique_days_active",
    ).orderBy("ship_type")
    write_json(scatter, args.assets_dir, "matched_scatter")

    # Top 50 emitters table -- RQ5
    top50 = (
        matched.select(
            "imo_number",
            "ship_name",
            "ship_type",
            F.round("total_co2_emissions", 2).alias("total_co2_emissions"),
            "ais_point_count",
            F.round("avg_speed", 2).alias("avg_speed"),
            "unique_days_active",
        )
        .orderBy(F.col("total_co2_emissions").desc())
        .limit(50)
    )
    write_json(top50, args.assets_dir, "top_emitters")

    # Headline KPI numbers for the dashboard stat cards
    agg = matched.agg(
        F.countDistinct("imo_number").alias("matched_vessels"),
        F.round(F.sum("total_co2_emissions"), 1).alias("total_co2_matched"),
        F.countDistinct("ship_type").alias("ship_types_matched"),
    ).collect()[0]
    stats = read_match_stats(spark, args.stats)
    summary = {
        "matched_vessels": agg["matched_vessels"],
        "total_co2_matched": agg["total_co2_matched"],
        "ship_types_matched": agg["ship_types_matched"],
        "ais_total_vessels": stats.get("AIS Total Vessels"),
        "mrv_total_vessels": stats.get("MRV Total Vessels"),
        "ais_match_rate": stats.get("AIS Match Rate (%)"),
        "mrv_match_rate": stats.get("MRV Match Rate (%)"),
        "ais_period": "2025",
        "mrv_period": "2024",
    }
    summary_path = os.path.join(args.assets_dir, "summary.json")
    with open(summary_path, "w") as handle:
        json.dump(summary, handle)
    print(f"Wrote summary -> {summary_path}")

    print("Dashboard data export done.")
    spark.stop()


if __name__ == "__main__":
    main()
