import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


DEFAULT_INPUT = "data/processed/aggregations"
DEFAULT_ASSETS_DIR = "dashboard/assets"
DEFAULT_DISPLAY_GRID_SIZE = 0.5
DEFAULT_MIN_POINTS = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export AIS grid aggregations as dashboard JSON (hotspot map, traffic/speed by type, congestion)."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help=f"Aggregations dir (step 02 output). Default {DEFAULT_INPUT}.")
    parser.add_argument("--assets-dir", default=DEFAULT_ASSETS_DIR, help=f"Output dir for dashboard JSON. Default {DEFAULT_ASSETS_DIR}.")
    parser.add_argument(
        "--display-grid-size",
        type=float,
        default=DEFAULT_DISPLAY_GRID_SIZE,
        help=f"Coarse cell size (deg) for the map. Default {DEFAULT_DISPLAY_GRID_SIZE}.",
    )
    parser.add_argument(
        "--min-points",
        type=int,
        default=DEFAULT_MIN_POINTS,
        help=f"Drop map cells below this AIS point count (removes the sparse tail). Default {DEFAULT_MIN_POINTS}.",
    )
    return parser.parse_args()


def write_json(df, assets_dir: str, name: str) -> None:
    """Collect a small aggregate and write it as a JSON array (no pandas dependency)."""
    rows = [row.asDict() for row in df.collect()]
    path = os.path.join(assets_dir, f"{name}.json")
    with open(path, "w") as handle:
        json.dump(rows, handle)
    print(f"Wrote {len(rows)} records -> {path}")


def add_coarse_keys(df, disp: float):
    """Bucket a fine (0.1deg) grid row into a coarse display cell by its center."""
    return df.withColumn(
        "clat", F.floor(F.col("grid_lat_center") / F.lit(disp)).cast("long")
    ).withColumn("clon", F.floor(F.col("grid_lon_center") / F.lit(disp)).cast("long"))


def build_hotspot_grid(hotspots, traffic_by_type, waiting, disp: float, min_points: int):
    """Coarsen the fine grid to `disp`-degree display cells for the Leaflet map.

    Note: distinct_vessels is summed across fine cells within a coarse cell, so
    it slightly over-counts vessels that span cell boundaries -- fine for a
    heatmap, not an exact unique count.
    """
    # Base traffic counts per coarse cell
    base = (
        add_coarse_keys(hotspots, disp)
        .groupBy("clat", "clon")
        .agg(
            F.sum("point_count").alias("point_count"),
            F.sum("distinct_vessels").alias("distinct_vessels"),
        )
    )

    # Per coarse-cell + vessel type point totals (for weighted speed + dominant type)
    by_type = (
        add_coarse_keys(traffic_by_type, disp)
        .groupBy("clat", "clon", "vessel_type_group")
        .agg(
            F.sum("point_count").alias("tpc"),
            F.sum(F.col("avg_sog") * F.col("point_count")).alias("sog_weighted"),
        )
    )
    sog = by_type.groupBy("clat", "clon").agg(
        (F.sum("sog_weighted") / F.sum("tpc")).alias("avg_sog")
    )
    rank = Window.partitionBy("clat", "clon").orderBy(
        F.col("tpc").desc(), F.col("vessel_type_group")
    )
    dominant = (
        by_type.withColumn("rn", F.row_number().over(rank))
        .filter(F.col("rn") == 1)
        .select("clat", "clon", F.col("vessel_type_group").alias("dominant_type"))
    )

    # Waiting points per coarse cell -> congestion percentage
    wait = (
        add_coarse_keys(waiting, disp)
        .groupBy("clat", "clon")
        .agg(F.sum("waiting_point_count").alias("waiting_point_count"))
    )

    disp_lit = F.lit(disp)
    grid = (
        base.join(sog, ["clat", "clon"], "left")
        .join(dominant, ["clat", "clon"], "left")
        .join(wait, ["clat", "clon"], "left")
        .withColumn("lat_min", F.round(F.col("clat") * disp_lit, 4))
        .withColumn("lat_max", F.round((F.col("clat") + F.lit(1)) * disp_lit, 4))
        .withColumn("lon_min", F.round(F.col("clon") * disp_lit, 4))
        .withColumn("lon_max", F.round((F.col("clon") + F.lit(1)) * disp_lit, 4))
        .withColumn("lat_c", F.round((F.col("lat_min") + F.col("lat_max")) / F.lit(2), 4))
        .withColumn("lon_c", F.round((F.col("lon_min") + F.col("lon_max")) / F.lit(2), 4))
        .withColumn("avg_sog", F.round(F.coalesce(F.col("avg_sog"), F.lit(0.0)), 2))
        .withColumn("dominant_type", F.coalesce(F.col("dominant_type"), F.lit("unknown")))
        .withColumn(
            "wait_pct",
            F.least(
                F.lit(100.0),
                F.round(
                    F.coalesce(F.col("waiting_point_count"), F.lit(0)) / F.col("point_count") * F.lit(100),
                    1,
                ),
            ),
        )
    )
    return (
        grid.filter(F.col("point_count") >= F.lit(min_points))
        .select(
            "lat_min", "lat_max", "lon_min", "lon_max", "lat_c", "lon_c",
            "point_count", "distinct_vessels", "avg_sog", "wait_pct", "dominant_type",
        )
        .orderBy(F.col("point_count").desc())
    )


def build_traffic_by_type(traffic_by_type):
    """RQ2: total AIS traffic intensity per vessel type group (full fleet)."""
    return (
        traffic_by_type.groupBy("vessel_type_group")
        .agg(
            F.sum("point_count").alias("point_count"),
            F.sum("distinct_vessels").alias("distinct_vessels"),  # approx (cross-cell)
            F.round(F.sum(F.col("avg_sog") * F.col("point_count")) / F.sum("point_count"), 2).alias("avg_sog"),
        )
        .orderBy(F.col("point_count").desc())
    )


def build_speed_by_type(speed):
    """RQ4: speed distribution per vessel type group (full fleet).

    avg_sog is an exact point-count-weighted mean. median_sog / p90_sog are
    weighted means of per-cell approximate percentiles -> approximate, labelled
    as such on the dashboard.
    """
    total = F.sum("point_count")
    return (
        speed.groupBy("vessel_type_group")
        .agg(
            F.round(F.sum(F.col("avg_sog") * F.col("point_count")) / total, 2).alias("avg_sog"),
            F.round(F.sum(F.col("median_sog") * F.col("point_count")) / total, 2).alias("median_sog"),
            F.round(F.sum(F.col("p90_sog") * F.col("point_count")) / total, 2).alias("p90_sog"),
            F.round(F.min("min_sog"), 2).alias("min_sog"),
            F.round(F.max("max_sog"), 2).alias("max_sog"),
            F.sum("point_count").alias("point_count"),
        )
        .orderBy(F.col("avg_sog").desc())
    )


def region_label(lat, lon):
    """Assign a fine grid cell to a named U.S. maritime region by its center.

    NOAA Marine Cadastre AIS covers U.S. waters, so these are approximate
    geographic groupings (bounding boxes, first match wins). Boundaries are
    interpretive, not official EEZ lines -- documented as an assumption.
    """
    def box(la0, la1, lo0, lo1):
        return (lat >= F.lit(la0)) & (lat < F.lit(la1)) & (lon >= F.lit(lo0)) & (lon < F.lit(lo1))

    return (
        F.when(box(50, 73, -180, -128), F.lit("Alaska & Bering"))
        .when(box(18, 23, -161, -154), F.lit("Hawaii"))
        .when(box(30, 49, -130, -116), F.lit("US West Coast"))
        .when(box(41, 49, -93, -76), F.lit("Great Lakes"))
        .when(box(18, 31, -98, -81), F.lit("Gulf of Mexico"))
        .when(box(24, 48, -81.9, -64), F.lit("US East Coast"))
        .when(box(17, 21, -68, -64), F.lit("Caribbean & Puerto Rico"))
        .otherwise(F.lit("Other US Waters"))
    )


def build_speed_by_region(speed, min_points: int = 100000):
    """RQ4 (region half): point-count-weighted speed per named maritime region.

    avg_sog is exact (weighted mean of per-cell means). median/p90 are weighted
    means of per-cell approximate percentiles -> approximate, labelled as such.
    """
    total = F.sum("point_count")
    return (
        speed.withColumn("region", region_label(F.col("grid_lat_center"), F.col("grid_lon_center")))
        .groupBy("region")
        .agg(
            F.round(F.sum(F.col("avg_sog") * F.col("point_count")) / total, 2).alias("avg_sog"),
            F.round(F.sum(F.col("median_sog") * F.col("point_count")) / total, 2).alias("median_sog"),
            F.round(F.sum(F.col("p90_sog") * F.col("point_count")) / total, 2).alias("p90_sog"),
            F.sum("point_count").alias("point_count"),
            F.sum("distinct_vessels").alias("distinct_vessels"),  # approx (cross-cell)
        )
        .filter(F.col("point_count") >= F.lit(min_points))
        .orderBy(F.col("point_count").desc())
    )


def build_congestion_by_region(waiting, limit: int = 15):
    """RQ3: regions with the most total waiting time (congestion proxy)."""
    return (
        waiting.select(
            F.round("grid_lat_center", 3).alias("grid_lat_center"),
            F.round("grid_lon_center", 3).alias("grid_lon_center"),
            F.round("total_waiting_minutes", 0).alias("total_waiting_minutes"),
            "waiting_event_count",
            "distinct_waiting_vessels",
            F.round("avg_waiting_minutes", 1).alias("avg_waiting_minutes"),
        )
        .orderBy(F.col("total_waiting_minutes").desc())
        .limit(limit)
    )


def main() -> None:
    args = parse_args()
    os.makedirs(args.assets_dir, exist_ok=True)

    spark = (
        SparkSession.builder.appName("export-hotspot-assets")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    base = args.input.rstrip("/")
    hotspots = spark.read.parquet(f"{base}/traffic_hotspots_by_grid")
    traffic_by_type = spark.read.parquet(f"{base}/traffic_by_vessel_type_grid")
    waiting = spark.read.parquet(f"{base}/waiting_by_grid")
    speed = spark.read.parquet(f"{base}/speed_by_vessel_type_grid")

    write_json(
        build_hotspot_grid(hotspots, traffic_by_type, waiting, args.display_grid_size, args.min_points),
        args.assets_dir,
        "hotspot_grid",
    )
    write_json(build_traffic_by_type(traffic_by_type), args.assets_dir, "traffic_by_type")
    write_json(build_speed_by_type(speed), args.assets_dir, "speed_by_type")
    write_json(build_speed_by_region(speed), args.assets_dir, "speed_by_region")
    write_json(build_congestion_by_region(waiting), args.assets_dir, "congestion_by_region")

    print("Hotspot/aggregation asset export done.")
    spark.stop()


if __name__ == "__main__":
    main()
