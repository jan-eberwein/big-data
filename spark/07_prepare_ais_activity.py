import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

DEFAULT_INPUT = "data/processed/ais_parquet"
DEFAULT_OUTPUT = "data/processed/ais_activity_by_imo.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate cleaned AIS Parquet data into ship-level activity metrics by IMO number."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=f"Input cleaned AIS Parquet directory. Defaults to {DEFAULT_INPUT}.",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output Parquet directory. Defaults to {DEFAULT_OUTPUT}.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("prepare-ais-activity-by-imo")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    print(f"Reading cleaned AIS Parquet from: {args.input}")
    ais_df = spark.read.parquet(args.input)

    # 1. Clean the IMO number column (strip non-digits, check 7 digits)
    ais_df = ais_df.withColumn(
        "cleaned_imo_str",
        F.regexp_replace(F.trim(F.col("imo")), "[^0-9]", ""),
    )
    ais_df = ais_df.withColumn(
        "imo_number",
        F.when(
            F.length(F.col("cleaned_imo_str")) == 7,
            F.col("cleaned_imo_str").cast(IntegerType()),
        ).otherwise(None),
    )

    # Filter out empty or invalid IMO numbers
    ais_df = ais_df.filter(F.col("imo_number").isNotNull())

    # 2. Add temporary grid cell column to calculate unique regions visited
    grid_lat = F.floor(F.col("latitude") / 0.1).cast("long")
    grid_lon = F.floor(F.col("longitude") / 0.1).cast("long")
    ais_df = ais_df.withColumn("grid_cell", F.concat_ws(":", grid_lat, grid_lon))

    # 3. Perform group aggregation by IMO number
    print("Aggregating AIS activity by IMO...")
    agg_df = ais_df.groupBy("imo_number").agg(
        F.first("vessel_type").alias("vessel_type"),  # Take the first associated vessel type
        F.count("*").alias("ais_point_count"),
        F.avg("sog").alias("avg_speed"),
        F.sum(F.when(F.col("sog") < 1.0, 1).otherwise(0)).alias("slow_movement_count"),
        F.countDistinct(F.to_date("base_date_time")).alias("unique_days_active"),
        F.countDistinct("grid_cell").alias("traffic_region_count"),
    )

    print(f"Writing aggregated AIS activity to Parquet: {args.output}")
    agg_df.write.mode("overwrite").parquet(args.output)

    print("AIS activity summary prepared successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
