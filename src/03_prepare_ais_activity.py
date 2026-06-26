import argparse
from grid_utils import DEFAULT_GRID_SIZE
from grid_utils import add_grid_index_columns
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
    parser.add_argument(
        "--shuffle-partitions",
        default="16",
        help="Spark shuffle partitions (raise for full-year data). Defaults to 16.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("prepare-ais-activity-by-imo")
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
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

    # 2. Add a grid cell column (shared helper) to count unique regions visited
    ais_df = add_grid_index_columns(ais_df, DEFAULT_GRID_SIZE)

    # 3. Perform group aggregation by IMO number
    print("Aggregating AIS activity by IMO...")
    agg_df = ais_df.groupBy("imo_number").agg(
        # vessel_type is effectively static per IMO; max() is used purely for
        # run-to-run determinism (first() would pick an arbitrary row).
        F.max("vessel_type").alias("vessel_type"),
        F.count("*").alias("ais_point_count"),
        F.avg("sog").alias("avg_speed"),
        F.sum(F.when(F.col("sog") < 1.0, 1).otherwise(0)).alias("slow_movement_count"),
        F.countDistinct(F.to_date("base_date_time")).alias("unique_days_active"),
        F.countDistinct("grid_id").alias("traffic_region_count"),
    )

    print(f"Writing aggregated AIS activity to Parquet: {args.output}")
    agg_df.write.mode("overwrite").parquet(args.output)

    print("AIS activity summary prepared successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
