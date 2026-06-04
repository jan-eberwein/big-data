import argparse
import posixpath
import time
from decimal import Decimal

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


DEFAULT_INPUT = "/workspace/data/processed/ais_parquet"
DEFAULT_OUTPUT = "/workspace/data/processed/aggregations"
DEFAULT_GRID_SIZE = 0.1
DEFAULT_WAITING_SPEED_THRESHOLD = 1.0
DEFAULT_WAITING_MIN_MINUTES = 30
DEFAULT_DISTINCT_RSD = 0.02
DEFAULT_EXECUTOR_MEMORY = "4g"
DEFAULT_EXECUTOR_MEMORY_OVERHEAD = "1g"
DEFAULT_EXECUTOR_CORES = "1"
DEFAULT_EXECUTOR_INSTANCES = "1"
DEFAULT_TOTAL_EXECUTOR_CORES = "1"
DEFAULT_SHUFFLE_PARTITIONS = "800"

GRID_COLUMNS = [
    "grid_id",
    "grid_lat_index",
    "grid_lon_index",
    "grid_lat_min",
    "grid_lat_max",
    "grid_lon_min",
    "grid_lon_max",
    "grid_lat_center",
    "grid_lon_center",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate cleaned AIS Parquet data into grid-based hotspot datasets."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=f"Input cleaned AIS Parquet directory. Defaults to {DEFAULT_INPUT}.",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output directory for aggregation Parquet folders. Defaults to {DEFAULT_OUTPUT}.",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=None,
        help="Optional input row limit.",
    )
    parser.add_argument(
        "--grid-size",
        type=float,
        default=DEFAULT_GRID_SIZE,
        help=f"Latitude/longitude grid size in degrees. Defaults to {DEFAULT_GRID_SIZE}.",
    )
    parser.add_argument(
        "--waiting-speed-threshold",
        type=float,
        default=DEFAULT_WAITING_SPEED_THRESHOLD,
        help=f"Waiting speed threshold in knots. Defaults to {DEFAULT_WAITING_SPEED_THRESHOLD}.",
    )
    parser.add_argument(
        "--waiting-min-minutes",
        type=int,
        default=DEFAULT_WAITING_MIN_MINUTES,
        help=f"Waiting duration threshold in minutes. Defaults to {DEFAULT_WAITING_MIN_MINUTES}.",
    )
    parser.add_argument(
        "--distinct-rsd",
        type=float,
        default=DEFAULT_DISTINCT_RSD,
        help=f"Approximate distinct-count RSD. Defaults to {DEFAULT_DISTINCT_RSD}.",
    )
    parser.add_argument(
        "--executor-memory",
        default=DEFAULT_EXECUTOR_MEMORY,
        help=f"Executor memory for the Spark job. Defaults to {DEFAULT_EXECUTOR_MEMORY}.",
    )
    parser.add_argument(
        "--executor-memory-overhead",
        default=DEFAULT_EXECUTOR_MEMORY_OVERHEAD,
        help=f"Executor memory overhead. Defaults to {DEFAULT_EXECUTOR_MEMORY_OVERHEAD}.",
    )
    parser.add_argument(
        "--executor-cores",
        default=DEFAULT_EXECUTOR_CORES,
        help=f"Executor cores for the Spark job. Defaults to {DEFAULT_EXECUTOR_CORES}.",
    )
    parser.add_argument(
        "--executor-instances",
        default=DEFAULT_EXECUTOR_INSTANCES,
        help=f"Spark executor instances. Defaults to {DEFAULT_EXECUTOR_INSTANCES}.",
    )
    parser.add_argument(
        "--total-executor-cores",
        default=DEFAULT_TOTAL_EXECUTOR_CORES,
        help=f"Maximum total executor cores. Defaults to {DEFAULT_TOTAL_EXECUTOR_CORES}.",
    )
    parser.add_argument(
        "--shuffle-partitions",
        default=DEFAULT_SHUFFLE_PARTITIONS,
        help=f"Spark shuffle partitions. Defaults to {DEFAULT_SHUFFLE_PARTITIONS}.",
    )
    return parser.parse_args()


def grid_precision(grid_size: float) -> int:
    exponent = Decimal(str(grid_size)).normalize().as_tuple().exponent
    return max(4, -exponent + 2)


def add_grid_columns(df: DataFrame, grid_size: float) -> DataFrame:
    precision = grid_precision(grid_size)
    grid_size_lit = F.lit(grid_size)
    df_with_index = df.withColumn(
        "grid_lat_index", F.floor(F.col("latitude") / grid_size_lit).cast("long")
    ).withColumn(
        "grid_lon_index", F.floor(F.col("longitude") / grid_size_lit).cast("long")
    )
    return (
        df_with_index.withColumn(
            "grid_id",
            F.concat_ws(
                ":",
                F.col("grid_lat_index").cast("string"),
                F.col("grid_lon_index").cast("string"),
            ),
        )
        .withColumn(
            "grid_lat_min", F.round(F.col("grid_lat_index") * grid_size_lit, precision)
        )
        .withColumn(
            "grid_lat_max",
            F.round((F.col("grid_lat_index") + F.lit(1)) * grid_size_lit, precision),
        )
        .withColumn(
            "grid_lon_min", F.round(F.col("grid_lon_index") * grid_size_lit, precision)
        )
        .withColumn(
            "grid_lon_max",
            F.round((F.col("grid_lon_index") + F.lit(1)) * grid_size_lit, precision),
        )
        .withColumn(
            "grid_lat_center",
            F.round((F.col("grid_lat_min") + F.col("grid_lat_max")) / F.lit(2), precision),
        )
        .withColumn(
            "grid_lon_center",
            F.round((F.col("grid_lon_min") + F.col("grid_lon_max")) / F.lit(2), precision),
        )
    )


def add_vessel_type_group(df: DataFrame) -> DataFrame:
    vessel_type = F.col("vessel_type")
    return df.withColumn(
        "vessel_type_group",
        F.when(vessel_type.isNull(), F.lit("unknown"))
        .when(vessel_type.between(70, 79), F.lit("cargo"))
        .when(vessel_type.between(80, 89), F.lit("tanker"))
        .when(vessel_type.between(60, 69), F.lit("passenger"))
        .when(vessel_type == 30, F.lit("fishing"))
        .when(vessel_type.between(31, 32), F.lit("tug_tow"))
        .when(vessel_type.between(36, 37), F.lit("pleasure_sailing"))
        .otherwise(F.lit("other")),
    )


def build_traffic_hotspots_by_grid(df: DataFrame, distinct_rsd: float) -> DataFrame:
    return (
        df.groupBy(*GRID_COLUMNS)
        .agg(
            F.count("*").alias("point_count"),
            F.approx_count_distinct("mmsi", distinct_rsd).alias("distinct_vessels"),
            F.countDistinct(F.date_trunc("hour", F.col("base_date_time"))).alias(
                "active_hours"
            ),
            F.min("base_date_time").alias("first_seen_at"),
            F.max("base_date_time").alias("last_seen_at"),
        )
    )


def build_traffic_by_vessel_type_grid(
    df: DataFrame, distinct_rsd: float
) -> DataFrame:
    return (
        df.groupBy(*GRID_COLUMNS, "vessel_type_group")
        .agg(
            F.count("*").alias("point_count"),
            F.approx_count_distinct("mmsi", distinct_rsd).alias("distinct_vessels"),
            F.avg("sog").alias("avg_sog"),
            F.min("base_date_time").alias("first_seen_at"),
            F.max("base_date_time").alias("last_seen_at"),
        )
    )


def build_waiting_events(
    df: DataFrame, waiting_speed_threshold: float, waiting_min_minutes: int
) -> DataFrame:
    vessel_window = Window.partitionBy("mmsi").orderBy("base_date_time")
    event_window = vessel_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    timestamp_seconds = F.unix_timestamp("base_date_time")
    previous_timestamp_seconds = F.unix_timestamp(F.lag("base_date_time").over(vessel_window))
    previous_is_waiting = F.lag("is_waiting_point").over(vessel_window)
    previous_grid_id = F.lag("grid_id").over(vessel_window)
    gap_minutes = (timestamp_seconds - previous_timestamp_seconds) / F.lit(60)

    sequenced_df = (
        df.withColumn("is_waiting_point", F.col("sog") <= F.lit(waiting_speed_threshold))
        .withColumn(
            "starts_waiting_event",
            F.when(
                F.col("is_waiting_point")
                & (
                    previous_is_waiting.isNull()
                    | (~previous_is_waiting)
                    | (previous_grid_id != F.col("grid_id"))
                    | (gap_minutes > F.lit(waiting_min_minutes))
                ),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("waiting_event_id", F.sum("starts_waiting_event").over(event_window))
    )

    waiting_points_df = sequenced_df.filter(F.col("is_waiting_point"))
    return (
        waiting_points_df.groupBy("mmsi", "waiting_event_id", *GRID_COLUMNS)
        .agg(
            F.min("base_date_time").alias("waiting_started_at"),
            F.max("base_date_time").alias("waiting_ended_at"),
            F.count("*").alias("waiting_point_count"),
            F.avg("sog").alias("avg_waiting_sog"),
        )
        .withColumn(
            "waiting_minutes",
            (
                F.unix_timestamp("waiting_ended_at")
                - F.unix_timestamp("waiting_started_at")
            )
            / F.lit(60),
        )
        .filter(F.col("waiting_minutes") >= F.lit(waiting_min_minutes))
    )


def build_waiting_by_grid(
    df: DataFrame,
    waiting_speed_threshold: float,
    waiting_min_minutes: int,
    distinct_rsd: float,
) -> DataFrame:
    waiting_events_df = build_waiting_events(
        df, waiting_speed_threshold, waiting_min_minutes
    )
    return (
        waiting_events_df.groupBy(*GRID_COLUMNS)
        .agg(
            F.count("*").alias("waiting_event_count"),
            F.approx_count_distinct("mmsi", distinct_rsd).alias(
                "distinct_waiting_vessels"
            ),
            F.sum("waiting_minutes").alias("total_waiting_minutes"),
            F.avg("waiting_minutes").alias("avg_waiting_minutes"),
            F.max("waiting_minutes").alias("max_waiting_minutes"),
            F.sum("waiting_point_count").alias("waiting_point_count"),
            F.avg("avg_waiting_sog").alias("avg_waiting_sog"),
        )
    )


def build_speed_by_vessel_type_grid(df: DataFrame, distinct_rsd: float) -> DataFrame:
    return (
        df.groupBy(*GRID_COLUMNS, "vessel_type_group")
        .agg(
            F.count("*").alias("point_count"),
            F.approx_count_distinct("mmsi", distinct_rsd).alias("distinct_vessels"),
            F.avg("sog").alias("avg_sog"),
            F.min("sog").alias("min_sog"),
            F.max("sog").alias("max_sog"),
            F.percentile_approx("sog", 0.5, 10000).alias("median_sog"),
            F.percentile_approx("sog", 0.9, 10000).alias("p90_sog"),
        )
    )


def timed_count(label: str, df: DataFrame) -> tuple[str, int, float]:
    started_at = time.perf_counter()
    row_count = df.count()
    elapsed_seconds = time.perf_counter() - started_at
    print(f"{label}: {row_count} rows in {elapsed_seconds:.2f}s")
    return label, row_count, elapsed_seconds


def write_aggregation(output_root: str, name: str, df: DataFrame) -> tuple[str, int, float]:
    output_path = posixpath.join(output_root, name)
    cached_df = df.persist(StorageLevel.MEMORY_AND_DISK)
    count_label, row_count, count_seconds = timed_count(f"{name} output", cached_df)
    started_at = time.perf_counter()
    cached_df.write.mode("overwrite").parquet(output_path)
    write_seconds = time.perf_counter() - started_at
    cached_df.unpersist()
    total_seconds = count_seconds + write_seconds
    print(f"{count_label}: wrote {output_path} in {write_seconds:.2f}s")
    return name, row_count, total_seconds


def print_summary(counts: list[tuple[str, int, float]], output_root: str) -> None:
    print("")
    print("AIS aggregation summary")
    print("-----------------------")
    for name, row_count, elapsed_seconds in counts:
        print(f"{name}: {row_count} rows ({elapsed_seconds:.2f}s count+write)")
    print(f"Output root: {output_root}")


def main() -> None:
    args = parse_args()
    total_started_at = time.perf_counter()

    spark = (
        SparkSession.builder.appName("aggregate-ais-hotspots")
        .config("spark.executor.memory", args.executor_memory)
        .config("spark.executor.memoryOverhead", args.executor_memory_overhead)
        .config("spark.executor.cores", args.executor_cores)
        .config("spark.executor.instances", args.executor_instances)
        .config("spark.cores.max", args.total_executor_cores)
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    print(f"Input path: {args.input}")
    print(f"Output path: {args.output}")
    if args.row_limit is not None:
        print(f"Row limit: {args.row_limit}")
    print(f"Grid size: {args.grid_size}")
    print(f"Waiting speed threshold: {args.waiting_speed_threshold}")
    print(f"Waiting minimum minutes: {args.waiting_min_minutes}")
    print(f"Distinct count RSD: {args.distinct_rsd}")
    print(f"Executor memory: {args.executor_memory}")
    print(f"Executor memory overhead: {args.executor_memory_overhead}")
    print(f"Executor cores: {args.executor_cores}")
    print(f"Executor instances: {args.executor_instances}")
    print(f"Total executor cores: {args.total_executor_cores}")
    print(f"Shuffle partitions: {args.shuffle_partitions}")

    ais_df = spark.read.parquet(args.input)
    if args.row_limit is not None:
        ais_df = ais_df.limit(args.row_limit)
    _, input_row_count, _ = timed_count("Input rows", ais_df)

    prepared_df = add_vessel_type_group(add_grid_columns(ais_df, args.grid_size))
    print(f"Prepared rows: {input_row_count} rows (grid/type columns only; no extra action)")

    output_counts = [
        write_aggregation(
            args.output,
            "traffic_hotspots_by_grid",
            build_traffic_hotspots_by_grid(prepared_df, args.distinct_rsd),
        ),
        write_aggregation(
            args.output,
            "traffic_by_vessel_type_grid",
            build_traffic_by_vessel_type_grid(prepared_df, args.distinct_rsd),
        ),
        write_aggregation(
            args.output,
            "waiting_by_grid",
            build_waiting_by_grid(
                prepared_df,
                args.waiting_speed_threshold,
                args.waiting_min_minutes,
                args.distinct_rsd,
            ),
        ),
        write_aggregation(
            args.output,
            "speed_by_vessel_type_grid",
            build_speed_by_vessel_type_grid(prepared_df, args.distinct_rsd),
        ),
    ]

    total_seconds = time.perf_counter() - total_started_at
    print_summary(output_counts, args.output)
    print(f"Total elapsed time: {total_seconds:.2f}s")

    spark.stop()


if __name__ == "__main__":
    main()
