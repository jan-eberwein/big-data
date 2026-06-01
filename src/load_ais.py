import argparse
import time

from ais_schema import AIS_SCHEMA
from clean_ais import deduplicate_positions
from clean_ais import filter_required_position_fields
from clean_ais import filter_valid_coordinates
from clean_ais import filter_valid_speed
from clean_ais import normalize_blank_strings
from pyspark.sql import SparkSession


DEFAULT_INPUT = "/workspace/data/raw"
DEFAULT_OUTPUT = "/workspace/data/processed/ais_parquet"
DEFAULT_SAMPLE_LIMIT = 10000
DEFAULT_EXECUTOR_MEMORY = "6g"
DEFAULT_EXECUTOR_CORES = "2"
DEFAULT_SHUFFLE_PARTITIONS = "800"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load prepared AIS CSV files with Spark and write cleaned Parquet output."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=f"Input CSV file or directory. Defaults to {DEFAULT_INPUT}.",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output Parquet directory. Defaults to {DEFAULT_OUTPUT}.",
    )
    parser.add_argument(
        "--sample-output",
        default=None,
        help="Optional Parquet output path for a cleaned AIS sample.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=DEFAULT_SAMPLE_LIMIT,
        help=f"Sample row limit. Defaults to {DEFAULT_SAMPLE_LIMIT}.",
    )
    parser.add_argument(
        "--executor-memory",
        default=DEFAULT_EXECUTOR_MEMORY,
        help=f"Executor memory for the Spark job. Defaults to {DEFAULT_EXECUTOR_MEMORY}.",
    )
    parser.add_argument(
        "--executor-cores",
        default=DEFAULT_EXECUTOR_CORES,
        help=f"Executor cores for the Spark job. Defaults to {DEFAULT_EXECUTOR_CORES}.",
    )
    parser.add_argument(
        "--shuffle-partitions",
        default=DEFAULT_SHUFFLE_PARTITIONS,
        help=f"Spark shuffle partitions. Defaults to {DEFAULT_SHUFFLE_PARTITIONS}.",
    )
    return parser.parse_args()


def timed_count(label: str, df) -> tuple[str, int, float]:
    started_at = time.perf_counter()
    row_count = df.count()
    elapsed_seconds = time.perf_counter() - started_at
    print(f"{label}: {row_count} rows in {elapsed_seconds:.2f}s")
    return label, row_count, elapsed_seconds


def timed_write_parquet(label: str, df, output_path: str) -> float:
    started_at = time.perf_counter()
    df.write.mode("overwrite").parquet(output_path)
    elapsed_seconds = time.perf_counter() - started_at
    print(f"{label}: wrote {output_path} in {elapsed_seconds:.2f}s")
    return elapsed_seconds


def print_cleaning_summary(
    counts: list[tuple[str, int, float]],
    output_path: str,
    write_seconds: float,
    sample_output_path: str | None,
    sample_write_seconds: float | None,
) -> None:
    print("")
    print("AIS cleaning summary")
    print("--------------------")
    for label, row_count, elapsed_seconds in counts:
        print(f"{label}: {row_count} rows ({elapsed_seconds:.2f}s)")
    print(f"Output path: {output_path}")
    print(f"Output write time: {write_seconds:.2f}s")
    if sample_output_path is not None and sample_write_seconds is not None:
        print(f"Sample output path: {sample_output_path}")
        print(f"Sample write time: {sample_write_seconds:.2f}s")


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("load-ais-csv-to-parquet")
        .config("spark.executor.memory", args.executor_memory)
        .config("spark.executor.cores", args.executor_cores)
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .getOrCreate()
    )

    print(f"Input path: {args.input}")
    print(f"Output path: {args.output}")
    if args.sample_output is not None:
        print(f"Sample output path: {args.sample_output}")
        print(f"Sample limit: {args.sample_limit}")
    print(f"Executor memory: {args.executor_memory}")
    print(f"Executor cores: {args.executor_cores}")
    print(f"Shuffle partitions: {args.shuffle_partitions}")

    raw_df = (
        spark.read.schema(AIS_SCHEMA)
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("recursiveFileLookup", "true")
        .csv(args.input)
    )

    print(f"Detected columns ({len(raw_df.columns)}): {raw_df.columns}")

    cleaning_counts = [timed_count("Input rows", raw_df)]

    required_df = filter_required_position_fields(raw_df)
    cleaning_counts.append(timed_count("Rows after required-field filter", required_df))

    coordinate_df = filter_valid_coordinates(required_df)
    cleaning_counts.append(timed_count("Rows after coordinate filter", coordinate_df))

    speed_df = filter_valid_speed(coordinate_df)
    cleaning_counts.append(timed_count("Rows after speed filter", speed_df))

    normalized_df = normalize_blank_strings(speed_df)
    cleaned_df = deduplicate_positions(normalized_df)
    cleaning_counts.append(timed_count("Rows after deduplication", cleaned_df))

    write_seconds = timed_write_parquet("Cleaned AIS Parquet", cleaned_df, args.output)

    sample_write_seconds = None
    if args.sample_output is not None:
        sample_df = cleaned_df.limit(args.sample_limit).coalesce(1)
        sample_write_seconds = timed_write_parquet(
            "Cleaned AIS sample Parquet", sample_df, args.sample_output
        )

    print_cleaning_summary(
        cleaning_counts,
        args.output,
        write_seconds,
        args.sample_output,
        sample_write_seconds,
    )

    spark.stop()


if __name__ == "__main__":
    main()
