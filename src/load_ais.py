import argparse

from ais_schema import AIS_SCHEMA
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_INPUT = "/workspace/data/raw"
DEFAULT_OUTPUT = "/workspace/data/processed/ais_parquet"
DEFAULT_EXECUTOR_MEMORY = "6g"
DEFAULT_EXECUTOR_CORES = "2"
DEFAULT_SHUFFLE_PARTITIONS = "800"
STRING_COLUMNS = ["vessel_name", "imo", "call_sign", "transceiver"]
DUPLICATE_POSITION_COLUMNS = ["mmsi", "base_date_time", "latitude", "longitude"]


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
        help=(
            "Number of shuffle partitions for wide operations such as deduplication. "
            f"Defaults to {DEFAULT_SHUFFLE_PARTITIONS}."
        ),
    )
    return parser.parse_args()


def normalize_blank_strings(df: DataFrame) -> DataFrame:
    cleaned_df = df
    for column_name in STRING_COLUMNS:
        cleaned_df = cleaned_df.withColumn(
            column_name,
            F.when(F.trim(F.col(column_name)) == "", None).otherwise(
                F.trim(F.col(column_name))
            ),
        )
    return cleaned_df


def count_rows(df: DataFrame) -> int:
    return df.count()


def print_cleaning_summary(counts: list[tuple[str, int]], output_path: str) -> None:
    print("")
    print("AIS cleaning summary")
    print("--------------------")
    for label, row_count in counts:
        print(f"{label}: {row_count}")
    print(f"Output path: {output_path}")


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

    cleaning_counts = [("Input rows", count_rows(raw_df))]

    required_df = raw_df.dropna(
        subset=["mmsi", "base_date_time", "latitude", "longitude"]
    )
    cleaning_counts.append(
        ("Rows after required-field filter", count_rows(required_df))
    )

    coordinate_df = required_df.filter(
        (F.col("latitude") >= -90)
        & (F.col("latitude") <= 90)
        & (F.col("longitude") >= -180)
        & (F.col("longitude") <= 180)
    )
    cleaning_counts.append(("Rows after coordinate filter", count_rows(coordinate_df)))

    speed_df = coordinate_df.filter((F.col("sog") >= 0) & (F.col("sog") <= 60))
    cleaning_counts.append(("Rows after speed filter", count_rows(speed_df)))

    normalized_df = normalize_blank_strings(speed_df)
    cleaned_df = normalized_df.dropDuplicates(DUPLICATE_POSITION_COLUMNS)
    cleaning_counts.append(("Rows after deduplication", count_rows(cleaned_df)))

    cleaned_df.write.mode("overwrite").parquet(args.output)

    print(f"Wrote cleaned Parquet output to: {args.output}")
    print_cleaning_summary(cleaning_counts, args.output)

    spark.stop()


if __name__ == "__main__":
    main()
