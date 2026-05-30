import argparse

from pyspark.sql import SparkSession


DEFAULT_INPUT = "/workspace/data/raw"
DEFAULT_OUTPUT = "/workspace/data/processed/ais_parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load prepared AIS CSV files with Spark and write Parquet output."
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("load-ais-csv-to-parquet")
        .getOrCreate()
    )

    print(f"Input path: {args.input}")
    print(f"Output path: {args.output}")

    ais_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .csv(args.input)
    )

    columns = ais_df.columns
    row_count = ais_df.count()
    partition_count = ais_df.rdd.getNumPartitions()

    print(f"Detected columns ({len(columns)}): {columns}")
    print(f"Row count: {row_count}")
    print(f"Partition count: {partition_count}")

    ais_df.write.mode("overwrite").parquet(args.output)

    print(f"Wrote Parquet output to: {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
