import argparse
import csv
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType

DEFAULT_INPUT = "data/raw/mrv/EU MRV Publication 2024.csv"
DEFAULT_OUTPUT = "data/processed/mrv_emissions_clean.parquet"
DEFAULT_SAMPLE_CSV = "output/tables/mrv_emissions_clean_sample.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load, clean, and standardize EU MRV emissions CSV using PySpark."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=f"Path to raw EU MRV CSV file. Defaults to {DEFAULT_INPUT}.",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Path for cleaned Parquet output. Defaults to {DEFAULT_OUTPUT}.",
    )
    parser.add_argument(
        "--sample-csv",
        default=DEFAULT_SAMPLE_CSV,
        help=f"Path for optional small CSV sample. Defaults to {DEFAULT_SAMPLE_CSV}.",
    )
    return parser.parse_args()


def parse_csv_line(line: str) -> list[str]:
    """Parses a single CSV line handling quotes and commas using standard csv library."""
    reader = csv.reader([line])
    return next(reader)


def make_unique_headers(headers: list[str]) -> list[str]:
    """Resolves duplicate header names by adding a numerical suffix to ensure unique column names in Spark."""
    seen = {}
    unique_headers = []
    for i, h in enumerate(headers):
        h_clean = h.strip()
        if not h_clean:
            h_clean = f"col_{i}"
        h_lower = h_clean.lower()
        if h_lower in seen:
            seen[h_lower] += 1
            unique_headers.append(f"{h_clean}_{seen[h_lower]}")
        else:
            seen[h_lower] = 1
            unique_headers.append(h_clean)
    return unique_headers


def find_column(df_columns: list[str], possible_names: list[str]) -> str | None:
    """Helper to find matching columns case-insensitively with support for standard variations."""
    for name in possible_names:
        normalized_name = name.strip().lower()
        for col in df_columns:
            normalized_col = col.strip().lower()
            if normalized_col == normalized_name:
                return col
    return None


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("prepare-mrv-emissions")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    print(f"Reading raw MRV emissions from: {args.input}")
    if not os.path.exists(args.input):
        print(f"Error: Input file {args.input} does not exist. Please check your path.")
        spark.stop()
        sys.exit(1)

    # The EU MRV CSV is not a plain table: rows 0-1 are publication metadata and
    # the real column header is on row 2 (0-based). Spark's DataFrame CSV reader
    # has no clean "skip N rows, then treat row N as the header" option, so we
    # read the raw lines as an RDD, take the true header from line index 2,
    # drop the first 3 rows, parse the remaining lines, and build a typed
    # DataFrame from that. Everything downstream is the standard DataFrame API.
    rdd = spark.sparkContext.textFile(args.input)

    # Extract the header row (index 2 in 0-based indexing)
    lines_header = rdd.take(3)
    if len(lines_header) < 3:
        print("Error: Input CSV does not contain at least 3 rows (header row expected at row 3).")
        spark.stop()
        sys.exit(1)

    raw_headers = parse_csv_line(lines_header[2])
    unique_headers = make_unique_headers(raw_headers)
    print(f"Parsed {len(unique_headers)} unique column headers.")

    # Filter out metadata lines (rows 0 and 1) and the header line (row 2)
    data_rdd = rdd.zipWithIndex().filter(lambda x: x[1] > 2).map(lambda x: x[0])

    # Parse CSV lines into lists of fields
    parsed_rdd = data_rdd.map(parse_csv_line)

    # Pad or truncate each parsed row to match the exact header length
    num_cols = len(unique_headers)
    def pad_or_truncate(row: list[str]) -> list[str]:
        if len(row) < num_cols:
            return row + [None] * (num_cols - len(row))
        else:
            return row[:num_cols]

    parsed_rdd = parsed_rdd.map(pad_or_truncate)

    # Define schema as string fields to load all parsed lines
    schema = StructType([StructField(h, StringType(), True) for h in unique_headers])

    # Create DataFrame from parsed RDD and schema
    raw_df = spark.createDataFrame(parsed_rdd, schema)

    cols = raw_df.columns

    # Map columns using case-insensitive lookup
    # Note: Since make_unique_headers appends suffixes for duplicate column names,
    # the first occurrence (which is what we want for IMO/Name) will retain its base name.
    imo_col = find_column(cols, ["IMO Number", "imo_number", "imo"])
    name_col = find_column(cols, ["Name", "ship_name", "ship name", "vessel_name"])
    type_col = find_column(cols, ["Ship type", "ship_type", "vessel_type", "type"])
    period_col = find_column(cols, ["Reporting Period", "reporting_period", "year", "period"])
    efficiency_col = find_column(cols, ["Technical efficiency", "technical_efficiency", "efficiency"])
    co2_col = find_column(
        cols,
        [
            "Total CO₂ emissions [m tonnes]",
            "Total CO2 emissions [m tonnes]",
            "Total emissions [m tonnes]",
            "total_co2_emissions",
            "co2_emissions",
        ],
    )
    time_col = find_column(
        cols,
        [
            "Time spent at sea [hours]",
            "Total time spent at sea [hours]",
            "Total time spent at sea",
            "time_spent_at_sea",
            "total_time_at_sea",
            "time_at_sea",
        ],
    )

    if not imo_col:
        print("Error: Could not locate IMO Number column in input CSV.")
        spark.stop()
        sys.exit(1)

    print("Mapping columns dynamically:")
    print(f"  - IMO: {imo_col}")
    print(f"  - Name: {name_col}")
    print(f"  - Type: {type_col}")
    print(f"  - Period: {period_col}")
    print(f"  - Efficiency: {efficiency_col}")
    print(f"  - CO2: {co2_col}")
    print(f"  - Time at Sea: {time_col}")

    # Clean and parse IMO Number (regex strip non-digits, verify 7 digits)
    clean_df = raw_df.withColumn(
        "cleaned_imo_str",
        F.regexp_replace(F.trim(F.col(imo_col)), "[^0-9]", ""),
    )
    clean_df = clean_df.withColumn(
        "imo_number",
        F.when(
            F.length(F.col("cleaned_imo_str")) == 7,
            F.col("cleaned_imo_str").cast(IntegerType()),
        ).otherwise(None),
    )

    # Filter out records where IMO number is null
    clean_df = clean_df.filter(F.col("imo_number").isNotNull())

    # Extract and format other fields
    clean_df = clean_df.withColumn(
        "ship_name",
        F.trim(F.col(name_col)) if name_col else F.lit(None).cast("string"),
    )
    clean_df = clean_df.withColumn(
        "ship_type",
        F.trim(F.col(type_col)) if type_col else F.lit(None).cast("string"),
    )
    clean_df = clean_df.withColumn(
        "reporting_period",
        F.col(period_col).cast(IntegerType()) if period_col else F.lit(None).cast(IntegerType()),
    )
    clean_df = clean_df.withColumn(
        "technical_efficiency",
        F.trim(F.col(efficiency_col)) if efficiency_col else F.lit(None).cast("string"),
    )

    # Replace comma decimal separator with dot and cast to Double
    if co2_col:
        clean_df = clean_df.withColumn(
            "total_co2_emissions",
            F.regexp_replace(F.col(co2_col), ",", ".").cast(DoubleType()),
        )
    else:
        clean_df = clean_df.withColumn("total_co2_emissions", F.lit(None).cast(DoubleType()))

    if time_col:
        clean_df = clean_df.withColumn(
            "total_time_at_sea",
            F.regexp_replace(F.col(time_col), ",", ".").cast(DoubleType()),
        )
    else:
        clean_df = clean_df.withColumn("total_time_at_sea", F.lit(None).cast(DoubleType()))

    # Select final columns
    output_cols = [
        "imo_number",
        "ship_name",
        "ship_type",
        "reporting_period",
        "technical_efficiency",
        "total_co2_emissions",
        "total_time_at_sea",
    ]
    final_df = clean_df.select(*output_cols)

    # Deduplicate by IMO number deterministically: keep the most recent
    # reporting period, breaking ties by the highest reported emissions.
    # (dropDuplicates would keep an arbitrary row, so results varied per run.)
    dedup_window = Window.partitionBy("imo_number").orderBy(
        F.col("reporting_period").desc_nulls_last(),
        F.col("total_co2_emissions").desc_nulls_last(),
    )
    final_df = (
        final_df.withColumn("_dedup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )

    # Write cleaned Parquet output
    print(f"Writing cleaned emissions to Parquet: {args.output}")
    final_df.write.mode("overwrite").parquet(args.output)

    # Write small sample CSV
    if args.sample_csv:
        sample_dir = os.path.dirname(args.sample_csv)
        if sample_dir and not os.path.exists(sample_dir):
            os.makedirs(sample_dir, exist_ok=True)
        print(f"Writing sample CSV for inspection: {args.sample_csv}")
        # Select first 100 rows to write as a local helper CSV
        final_df.limit(100).coalesce(1).write.mode("overwrite").option("header", "true").csv(args.sample_csv)

    print("MRV preparation completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
