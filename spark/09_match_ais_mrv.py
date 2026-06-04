import argparse
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match AIS activity indicators with cleaned EU MRV emissions data using IMO numbers."
    )
    parser.add_argument(
        "--ais-input",
        default="data/processed/ais_activity_by_imo.parquet",
        help="Path to AIS activity indicators (Parquet/CSV).",
    )
    parser.add_argument(
        "--mrv-input",
        default="data/processed/mrv_emissions_clean.parquet",
        help="Path to cleaned MRV emissions dataset (Parquet/CSV).",
    )
    parser.add_argument(
        "--output",
        default="data/processed/ais_mrv_matched.parquet",
        help="Path for matched Parquet output.",
    )
    parser.add_argument(
        "--stats-output",
        default="output/tables/matching_stats_summary.csv",
        help="Path for matching statistics CSV.",
    )
    return parser.parse_args()


def load_dataframe(spark, path: str):
    """Loads a DataFrame supporting both Parquet folders/files and CSVs for local testing."""
    if not os.path.exists(path):
        print(f"Error: Path {path} does not exist.")
        return None

    if path.endswith(".csv"):
        print(f"Loading CSV from {path}")
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    else:
        print(f"Loading Parquet from {path}")
        return spark.read.parquet(path)


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("match-ais-mrv-emissions")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    print("Loading datasets...")

    # Load AIS activity data (handle local CSV fallback for testing)
    ais_df = load_dataframe(spark, args.ais_input)
    if ais_df is None:
        fallback_path = "data/sample/ais_activity_by_imo_sample.csv"
        print(f"Attempting fallback to sample AIS data: {fallback_path}")
        ais_df = load_dataframe(spark, fallback_path)
        if ais_df is None:
            print("Failed to load AIS activity dataset. Exiting.")
            spark.stop()
            sys.exit(1)

    # Load Cleaned MRV emissions data
    mrv_df = load_dataframe(spark, args.mrv_input)
    if mrv_df is None:
        fallback_path = "data/sample/mrv_emissions_sample.csv"
        print(f"Attempting fallback to sample MRV emissions data: {fallback_path}")
        # Note: If loading raw sample, we should clean it first.
        # But we assume preparation script was run. Let's try to load the cleaned version first.
        # If it doesn't exist, we will report error.
        print("Cleaned MRV emissions data not found. Please run prepare_mrv_emissions first.")
        spark.stop()
        sys.exit(1)

    # Standardize IMO column name for joining
    # AIS should have `imo_number` (or `imo`), MRV has `imo_number`
    ais_imo_col = "imo_number" if "imo_number" in ais_df.columns else "imo"
    if ais_imo_col not in ais_df.columns:
        for col in ais_df.columns:
            if col.lower() == "imo_number" or col.lower() == "imo":
                ais_imo_col = col
                break

    # Rename AIS column to imo_number to match MRV's naming
    if ais_imo_col != "imo_number":
        ais_df = ais_df.withColumnRenamed(ais_imo_col, "imo_number")

    # Cast both IMO columns to Integer to ensure type compatibility and filter nulls
    ais_df = ais_df.withColumn("imo_number", F.col("imo_number").cast("int")).filter(
        F.col("imo_number").isNotNull()
    )
    mrv_df = mrv_df.withColumn("imo_number", F.col("imo_number").cast("int")).filter(
        F.col("imo_number").isNotNull()
    )

    print("Calculating dataset counts...")
    ais_total_vessels = ais_df.select("imo_number").distinct().count()
    mrv_total_vessels = mrv_df.select("imo_number").distinct().count()

    print(f"Total unique vessels in AIS dataset: {ais_total_vessels}")
    print(f"Total unique vessels in MRV emissions dataset: {mrv_total_vessels}")

    # Perform matching via INNER JOIN on standardized 'imo_number' string key
    # Using the string key 'imo_number' automatically resolves duplicate columns in Spark
    print("Performing inner join on IMO number...")
    matched_df = ais_df.join(mrv_df, "imo_number", "inner")

    # Persist for stats calculation
    matched_df.cache()

    matched_vessels = matched_df.select("imo_number").distinct().count()
    print(f"Total successfully matched vessels: {matched_vessels}")

    # Calculate match rates
    ais_match_rate = (matched_vessels / ais_total_vessels) * 100 if ais_total_vessels > 0 else 0.0
    mrv_match_rate = (matched_vessels / mrv_total_vessels) * 100 if mrv_total_vessels > 0 else 0.0

    print(f"AIS Vessel Match Rate: {ais_match_rate:.2f}%")
    print(f"MRV Vessel Match Rate: {mrv_match_rate:.2f}%")

    # Write joined output to Parquet
    print(f"Writing matched dataset to Parquet: {args.output}")
    matched_df.write.mode("overwrite").parquet(args.output)

    # Create directories for stats and small tables
    stats_dir = os.path.dirname(args.stats_output)
    if stats_dir and not os.path.exists(stats_dir):
        os.makedirs(stats_dir, exist_ok=True)

    # Save summary stats
    print(f"Saving matching statistics summary to: {args.stats_output}")
    stats_data = [
        ("AIS Total Vessels", float(ais_total_vessels)),
        ("MRV Total Vessels", float(mrv_total_vessels)),
        ("Matched Vessels", float(matched_vessels)),
        ("AIS Match Rate (%)", round(ais_match_rate, 2)),
        ("MRV Match Rate (%)", round(mrv_match_rate, 2)),
    ]
    stats_df = spark.createDataFrame(stats_data, ["metric", "value"])
    stats_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.stats_output)

    # Save a small sample of matched records for verification
    sample_matched_path = "output/tables/ais_mrv_matched_sample.csv"
    print(f"Saving matched sample CSV for easy inspection to: {sample_matched_path}")
    matched_df.limit(100).coalesce(1).write.mode("overwrite").option("header", "true").csv(
        sample_matched_path
    )

    print("AIS-MRV Matching completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
