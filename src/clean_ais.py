from pyspark.sql import DataFrame
from pyspark.sql import functions as F


REQUIRED_POSITION_COLUMNS = ["mmsi", "base_date_time", "latitude", "longitude"]
STRING_COLUMNS = ["vessel_name", "imo", "call_sign", "transceiver"]
DUPLICATE_POSITION_COLUMNS = ["mmsi", "base_date_time", "latitude", "longitude"]


def filter_required_position_fields(df: DataFrame) -> DataFrame:
    return df.dropna(subset=REQUIRED_POSITION_COLUMNS)


def filter_valid_coordinates(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("latitude") >= -90)
        & (F.col("latitude") <= 90)
        & (F.col("longitude") >= -180)
        & (F.col("longitude") <= 180)
    )


def filter_valid_speed(df: DataFrame) -> DataFrame:
    return df.filter((F.col("sog") >= 0) & (F.col("sog") <= 60))


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


def deduplicate_positions(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(DUPLICATE_POSITION_COLUMNS)
