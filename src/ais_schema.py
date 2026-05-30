from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


AIS_SCHEMA = StructType(
    [
        StructField("mmsi", LongType(), True),
        StructField("base_date_time", TimestampType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("sog", DoubleType(), True),
        StructField("cog", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("vessel_name", StringType(), True),
        StructField("imo", StringType(), True),
        StructField("call_sign", StringType(), True),
        StructField("vessel_type", IntegerType(), True),
        StructField("status", IntegerType(), True),
        StructField("length", DoubleType(), True),
        StructField("width", DoubleType(), True),
        StructField("draft", DoubleType(), True),
        StructField("cargo", IntegerType(), True),
        StructField("transceiver", StringType(), True),
    ]
)
