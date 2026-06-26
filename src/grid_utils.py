from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Default spatial grid resolution in degrees. Shared so every job that bins
# AIS positions into cells uses the same grid (otherwise grid_id values from
# different scripts would not line up).
DEFAULT_GRID_SIZE = 0.1


def add_grid_index_columns(df: DataFrame, grid_size: float) -> DataFrame:
    """Add grid_lat_index, grid_lon_index and grid_id columns.

    Each position is floored into a ``grid_size`` x ``grid_size`` degree cell.
    ``grid_id`` is the "<lat_index>:<lon_index>" string used as the cell key.
    """
    grid_size_lit = F.lit(grid_size)
    return (
        df.withColumn(
            "grid_lat_index", F.floor(F.col("latitude") / grid_size_lit).cast("long")
        )
        .withColumn(
            "grid_lon_index", F.floor(F.col("longitude") / grid_size_lit).cast("long")
        )
        .withColumn(
            "grid_id",
            F.concat_ws(
                ":",
                F.col("grid_lat_index").cast("string"),
                F.col("grid_lon_index").cast("string"),
            ),
        )
    )
