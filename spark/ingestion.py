"""Read monthly TLC trip data and store the unchanged records in Bronze."""

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark.config import BRONZE_DIR, RAW_DIR
from spark.session import get_spark

LOGGER = logging.getLogger(__name__)

_EXPLORATION_SCHEMA = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp_ntz",
    "tpep_dropoff_datetime": "timestamp_ntz",
    "passenger_count": "bigint",
    "trip_distance": "double",
    "RatecodeID": "bigint",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "bigint",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "Airport_fee": "double",
}
_TECHNICAL_COLUMNS = {"ingestion_ts", "source_file", "source_month"}


def _validate_month(year: int, month: int) -> None:
    """Validate a calendar year and month before constructing input paths."""
    if year < 1:
        raise ValueError(f"year must be positive; received {year}")
    if not 1 <= month <= 12:
        raise ValueError(f"month must be between 1 and 12; received {month}")


def _log_schema_differences(dataframe: DataFrame, file_name: str) -> None:
    """Warn when the incoming schema differs from the exploration baseline."""
    actual_schema = {
        field.name: field.dataType.simpleString() for field in dataframe.schema.fields
    }
    missing_columns = sorted(_EXPLORATION_SCHEMA.keys() - actual_schema.keys())
    added_columns = sorted(actual_schema.keys() - _EXPLORATION_SCHEMA.keys())
    changed_types = sorted(
        (
            name,
            _EXPLORATION_SCHEMA[name],
            actual_schema[name],
        )
        for name in _EXPLORATION_SCHEMA.keys() & actual_schema.keys()
        if _EXPLORATION_SCHEMA[name] != actual_schema[name]
    )
    if missing_columns or added_columns or changed_types:
        LOGGER.warning(
            "Schema differs from January 2024 exploration baseline for %s: "
            "missing=%s added=%s changed_types=%s",
            file_name,
            missing_columns,
            added_columns,
            changed_types,
        )


def read_raw_month(
    spark: SparkSession, year: int, month: int
) -> tuple[DataFrame, str]:
    """Read one monthly source Parquet without changing its columns or values."""
    _validate_month(year, month)
    file_name = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    input_path = RAW_DIR / file_name
    if not input_path.is_file():
        raise FileNotFoundError(
            f"Raw trip file for {year:04d}-{month:02d} was not found: {input_path}"
        )

    dataframe = spark.read.parquet(str(input_path))
    _log_schema_differences(dataframe, file_name)
    return dataframe, file_name


def add_technical_columns(
    dataframe: DataFrame, file_name: str, source_month: str
) -> DataFrame:
    """Add ingestion metadata while preserving every source column and value."""
    collisions = _TECHNICAL_COLUMNS.intersection(dataframe.columns)
    if collisions:
        raise ValueError(
            "Raw data already contains reserved technical columns: "
            f"{sorted(collisions)}"
        )

    return (
        dataframe.withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.lit(file_name))
        .withColumn("source_month", F.lit(source_month))
    )


def write_bronze(
    dataframe: DataFrame, output_path: str | Path | None = None
) -> None:
    """Overwrite only the input's source-month partition in the Bronze Parquet store."""
    if "source_month" not in dataframe.columns:
        raise ValueError("Bronze DataFrame must contain a source_month column")

    target_path = (
        Path(output_path) if output_path is not None else BRONZE_DIR / "yellow_trips"
    )
    dataframe.sparkSession.conf.set(
        "spark.sql.sources.partitionOverwriteMode", "dynamic"
    )
    (
        dataframe.write.mode("overwrite")
        .partitionBy("source_month")
        .parquet(str(target_path))
    )


def verify_row_count(
    source_row_count: int, bronze_row_count: int, source_month: str
) -> None:
    """Raise when the Bronze partition does not preserve the source row count."""
    if source_row_count != bronze_row_count:
        raise RuntimeError(
            f"Bronze row-count verification failed for {source_month}: "
            f"source={source_row_count}, bronze={bronze_row_count}"
        )
    LOGGER.info(
        "Bronze row-count verification passed for %s: %s rows",
        source_month,
        bronze_row_count,
    )


def ingest_month(spark: SparkSession, year: int, month: int) -> int:
    """Ingest one raw month into Bronze and verify its persisted row count."""
    _validate_month(year, month)
    source_month = f"{year:04d}-{month:02d}"
    source, file_name = read_raw_month(spark, year, month)
    LOGGER.info("Raw schema for %s: %s", file_name, source.schema.simpleString())
    source_row_count = source.count()
    LOGGER.info("Source rows for %s: %s", source_month, source_row_count)

    bronze = add_technical_columns(source, file_name, source_month)
    write_bronze(bronze)

    bronze_month_path = BRONZE_DIR / "yellow_trips"
    persisted_month = (
        spark.read.parquet(str(bronze_month_path))
        .filter(F.col("source_month") == source_month)
        .count()
    )
    LOGGER.info("Bronze rows for %s: %s", source_month, persisted_month)
    verify_row_count(source_row_count, persisted_month, source_month)
    return persisted_month


def main() -> None:
    """Run monthly Bronze ingestion from the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True, help="Source file year")
    parser.add_argument("--month", type=int, required=True, help="Source file month")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    spark = get_spark(f"bronze-ingestion-{args.year:04d}-{args.month:02d}")
    try:
        ingest_month(spark, args.year, args.month)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
