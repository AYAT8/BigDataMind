"""Tests for read-only Bronze ingestion behavior."""

from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.ingestion import (
    add_technical_columns,
    verify_row_count,
    write_bronze,
)


@pytest.fixture
def tiny_trips(spark: SparkSession) -> DataFrame:
    """Create a small source-shaped DataFrame with an out-of-month pickup."""
    schema = StructType(
        [
            StructField("PULocationID", StringType(), nullable=False),
            StructField("tpep_pickup_datetime", TimestampType(), nullable=False),
        ]
    )
    rows = [
        ("100", datetime(2023, 12, 31, 23, 58)),
        ("101", datetime(2024, 1, 15, 12, 0)),
    ]
    return spark.createDataFrame(rows, schema=schema)


def test_add_technical_columns_preserves_source_data(tiny_trips):
    original_columns = tiny_trips.columns
    original_schema = tiny_trips.schema
    original_rows = tiny_trips.collect()

    bronze = add_technical_columns(
        tiny_trips, "yellow_tripdata_2024-01.parquet", "2024-01"
    )

    assert bronze.columns == original_columns + [
        "ingestion_ts",
        "source_file",
        "source_month",
    ]
    assert bronze.schema[: len(original_columns)] == original_schema
    assert bronze.select(*original_columns).collect() == original_rows
    assert bronze.schema["ingestion_ts"].dataType == TimestampType()
    assert {row.source_file for row in bronze.select("source_file").collect()} == {
        "yellow_tripdata_2024-01.parquet"
    }


def test_source_month_comes_from_requested_file_month(tiny_trips):
    bronze = add_technical_columns(
        tiny_trips, "yellow_tripdata_2024-01.parquet", "2024-01"
    )

    rows = bronze.select("tpep_pickup_datetime", "source_month").collect()
    out_of_month = next(row for row in rows if row.tpep_pickup_datetime.year == 2023)
    assert out_of_month.source_month == "2024-01"


def test_writing_same_month_twice_is_idempotent(tiny_trips, tmp_path):
    bronze = add_technical_columns(
        tiny_trips, "yellow_tripdata_2024-01.parquet", "2024-01"
    )
    output_path = tmp_path / "yellow_trips"

    write_bronze(bronze, output_path)
    first_count = (
        tiny_trips.sparkSession.read.parquet(str(output_path))
        .filter("source_month = '2024-01'")
        .count()
    )
    write_bronze(bronze, output_path)
    second_count = (
        tiny_trips.sparkSession.read.parquet(str(output_path))
        .filter("source_month = '2024-01'")
        .count()
    )

    assert first_count == 2
    assert second_count == first_count


def test_verify_row_count_raises_on_mismatch():
    with pytest.raises(RuntimeError, match="source=2, bronze=1"):
        verify_row_count(2, 1, "2024-01")


def test_verify_row_count_accepts_matching_counts():
    verify_row_count(2, 2, "2024-01")
