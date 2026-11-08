"""Tests for Silver standardization, rejection, and zone enrichment."""

from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.cleaning import (
    add_derived_columns,
    apply_passenger_rule,
    assert_reconciliation,
    assert_silver_quality,
    drop_duplicates,
    enrich_with_zones,
    flag_rejections,
    split_valid_rejected,
    standardize_columns,
    _write_delta_partition,
)


_TRIP_SCHEMA = StructType(
    [
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("source_month", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
    ]
)


def _trip_row(**overrides):
    row = {
        "pickup_datetime": datetime(2024, 1, 15, 10, 0),
        "dropoff_datetime": datetime(2024, 1, 15, 11, 0),
        "source_month": "2024-01",
        "fare_amount": 10.0,
        "total_amount": 12.0,
        "trip_distance": 10.0,
        "payment_type": 2,
        "passenger_count": 1,
        "ratecode_id": 1,
        "store_and_fwd_flag": "N",
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
        "pu_location_id": 100,
        "do_location_id": 200,
    }
    row.update(overrides)
    return tuple(row[field.name] for field in _TRIP_SCHEMA.fields)


def _trips(spark: SparkSession, *rows) -> DataFrame:
    return spark.createDataFrame(rows, schema=_TRIP_SCHEMA)


def _reasons(spark: SparkSession, **overrides) -> set[str]:
    derived = add_derived_columns(_trips(spark, _trip_row(**overrides)))
    reasons = flag_rejections(derived).select("reject_reasons").first()[0]
    return set(reasons)


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        (
            {
                "pickup_datetime": datetime(2023, 12, 31, 23, 0),
                "dropoff_datetime": datetime(2023, 12, 31, 23, 30),
            },
            "OUT_OF_SOURCE_MONTH",
        ),
        ({"fare_amount": -1.0}, "NEGATIVE_AMOUNT"),
        ({"total_amount": -1.0}, "NEGATIVE_AMOUNT"),
        ({"trip_distance": 0.0}, "ZERO_DISTANCE"),
        (
            {"dropoff_datetime": datetime(2024, 1, 15, 10, 0)},
            "NON_POSITIVE_DURATION",
        ),
        (
            {
                "trip_distance": 3.0,
                "dropoff_datetime": datetime(2024, 1, 15, 10, 1),
            },
            "IMPLAUSIBLE_SPEED",
        ),
        (
            {
                "trip_distance": 501.0,
                "dropoff_datetime": datetime(2024, 1, 15, 17, 0),
            },
            "IMPLAUSIBLE_DISTANCE",
        ),
        (
            {
                "trip_distance": 10.0,
                "dropoff_datetime": datetime(2024, 1, 16, 11, 1),
            },
            "LONG_DURATION",
        ),
    ],
)
def test_each_rejection_rule_flags_expected_reason(spark, overrides, reason):
    assert _reasons(spark, **overrides) == {reason}


def test_multiple_rejection_reasons_are_retained(spark):
    reasons = _reasons(
        spark,
        pickup_datetime=datetime(2023, 12, 31, 23, 0),
        dropoff_datetime=datetime(2023, 12, 31, 23, 0),
        fare_amount=-2.0,
        trip_distance=0.0,
    )

    assert reasons == {
        "OUT_OF_SOURCE_MONTH",
        "NEGATIVE_AMOUNT",
        "ZERO_DISTANCE",
        "NON_POSITIVE_DURATION",
    }


def test_payment_type_zero_and_nulls_are_kept_without_imputation(spark):
    row = _trip_row(
        payment_type=0,
        passenger_count=None,
        ratecode_id=None,
        store_and_fwd_flag=None,
        congestion_surcharge=None,
        airport_fee=None,
    )
    standardized = standardize_columns(_trips(spark, row))
    cleaned = apply_passenger_rule(
        flag_rejections(add_derived_columns(standardized))
    )
    valid, rejected = split_valid_rejected(cleaned)
    result = valid.first()

    assert valid.count() == 1
    assert rejected.count() == 0
    assert result.payment_type == 0
    assert result.payment_type_label == "Flex Fare"
    assert result.passenger_count is None
    assert result.ratecode_id is None
    assert result.store_and_fwd_flag is None
    assert result.congestion_surcharge is None
    assert result.airport_fee is None


def test_standardize_columns_renames_and_casts_without_dropping_metadata(spark):
    source_schema = StructType(
        [
            StructField("VendorID", StringType(), True),
            StructField("PULocationID", StringType(), True),
            StructField("Airport_fee", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("source_month", StringType(), True),
        ]
    )
    source = spark.createDataFrame(
        [("2", "100", "1.25", "yellow_tripdata_2024-01.parquet", "2024-01")],
        source_schema,
    )

    standardized = standardize_columns(source)
    row = standardized.first()

    assert standardized.columns == [
        "vendor_id",
        "pu_location_id",
        "airport_fee",
        "source_file",
        "source_month",
    ]
    assert standardized.schema["vendor_id"].dataType == IntegerType()
    assert standardized.schema["pu_location_id"].dataType == IntegerType()
    assert standardized.schema["airport_fee"].dataType == DoubleType()
    assert row.vendor_id == 2
    assert row.pu_location_id == 100
    assert row.airport_fee == 1.25
    assert row.source_month == "2024-01"


def test_passenger_counts_zero_and_over_six_become_null(spark):
    trips = _trips(
        spark,
        _trip_row(passenger_count=0),
        _trip_row(passenger_count=7),
        _trip_row(passenger_count=1),
    )

    counts = [
        row.passenger_count
        for row in apply_passenger_rule(trips)
        .select("passenger_count")
        .orderBy(F.col("passenger_count").asc_nulls_first())
        .collect()
    ]

    assert counts == [None, None, 1]


def test_zone_join_keeps_row_count_and_labels_unmatched_ids(spark):
    trips = _trips(
        spark,
        _trip_row(pu_location_id=100, do_location_id=200),
        _trip_row(pu_location_id=999, do_location_id=200),
    )
    zones = spark.createDataFrame(
        [(100, "Zone A", "Borough A"), (200, "Zone B", "Borough B")],
        ["LocationID", "Zone", "Borough"],
    )

    enriched = enrich_with_zones(trips, zones).orderBy("pu_location_id")
    rows = enriched.collect()

    assert len(rows) == trips.count()
    assert rows[0].pu_zone == "Zone A"
    assert rows[0].pu_borough == "Borough A"
    assert rows[0].do_zone == "Zone B"
    assert rows[1].pu_zone == "Unknown"
    assert rows[1].pu_borough == "Unknown"


def test_silver_delta_partition_rewrite_is_idempotent(spark, tmp_path):
    output_path = tmp_path / "silver"
    first = spark.createDataFrame(
        [("2024-01", "one"), ("2024-01", "two")],
        ["source_month", "value"],
    )
    replacement = spark.createDataFrame(
        [("2024-01", "one"), ("2024-01", "two"), ("2024-01", "three")],
        ["source_month", "value"],
    )

    _write_delta_partition(first, output_path, "2024-01")
    _write_delta_partition(replacement, output_path, "2024-01")
    persisted = spark.read.format("delta").load(str(output_path))

    assert persisted.count() == 3
    assert persisted.filter("source_month = '2024-01'").count() == 3


def test_reconciliation_invariant_and_failure():
    assert_reconciliation(10, 8, 2, 0)
    with pytest.raises(AssertionError, match="bronze=10"):
        assert_reconciliation(10, 8, 1, 0)


def test_quality_gate_raises_when_silver_violation_is_injected(spark):
    trips = add_derived_columns(_trips(spark, _trip_row()))
    invalid = trips.withColumn("trip_distance", F.lit(0.0))

    with pytest.raises(AssertionError, match="non_positive_distance"):
        assert_silver_quality(invalid, "2024-01")


def test_quality_gate_checks_reconciliation_counts(spark):
    trips = add_derived_columns(_trips(spark, _trip_row()))

    with pytest.raises(AssertionError, match="reconciliation"):
        assert_silver_quality(
            trips,
            "2024-01",
            bronze_rows=2,
            rejected_rows=0,
            duplicates_removed=0,
        )


def test_drop_duplicates_reports_removed_count(spark):
    rows = _trips(spark, _trip_row(), _trip_row())

    distinct, removed = drop_duplicates(rows)

    assert distinct.count() == 1
    assert removed == 1
