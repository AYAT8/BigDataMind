"""Tests for the Gold aggregation builders and write behavior."""

from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark import aggregations
from spark.gold_catalog import GOLD_TABLES


@pytest.fixture
def tiny_silver(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("pickup_date", DateType(), nullable=False),
            StructField("pickup_hour", IntegerType(), nullable=False),
            StructField("pickup_dow", IntegerType(), nullable=False),
            StructField("total_amount", DoubleType(), nullable=False),
            StructField("fare_amount", DoubleType(), nullable=False),
            StructField("trip_distance", DoubleType(), nullable=False),
            StructField("trip_duration_min", DoubleType(), nullable=False),
            StructField("avg_speed_mph", DoubleType(), nullable=True),
            StructField("pu_location_id", IntegerType(), nullable=False),
            StructField("pu_zone", StringType(), nullable=False),
            StructField("pu_borough", StringType(), nullable=False),
            StructField("payment_type", IntegerType(), nullable=False),
            StructField("payment_type_label", StringType(), nullable=False),
        ]
    )
    rows = [
        (
            date(2024, 1, 1), 8, 2, 10.0, 8.0, 1.0, 10.0, 6.0, 1,
            "Unknown", "Unknown", 1, "Credit card",
        ),
        (
            date(2024, 1, 1), 8, 2, 20.0, 16.0, 3.0, 20.0, 9.0, 1,
            "Unknown", "Unknown", 2, "Cash",
        ),
        (
            date(2024, 1, 1), 9, 2, 30.0, 24.0, 5.0, 30.0, 10.0, 2,
            "Zone A", "Manhattan", 1, "Credit card",
        ),
        (
            date(2024, 1, 1), 9, 2, 40.0, 32.0, 10.0, 40.0, 12.0, 2,
            "Zone A", "Manhattan", 1, "Credit card",
        ),
        (
            date(2024, 1, 1), 10, 2, 50.0, 40.0, 20.0, 50.0, 15.0, 3,
            "Zone B", "Queens", 2, "Cash",
        ),
        (
            date(2024, 1, 1), 10, 2, 60.0, 48.0, 0.9, 60.0, 0.9, 3,
            "Zone B", "Queens", 1, "Credit card",
        ),
        (
            date(2024, 1, 1), 11, 2, 70.0, 56.0, 2.0, 70.0, 1.7, 4,
            "Zone C", "Brooklyn", 1, "Credit card",
        ),
        (
            date(2024, 1, 8), 8, 2, 80.0, 64.0, 2.0, 80.0, 1.5, 1,
            "Unknown", "Unknown", 2, "Cash",
        ),
        (
            date(2024, 1, 8), 8, 2, 90.0, 72.0, 4.0, 90.0, 2.7, 1,
            "Unknown", "Unknown", 1, "Credit card",
        ),
    ]
    return spark.createDataFrame(rows, schema=schema)


def test_hourly_and_daily_demand_use_hand_computed_sums(tiny_silver):
    hourly = aggregations.hourly_demand(tiny_silver)
    jan_1_hour_8 = hourly.filter(
        "pickup_date = DATE'2024-01-01' AND pickup_hour = 8"
    ).first()
    assert jan_1_hour_8.trips == 2
    assert jan_1_hour_8.total_revenue == 30.0
    assert jan_1_hour_8.avg_fare == 12.0

    daily = aggregations.daily_demand(tiny_silver)
    jan_1 = daily.filter("pickup_date = DATE'2024-01-01'").first()
    assert jan_1.trips == 7
    assert jan_1.total_revenue == 280.0
    assert jan_1.day_of_week == 2
    assert jan_1.day_name == "Monday"


def test_zone_aggregation_keeps_unknown(tiny_silver):
    zones = aggregations.revenue_by_zone(tiny_silver)
    unknown = zones.filter("zone = 'Unknown'").first()
    assert unknown.pu_location_id == 1
    assert unknown.trips == 4
    assert unknown.total_revenue == 200.0


def test_peak_hours_use_average_per_day_and_keep_ties(tiny_silver):
    peaks = aggregations.peak_hours(tiny_silver)
    rank_one = peaks.filter("peak_rank = 1").select(
        "pickup_hour", "avg_trips_per_day"
    ).collect()
    assert {(row.pickup_hour, row.avg_trips_per_day) for row in rank_one} == {
        (8, 2.0),
        (9, 2.0),
        (10, 2.0),
    }
    assert peaks.filter("pickup_hour = 11").first().peak_rank == 2


def test_payment_shares_sum_to_one_hundred(tiny_silver):
    payment = aggregations.payment_mix(tiny_silver)
    shares = payment.agg({"share_of_trips_pct": "sum"}).first()[0]
    assert shares == 100.0
    credit = payment.filter("payment_type_id = 1").first()
    assert credit.trips == 6
    assert credit.share_of_trips_pct == pytest.approx(66.67)


def test_distance_bucket_boundaries_are_lower_inclusive(tiny_silver):
    stats = aggregations.distance_duration_stats(tiny_silver)
    counts = {
        row.distance_bucket: row.trips
        for row in stats.select("distance_bucket", "trips").collect()
    }
    assert counts == {
        "0-1": 1,
        "1-3": 3,
        "3-5": 2,
        "5-10": 1,
        "10-20": 1,
        "20+": 1,
    }


def test_every_builder_matches_catalog_schema(tiny_silver):
    builders = {
        "gold_hourly_demand": aggregations.hourly_demand,
        "gold_daily_demand": aggregations.daily_demand,
        "gold_revenue_by_zone": aggregations.revenue_by_zone,
        "gold_peak_hours": aggregations.peak_hours,
        "gold_payment_mix": aggregations.payment_mix,
        "gold_distance_duration_stats": aggregations.distance_duration_stats,
    }
    catalog = {table.name: table for table in GOLD_TABLES}
    for name, builder in builders.items():
        frame = builder(tiny_silver)
        assert [
            (field.name, field.dataType.simpleString()) for field in frame.schema.fields
        ] == [(column.name, column.spark_type) for column in catalog[name].columns]


def test_delta_write_is_idempotent(tiny_silver, monkeypatch, tmp_path):
    frame = aggregations.hourly_demand(tiny_silver)
    path = tmp_path / "gold_hourly_demand"
    monkeypatch.setattr(aggregations, "gold_path", lambda name: path)

    aggregations._write_delta_table(frame, "gold_hourly_demand")
    first = tiny_silver.sparkSession.read.format("delta").load(str(path)).collect()
    aggregations._write_delta_table(frame, "gold_hourly_demand")
    second = tiny_silver.sparkSession.read.format("delta").load(str(path)).collect()

    assert first == second


@pytest.mark.parametrize(
    ("frame_factory", "message"),
    [
        (
            lambda silver: aggregations.hourly_demand(silver).unionByName(
                aggregations.hourly_demand(silver).limit(1)
            ),
            "Duplicate grain",
        ),
        (
            lambda silver: aggregations.hourly_demand(silver).withColumn(
                "pickup_date",
                F.lit(None).cast("date"),
            ),
            "Null key",
        ),
    ],
)
def test_quality_check_rejects_injected_violations(
    tiny_silver, frame_factory, message
):
    invalid = frame_factory(tiny_silver)
    with pytest.raises(ValueError, match=message):
        aggregations.assert_gold_quality(
            tiny_silver,
            {"gold_hourly_demand": invalid},
            full_check=False,
        )
