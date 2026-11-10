"""Build the Gold layer from the valid Silver Delta table."""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Sequence
from typing import Any

from pyspark.sql import DataFrame, Window, functions as F

from spark.config import SILVER_TRIPS, gold_path
from spark.gold_catalog import GOLD_TABLES, TABLES_BY_NAME
from spark.session import get_spark

LOGGER = logging.getLogger(__name__)

TABLE_NAMES = [table.name for table in GOLD_TABLES]


def _day_name_expr() -> Any:
    """Return a Spark expression converting Silver pickup_dow into a label."""
    return F.create_map(
        F.lit(1), F.lit("Sunday"),
        F.lit(2), F.lit("Monday"),
        F.lit(3), F.lit("Tuesday"),
        F.lit(4), F.lit("Wednesday"),
        F.lit(5), F.lit("Thursday"),
        F.lit(6), F.lit("Friday"),
        F.lit(7), F.lit("Saturday"),
    )


def _distance_bucket_expr() -> Any:
    """Return the Spark expression for the Gold distance bucket logic."""
    return (
        F.when(F.col("trip_distance") >= 20, F.lit("20+"))
        .when(F.col("trip_distance") >= 10, F.lit("10-20"))
        .when(F.col("trip_distance") >= 5, F.lit("5-10"))
        .when(F.col("trip_distance") >= 3, F.lit("3-5"))
        .when(F.col("trip_distance") >= 1, F.lit("1-3"))
        .otherwise(F.lit("0-1"))
    )


def _distance_bucket_order_expr() -> Any:
    """Return the bucket-order expression used to sort bucket labels."""
    return F.when(F.col("distance_bucket") == "0-1", F.lit(1)).when(
        F.col("distance_bucket") == "1-3",
        F.lit(2),
    ).when(
        F.col("distance_bucket") == "3-5",
        F.lit(3),
    ).when(
        F.col("distance_bucket") == "5-10",
        F.lit(4),
    ).when(
        F.col("distance_bucket") == "10-20",
        F.lit(5),
    ).otherwise(F.lit(6))


def hourly_demand(silver: DataFrame) -> DataFrame:
    """Aggregate one row per pickup date and hour."""
    return (
        silver.groupBy("pickup_date", "pickup_hour")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance_miles"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
        )
        .select(
            F.col("pickup_date").cast("date").alias("pickup_date"),
            F.col("pickup_hour").cast("int").alias("pickup_hour"),
            F.col("trips").cast("bigint"),
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.round(F.col("avg_fare").cast("double"), 2).alias("avg_fare"),
            F.round(F.col("avg_distance_miles").cast("double"), 2).alias(
                "avg_distance_miles"
            ),
            F.round(F.col("avg_duration_min").cast("double"), 2).alias(
                "avg_duration_min"
            ),
        )
        .orderBy("pickup_date", "pickup_hour")
    )


def daily_demand(silver: DataFrame) -> DataFrame:
    """Aggregate one row per pickup date with a weekday label."""
    return (
        silver.groupBy("pickup_date")
        .agg(
            F.min("pickup_dow").cast("int").alias("day_of_week"),
            F.count(F.lit(1)).cast("bigint").alias("trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance_miles"),
        )
        .withColumn("day_name", F.element_at(_day_name_expr(), F.col("day_of_week")))
        .select(
            F.col("pickup_date").cast("date").alias("pickup_date"),
            F.col("day_of_week").cast("int"),
            F.col("day_name").cast("string").alias("day_name"),
            F.col("trips").cast("bigint"),
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.round(F.col("avg_fare").cast("double"), 2).alias("avg_fare"),
            F.round(F.col("avg_distance_miles").cast("double"), 2).alias(
                "avg_distance_miles"
            ),
        )
        .orderBy("pickup_date")
    )


def revenue_by_zone(silver: DataFrame) -> DataFrame:
    """Aggregate one row per pickup zone including Unknown zones."""
    return (
        silver.groupBy("pu_location_id", "pu_zone", "pu_borough")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        .select(
            F.col("pu_location_id").cast("int").alias("pu_location_id"),
            F.col("pu_zone").cast("string").alias("zone"),
            F.col("pu_borough").cast("string").alias("borough"),
            F.col("trips").cast("bigint"),
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.round(F.col("avg_fare").cast("double"), 2).alias("avg_fare"),
        )
        .orderBy(F.col("trips").desc(), F.col("pu_location_id").asc())
    )


def peak_hours(silver: DataFrame) -> DataFrame:
    """Create weekday-hour slots ranked by average trips per day."""
    by_slot = (
        silver.groupBy("pickup_dow", "pickup_hour")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("trips"),
            F.avg("fare_amount").alias("avg_fare"),
            F.countDistinct("pickup_date").cast("int").alias("distinct_days"),
        )
        .withColumn(
            "avg_trips_per_day",
            F.col("trips") / F.col("distinct_days").cast("double"),
        )
        .withColumn("day_of_week", F.col("pickup_dow").cast("int"))
        .withColumn("day_name", F.element_at(_day_name_expr(), F.col("day_of_week")))
        .withColumn(
            "peak_rank",
            F.dense_rank().over(Window.orderBy(F.col("avg_trips_per_day").desc())),
        )
        .select(
            F.col("day_of_week").cast("int"),
            F.col("day_name").cast("string").alias("day_name"),
            F.col("pickup_hour").cast("int"),
            F.col("trips").cast("bigint"),
            F.round(F.col("avg_trips_per_day").cast("double"), 2).alias(
                "avg_trips_per_day"
            ),
            F.round(F.col("avg_fare").cast("double"), 2).alias("avg_fare"),
            F.col("peak_rank").cast("int"),
        )
        .orderBy(F.col("avg_trips_per_day").desc(), F.col("day_of_week"), F.col("pickup_hour"))
    )
    return by_slot


def payment_mix(silver: DataFrame) -> DataFrame:
    """Aggregate payment usage and share-of-trips by payment code."""
    total_trips = silver.count()
    return (
        silver.groupBy("payment_type", "payment_type_label")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("trips"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .withColumn(
            "share_of_trips_pct",
            (F.col("trips") / F.lit(total_trips)) * F.lit(100.0),
        )
        .select(
            F.col("payment_type").cast("int").alias("payment_type_id"),
            F.col("payment_type_label").cast("string").alias("payment_type_name"),
            F.col("trips").cast("bigint"),
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.round(F.col("share_of_trips_pct").cast("double"), 2).alias(
                "share_of_trips_pct"
            ),
        )
        .orderBy(F.col("payment_type_id").asc())
    )


def distance_duration_stats(silver: DataFrame) -> DataFrame:
    """Create distance-band summaries for duration, fare, and speed."""
    return (
        silver.withColumn("distance_bucket", _distance_bucket_expr())
        .withColumn("bucket_order", _distance_bucket_order_expr())
        .groupBy("distance_bucket", "bucket_order")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("trips"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("avg_speed_mph").alias("avg_speed_mph"),
        )
        .select(
            F.col("distance_bucket").cast("string"),
            F.col("bucket_order").cast("int"),
            F.col("trips").cast("bigint"),
            F.round(F.col("avg_duration_min").cast("double"), 2).alias(
                "avg_duration_min"
            ),
            F.round(F.col("avg_fare").cast("double"), 2).alias("avg_fare"),
            F.round(F.col("avg_speed_mph").cast("double"), 2).alias("avg_speed_mph"),
        )
        .orderBy("bucket_order")
    )


def _get_catalog(table_name: str) -> Any:
    """Return the metadata definition for one Gold table."""
    try:
        return TABLES_BY_NAME[table_name]
    except KeyError as exc:  # pragma: no cover - defensive guard.
        raise ValueError(f"Unknown Gold table: {table_name}") from exc


def _validate_schema_against_catalog(dataframe: DataFrame, table_name: str) -> None:
    """Validate the DataFrame's names and Spark types against the catalog."""
    catalog = _get_catalog(table_name)
    actual = dataframe.schema.fields
    if len(actual) != len(catalog.columns):
        raise ValueError(
            f"Schema length mismatch for {table_name}: expected {len(catalog.columns)}, got {len(actual)}"
        )
    for field, column in zip(actual, catalog.columns):
        if field.name != column.name:
            raise ValueError(
                f"Column name mismatch for {table_name}: expected {column.name}, got {field.name}"
            )
        actual_type = field.dataType.simpleString()
        expected_type = column.spark_type
        if actual_type != expected_type:
            raise ValueError(
                f"Type mismatch for {table_name}.{field.name}: expected {expected_type}, got {actual_type}"
            )


def _write_delta_table(dataframe: DataFrame, table_name: str) -> None:
    """Write a single-file Gold Delta table to disk using overwrite semantics."""
    path = gold_path(table_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.coalesce(1).write.format("delta").mode("overwrite").save(str(path))
    LOGGER.info("Wrote %s to %s (%s rows)", table_name, path, dataframe.count())


def assert_gold_quality(*args: Any, **kwargs: Any) -> None:
    """Validate the Gold layer against the catalog and data quality rules.

    When only a subset of Gold tables is provided, the check focuses on those
    rows and skips cross-table reconciliation that requires the full set.
    """
    full_check = kwargs.get("full_check", True)
    if args and isinstance(args[0], dict):
        gold_tables = args[0]
        if len(args) == 2 and isinstance(args[1], DataFrame):
            silver = args[1]
        else:
            silver = kwargs.get("silver")
            if silver is None:
                spark = get_spark("gold-quality")
                silver = spark.read.format("delta").load(str(SILVER_TRIPS))
                spark.stop()
    elif len(args) == 2:
        left, right = args
        if isinstance(left, DataFrame) and isinstance(right, dict):
            silver = left
            gold_tables = right
        elif isinstance(left, dict) and isinstance(right, DataFrame):
            gold_tables = left
            silver = right
        else:
            raise TypeError(
                "assert_gold_quality expects (silver_df, gold_tables) or (gold_tables, silver_df)"
            )
    elif len(args) == 1 and isinstance(args[0], DataFrame):
        silver = args[0]
        spark = get_spark("gold-readback")
        gold_tables = {
            table_name: spark.read.format("delta").load(str(gold_path(table_name)))
            for table_name in TABLE_NAMES
        }
        spark.stop()
    elif len(args) == 0:
        silver = kwargs.get("silver")
        gold_tables = kwargs.get("gold_tables")
        if silver is None:
            spark = get_spark("gold-quality")
            silver = spark.read.format("delta").load(str(SILVER_TRIPS))
            spark.stop()
        if gold_tables is None:
            spark = get_spark("gold-readback")
            gold_tables = {
                table_name: spark.read.format("delta").load(str(gold_path(table_name)))
                for table_name in TABLE_NAMES
            }
            spark.stop()
    else:
        raise TypeError("Unsupported assert_gold_quality invocation")

    if gold_tables is None:
        raise ValueError("No Gold tables supplied for validation")

    silver_count = silver.count()
    for table_name, dataframe in gold_tables.items():
        if table_name not in TABLES_BY_NAME:
            raise ValueError(f"Unexpected Gold table name: {table_name}")
        _validate_schema_against_catalog(dataframe, table_name)

        key_columns = {
            "gold_hourly_demand": ["pickup_date", "pickup_hour"],
            "gold_daily_demand": ["pickup_date"],
            "gold_revenue_by_zone": ["pu_location_id"],
            "gold_peak_hours": ["day_of_week", "pickup_hour"],
            "gold_payment_mix": ["payment_type_id"],
            "gold_distance_duration_stats": ["distance_bucket"],
        }[table_name]

        null_predicate = " OR ".join(f"{column} IS NULL" for column in key_columns)
        if null_predicate:
            null_count = dataframe.filter(null_predicate).count()
            if null_count > 0:
                raise ValueError(f"Null key values detected in {table_name}")

        duplicate_count = (
            dataframe.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
        )
        if duplicate_count > 0:
            raise ValueError(f"Duplicate grain detected in {table_name}")

        if full_check:
            trip_total = dataframe.agg(F.sum("trips").cast("bigint")).first()[0]
            if trip_total != silver_count:
                raise ValueError(
                    f"{table_name} trips do not reconcile to Silver row count: silver={silver_count}, gold={trip_total}"
                )

    if not full_check:
        return

    for table_name in (
        "gold_hourly_demand",
        "gold_daily_demand",
        "gold_revenue_by_zone",
        "gold_payment_mix",
    ):
        table = gold_tables[table_name]
        expected_revenue = silver.agg(F.sum("total_amount").alias("silver_total_amount")).first()[0]
        actual_revenue = table.agg(F.sum("total_revenue").alias("gold_total_revenue")).first()[0]
        tolerance = 0.005 * table.count()
        if abs(actual_revenue - expected_revenue) > tolerance:
            raise ValueError(
                f"Revenue mismatch in {table_name}: silver={expected_revenue}, gold={actual_revenue}, tolerance={tolerance}"
            )

    share_total = gold_tables["gold_payment_mix"].agg(F.sum("share_of_trips_pct")).first()[0]
    if abs(share_total - 100.0) > 0.05:
        raise ValueError(
            f"Payment mix shares sum to {share_total}, outside tolerance 100 +/- 0.05"
        )

    peak_table = gold_tables["gold_peak_hours"]
    if peak_table.filter(F.col("peak_rank") < 1).count() > 0:
        raise ValueError("peak_rank contains values lower than 1")
    expected_rank = peak_table.withColumn(
        "expected_peak_rank",
        F.dense_rank().over(Window.orderBy(F.col("avg_trips_per_day").desc())),
    )
    if expected_rank.filter(F.col("peak_rank") != F.col("expected_peak_rank")).count() > 0:
        raise ValueError("peak_rank does not match dense ranking by avg_trips_per_day")


def build_gold(silver: DataFrame | None = None, table_name: str | None = None) -> dict[str, DataFrame]:
    """Build all Gold tables from the valid Silver Delta table and write them to disk."""
    spark = get_spark("gold-build")
    try:
        if silver is None:
            silver = spark.read.format("delta").load(str(SILVER_TRIPS))
        silver = silver.cache()
        LOGGER.info("Silver row count before Gold build: %s", silver.count())

        selected_tables = [table_name] if table_name else TABLE_NAMES
        tables: dict[str, DataFrame] = {}
        for current_name in selected_tables:
            if current_name not in TABLES_BY_NAME:
                raise ValueError(f"Unknown Gold table: {current_name}")
            builder = {
                "gold_hourly_demand": hourly_demand,
                "gold_daily_demand": daily_demand,
                "gold_revenue_by_zone": revenue_by_zone,
                "gold_peak_hours": peak_hours,
                "gold_payment_mix": payment_mix,
                "gold_distance_duration_stats": distance_duration_stats,
            }[current_name]
            start = time.perf_counter()
            golden = builder(silver)
            elapsed = time.perf_counter() - start
            LOGGER.info("Built %s in %.2f seconds (%s rows)", current_name, elapsed, golden.count())
            _validate_schema_against_catalog(golden, current_name)
            tables[current_name] = golden
            _write_delta_table(golden, current_name)

        if len(tables) == len(TABLE_NAMES):
            assert_gold_quality(silver, tables)
        else:
            for name, dataframe in tables.items():
                _validate_schema_against_catalog(dataframe, name)
        return tables
    finally:
        try:
            if silver is not None:
                silver.unpersist()
        finally:
            spark.stop()


def main() -> None:
    """CLI entry point for Gold-table generation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table",
        choices=TABLE_NAMES,
        help="Optional Gold table to rebuild. If omitted, all Gold tables are rebuilt.",
    )
    rest = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    build_gold(table_name=rest.table)


if __name__ == "__main__":
    main()
