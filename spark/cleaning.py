"""Standardize Bronze trip records, quarantine invalid rows, and write Silver."""

import argparse
import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.config import (
    BRONZE_DIR,
    CLEANING_THRESHOLDS,
    QUALITY_REPORT,
    RAW_DIR,
    SILVER_REJECTED,
    SILVER_TRIPS,
    CleaningThresholds,
)
from spark.session import get_spark

LOGGER = logging.getLogger(__name__)

_COLUMN_NAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}
_INTEGER_COLUMNS = {
    "vendor_id",
    "ratecode_id",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "passenger_count",
}
_DOUBLE_COLUMNS = {
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
}
_PAYMENT_TYPE_LABELS = {
    0: "Flex Fare",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}
_QUALITY_REPORT_SCHEMA = StructType(
    [
        StructField("run_ts", TimestampType(), nullable=False),
        StructField("source_month", StringType(), nullable=False),
        StructField("rule_id", StringType(), nullable=False),
        StructField("description", StringType(), nullable=False),
        StructField("rows_affected", LongType(), nullable=True),
        StructField("action", StringType(), nullable=False),
        StructField("metric_value", DoubleType(), nullable=True),
        StructField("threshold_value", DoubleType(), nullable=True),
    ]
)
_RULE_DESCRIPTIONS = {
    "OUT_OF_SOURCE_MONTH": (
        "Pickup month differs from the source file month."
    ),
    "NEGATIVE_AMOUNT": "Fare or total amount is negative.",
    "ZERO_DISTANCE": "Trip distance is zero or negative.",
    "NON_POSITIVE_DURATION": "Drop-off is not later than pickup.",
    "IMPLAUSIBLE_SPEED": "Average trip speed exceeds the configured limit.",
    "IMPLAUSIBLE_DISTANCE": "Trip distance exceeds the configured limit in miles.",
    "LONG_DURATION": "Trip duration exceeds the provisional 24-hour limit.",
    "DISTINCT_REJECTED_ROWS": "Rows quarantined with one or more rejection reasons.",
    "DUPLICATES_REMOVED": "Exact duplicate valid rows removed from Silver.",
    "PASSENGER_COUNT_NULLIFIED": (
        "Passenger counts of zero or greater than six set to null."
    ),
}


def standardize_columns(dataframe: DataFrame) -> DataFrame:
    """Rename TLC fields and cast known columns without filtering source rows."""
    renamed = dataframe
    for source_name, target_name in _COLUMN_NAMES.items():
        if source_name in renamed.columns and source_name != target_name:
            if target_name in renamed.columns:
                raise ValueError(
                    f"Cannot rename {source_name} to existing column {target_name}"
                )
            renamed = renamed.withColumnRenamed(source_name, target_name)

    for column in _INTEGER_COLUMNS.intersection(renamed.columns):
        renamed = renamed.withColumn(column, F.col(column).cast("integer"))
    for column in _DOUBLE_COLUMNS.intersection(renamed.columns):
        renamed = renamed.withColumn(column, F.col(column).cast("double"))
    for column in ("pickup_datetime", "dropoff_datetime"):
        if column in renamed.columns:
            renamed = renamed.withColumn(column, F.col(column).cast("timestamp"))
    return renamed


def add_derived_columns(dataframe: DataFrame) -> DataFrame:
    """Add duration, speed, pickup calendar fields, and dictionary labels."""
    required = {"pickup_datetime", "dropoff_datetime"}
    missing = required - set(dataframe.columns)
    if missing:
        raise ValueError(f"Cannot derive trip fields; missing columns: {sorted(missing)}")

    duration_seconds = (
        F.unix_timestamp("dropoff_datetime")
        - F.unix_timestamp("pickup_datetime")
    )
    labels = F.create_map(
        *[
            value
            for code, label in _PAYMENT_TYPE_LABELS.items()
            for value in (F.lit(code), F.lit(label))
        ]
    )
    derived = (
        dataframe.withColumn("trip_duration_min", duration_seconds / F.lit(60.0))
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.col("trip_distance") * F.lit(60.0) / F.col("trip_duration_min"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
    )
    if "payment_type" in derived.columns:
        derived = derived.withColumn(
            "payment_type_label", F.element_at(labels, F.col("payment_type"))
        )
    return derived


def _rejection_conditions(
    thresholds: CleaningThresholds,
) -> list[tuple[str, Column]]:
    """Build the active rejection checks for a derived trip DataFrame."""
    return [
        (
            "OUT_OF_SOURCE_MONTH",
            F.date_format("pickup_datetime", "yyyy-MM")
            != F.col("source_month"),
        ),
        (
            "NEGATIVE_AMOUNT",
            (F.col("fare_amount") < 0) | (F.col("total_amount") < 0),
        ),
        ("ZERO_DISTANCE", F.col("trip_distance") <= 0),
        ("NON_POSITIVE_DURATION", F.col("trip_duration_min") <= 0),
        (
            "IMPLAUSIBLE_SPEED",
            F.col("avg_speed_mph") > thresholds.max_avg_speed_mph,
        ),
        (
            "IMPLAUSIBLE_DISTANCE",
            F.col("trip_distance") > thresholds.max_trip_distance_miles,
        ),
        (
            "LONG_DURATION",
            F.col("trip_duration_min") > thresholds.max_trip_duration_minutes,
        ),
    ]


def flag_rejections(
    dataframe: DataFrame, thresholds: CleaningThresholds = CLEANING_THRESHOLDS
) -> DataFrame:
    """Attach every applicable D1/D3-D6 reason as an array of strings."""
    required = {
        "pickup_datetime",
        "source_month",
        "fare_amount",
        "total_amount",
        "trip_distance",
        "trip_duration_min",
        "avg_speed_mph",
    }
    missing = required - set(dataframe.columns)
    if missing:
        raise ValueError(f"Cannot flag rejections; missing columns: {sorted(missing)}")
    if "reject_reasons" in dataframe.columns:
        raise ValueError("DataFrame already contains reject_reasons")

    reasons = [
        F.when(condition, F.lit(reason))
        for reason, condition in _rejection_conditions(thresholds)
    ]
    return dataframe.withColumn(
        "reject_reasons",
        F.filter(
            F.array(*reasons),
            lambda reason: reason.isNotNull(),
        ),
    )


def apply_passenger_rule(
    dataframe: DataFrame, thresholds: CleaningThresholds = CLEANING_THRESHOLDS
) -> DataFrame:
    """Set passenger counts of zero or greater than the configured maximum to null."""
    if "passenger_count" not in dataframe.columns:
        raise ValueError("Cannot apply passenger rule; passenger_count is missing")
    invalid_count = (F.col("passenger_count") == 0) | (
        F.col("passenger_count") > thresholds.max_passenger_count
    )
    return dataframe.withColumn(
        "passenger_count",
        F.when(invalid_count, F.lit(None).cast("integer")).otherwise(
            F.col("passenger_count")
        ),
    )


def split_valid_rejected(dataframe: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows with empty rejection reasons from rows requiring quarantine."""
    if "reject_reasons" not in dataframe.columns:
        raise ValueError("Cannot split rows; reject_reasons is missing")
    return (
        dataframe.filter(F.size("reject_reasons") == 0),
        dataframe.filter(F.size("reject_reasons") > 0),
    )


def drop_duplicates(dataframe: DataFrame) -> tuple[DataFrame, int]:
    """Remove exact duplicate rows and return the number removed."""
    original_count = dataframe.count()
    distinct = dataframe.dropDuplicates()
    removed_count = original_count - distinct.count()
    return distinct, removed_count


def enrich_with_zones(dataframe: DataFrame, zones: DataFrame) -> DataFrame:
    """Broadcast-join pickup/drop-off zone names, preserving row count."""
    required_trip_fields = {"pu_location_id", "do_location_id"}
    missing_trip = required_trip_fields - set(dataframe.columns)
    if missing_trip:
        raise ValueError(f"Cannot enrich zones; missing columns: {sorted(missing_trip)}")
    required_zone_fields = {"LocationID", "Zone", "Borough"}
    missing_zone = required_zone_fields - set(zones.columns)
    if missing_zone:
        raise ValueError(f"Zone lookup is missing columns: {sorted(missing_zone)}")

    duplicated_ids = (
        zones.groupBy("LocationID")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicated_ids:
        raise ValueError("Zone lookup contains duplicate LocationID values")

    pickup_zones = F.broadcast(
        zones.select(
            F.col("LocationID").cast("integer").alias("_pu_zone_id"),
            F.col("Zone").alias("pu_zone"),
            F.col("Borough").alias("pu_borough"),
        )
    )
    enriched = dataframe.join(
        pickup_zones,
        dataframe.pu_location_id == pickup_zones._pu_zone_id,
        "left",
    ).drop("_pu_zone_id")

    dropoff_zones = F.broadcast(
        zones.select(
            F.col("LocationID").cast("integer").alias("_do_zone_id"),
            F.col("Zone").alias("do_zone"),
            F.col("Borough").alias("do_borough"),
        )
    )
    enriched = enriched.join(
        dropoff_zones,
        enriched.do_location_id == dropoff_zones._do_zone_id,
        "left",
    ).drop("_do_zone_id")
    for column in ("pu_zone", "pu_borough", "do_zone", "do_borough"):
        enriched = enriched.withColumn(
            column, F.coalesce(F.col(column), F.lit("Unknown"))
        )
    return enriched


def assert_reconciliation(
    bronze_rows: int,
    silver_rows: int,
    rejected_rows: int,
    duplicates_removed: int,
) -> None:
    """Assert each Bronze row is Silver, quarantined, or an exact duplicate."""
    accounted_rows = silver_rows + rejected_rows + duplicates_removed
    if bronze_rows != accounted_rows:
        raise AssertionError(
            "Silver reconciliation failed: "
            f"bronze={bronze_rows}, silver={silver_rows}, "
            f"rejected={rejected_rows}, duplicates_removed={duplicates_removed}"
        )


def assert_silver_quality(
    dataframe: DataFrame,
    source_month: str,
    *,
    bronze_rows: int | None = None,
    rejected_rows: int | None = None,
    duplicates_removed: int | None = None,
    thresholds: CleaningThresholds = CLEANING_THRESHOLDS,
) -> None:
    """Raise if Silver violates required schema, date, amount, distance, or count rules."""
    key_columns = (
        "source_month",
        "pickup_datetime",
        "dropoff_datetime",
        "pu_location_id",
        "do_location_id",
    )
    missing = set(key_columns) - set(dataframe.columns)
    if missing:
        raise AssertionError(f"Silver is missing required key columns: {sorted(missing)}")

    predicates = {
        "null_key_columns": F.expr(
            " OR ".join(f"`{column}` IS NULL" for column in key_columns)
        ),
        "source_month_mismatch": F.col("source_month") != source_month,
        "dropoff_not_after_pickup": F.col("dropoff_datetime")
        <= F.col("pickup_datetime"),
        "pickup_outside_source_month": F.date_format(
            "pickup_datetime", "yyyy-MM"
        )
        != F.col("source_month"),
        "negative_amount": (F.col("fare_amount") < 0)
        | (F.col("total_amount") < 0),
        "non_positive_distance": F.col("trip_distance") <= 0,
        "speed_over_threshold": F.col("avg_speed_mph")
        > thresholds.max_avg_speed_mph,
        "distance_over_threshold": F.col("trip_distance")
        > thresholds.max_trip_distance_miles,
        "duration_over_threshold": F.col("trip_duration_min")
        > thresholds.max_trip_duration_minutes,
    }
    counts = dataframe.agg(
        *[
            F.sum(F.when(predicate, 1).otherwise(0)).alias(name)
            for name, predicate in predicates.items()
        ]
    ).first().asDict()
    violations = {name: count for name, count in counts.items() if count}

    total_rows = dataframe.count()
    duplicate_count = total_rows - dataframe.dropDuplicates().count()
    if duplicate_count:
        violations["duplicates"] = duplicate_count

    reconciliation_values = (bronze_rows, rejected_rows, duplicates_removed)
    if any(value is not None for value in reconciliation_values):
        if any(value is None for value in reconciliation_values):
            raise ValueError(
                "bronze_rows, rejected_rows, and duplicates_removed "
                "must be supplied together"
            )
        if (
            bronze_rows is None
            or rejected_rows is None
            or duplicates_removed is None
        ):
            raise ValueError("Reconciliation counts cannot be null")
        try:
            assert_reconciliation(
                bronze_rows,
                total_rows,
                rejected_rows,
                duplicates_removed,
            )
        except AssertionError as error:
            violations["reconciliation"] = str(error)

    if violations:
        raise AssertionError(
            f"Silver quality gate failed for {source_month}: {violations}"
        )
    LOGGER.info("Silver quality gate passed for %s (%s rows)", source_month, total_rows)


def _profile_rules(
    dataframe: DataFrame,
    thresholds: CleaningThresholds,
) -> tuple[dict[str, int], dict[str, dict[float, int]], dict[str, float]]:
    """Count active and sensitivity rules and summarize trip-duration percentiles."""
    conditions = dict(_rejection_conditions(thresholds))
    expressions = [
        F.sum(F.when(condition, 1).otherwise(0)).alias(rule_id)
        for rule_id, condition in conditions.items()
    ]
    passenger_rule = (F.col("passenger_count") == 0) | (
        F.col("passenger_count") > thresholds.max_passenger_count
    )
    expressions.append(
        F.sum(F.when(passenger_rule, 1).otherwise(0)).alias(
            "PASSENGER_COUNT_NULLIFIED"
        )
    )

    sensitivity_names: dict[str, tuple[str, float]] = {}
    for speed in thresholds.speed_sensitivity_mph:
        rule_id = f"SPEED_OVER_{speed:g}_MPH"
        sensitivity_names[rule_id] = ("speed", speed)
        expressions.append(
            F.sum(
                F.when(F.col("avg_speed_mph") > speed, 1).otherwise(0)
            ).alias(rule_id)
        )
    for distance in thresholds.distance_sensitivity_miles:
        rule_id = f"DISTANCE_OVER_{distance:g}_MILES"
        sensitivity_names[rule_id] = ("distance", distance)
        expressions.append(
            F.sum(F.when(F.col("trip_distance") > distance, 1).otherwise(0)).alias(
                rule_id
            )
        )

    percentiles = (0.50, 0.90, 0.95, 0.99, 0.999)
    percentile_array = ", ".join(str(value) for value in percentiles)
    expressions.append(
        F.expr(
            "percentile_approx("
            f"trip_duration_min, array({percentile_array}), 10000)"
        ).alias("duration_percentiles")
    )
    profile = dataframe.agg(*expressions).first().asDict()

    rule_counts = {
        rule_id: int(profile[rule_id] or 0) for rule_id in conditions
    }
    rule_counts["PASSENGER_COUNT_NULLIFIED"] = int(
        profile["PASSENGER_COUNT_NULLIFIED"] or 0
    )
    sensitivity: dict[str, dict[float, int]] = {"speed": {}, "distance": {}}
    for rule_id, (kind, threshold) in sensitivity_names.items():
        sensitivity[kind][threshold] = int(profile[rule_id] or 0)
    duration_values = profile["duration_percentiles"] or []
    duration_distribution = {
        f"p{percentile * 100:g}_minutes": float(value)
        for percentile, value in zip(percentiles, duration_values)
    }
    return rule_counts, sensitivity, duration_distribution


def build_quality_report(
    spark: SparkSession,
    source_month: str,
    rule_counts: Mapping[str, int],
    sensitivity_counts: Mapping[str, Mapping[float, int]],
    duration_percentiles: Mapping[str, float],
    distinct_rejected_rows: int,
    duplicates_removed: int,
) -> DataFrame:
    """Build one structured report row per rule, sensitivity, and percentile."""
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[tuple[Any, ...]] = []
    for rule_id, count in rule_counts.items():
        description = _RULE_DESCRIPTIONS.get(rule_id, rule_id)
        action = (
            "KEEP_AND_SET_NULL"
            if rule_id == "PASSENGER_COUNT_NULLIFIED"
            else "REJECT_TO_QUARANTINE"
        )
        rows.append(
            (run_ts, source_month, rule_id, description, count, action, None, None)
        )
    rows.extend(
        [
            (
                run_ts,
                source_month,
                "DISTINCT_REJECTED_ROWS",
                _RULE_DESCRIPTIONS["DISTINCT_REJECTED_ROWS"],
                distinct_rejected_rows,
                "QUARANTINE",
                None,
                None,
            ),
            (
                run_ts,
                source_month,
                "DUPLICATES_REMOVED",
                _RULE_DESCRIPTIONS["DUPLICATES_REMOVED"],
                duplicates_removed,
                "REMOVE_EXACT_DUPLICATES",
                None,
                None,
            ),
        ]
    )
    for kind, counts in sensitivity_counts.items():
        for threshold, count in sorted(counts.items()):
            unit = "mph" if kind == "speed" else "miles"
            rows.append(
                (
                    run_ts,
                    source_month,
                    f"{kind.upper()}_SENSITIVITY_{threshold:g}",
                    f"Rows over {threshold:g} {unit}.",
                    count,
                    "REPORT_ONLY",
                    None,
                    threshold,
                )
            )
    for metric, value in duration_percentiles.items():
        rows.append(
            (
                run_ts,
                source_month,
                f"DURATION_{metric.upper()}",
                f"Trip-duration distribution: {metric.replace('_', ' ')}.",
                None,
                "REPORT_ONLY",
                value,
                None,
            )
        )
    return spark.createDataFrame(rows, schema=_QUALITY_REPORT_SCHEMA)


def _write_delta_partition(
    dataframe: DataFrame, output_path: Path, source_month: str
) -> None:
    """Replace one source-month partition in a Delta table."""
    predicate = f"source_month = '{source_month}'"
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .partitionBy("source_month")
        .save(str(output_path))
    )


def _read_month(
    spark: SparkSession, path: Path, source_month: str
) -> DataFrame:
    """Read one month from a partitioned Delta table."""
    return spark.read.format("delta").load(str(path)).filter(
        F.col("source_month") == source_month
    )


def clean_month(
    spark: SparkSession,
    year: int,
    month: int,
    thresholds: CleaningThresholds = CLEANING_THRESHOLDS,
) -> dict[str, Any]:
    """Clean one Bronze partition, quarantine rejected rows, and verify Silver."""
    if year < 1 or not 1 <= month <= 12:
        raise ValueError(f"Invalid source month: year={year}, month={month}")
    source_month = f"{year:04d}-{month:02d}"
    bronze_path = BRONZE_DIR / "yellow_trips"
    if not bronze_path.is_dir():
        raise FileNotFoundError(f"Bronze trips table was not found: {bronze_path}")

    bronze = spark.read.parquet(str(bronze_path)).filter(
        F.col("source_month") == source_month
    )
    bronze_rows = bronze.count()
    if bronze_rows == 0:
        raise ValueError(f"Bronze partition {source_month} is empty or missing")
    LOGGER.info("Bronze rows for %s: %s", source_month, bronze_rows)

    standardized = standardize_columns(bronze)
    derived = add_derived_columns(standardized)
    rule_counts, sensitivity, duration_distribution = _profile_rules(
        derived, thresholds
    )
    flagged = flag_rejections(derived, thresholds)
    passenger_adjusted = apply_passenger_rule(flagged, thresholds)
    valid, rejected = split_valid_rejected(passenger_adjusted)
    rejected_rows = rejected.count()
    valid, duplicates_removed = drop_duplicates(valid)
    silver = valid.count()
    assert_reconciliation(
        bronze_rows,
        silver,
        rejected_rows,
        duplicates_removed,
    )

    zone_path = RAW_DIR / "taxi_zone_lookup.csv"
    if not zone_path.is_file():
        raise FileNotFoundError(f"Taxi zone lookup was not found: {zone_path}")
    zones = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(zone_path))
    )
    silver = enrich_with_zones(valid, zones)
    rejected = enrich_with_zones(rejected, zones)

    # Validate the candidate before replacing any existing month partition.
    assert_silver_quality(
        silver,
        source_month,
        bronze_rows=bronze_rows,
        rejected_rows=rejected_rows,
        duplicates_removed=duplicates_removed,
        thresholds=thresholds,
    )
    report = build_quality_report(
        spark,
        source_month,
        rule_counts,
        sensitivity,
        duration_distribution,
        rejected_rows,
        duplicates_removed,
    )

    _write_delta_partition(silver, SILVER_TRIPS, source_month)
    _write_delta_partition(rejected, SILVER_REJECTED, source_month)
    _write_delta_partition(report, QUALITY_REPORT, source_month)

    persisted_silver = _read_month(spark, SILVER_TRIPS, source_month)
    persisted_rejected = _read_month(spark, SILVER_REJECTED, source_month)
    persisted_silver_rows = persisted_silver.count()
    persisted_rejected_rows = persisted_rejected.count()
    assert_silver_quality(
        persisted_silver,
        source_month,
        bronze_rows=bronze_rows,
        rejected_rows=persisted_rejected_rows,
        duplicates_removed=duplicates_removed,
        thresholds=thresholds,
    )
    assert_reconciliation(
        bronze_rows,
        persisted_silver_rows,
        persisted_rejected_rows,
        duplicates_removed,
    )

    payment_zero_rows = persisted_silver.filter(F.col("payment_type") == 0).count()
    LOGGER.info("Silver rows for %s: %s", source_month, persisted_silver_rows)
    LOGGER.info("Rejected rows for %s: %s", source_month, persisted_rejected_rows)
    LOGGER.info("Duplicates removed: %s", duplicates_removed)
    LOGGER.info("Per-rule rows affected: %s", rule_counts)
    LOGGER.info("Distinct rejected rows: %s", persisted_rejected_rows)
    LOGGER.info("Threshold sensitivity counts: %s", sensitivity)
    LOGGER.info("Duration percentiles in minutes: %s", duration_distribution)
    LOGGER.info(
        "payment_type=0 rows retained in Silver: %s",
        payment_zero_rows,
    )
    sample_columns = [
        column
        for column in (
            "pickup_datetime",
            "dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "source_month",
            "reject_reasons",
        )
        if column in persisted_rejected.columns
    ]
    LOGGER.info(
        "Sample rejected rows: %s",
        [
            row.asDict(recursive=True)
            for row in persisted_rejected.select(*sample_columns).limit(10).collect()
        ],
    )
    history = (
        DeltaTable.forPath(spark, str(SILVER_TRIPS))
        .history(1)
        .select("version", "timestamp", "operation", "operationParameters")
        .collect()
    )
    LOGGER.info(
        "Latest Silver Delta history: %s",
        [row.asDict(recursive=True) for row in history],
    )
    return {
        "source_month": source_month,
        "bronze_rows": bronze_rows,
        "silver_rows": persisted_silver_rows,
        "rejected_rows": persisted_rejected_rows,
        "duplicates_removed": duplicates_removed,
        "rule_counts": rule_counts,
        "sensitivity_counts": sensitivity,
        "duration_percentiles": duration_distribution,
        "payment_type_zero_silver_rows": payment_zero_rows,
    }


def main() -> None:
    """Run monthly Bronze-to-Silver cleaning from the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True, help="Source file year")
    parser.add_argument("--month", type=int, required=True, help="Source file month")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    spark = get_spark(f"silver-cleaning-{args.year:04d}-{args.month:02d}")
    try:
        clean_month(spark, args.year, args.month)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
