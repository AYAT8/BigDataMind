# Silver and quality-report schemas

Silver is the standardized, enriched table at `data/silver/yellow_trips`,
partitioned by `source_month`. It contains valid rows only. Rows rejected by
cleaning rules are preserved in `data/silver/yellow_trips_rejected` with their
`reject_reasons` array. The rejected table has the same columns as Silver.

Bronze source columns are renamed and explicitly cast in Silver; their source
values are otherwise preserved unless a rule below explicitly changes them.
Silver adds derived analytics fields, payment-code labels, zone descriptions,
and rejection metadata.

## Trip columns

| Column | Spark type | Meaning |
|---|---|---|
| `vendor_id` | integer | TLC code identifying the trip-record provider. |
| `pickup_datetime` | timestamp | Time when the taximeter was engaged. |
| `dropoff_datetime` | timestamp | Time when the taximeter was disengaged. |
| `passenger_count` | integer | Reported passenger count; 0 or greater than 6 is changed to null. Existing nulls remain null. |
| `trip_distance` | double | Meter-reported trip distance in miles. |
| `ratecode_id` | integer | Rate code at trip end; TLC code 99 denotes null/unknown. |
| `store_and_fwd_flag` | string | `Y` when the record was stored in vehicle memory before transmission; `N` otherwise. Nulls remain null. |
| `pu_location_id` | integer | TLC taxi-zone ID where the meter was engaged. |
| `do_location_id` | integer | TLC taxi-zone ID where the meter was disengaged. |
| `payment_type` | integer | TLC payment code; code 0 is Flex Fare. |
| `fare_amount` | double | Time-and-distance meter fare. Negative values are quarantined. |
| `extra` | double | Miscellaneous extras and surcharges. |
| `mta_tax` | double | Automatically triggered MTA tax for the metered rate. |
| `tip_amount` | double | Reported tip; cash tips are not included. |
| `tolls_amount` | double | Total tolls paid during the trip. |
| `improvement_surcharge` | double | Improvement surcharge assessed at flag drop. |
| `total_amount` | double | Total charged to passengers, excluding cash tips. Negative values are quarantined. |
| `congestion_surcharge` | double | Collected New York State congestion surcharge. Existing nulls remain null. |
| `airport_fee` | double | Airport fee for pickup at LaGuardia or JFK. Existing nulls remain null. |
| `ingestion_ts` | timestamp | Timestamp recorded by Bronze when the monthly file was ingested. |
| `source_file` | string | Name of the raw monthly Parquet file recorded by Bronze. |
| `source_month` | string | `YYYY-MM` month derived from the source file; partition column in Bronze and Silver. |
| `trip_duration_min` | double | Drop-off minus pickup timestamp, expressed in minutes. |
| `avg_speed_mph` | double | `trip_distance / duration_hours`; null when duration is not positive. |
| `pickup_date` | date | Calendar date derived from `pickup_datetime`. |
| `pickup_hour` | integer | Hour of day derived from `pickup_datetime`, in the range 0–23. |
| `pickup_dow` | integer | Spark day-of-week value derived from `pickup_datetime`: 1 is Sunday and 7 is Saturday. |
| `pickup_month` | integer | Calendar month number derived from `pickup_datetime`, in the range 1–12. |
| `payment_type_label` | string | Human-readable TLC payment code label from the data dictionary; code 0 is `Flex Fare`. |
| `reject_reasons` | array\<string\> | Empty array in Silver; all applicable rule identifiers in rejected rows. |
| `pu_zone` | string | Pickup zone name from the TLC taxi-zone lookup, or `Unknown` if unmatched. |
| `pu_borough` | string | Pickup borough from the TLC taxi-zone lookup, or `Unknown` if unmatched. |
| `do_zone` | string | Drop-off zone name from the TLC taxi-zone lookup, or `Unknown` if unmatched. |
| `do_borough` | string | Drop-off borough from the TLC taxi-zone lookup, or `Unknown` if unmatched. |

The payment code descriptions and source field meanings were checked against
`data/raw/data_dictionary_trip_records_yellow.pdf` (TLC dictionary dated
18 March 2025). Its payment codes are 0 Flex Fare, 1 Credit card, 2 Cash,
3 No charge, 4 Dispute, 5 Unknown, and 6 Voided trip. The January 2024 input
contains codes 0 through 4.

## Rejection reasons

| Reason | Condition | Action |
|---|---|---|
| `OUT_OF_SOURCE_MONTH` | Pickup year-month does not equal `source_month`. | Quarantine. |
| `NEGATIVE_AMOUNT` | `fare_amount < 0` or `total_amount < 0`. | Quarantine. |
| `ZERO_DISTANCE` | `trip_distance <= 0`. | Quarantine. |
| `NON_POSITIVE_DURATION` | `dropoff_datetime <= pickup_datetime`. | Quarantine. |
| `IMPLAUSIBLE_SPEED` | `avg_speed_mph > 100`. | Quarantine. |
| `IMPLAUSIBLE_DISTANCE` | `trip_distance > 500` miles. | Quarantine. |
| `LONG_DURATION` | `trip_duration_min > 1,440`. | Quarantine; provisional threshold. |

A record can carry several reasons. Per-rule affected counts overlap; use the
distinct rejected-row count when reconciling records. A passenger-count rule is
not a rejection reason: counts of zero or greater than six are set to null and
the trip remains eligible for Silver.

## Quality-report columns

`data/silver/quality_report` is a Delta table partitioned by `source_month`.
Each run replaces that month's report partition.

| Column | Spark type | Meaning |
|---|---|---|
| `run_ts` | timestamp | UTC timestamp recorded for the cleaning run. |
| `source_month` | string | Month of the source file and report partition. |
| `rule_id` | string | Rule, summary, sensitivity threshold, or duration percentile identifier. |
| `description` | string | Human-readable explanation of the metric. |
| `rows_affected` | long | Number of rows matching a rule or threshold; null for percentile values. |
| `action` | string | Action applied or `REPORT_ONLY`. |
| `metric_value` | double | Duration percentile in minutes; null for row-count metrics. |
| `threshold_value` | double | Threshold used for sensitivity reporting; null when not applicable. |

## Storage and reruns

Silver, rejected, and quality-report Delta writes use `replaceWhere` constrained
to the source-month partition. Re-running a month replaces that month's output
rather than appending duplicates or changing other months. Bronze and raw inputs
are read-only during this milestone.
