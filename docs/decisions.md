# Decisions

## 2026-11-08 — Preserve source data and defer cleaning thresholds

- Keep the downloaded Parquet and lookup file unchanged as raw source data. The
  exploration is read-only; transformation and removal rules belong in later
  pipeline stages.
- Do not infer that a zero distance, negative fare/total, unusual duration, or
  out-of-month pickup is invalid from this profile alone. Preserve the signals and
  verify their meaning against the TLC data dictionary before defining Silver rules.
- Do not impute the five null fields in rows with `payment_type=0` yet. Their
  co-occurrence is a data pattern, not enough evidence to invent replacement values.
- Define whether monthly analytics are scoped by source-file month or pickup
  timestamp before filtering these 18 boundary/outlier-date rows.
- Exact full-row duplicate count is zero for this input. No business-key
  deduplication rule is selected by this result.

## 2026-11-08 — Bronze ingestion preserves source records

- Derive `source_month` from the ingested source file name/year and month, not trip
  pickup timestamps. Boundary and anomalous timestamp rows therefore remain in the
  source file's monthly partition.
- Partition Bronze by `source_month` so a monthly ingestion can replace one month
  without rewriting other months.
- Keep all source column names, data types, and values unchanged in Bronze. Add only
  `ingestion_ts`, `source_file`, and `source_month` as technical traceability fields.
- Use Spark dynamic partition overwrite with overwrite mode for idempotent reruns:
  the target month is replaced rather than appended, and other month partitions are
  left intact.
- Compare incoming schemas with the January exploration baseline and log
  differences; do not fail ingestion solely because the TLC source schema changes.
- Keep cleaning and quality decisions in Silver; Bronze is a traceable copy of the
  raw rows, not a validation or repair layer.

## 2026-11-08 — Silver cleaning and quarantine decisions (D1–D9)

The TLC Yellow Taxi data dictionary in
`data/raw/data_dictionary_trip_records_yellow.pdf` (dated 18 March 2025) was
checked before payment codes were labeled. It defines payment code 0 as Flex Fare,
1 as Credit card, 2 as Cash, 3 as No charge, 4 as Dispute, 5 as Unknown, and 6 as
Voided trip. The January 2024 file contains codes 0 through 4.

1. **D1 — Source-month boundary:** Compare pickup year-month with `source_month`,
   which comes from the source file. Route mismatches to the rejected table.
2. **D2 — Flex Fare and nulls:** Keep `payment_type=0` rows unless another explicit
   rejection rule applies. Preserve the source nulls without imputation and label
   code 0 as Flex Fare from the data dictionary.
3. **D3 — Negative amounts:** Quarantine a row when `fare_amount < 0` or
   `total_amount < 0`, with reason `NEGATIVE_AMOUNT`.
4. **D4 — Zero distance and duration:** Quarantine `trip_distance <= 0` as
   `ZERO_DISTANCE`, and `dropoff_datetime <= pickup_datetime` as
   `NON_POSITIVE_DURATION`.
5. **D5 — Implausible speed/distance:** Quarantine average speed above 100 mph as
   `IMPLAUSIBLE_SPEED`, or distance above 500 miles as `IMPLAUSIBLE_DISTANCE`.
   Thresholds are centralized in `CleaningThresholds`. Report sensitivity counts
   for speeds above 80/100/120 mph and distances above 100/200/500 miles.
6. **D6 — Long duration:** Quarantine trips over 1,440 minutes with
   `LONG_DURATION`. This remains provisional; the recorded percentiles are available
   for review before changing the limit.
7. **D7 — Passenger count:** Keep a trip but set `passenger_count` to null when the
   value is zero or greater than six. Do not impute existing nulls.
8. **D8 — Multiple reasons and reconciliation:** Store every applicable reason in
   `reject_reasons` as `array<string>`. Report per-rule counts (which may overlap)
   and distinct rejected rows. Assert
   `bronze_rows = silver_rows + rejected_rows + duplicates_removed`.
9. **D9 — Delta storage and reruns:** Store Silver, rejected rows, and the quality
   report as Delta tables partitioned by `source_month`; use a month-constrained
   `replaceWhere` overwrite for reruns.

Delta Lake 3.3.2 was selected for PySpark 3.5.3 using the official
[Delta/Spark compatibility table](https://docs.delta.io/releases/#compatibility-with-apache-spark-and-unity-catalog).
Schema meanings and payment labels were checked against the TLC PDF, not inferred
from observed distributions.

## 2026-11-10 — Gold aggregation conventions

- Build all six Gold outputs from valid Silver rows; keep calculations in Spark
  and preserve one row per catalog grain.
- Retain unmatched pickup zones as `Unknown` instead of dropping trip volume.
- Preserve Spark's weekday numbering in Gold: 1 = Sunday and 7 = Saturday.
  Peak-hour averages divide the slot's trips by distinct operating dates for
  that weekday, and dense ranking preserves ties.
- Distance buckets are lower-bound inclusive and upper-bound exclusive:
  `0-1`, `1-3`, `3-5`, `5-10`, `10-20`, and `20+`. Values exactly at 1, 3, 5,
  10, and 20 enter the bucket beginning at that boundary.
- `total_amount` is the total charged to the passenger as reported by TLC;
  cash tips are not included. Average speed is computed per trip and then
  averaged within each distance bucket; trips without valid duration do not
  contribute to speed.
- Validate table schema and grain, exact trip reconciliation, revenue
  reconciliation within floating-point tolerance, payment shares, and dense
  peak ranks before considering a full Gold build complete.
