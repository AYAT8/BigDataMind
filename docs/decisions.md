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
