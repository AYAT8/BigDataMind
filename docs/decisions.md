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
