# Project journal

## Milestone 1.3 — Explore January 2024 yellow taxi data

- Read the raw Parquet and taxi zone lookup with local PySpark; no source files were
  modified.
- The Parquet contains 2,964,624 rows and 19 columns. Its schema has timestamp
  pickup/drop-off fields, integer location/vendor IDs, and numeric trip/payment
  measures.
- Exact duplicate full rows found: 0. All 265 distinct location IDs in the lookup
  matched both pickup and drop-off IDs in the trip data.
- 18 pickup timestamps fall outside January 2024: 15 before January 1 (including
  records dated 2002 and 2009) and 3 on February 1.
- 140,162 rows have `payment_type=0`; those same rows have null values for
  `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, and
  `Airport_fee`. No other field in the source had nulls.
- Other signals: 870 nonpositive-duration trips; 60,371 zero-distance trips and no
  negative distances; 37,448 negative fares; 35,504 negative total amounts.
- The largest recorded distance is 312,722.3, while the median and 99th-percentile
  distances are 1.68 and 20.0. There are 23 trips above 1,000 distance units, 20 of
  which have `payment_type=0`. Review extreme distances with related fare, duration,
  and payment fields rather than removing them using an arbitrary threshold.
- These are profiling results, not approved Silver filters. In particular,
  payment-code meanings, adjustment/refund rows, and out-of-month records still need
  to be checked against the TLC data dictionary and the project's reporting scope.

Re-run the profile from the repository root with:

```bash
.venv/bin/python notebooks/01_data_exploration.py
```

The script logs its findings and reads only `data/raw/`; it does not write or alter
the source data.
