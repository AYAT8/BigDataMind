"""Project data paths, configurable through the environment."""

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_configured_data_root = os.getenv("BDM_DATA_ROOT") or str(PROJECT_ROOT / "data")
DATA_ROOT = Path(_configured_data_root).expanduser()
if not DATA_ROOT.is_absolute():
    DATA_ROOT = PROJECT_ROOT / DATA_ROOT

RAW_DIR = DATA_ROOT / "raw"
BRONZE_DIR = DATA_ROOT / "bronze"
SILVER_DIR = DATA_ROOT / "silver"
GOLD_DIR = DATA_ROOT / "gold"
SILVER_TRIPS = SILVER_DIR / "yellow_trips"
SILVER_REJECTED = SILVER_DIR / "yellow_trips_rejected"
QUALITY_REPORT = SILVER_DIR / "quality_report"


@dataclass(frozen=True)
class CleaningThresholds:
    """Silver-layer limits, centralized for review and sensitivity analysis."""

    max_avg_speed_mph: float = 100.0
    max_trip_distance_miles: float = 500.0
    max_trip_duration_minutes: float = 1440.0
    max_passenger_count: int = 6
    speed_sensitivity_mph: tuple[float, ...] = (80.0, 100.0, 120.0)
    distance_sensitivity_miles: tuple[float, ...] = (100.0, 200.0, 500.0)


CLEANING_THRESHOLDS = CleaningThresholds()
