"""Project data paths, configurable through the environment."""

import os
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
