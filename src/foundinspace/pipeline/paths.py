"""Filesystem path defaults for catalog downloads and processed outputs.

Environment variables:
- FIS_CATALOGS_DIR (default: data/catalogs)
- FIS_PROCESSED_DIR (default: data/processed)
"""

from __future__ import annotations

from pathlib import Path

from decouple import config

CATALOGS_DIR = Path(config("FIS_CATALOGS_DIR", default="data/catalogs")).expanduser()
PROCESSED_DIR = Path(config("FIS_PROCESSED_DIR", default="data/processed")).expanduser()

# Source-aware processed defaults.
PROCESSED_GAIA_DIR = PROCESSED_DIR / "gaia"
GAIA_HIP_MAP_OUTPUT = PROCESSED_DIR / "gaia_hip_map.parquet"
HIP_STARS_OUTPUT = PROCESSED_DIR / "hip_stars.parquet"
IDENTIFIERS_MAP_OUTPUT = PROCESSED_DIR / "identifiers_map.parquet"
OVERRIDES_OUTPUT = PROCESSED_DIR / "overrides.parquet"
