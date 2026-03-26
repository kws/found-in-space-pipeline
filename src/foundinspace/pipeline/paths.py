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

# Gaia↔Hipparcos cross-match raw download (Gaia archive TAP).
GAIA_HIP_BEST_NEIGHBOUR_ECSV = CATALOGS_DIR / "gaia_hipparcos2_best_neighbour.ecsv"
HIPPARCOS2_ECSV = CATALOGS_DIR / "hipparcos2.ecsv"

# Source-aware processed defaults.
PROCESSED_GAIA_DIR = PROCESSED_DIR / "gaia"
GAIA_HIP_MAP_OUTPUT = PROCESSED_DIR / "gaia_hip_map.parquet"
HIP_STARS_OUTPUT = PROCESSED_DIR / "hip_stars.parquet"
IDENTIFIERS_MAP_OUTPUT = PROCESSED_DIR / "identifiers_map.parquet"
OVERRIDES_OUTPUT = PROCESSED_DIR / "overrides.parquet"
MERGED_OUTPUT_DIR = PROCESSED_DIR / "merged"
