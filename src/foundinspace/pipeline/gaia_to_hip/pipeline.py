"""Build Gaia↔HIP mapping Parquet from `hipparcos2_best_neighbour` ECSV."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table

GAIA_HIP_MAP_COLS = ["gaia_source_id", "hip_source_id", "mapping_source"]
MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR = "hipparcos2_best_neighbour"


def empty_gaia_hip_mapping() -> pd.DataFrame:
    """Return an empty Gaia↔HIP mapping frame with stable dtypes."""
    return pd.DataFrame(
        {
            "gaia_source_id": pd.Series(dtype="uint64"),
            "hip_source_id": pd.Series(dtype="uint64"),
            "mapping_source": pd.Series(dtype="object"),
        }
    )


def _coerce_positive_integers(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Coerce a series to positive integers and return values plus validity mask."""
    numeric = pd.to_numeric(series, errors="coerce")
    finite = np.isfinite(numeric.to_numpy(dtype=float, copy=False))
    integral = np.equal(
        np.floor(numeric.to_numpy(dtype=float, copy=False)),
        numeric.to_numpy(dtype=float, copy=False),
    )
    positive = numeric > 0
    valid = finite & integral & positive.to_numpy(dtype=bool, copy=False)
    return numeric, pd.Series(valid, index=series.index)


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names for robust ECSV reads."""
    out = df.copy()
    out.columns = [str(c).lower() for c in out.columns]
    return out


def build_gaia_hip_mapping_from_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Map `source_id` / `original_ext_source_id` rows to the sidecar schema."""
    df = _normalize_column_names(df)
    if "source_id" not in df.columns or "original_ext_source_id" not in df.columns:
        return empty_gaia_hip_mapping()

    gaia_numeric, gaia_valid = _coerce_positive_integers(df["source_id"])
    hip_numeric, hip_valid = _coerce_positive_integers(df["original_ext_source_id"])
    valid = gaia_valid & hip_valid
    if not valid.any():
        return empty_gaia_hip_mapping()

    out = pd.DataFrame(
        {
            "gaia_source_id": gaia_numeric[valid].astype("uint64"),
            "hip_source_id": hip_numeric[valid].astype("uint64"),
            "mapping_source": MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR,
        }
    )
    out = out.drop_duplicates(subset=["gaia_source_id", "hip_source_id"])
    return out.sort_values(
        by=["gaia_source_id", "hip_source_id"],
        kind="mergesort",
        ignore_index=True,
    )


def write_gaia_hip_mapping(mapping_df: pd.DataFrame, output_path: Path) -> None:
    """Write Gaia↔HIP mapping sidecar as Parquet with deterministic schema."""
    table = pa.Table.from_pandas(mapping_df[GAIA_HIP_MAP_COLS], preserve_index=False)
    pq.write_table(table, str(output_path), compression="zstd")


def prepare_gaia_hip_mapping(
    input_ecsv: Path,
    output_parquet: Path,
    *,
    overwrite: bool = False,
) -> Path:
    """Read cross-match ECSV and write `gaia_hip_map.parquet`."""
    input_ecsv = Path(input_ecsv).expanduser()
    output_parquet = Path(output_parquet).expanduser()

    if not input_ecsv.is_file():
        raise FileNotFoundError(str(input_ecsv))
    if output_parquet.exists() and not overwrite:
        raise FileExistsError(str(output_parquet))

    table = Table.read(input_ecsv, format="ascii.ecsv")
    df = table.to_pandas()
    mapping = build_gaia_hip_mapping_from_dataframe(df)

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    write_gaia_hip_mapping(mapping, output_parquet)
    return output_parquet
