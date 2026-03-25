"""Gaia-only pipeline: stream VOTable -> {input_stem}.parquet (batched, same dir as input).

Reads Gaia data via votpipe in 500k-row batches, runs Gaia-specific astrometry,
photometry, filter, coordinates, mag_abs, teff, log_g; appends each non-empty
batch to a single compressed Parquet file. No overrides; composition happens later.
"""

import gzip
import lzma
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from votpipe import parse_votable

from foundinspace.pipeline.common.coords import calculate_coordinates_fast
from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.gaia.astrometry import select_astrometry_gaia
from foundinspace.pipeline.gaia.photometry import (
    assign_photometry_gaia,
    compute_mag_abs_gaia,
    compute_teff_gaia,
)

BATCH_SIZE = 1_000_000
GAIA_HIP_MAP_COLS = ["gaia_source_id", "hip_source_id", "mapping_source"]
MAPPING_SOURCE_GAIA_QUERY_HIP = "gaia_query_hip"
HIP_CANDIDATE_COLS = ("hip", "hip_id", "hip_source_id", "hipparcos")


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


def _hip_column_name(df: pd.DataFrame) -> str | None:
    for col in HIP_CANDIDATE_COLS:
        if col in df.columns:
            return col
    return None


def extract_gaia_hip_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Extract Gaia↔HIP mapping rows from a raw Gaia batch DataFrame."""
    if "source_id" not in df.columns:
        return empty_gaia_hip_mapping()
    hip_col = _hip_column_name(df)
    if hip_col is None:
        return empty_gaia_hip_mapping()

    gaia_numeric, gaia_valid = _coerce_positive_integers(df["source_id"])
    hip_numeric, hip_valid = _coerce_positive_integers(df[hip_col])
    valid = gaia_valid & hip_valid
    if not valid.any():
        return empty_gaia_hip_mapping()

    out = pd.DataFrame(
        {
            "gaia_source_id": gaia_numeric[valid].astype("uint64"),
            "hip_source_id": hip_numeric[valid].astype("uint64"),
            "mapping_source": MAPPING_SOURCE_GAIA_QUERY_HIP,
        }
    )
    out = out.drop_duplicates(subset=["gaia_source_id", "hip_source_id"])
    return out.sort_values(
        by=["gaia_source_id", "hip_source_id"],
        kind="mergesort",
        ignore_index=True,
    )


def combine_gaia_hip_mappings(chunks: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine and normalize per-batch mapping chunks for one run."""
    if not chunks:
        return empty_gaia_hip_mapping()
    combined = pd.concat(chunks, ignore_index=True)
    combined = combined.drop_duplicates(subset=["gaia_source_id", "hip_source_id"])
    return combined.sort_values(
        by=["gaia_source_id", "hip_source_id"],
        kind="mergesort",
        ignore_index=True,
    )


def write_gaia_hip_mapping(mapping_df: pd.DataFrame, output_path: Path) -> None:
    """Write Gaia↔HIP mapping sidecar as Parquet with deterministic schema."""
    table = pa.Table.from_pandas(mapping_df[GAIA_HIP_MAP_COLS], preserve_index=False)
    pq.write_table(table, str(output_path), compression="zstd")


def _run_gaia_pipeline_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Run the Gaia pipeline on a single batch; returns DataFrame with _OUTPUT_COLS only."""
    df = df.copy()
    bp = df.get("phot_bp_mean_mag", pd.Series(np.nan, index=df.index)).astype(float)
    rp = df.get("phot_rp_mean_mag", pd.Series(np.nan, index=df.index)).astype(float)
    df["bp_rp"] = bp - rp

    work = select_astrometry_gaia(df)
    work = assign_photometry_gaia(work)
    # work = filter_positional_gaia(work)
    work = calculate_coordinates_fast(work)
    work = compute_mag_abs_gaia(work)
    work = compute_teff_gaia(work)
    # work = compute_log_g_gaia(work)
    work["source"] = "gaia"
    work["source_id"] = work["source_id"].astype("uint64")
    return work[OUTPUT_COLS]


def main(
    input_path: Path,
    output_path: Path,
    *,
    skip_if_exists: bool = True,
    limit: int | None = None,
) -> pd.DataFrame:
    """Stream input_path (VOTable), run pipeline per batch, write {input_stem}.parquet to same dir."""
    if skip_if_exists and output_path.exists():
        print(f"Skipping {input_path} (output exists: {output_path})")
        return empty_gaia_hip_mapping()

    name_lower = input_path.name.lower()
    if name_lower.endswith(".vot.gz"):
        open_fn = gzip.open
    elif name_lower.endswith(".vot.xz"):
        open_fn = lzma.open
    else:
        open_fn = open
    writer = None
    written_rows = 0
    batch_count = 0
    mapping_chunks: list[pd.DataFrame] = []
    extracted_mapping_rows = 0

    def on_batch(fields: list, rows: list) -> None:
        nonlocal writer, written_rows, batch_count, extracted_mapping_rows
        if limit is not None and written_rows >= limit:
            print(f"Reached limit of {limit:,} rows")
            return
        if not rows:
            return
        names = [f["name"] for f in fields]
        names_lower = [n.lower() for n in names]
        df = pd.DataFrame(rows, columns=names_lower)
        mapping = extract_gaia_hip_mapping(df)
        if not mapping.empty:
            mapping_chunks.append(mapping)
            extracted_mapping_rows += len(mapping)
        result = _run_gaia_pipeline_batch(df)
        if len(result) == 0:
            return
        table = pa.Table.from_pandas(result, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(
                str(output_path),
                table.schema,
                compression="zstd",
            )
        writer.write_table(table)
        written_rows += len(result)
        batch_count += 1
        print(f"  Wrote batch {batch_count:,}: {len(result):,} rows")

    print(f"Streaming {input_path} (batch_size={BATCH_SIZE:,})...")
    try:
        with open_fn(input_path, "rb") as f:
            parse_votable(f, on_batch, batch_size=BATCH_SIZE)
    finally:
        if writer is not None:
            writer.close()
    print(f"Done. Wrote {written_rows:,} rows to {output_path}")
    combined_mapping = combine_gaia_hip_mappings(mapping_chunks)
    print(
        "Mapping rows extracted:",
        f"{extracted_mapping_rows:,}; deduplicated:",
        f"{len(combined_mapping):,}",
    )
    return combined_mapping
