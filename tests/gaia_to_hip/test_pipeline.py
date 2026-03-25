"""Tests for Gaia↔HIP cross-match pipeline."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from astropy.table import Table

from foundinspace.pipeline.gaia_to_hip import pipeline as gh


def test_build_gaia_hip_mapping_from_dataframe_filters_and_normalizes():
    df = pd.DataFrame(
        {
            "source_id": [1, "2", 3, 4, 5],
            "original_ext_source_id": [10, np.nan, -1, 14.5, "7"],
        }
    )

    out = gh.build_gaia_hip_mapping_from_dataframe(df)

    assert out.to_dict(orient="records") == [
        {
            "gaia_source_id": 1,
            "hip_source_id": 10,
            "mapping_source": gh.MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR,
        },
        {
            "gaia_source_id": 5,
            "hip_source_id": 7,
            "mapping_source": gh.MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR,
        },
    ]
    assert list(out.columns) == gh.GAIA_HIP_MAP_COLS


def test_build_gaia_hip_mapping_missing_columns_empty():
    df = pd.DataFrame({"source_id": [1, 2, 3]})
    out = gh.build_gaia_hip_mapping_from_dataframe(df)
    assert out.empty
    assert list(out.columns) == gh.GAIA_HIP_MAP_COLS


def test_build_gaia_hip_mapping_deduplicates_and_sorts():
    df = pd.DataFrame(
        {
            "source_id": [5, 2, 2, 1],
            "original_ext_source_id": [50, 20, 20, 10],
        }
    )
    out = gh.build_gaia_hip_mapping_from_dataframe(df)
    assert out[["gaia_source_id", "hip_source_id"]].values.tolist() == [
        [1, 10],
        [2, 20],
        [5, 50],
    ]


def test_prepare_gaia_hip_mapping_writes_parquet(tmp_path: Path):
    ecsv = tmp_path / "cross.ecsv"
    out_parquet = tmp_path / "map.parquet"
    t = Table(
        rows=[[100, 42], [200, 99]],
        names=("source_id", "original_ext_source_id"),
    )
    t.write(ecsv, format="ascii.ecsv", overwrite=True)

    gh.prepare_gaia_hip_mapping(ecsv, out_parquet, overwrite=True)

    assert out_parquet.exists()
    read_back = pd.read_parquet(out_parquet)
    assert list(read_back.columns) == gh.GAIA_HIP_MAP_COLS
    assert read_back["gaia_source_id"].tolist() == [100, 200]
    assert read_back["hip_source_id"].tolist() == [42, 99]


def test_prepare_gaia_hip_mapping_raises_when_output_exists(tmp_path: Path):
    ecsv = tmp_path / "cross.ecsv"
    out_parquet = tmp_path / "map.parquet"
    out_parquet.write_text("x", encoding="utf-8")
    t = Table(rows=[[1, 1]], names=("source_id", "original_ext_source_id"))
    t.write(ecsv, format="ascii.ecsv", overwrite=True)

    with pytest.raises(FileExistsError):
        gh.prepare_gaia_hip_mapping(ecsv, out_parquet, overwrite=False)
