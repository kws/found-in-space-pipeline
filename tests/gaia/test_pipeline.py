"""Tests for Gaia pipeline batching and mapping sidecar behavior."""

from pathlib import Path

import numpy as np
import pandas as pd
from click.testing import CliRunner

from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.gaia import pipeline as gaia_pipeline
from foundinspace.pipeline.gaia.cli import cli


def test_extract_gaia_hip_mapping_filters_and_normalizes():
    df = pd.DataFrame(
        {
            "source_id": [1, "2", 3, 4, 5],
            "hip": [10, np.nan, -1, 14.5, "7"],
        }
    )

    out = gaia_pipeline.extract_gaia_hip_mapping(df)

    assert out.to_dict(orient="records") == [
        {
            "gaia_source_id": 1,
            "hip_source_id": 10,
            "mapping_source": gaia_pipeline.MAPPING_SOURCE_GAIA_QUERY_HIP,
        },
        {
            "gaia_source_id": 5,
            "hip_source_id": 7,
            "mapping_source": gaia_pipeline.MAPPING_SOURCE_GAIA_QUERY_HIP,
        },
    ]
    assert list(out.columns) == gaia_pipeline.GAIA_HIP_MAP_COLS


def test_extract_gaia_hip_mapping_without_hip_column_is_empty():
    df = pd.DataFrame({"source_id": [1, 2, 3]})
    out = gaia_pipeline.extract_gaia_hip_mapping(df)
    assert out.empty
    assert list(out.columns) == gaia_pipeline.GAIA_HIP_MAP_COLS


def test_combine_gaia_hip_mappings_deduplicates_and_sorts():
    c1 = pd.DataFrame(
        {
            "gaia_source_id": [5, 2, 2],
            "hip_source_id": [50, 20, 20],
            "mapping_source": [gaia_pipeline.MAPPING_SOURCE_GAIA_QUERY_HIP] * 3,
        }
    )
    c2 = pd.DataFrame(
        {
            "gaia_source_id": [1, 5],
            "hip_source_id": [10, 50],
            "mapping_source": [gaia_pipeline.MAPPING_SOURCE_GAIA_QUERY_HIP] * 2,
        }
    )

    out = gaia_pipeline.combine_gaia_hip_mappings([c1, c2])
    assert out[["gaia_source_id", "hip_source_id"]].values.tolist() == [
        [1, 10],
        [2, 20],
        [5, 50],
    ]


def test_run_pipeline_batch_still_emits_output_cols(monkeypatch):
    def _select(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"source_id": [1, 2]})

    def _coords(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["x_icrs_pc"] = [0.0, 1.0]
        out["y_icrs_pc"] = [0.0, 1.0]
        out["z_icrs_pc"] = [0.0, 1.0]
        return out

    def _mag_abs(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["mag_abs"] = [1.0, 2.0]
        out["quality_flags"] = np.array([0, 0], dtype=np.uint16)
        out["astrometry_quality"] = [0.1, 0.2]
        out["photometry_quality"] = [0.3, 0.4]
        return out

    def _teff(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["teff"] = [5800.0, 6000.0]
        return out

    monkeypatch.setattr(gaia_pipeline, "select_astrometry_gaia", _select)
    monkeypatch.setattr(gaia_pipeline, "assign_photometry_gaia", lambda d: d)
    monkeypatch.setattr(gaia_pipeline, "calculate_coordinates_fast", _coords)
    monkeypatch.setattr(gaia_pipeline, "compute_mag_abs_gaia", _mag_abs)
    monkeypatch.setattr(gaia_pipeline, "compute_teff_gaia", _teff)

    out = gaia_pipeline._run_gaia_pipeline_batch(pd.DataFrame({"source_id": [1, 2]}))

    assert list(out.columns) == OUTPUT_COLS
    assert (out["source"] == "gaia").all()
    assert out["source_id"].dtype == "uint64"


def test_cli_import_writes_single_combined_mapping_file(tmp_path: Path, monkeypatch):
    input_a = tmp_path / "a.vot"
    input_b = tmp_path / "b.vot"
    input_a.write_text("x", encoding="utf-8")
    input_b.write_text("x", encoding="utf-8")
    mapping_out = tmp_path / "gaia_hip_map.parquet"

    def _fake_main(input_path: Path, output_path: Path, **kwargs) -> pd.DataFrame:
        if input_path.name == "a.vot":
            return pd.DataFrame(
                {
                    "gaia_source_id": [2, 1],
                    "hip_source_id": [20, 10],
                    "mapping_source": [gaia_pipeline.MAPPING_SOURCE_GAIA_QUERY_HIP] * 2,
                }
            )
        return pd.DataFrame(
            {
                "gaia_source_id": [1, 3],
                "hip_source_id": [10, 30],
                "mapping_source": [gaia_pipeline.MAPPING_SOURCE_GAIA_QUERY_HIP] * 2,
            }
        )

    monkeypatch.setattr("foundinspace.pipeline.gaia.cli.main", _fake_main)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "import",
            str(input_a),
            str(input_b),
            "--mapping-output",
            str(mapping_out),
        ],
    )

    assert result.exit_code == 0
    assert mapping_out.exists()
    mapping_df = pd.read_parquet(mapping_out)
    assert mapping_df[["gaia_source_id", "hip_source_id"]].values.tolist() == [
        [1, 10],
        [2, 20],
        [3, 30],
    ]


def test_cli_mapping_output_requires_force_to_overwrite(tmp_path: Path, monkeypatch):
    input_a = tmp_path / "a.vot"
    input_a.write_text("x", encoding="utf-8")
    mapping_out = tmp_path / "gaia_hip_map.parquet"
    mapping_out.write_text("preexisting", encoding="utf-8")

    monkeypatch.setattr(
        "foundinspace.pipeline.gaia.cli.main",
        lambda *args, **kwargs: gaia_pipeline.empty_gaia_hip_mapping(),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["import", str(input_a), "--mapping-output", str(mapping_out)],
    )
    assert result.exit_code == 0
    assert "Mapping output exists and --force not set" in result.output
    assert mapping_out.read_text(encoding="utf-8") == "preexisting"
