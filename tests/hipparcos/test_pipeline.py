"""Tests for Hipparcos import pipeline."""

from pathlib import Path

import pandas as pd
from astropy.table import Table
from click.testing import CliRunner

from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.hipparcos.cli import cli
from foundinspace.pipeline.hipparcos.pipeline import _run_hipparcos_pipeline, main


def _sample_hip_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "HIP": 1,
                "RArad": 10.0,
                "DErad": 20.0,
                "Plx": 10.0,
                "e_Plx": 1.0,
                "pmRA": 0.0,
                "pmDE": 0.0,
                "Hpmag": 7.0,
                "B-V": 0.6,
            },
            {
                "HIP": 2,
                "RArad": 30.0,
                "DErad": -10.0,
                "Plx": 5.0,
                "e_Plx": 0.5,
                "pmRA": 1.0,
                "pmDE": -1.0,
                "Hpmag": 9.0,
                "B-V": 1.0,
            },
        ]
    )


def _write_ecsv(df: pd.DataFrame, path: Path) -> None:
    table = Table.from_pandas(df, index=False)
    table.write(path, format="ascii.ecsv", overwrite=True)


def test_run_pipeline_outputs_canonical_schema_and_source_fields():
    out = _run_hipparcos_pipeline(_sample_hip_df())
    assert list(out.columns) == OUTPUT_COLS
    assert (out["source"] == "hip").all()
    assert out["source_id"].dtype == "uint64"
    assert out["source_id"].tolist() == [1, 2]


def test_main_writes_parquet_with_limit(tmp_path: Path):
    input_file = tmp_path / "hip.ecsv"
    output_file = tmp_path / "hip.parquet"
    _write_ecsv(_sample_hip_df(), input_file)

    main(input_file, output_file, skip_if_exists=False, limit=1)

    assert output_file.exists()
    out = pd.read_parquet(output_file)
    assert len(out) == 1
    assert list(out.columns) == OUTPUT_COLS
    assert out["source"].iloc[0] == "hip"
    assert out["source_id"].iloc[0] == 1


def test_cli_prepare_writes_output(tmp_path: Path):
    input_file = tmp_path / "hip.ecsv"
    output_file = tmp_path / "out" / "hip_stars.parquet"
    _write_ecsv(_sample_hip_df(), input_file)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "prepare",
            "--input",
            str(input_file),
            "--output",
            str(output_file),
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    out = pd.read_parquet(output_file)
    assert len(out) == 1
    assert out["source"].iloc[0] == "hip"
