"""CLI tests for gaia-to-hip."""

from pathlib import Path

import pandas as pd
from astropy.table import Table
from click.testing import CliRunner

from foundinspace.pipeline.gaia_to_hip.cli import cli


def test_prepare_command(tmp_path: Path):
    ecsv = tmp_path / "cross.ecsv"
    out_parquet = tmp_path / "gaia_hip_map.parquet"
    t = Table(rows=[[10, 5]], names=("source_id", "original_ext_source_id"))
    t.write(ecsv, format="ascii.ecsv", overwrite=True)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "prepare",
            "--input",
            str(ecsv),
            "--output",
            str(out_parquet),
        ],
    )

    assert result.exit_code == 0
    assert out_parquet.exists()
    df = pd.read_parquet(out_parquet)
    assert df["gaia_source_id"].iloc[0] == 10
    assert df["hip_source_id"].iloc[0] == 5


def test_build_command_monkeypatched_download(tmp_path: Path, monkeypatch):
    ecsv = tmp_path / "cross.ecsv"
    out_parquet = tmp_path / "gaia_hip_map.parquet"
    t = Table(rows=[[7, 3]], names=("source_id", "original_ext_source_id"))
    t.write(ecsv, format="ascii.ecsv", overwrite=True)

    def _fake_ensure(path, *, force=False):
        return Path(path)

    monkeypatch.setattr(
        "foundinspace.pipeline.gaia_to_hip.cli.download.ensure_hipparcos2_best_neighbour_ecsv",
        _fake_ensure,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "build",
            "--download-output",
            str(ecsv),
            "--output",
            str(out_parquet),
        ],
    )

    assert result.exit_code == 0
    df = pd.read_parquet(out_parquet)
    assert df["gaia_source_id"].iloc[0] == 7
    assert df["hip_source_id"].iloc[0] == 3
