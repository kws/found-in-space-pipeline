from pathlib import Path

import pandas as pd
from astropy.table import Table
from click.testing import CliRunner

from foundinspace.pipeline.identifiers.cli import cli
from foundinspace.pipeline.identifiers.pipeline import (
    _bayer_code_to_display,
    prepare_identifiers_sidecar,
)


def _write_ecsv(df: pd.DataFrame, path: Path) -> None:
    table = Table.from_pandas(df, index=False)
    table.write(path, format="ascii.ecsv", overwrite=True)


def test_bayer_code_display_normalizes_greek_and_suffixes():
    assert _bayer_code_to_display("alp", "Cas") == "alpha Cas"
    assert _bayer_code_to_display("kap01", "Dra") == "kappa1 Dra"
    assert _bayer_code_to_display("A.", "Ori") == "A Ori"
    assert _bayer_code_to_display("", "Ori") is None


def test_prepare_identifiers_sidecar_writes_wide_rows(tmp_path: Path):
    hip_hd_path = tmp_path / "hip_hd.ecsv"
    catalog_path = tmp_path / "iv27a_catalog.ecsv"
    proper_path = tmp_path / "iv27a_proper_names.ecsv"
    output_path = tmp_path / "star_identifiers.parquet"

    _write_ecsv(
        pd.DataFrame(
            {
                "HIP": [1, 2],
                "HD": [10, 20],
            }
        ),
        hip_hd_path,
    )
    _write_ecsv(
        pd.DataFrame(
            {
                "HIP": [1, 3, 3, 4],
                "HD": [10, 30, 31, 40],
                "Bayer": ["", "alp", "bet", ""],
                "Fl": [pd.NA, 1, 2, 61],
                "Cst": ["", "Cas", "Cas", "Cyg"],
            }
        ),
        catalog_path,
    )
    _write_ecsv(
        pd.DataFrame(
            {
                "HD": [10, 10, 30, 40],
                "Name": ["Sirius", "Ignored Duplicate", "Schedar", "Deneb"],
            }
        ),
        proper_path,
    )

    out = prepare_identifiers_sidecar(
        hip_hd_path,
        catalog_path,
        proper_path,
        output_path,
        overwrite=False,
    )
    assert out == output_path
    sidecar = pd.read_parquet(output_path)

    assert sidecar["hip_source_id"].tolist() == [1, 3, 4]
    assert sidecar.loc[sidecar["hip_source_id"] == 1, "proper_name"].iloc[0] == "Sirius"
    assert sidecar.loc[sidecar["hip_source_id"] == 3, "hd"].iloc[0] == 30
    assert sidecar.loc[sidecar["hip_source_id"] == 3, "bayer"].iloc[0] == "alpha Cas"
    assert sidecar.loc[sidecar["hip_source_id"] == 3, "proper_name"].iloc[0] == "Schedar"
    assert sidecar.loc[sidecar["hip_source_id"] == 4, "bayer"].isna().iloc[0]
    assert sidecar.loc[sidecar["hip_source_id"] == 4, "proper_name"].iloc[0] == "Deneb"


def test_cli_prepare_writes_sidecar(tmp_path: Path):
    hip_hd_path = tmp_path / "hip_hd.ecsv"
    catalog_path = tmp_path / "iv27a_catalog.ecsv"
    proper_path = tmp_path / "iv27a_proper_names.ecsv"
    output_path = tmp_path / "star_identifiers.parquet"
    _write_ecsv(pd.DataFrame({"HIP": [1], "HD": [10]}), hip_hd_path)
    _write_ecsv(
        pd.DataFrame({"HIP": [1], "HD": [10], "Bayer": ["alp"], "Fl": [1], "Cst": ["Cas"]}),
        catalog_path,
    )
    _write_ecsv(pd.DataFrame({"HD": [10], "Name": ["Sirius"]}), proper_path)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "prepare",
            "--hip-hd",
            str(hip_hd_path),
            "--catalog",
            str(catalog_path),
            "--proper-names",
            str(proper_path),
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
