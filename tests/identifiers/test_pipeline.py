from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table
from click.testing import CliRunner

from foundinspace.pipeline.identifiers.cli import cli
from foundinspace.pipeline.identifiers.pipeline import (
    IDENTIFIER_OUTPUT_COLS,
    _bayer_code_to_display,
    prepare_identifiers_sidecar,
)


def _write_ecsv(df: pd.DataFrame, path: Path) -> None:
    table = Table.from_pandas(df, index=False)
    table.write(path, format="ascii.ecsv", overwrite=True)


def _write_crossmatch(path: Path, rows: list[tuple[int, int]]) -> None:
    df = pd.DataFrame(
        rows,
        columns=["gaia_source_id", "hip_source_id"],
    )
    df["mapping_source"] = "test"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), str(path), compression="zstd")


def test_bayer_code_display_normalizes_greek_and_suffixes():
    assert _bayer_code_to_display("alp", "Cas") == "alpha Cas"
    assert _bayer_code_to_display("kap01", "Dra") == "kappa1 Dra"
    assert _bayer_code_to_display("A.", "Ori") == "A Ori"
    assert _bayer_code_to_display("", "Ori") is None


def test_prepare_identifiers_sidecar_writes_compound_key_and_gaia_ids(tmp_path: Path):
    hip_hd_path = tmp_path / "hip_hd.ecsv"
    catalog_path = tmp_path / "iv27a_catalog.ecsv"
    proper_path = tmp_path / "iv27a_proper_names.ecsv"
    cross_path = tmp_path / "gaia_hip_map.parquet"
    output_path = tmp_path / "identifiers_map.parquet"

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
    _write_crossmatch(
        cross_path,
        [
            (100, 1),
            (300, 3),
            (400, 4),
        ],
    )

    out = prepare_identifiers_sidecar(
        hip_hd_path,
        catalog_path,
        proper_path,
        output_path,
        crossmatch_parquet=cross_path,
        overrides_data_dir=None,
        overwrite=False,
    )
    assert out == output_path
    sidecar = pd.read_parquet(output_path)

    assert list(sidecar.columns) == IDENTIFIER_OUTPUT_COLS
    assert sidecar.loc[sidecar["source_id"] == "1", "proper_name"].iloc[0] == "Sirius"
    assert int(sidecar.loc[sidecar["source_id"] == "1", "gaia_source_id"].iloc[0]) == 100
    assert sidecar.loc[sidecar["source_id"] == "3", "hd"].iloc[0] == 30
    assert sidecar.loc[sidecar["source_id"] == "3", "bayer"].iloc[0] == "alpha Cas"
    assert sidecar.loc[sidecar["source_id"] == "3", "proper_name"].iloc[0] == "Schedar"
    assert int(sidecar.loc[sidecar["source_id"] == "3", "gaia_source_id"].iloc[0]) == 300
    assert sidecar.loc[sidecar["source_id"] == "4", "bayer"].isna().iloc[0]
    assert sidecar.loc[sidecar["source_id"] == "4", "proper_name"].iloc[0] == "Deneb"
    assert int(sidecar.loc[sidecar["source_id"] == "4", "gaia_source_id"].iloc[0]) == 400


def test_prepare_identifiers_merges_override_yaml_identifiers(tmp_path: Path):
    hip_hd_path = tmp_path / "hip_hd.ecsv"
    catalog_path = tmp_path / "iv27a_catalog.ecsv"
    proper_path = tmp_path / "iv27a_proper_names.ecsv"
    output_path = tmp_path / "identifiers_map.parquet"
    ov_dir = tmp_path / "overrides_extra"
    ov_dir.mkdir()

    _write_ecsv(pd.DataFrame({"HIP": [1], "HD": [10]}), hip_hd_path)
    _write_ecsv(
        pd.DataFrame({"HIP": [1], "HD": [10], "Bayer": ["alp"], "Fl": [1], "Cst": ["Cas"]}),
        catalog_path,
    )
    _write_ecsv(pd.DataFrame({"HD": [10], "Name": ["Sirius"]}), proper_path)

    (ov_dir / "extra.yaml").write_text(
        "stars:\n"
        "  - source: manual\n"
        "    source_id: sun\n"
        "    identifiers:\n"
        "      proper_name: Sun\n",
        encoding="utf-8",
    )

    out = prepare_identifiers_sidecar(
        hip_hd_path,
        catalog_path,
        proper_path,
        output_path,
        crossmatch_parquet=None,
        overrides_data_dir=ov_dir,
        overwrite=False,
    )
    assert out == output_path
    sidecar = pd.read_parquet(output_path)
    sun = sidecar.loc[(sidecar["source"] == "manual") & (sidecar["source_id"] == "sun")]
    assert len(sun) == 1
    assert sun["proper_name"].iloc[0] == "Sun"
    assert len(sidecar[sidecar["source"] == "hip"]) >= 1


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
    df = pd.read_parquet(output_path)
    assert list(df.columns) == IDENTIFIER_OUTPUT_COLS
