from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.merge.pipeline import run_merge


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), str(path), compression="zstd")


def test_run_merge_streaming_with_overrides_and_missing_partners(tmp_path: Path):
    gaia_dir = tmp_path / "gaia"
    hip_path = tmp_path / "hip_stars.parquet"
    crossmatch_path = tmp_path / "gaia_hip_map.parquet"
    overrides_path = tmp_path / "overrides.parquet"
    output_dir = tmp_path / "merged"

    gaia_df = pd.DataFrame(
        [
            {
                "source": "gaia",
                "source_id": 1001,
                "x_icrs_pc": 1.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 1.0,
                "teff": 5000.0,
                "quality_flags": 1,
                "astrometry_quality": 0.1,
                "photometry_quality": 0.1,
            },
            {
                "source": "gaia",
                "source_id": 1002,
                "x_icrs_pc": 0.0,
                "y_icrs_pc": 1.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 2.0,
                "teff": 5100.0,
                "quality_flags": 1,
                "astrometry_quality": 0.2,
                "photometry_quality": 0.1,
            },
            {
                "source": "gaia",
                "source_id": 1004,
                "x_icrs_pc": 0.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": 1.0,
                "mag_abs": 3.0,
                "teff": 5200.0,
                "quality_flags": 1,
                "astrometry_quality": 0.4,
                "photometry_quality": 0.1,
            },
            {
                "source": "gaia",
                "source_id": 1010,
                "x_icrs_pc": -1.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 4.0,
                "teff": 5300.0,
                "quality_flags": 1,
                "astrometry_quality": 0.2,
                "photometry_quality": 0.1,
            },
        ]
    )
    _write_parquet(gaia_df[OUTPUT_COLS], gaia_dir / "b1.parquet")

    hip_df = pd.DataFrame(
        [
            {
                "source": "hip",
                "source_id": 2001,
                "x_icrs_pc": 1.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 1.5,
                "teff": 6000.0,
                "quality_flags": 2,
                "astrometry_quality": 0.3,
                "photometry_quality": 0.2,
            },
            {
                "source": "hip",
                "source_id": 2002,
                "x_icrs_pc": 0.0,
                "y_icrs_pc": 1.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 2.5,
                "teff": 6100.0,
                "quality_flags": 2,
                "astrometry_quality": 0.2,
                "photometry_quality": 0.2,
            },
            {
                "source": "hip",
                "source_id": 2003,
                "x_icrs_pc": 0.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": -1.0,
                "mag_abs": 3.5,
                "teff": 6200.0,
                "quality_flags": 2,
                "astrometry_quality": 0.5,
                "photometry_quality": 0.2,
            },
            {
                "source": "hip",
                "source_id": 2004,
                "x_icrs_pc": 1.0,
                "y_icrs_pc": 1.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 4.5,
                "teff": 6300.0,
                "quality_flags": 2,
                "astrometry_quality": 0.1,
                "photometry_quality": 0.2,
            },
            {
                "source": "hip",
                "source_id": 2010,
                "x_icrs_pc": 0.5,
                "y_icrs_pc": -0.5,
                "z_icrs_pc": 0.0,
                "mag_abs": 5.0,
                "teff": 6400.0,
                "quality_flags": 2,
                "astrometry_quality": 0.2,
                "photometry_quality": 0.2,
            },
        ]
    )
    _write_parquet(hip_df[OUTPUT_COLS], hip_path)

    crossmatch_df = pd.DataFrame(
        [
            {"gaia_source_id": 1001, "hip_source_id": 2001, "mapping_source": "test"},
            {"gaia_source_id": 1002, "hip_source_id": 2002, "mapping_source": "test"},
            {"gaia_source_id": 1003, "hip_source_id": 2003, "mapping_source": "test"},
            {"gaia_source_id": 1004, "hip_source_id": 2004, "mapping_source": "test"},
            {"gaia_source_id": 1005, "hip_source_id": 2005, "mapping_source": "test"},
        ]
    )
    _write_parquet(crossmatch_df, crossmatch_path)

    overrides_df = pd.DataFrame(
        [
            {
                "source": "hip",
                "source_id": "2004",
                "x_icrs_pc": 2.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 10.0,
                "teff": 7000.0,
                "quality_flags": 10,
                "astrometry_quality": 0.0,
                "photometry_quality": 0.0,
                "override_id": "ov.hip.replace",
                "action": "replace",
                "override_reason": "test_replace_hip",
                "override_policy_version": "v1",
            },
            {
                "source": "gaia",
                "source_id": "1003",
                "x_icrs_pc": 0.0,
                "y_icrs_pc": 2.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 11.0,
                "teff": 7100.0,
                "quality_flags": 10,
                "astrometry_quality": 0.0,
                "photometry_quality": 0.0,
                "override_id": "ov.gaia.replace_missing_target",
                "action": "replace",
                "override_reason": "test_replace_gaia_missing_target",
                "override_policy_version": "v1",
            },
            {
                "source": "gaia",
                "source_id": "1005",
                "x_icrs_pc": pd.NA,
                "y_icrs_pc": pd.NA,
                "z_icrs_pc": pd.NA,
                "mag_abs": pd.NA,
                "teff": pd.NA,
                "quality_flags": pd.NA,
                "astrometry_quality": pd.NA,
                "photometry_quality": pd.NA,
                "override_id": "ov.gaia.drop_no_effect",
                "action": "drop",
                "override_reason": "test_drop_no_effect",
                "override_policy_version": "v1",
            },
            {
                "source": "manual",
                "source_id": "sun",
                "x_icrs_pc": 0.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 4.83,
                "teff": 5772.0,
                "quality_flags": 10,
                "astrometry_quality": 0.0,
                "photometry_quality": 0.0,
                "override_id": "ov.manual.add_sun",
                "action": "add",
                "override_reason": "test_add",
                "override_policy_version": "v1",
            },
        ]
    )
    _write_parquet(overrides_df, overrides_path)

    report = run_merge(
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        crossmatch_path=crossmatch_path,
        overrides_path=overrides_path,
        output_dir=output_dir,
        healpix_order=1,
        force=True,
    )

    merged_files = sorted((output_dir / "healpix").glob("*/*.parquet"))
    assert merged_files
    merged_df = pd.concat([pd.read_parquet(p) for p in merged_files], ignore_index=True)

    assert len(merged_df) == 7
    assert merged_df["source_id"].is_unique
    assert set(merged_df["source_id"].astype(str)) == {
        "1001",
        "1002",
        "1003",
        "1010",
        "2004",
        "2010",
        "sun",
    }

    manual_rows = merged_df[merged_df["catalog_source"] == "manual"]
    assert set(manual_rows["source_id"].astype(str)) == {"1003", "2004", "sun"}

    assert report.matched_pairs_scored == 2
    assert report.matched_winner_gaia == 2
    assert report.matched_winner_hip == 0
    assert report.unmatched_gaia == 1
    assert report.unmatched_hip == 1
    assert report.override_replace_applied == 2
    assert report.override_add_applied == 1
    assert report.override_drop_applied == 0
    assert report.override_no_effect == 1
    assert report.rows_emitted_total == 7

    decisions_df = pd.read_parquet(output_dir / "merge_decisions.parquet")
    assert len(decisions_df) >= 5
    assert "override_no_effect" in set(decisions_df["decision_type"].astype(str))

    report_json = json.loads((output_dir / "merge_report.json").read_text(encoding="utf-8"))
    assert report_json["rows_emitted_total"] == 7
    assert report_json["healpix_order"] == 1
    assert report_json["merge_policy_version"] == "v1"
    assert report_json["gaia_dir"] == str(gaia_dir)
    assert report_json["hip_path"] == str(hip_path)
    assert report_json["crossmatch_path"] == str(crossmatch_path)
    assert report_json["overrides_path"] == str(overrides_path)


def test_run_merge_rejects_drop_override_with_payload(tmp_path: Path):
    gaia_dir = tmp_path / "gaia"
    hip_path = tmp_path / "hip_stars.parquet"
    crossmatch_path = tmp_path / "gaia_hip_map.parquet"
    overrides_path = tmp_path / "overrides.parquet"
    output_dir = tmp_path / "merged"

    _write_parquet(pd.DataFrame(columns=OUTPUT_COLS), gaia_dir / "b1.parquet")
    _write_parquet(
        pd.DataFrame(
            [
                {
                    "source": "hip",
                    "source_id": 2001,
                    "x_icrs_pc": 1.0,
                    "y_icrs_pc": 0.0,
                    "z_icrs_pc": 0.0,
                    "mag_abs": 1.5,
                    "teff": 6000.0,
                    "quality_flags": 2,
                    "astrometry_quality": 0.3,
                    "photometry_quality": 0.2,
                }
            ]
        )[OUTPUT_COLS],
        hip_path,
    )
    _write_parquet(
        pd.DataFrame([{"gaia_source_id": 1001, "hip_source_id": 2001}]),
        crossmatch_path,
    )
    _write_parquet(
        pd.DataFrame(
            [
                {
                    "source": "hip",
                    "source_id": "2001",
                    "x_icrs_pc": 99.0,
                    "y_icrs_pc": pd.NA,
                    "z_icrs_pc": pd.NA,
                    "mag_abs": pd.NA,
                    "teff": pd.NA,
                    "quality_flags": pd.NA,
                    "astrometry_quality": pd.NA,
                    "photometry_quality": pd.NA,
                    "override_id": "ov.invalid.drop.payload",
                    "action": "drop",
                    "override_reason": "invalid_payload",
                    "override_policy_version": "v1",
                }
            ]
        ),
        overrides_path,
    )

    with pytest.raises(ValueError, match="must not include payload columns"):
        run_merge(
            gaia_dir=gaia_dir,
            hip_path=hip_path,
            crossmatch_path=crossmatch_path,
            overrides_path=overrides_path,
            output_dir=output_dir,
            healpix_order=1,
            force=True,
        )

