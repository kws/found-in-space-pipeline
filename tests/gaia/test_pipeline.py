"""Tests for Gaia pipeline batching."""

import numpy as np
import pandas as pd

from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.gaia import pipeline as gaia_pipeline


def test_run_pipeline_batch_still_emits_output_cols(monkeypatch):
    def _select(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"source_id": [1, 2]})

    def _coords(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["x_icrs_pc"] = [0.0, 1.0]
        out["y_icrs_pc"] = [0.0, 1.0]
        out["z_icrs_pc"] = [0.0, 1.0]
        out["ra_deg"] = [0.0, 45.0]
        out["dec_deg"] = [0.0, 0.0]
        out["r_pc"] = [0.0, 1.41421356]
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
