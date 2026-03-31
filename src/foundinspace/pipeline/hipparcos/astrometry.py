import numpy as np
import pandas as pd

from foundinspace.pipeline.constants import (
    DIST_SRC_HIP,
    FLAG_DIST_PLAUSIBLE,
    FLAG_DIST_VALID,
    FLAG_NEEDS_REVIEW,
)

HIPPARCOS_EPOCH_JYEAR = 1991.25


def select_astrometry_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only astrometry: use Hipparcos Plx, ra_deg, dec_deg, pmRA, pmDE.

    Sets best_source, astrometry_quality (f_hip), quality_flags, and all *_use_*
    columns for downstream coords. No Gaia/BJ columns needed.

    Invalid or non-positive parallax yields NaN distance (no fabricated 1/plx floor).
    """
    plx_arr = df["Plx"].astype(float).to_numpy()
    e_plx = df["e_Plx"].astype(float).to_numpy()
    valid = np.isfinite(plx_arr) & np.isfinite(e_plx) & (plx_arr > 0) & (e_plx > 0)
    f_hip = np.full(plx_arr.shape, np.nan, dtype=float)
    np.divide(e_plx, plx_arr, out=f_hip, where=valid)

    df["best_source"] = "HIP"
    df["best_score"] = f_hip
    df["astrometry_quality"] = f_hip
    dist_pc = np.full(plx_arr.shape, np.nan, dtype=float)
    np.divide(1000.0, plx_arr, out=dist_pc, where=valid)
    df["r_med_best"] = dist_pc
    df["distance_use_pc"] = dist_pc
    plaus = np.isfinite(dist_pc) & (dist_pc > 0.1) & (dist_pc < 200_000)
    flags = (
        np.uint16(DIST_SRC_HIP)
        | np.where(valid, FLAG_DIST_VALID, 0).astype(np.uint16)
        | np.where(~valid, FLAG_NEEDS_REVIEW, 0).astype(np.uint16)
        | np.where(plaus, FLAG_DIST_PLAUSIBLE, 0).astype(np.uint16)
    )
    df["quality_flags"] = flags
    df["ra_use_deg"] = df["ra_deg"].astype(float)
    df["dec_use_deg"] = df["dec_deg"].astype(float)
    df["pmra_use_masyr"] = df["pmRA"].astype(float)
    df["pmdec_use_masyr"] = df["pmDE"].astype(float)
    df["epoch_yr"] = HIPPARCOS_EPOCH_JYEAR
    return df
