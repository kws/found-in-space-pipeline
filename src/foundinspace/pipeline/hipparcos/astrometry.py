import numpy as np
import pandas as pd

from foundinspace.pipeline.constants import (
    DIST_SRC_HIP,
    EPS,
    FLAG_DIST_PLAUSIBLE,
    FLAG_DIST_VALID,
    FLAG_NEEDS_REVIEW,
)

HIPPARCOS_EPOCH_JYEAR = 1991.25


def select_astrometry_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only astrometry: use Hipparcos Plx, ra_deg, dec_deg, pmRA, pmDE.

    Sets best_source, astrometry_quality (f_hip), quality_flags, and all *_use_*
    columns for downstream coords. No Gaia/BJ columns needed.
    """
    plx = np.maximum(df["Plx"].astype(float), EPS)
    f_hip = df["e_Plx"].astype(float) / plx
    valid = (
        np.isfinite(f_hip)
        & (df["Plx"].astype(float) > 0)
        & (df["e_Plx"].astype(float) > 0)
    )
    df["best_source"] = "HIP"
    df["best_score"] = np.where(valid, f_hip, np.nan)
    df["astrometry_quality"] = df["best_score"]
    df["r_med_best"] = 1000.0 / plx
    df["distance_use_pc"] = df["r_med_best"]
    dist = df["r_med_best"].to_numpy(float)
    plaus = np.isfinite(dist) & (dist > 0.1) & (dist < 200_000)
    flags = (
        np.uint16(DIST_SRC_HIP)
        | np.where(valid, FLAG_DIST_VALID, 0).astype(np.uint16)
        | np.where(~valid, FLAG_NEEDS_REVIEW, 0).astype(np.uint16)
        | np.where(plaus, FLAG_DIST_PLAUSIBLE, 0).astype(np.uint16)
    )
    df["quality_flags"] = flags
    df["plx_use_mas"] = 1000.0 / df["r_med_best"]
    df["ra_use_deg"] = df["ra_deg"].astype(float)
    df["dec_use_deg"] = df["dec_deg"].astype(float)
    df["pmra_use_masyr"] = df["pmRA"].astype(float)
    df["pmdec_use_masyr"] = df["pmDE"].astype(float)
    df["epoch_yr"] = HIPPARCOS_EPOCH_JYEAR
    return df
