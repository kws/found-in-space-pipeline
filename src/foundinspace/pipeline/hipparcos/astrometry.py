import numpy as np
import pandas as pd

from foundinspace.pipeline.constants import EPS

HIPPARCOS_EPOCH_JYEAR = 1991.25


def select_astrometry_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only astrometry: use Hipparcos Plx, ra_deg, dec_deg, pmRA, pmDE.

    Sets best_source = "HIP", astrometry_method, astrometry_quality (f_hip),
    and all *_use_* columns for downstream coords. No Gaia/BJ columns needed.
    """
    plx = np.maximum(df["Plx"].astype(float), EPS)
    f_hip = df["e_Plx"].astype(float) / plx
    df["best_source"] = "HIP"
    df["best_score"] = np.where(
        np.isfinite(f_hip) & (df["Plx"] > 0) & (df["e_Plx"] > 0),
        f_hip,
        np.nan,
    )
    df["astrometry_method"] = "HIP"
    df["astrometry_quality"] = df["best_score"]
    df["r_med_best"] = 1000.0 / plx
    df["distance_use_pc"] = df["r_med_best"]
    df["plx_use_mas"] = 1000.0 / df["r_med_best"]
    df["ra_use_deg"] = df["ra_deg"].astype(float)
    df["dec_use_deg"] = df["dec_deg"].astype(float)
    df["pmra_use_masyr"] = df["pmRA"].astype(float)
    df["pmdec_use_masyr"] = df["pmDE"].astype(float)
    df["epoch_yr"] = HIPPARCOS_EPOCH_JYEAR
    return df
