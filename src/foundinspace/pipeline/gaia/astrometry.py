import numpy as np
import pandas as pd

from ..constants import CANONICAL_EPOCH_JYEAR, EPS

LAMBDA_PHOTO = 0.02  # tweak: 0.0 to disable, 0.01–0.05 typical
LAMBDA_GEO = 0.01


def select_astrometry_gaia(df: pd.DataFrame) -> pd.DataFrame:
    """Gaia-only astrometry: best of DR3, BJ_GEO, BJ_PHOTOGEO (no HIP).

    Sets best_source, astrometry_method, astrometry_quality (best_score),
    and all *_use_* columns. Expects ra, dec, pmra, pmdec; adds ra_deg, dec_deg.
    """
    cols = [
        "parallax",
        "parallax_error",
        "r_lo_geo",
        "r_med_geo",
        "r_hi_geo",
        "r_lo_photogeo",
        "r_med_photogeo",
        "r_hi_photogeo",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = df[c].astype(float)
    df["ra_deg"] = df["ra"]
    df["dec_deg"] = df["dec"]

    df["f_dr3"] = df["parallax_error"] / np.maximum(df["parallax"], EPS)
    df["f_geo"] = (df["r_hi_geo"] - df["r_lo_geo"]) / (
        2.0 * np.maximum(df["r_med_geo"], EPS)
    )
    df["f_photogeo"] = (df["r_hi_photogeo"] - df["r_lo_photogeo"]) / (
        2.0 * np.maximum(df["r_med_photogeo"], EPS)
    )

    valid_dr3 = (
        np.isfinite(df["f_dr3"]) & (df["parallax"] > 0) & (df["parallax_error"] > 0)
    )
    valid_geo = np.isfinite(df["f_geo"]) & (df["r_med_geo"] > 0)
    valid_photogeo = np.isfinite(df["f_photogeo"]) & (df["r_med_photogeo"] > 0)

    cand = np.column_stack(
        [
            np.where(valid_dr3, df["f_dr3"], np.inf),
            np.where(valid_geo, df["f_geo"] + LAMBDA_GEO, np.inf),
            np.where(valid_photogeo, df["f_photogeo"] + LAMBDA_PHOTO, np.inf),
        ]
    )
    sources = np.array(["DR3", "BJ_GEO", "BJ_PHOTOGEO"])
    best_idx = np.argmin(cand, axis=1)
    df["best_source"] = sources[best_idx]
    df["best_score"] = cand[np.arange(len(df)), best_idx]

    df["r_med_best"] = np.select(
        [
            df["best_source"].eq("DR3"),
            df["best_source"].eq("BJ_GEO"),
            df["best_source"].eq("BJ_PHOTOGEO"),
        ],
        [
            1000.0 / df["parallax"],
            df["r_med_geo"],
            df["r_med_photogeo"],
        ],
        default=np.nan,
    )
    df["astrometry_method"] = df["best_source"]
    df["astrometry_quality"] = df["best_score"]

    bj_mask = df["best_source"].isin(["BJ_GEO", "BJ_PHOTOGEO"])
    df["r_lo_pc"] = np.nan
    df["r_hi_pc"] = np.nan
    if bj_mask.any():
        df.loc[bj_mask, "r_lo_pc"] = df.loc[bj_mask, "r_lo_photogeo"].astype(float)
        df.loc[bj_mask, "r_hi_pc"] = df.loc[bj_mask, "r_hi_photogeo"].astype(float)

    df["distance_use_pc"] = df["r_med_best"]
    df["ra_use_deg"] = df["ra"].astype(float)
    df["dec_use_deg"] = df["dec"].astype(float)
    df["pmra_use_masyr"] = df["pmra"].astype(float)
    df["pmdec_use_masyr"] = df["pmdec"].astype(float)

    df["plx_mas"] = 1000.0 / df["r_med_best"]
    df["epoch_yr"] = CANONICAL_EPOCH_JYEAR

    return df
