"""Photometry and effective-temperature functions for the unification pipeline.

This module handles Steps 5, 7, and 8 of the pipeline:
  - Step 5: assign apparent magnitude, color index, and photometric system labels
  - Step 7: compute absolute magnitude via a best-of cascade
  - Step 8: derive effective temperature (Teff) via a best-of cascade
"""

import numpy as np
import pandas as pd

from foundinspace.pipeline.common.photometry import bv_to_teff
from foundinspace.pipeline.constants import PHOTOMETRY_QUALITY_DM_FACTOR, TEFF_DEFAULT_K


def assign_photometry_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only: mag = Hpmag, photometry_method = Hip_Hp, color = bv."""
    df["mag"] = df["Hpmag"].astype(float)
    df["photometry_method"] = "Hip_Hp"
    df["color"] = df["bv"].astype(float)
    return df


def compute_mag_abs_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only: distance modulus only. photometry_quality = 2.17 * (e_Plx/Plx)."""
    r_pc = df["distance_use_pc"].astype(float)
    mag = df["mag"].astype(float)
    dm = np.where(r_pc > 0, 5 * np.log10(r_pc / 10), np.nan)
    df["mag_abs"] = mag - dm
    df["photometry_method"] = "Hip_Hp"
    f_hip = df["e_Plx"].astype(float) / np.maximum(df["Plx"].astype(float), 1e-12)
    df["photometry_quality"] = PHOTOMETRY_QUALITY_DM_FACTOR * np.where(
        np.isfinite(f_hip), f_hip, np.nan
    )
    return df


def compute_teff_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only: bv_to_teff and default only."""
    bv = df.get("bv", pd.Series(np.nan, index=df.index)).astype(float)
    has_bv = pd.notnull(bv) & np.isfinite(bv)
    teff_from_bv = bv_to_teff(bv.to_numpy())
    df["teff"] = np.where(has_bv, teff_from_bv, TEFF_DEFAULT_K)
    df["teff"] = np.where(
        pd.isnull(df["teff"]) | ~np.isfinite(df["teff"]),
        TEFF_DEFAULT_K,
        df["teff"],
    )
    return df


def compute_log_g_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only: log_g = NaN for all rows."""
    df["log_g"] = np.nan
    return df
