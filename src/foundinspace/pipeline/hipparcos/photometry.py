"""Photometry and effective-temperature functions for the unification pipeline.

This module handles Steps 5, 7, and 8 of the pipeline:
  - Step 5: assign apparent magnitude, color index, and photometric system labels
  - Step 7: compute absolute magnitude via a best-of cascade
  - Step 8: derive effective temperature (Teff) via a best-of cascade
"""

import numpy as np
import pandas as pd

from foundinspace.pipeline.common.photometry import bv_to_teff
from foundinspace.pipeline.constants import (
    PHOT_SRC_HIP_HP,
    PHOT_SRC_SHIFT,
    PHOTOMETRY_QUALITY_DM_FACTOR,
    TEFF_DEFAULT_K,
    TEFF_SRC_BV,
    TEFF_SRC_DEFAULT,
    TEFF_SRC_SHIFT,
)


def assign_photometry_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only: mag = Hpmag, color = bv; OR phot_src into quality_flags."""
    df["mag"] = df["Hpmag"].astype(float)
    df["color"] = df["bv"].astype(float)
    flags = df.get("quality_flags", pd.Series(0, index=df.index)).astype(np.uint16)
    df["quality_flags"] = (flags | (PHOT_SRC_HIP_HP << PHOT_SRC_SHIFT)).astype(
        np.uint16
    )
    return df


def compute_mag_abs_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only: distance modulus only when distance_use_pc is finite and positive.

    photometry_quality = 2.17 * (e_Plx/Plx) only for valid positive parallax;
    otherwise NaN (no max(Plx, eps) blow-up).
    """
    r_pc = df["distance_use_pc"].astype(float).to_numpy()
    mag = df["mag"].astype(float).to_numpy()
    plx_arr = df["Plx"].astype(float).to_numpy()
    e_plx = df["e_Plx"].astype(float).to_numpy()
    valid_plx = (
        np.isfinite(plx_arr)
        & (plx_arr > 0)
        & np.isfinite(e_plx)
        & (e_plx > 0)
    )
    f_hip = np.full(plx_arr.shape, np.nan, dtype=float)
    np.divide(e_plx, plx_arr, out=f_hip, where=valid_plx)

    dm = np.where(np.isfinite(r_pc) & (r_pc > 0), 5.0 * np.log10(r_pc / 10.0), np.nan)
    df["mag_abs"] = np.where(np.isfinite(dm), mag - dm, np.nan)
    df["photometry_quality"] = np.where(
        np.isfinite(f_hip),
        PHOTOMETRY_QUALITY_DM_FACTOR * f_hip,
        np.nan,
    )
    return df


def compute_teff_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only: bv_to_teff and default only. OR teff_src into quality_flags."""
    bv = df.get("bv", pd.Series(np.nan, index=df.index)).astype(float)
    has_bv = pd.notnull(bv) & np.isfinite(bv)
    teff_from_bv = bv_to_teff(bv.to_numpy())
    df["teff"] = np.where(has_bv, teff_from_bv, TEFF_DEFAULT_K)
    df["teff"] = np.where(
        pd.isnull(df["teff"]) | ~np.isfinite(df["teff"]),
        TEFF_DEFAULT_K,
        df["teff"],
    )
    teff_src_bits = np.where(has_bv, TEFF_SRC_BV, TEFF_SRC_DEFAULT).astype(np.uint16)
    flags = df.get("quality_flags", pd.Series(0, index=df.index)).astype(np.uint16)
    df["quality_flags"] = (flags | (teff_src_bits << TEFF_SRC_SHIFT)).astype(np.uint16)
    return df


def compute_log_g_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only: log_g = NaN for all rows."""
    df["log_g"] = np.nan
    return df
