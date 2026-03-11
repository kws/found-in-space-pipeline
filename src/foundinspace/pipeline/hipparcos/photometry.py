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
    """HIP-only: distance modulus only. photometry_quality = 2.17 * (e_Plx/Plx)."""
    r_pc = df["distance_use_pc"].astype(float)
    mag = df["mag"].astype(float)
    dm = np.where(r_pc > 0, 5 * np.log10(r_pc / 10), np.nan)
    df["mag_abs"] = mag - dm
    f_hip = df["e_Plx"].astype(float) / np.maximum(df["Plx"].astype(float), 1e-12)
    df["photometry_quality"] = PHOTOMETRY_QUALITY_DM_FACTOR * np.where(
        np.isfinite(f_hip), f_hip, np.nan
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
