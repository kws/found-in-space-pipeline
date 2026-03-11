import numpy as np
import pandas as pd

CUBE_WIDTH_PC = 400_000.0
HALF_WIDTH_PC = CUBE_WIDTH_PC / 2.0
DEPTH = 21
GRID_SIZE = 1 << DEPTH  # 2097152
GRID_MAX = GRID_SIZE - 1  # 2097151
SCALE = GRID_SIZE / CUBE_WIDTH_PC


def _spread21(v: np.ndarray) -> np.ndarray:
    """
    Expand 21 low bits of v so they occupy every 3rd bit in a uint64.
    Result uses bit positions 0,3,6,9,...
    """
    v = v.astype(np.uint64, copy=False) & np.uint64(0x1FFFFF)  # 21 bits

    v = (v | (v << 32)) & np.uint64(0x1F00000000FFFF)
    v = (v | (v << 16)) & np.uint64(0x1F0000FF0000FF)
    v = (v | (v << 8)) & np.uint64(0x100F00F00F00F00F)
    v = (v | (v << 4)) & np.uint64(0x10C30C30C30C30C3)
    v = (v | (v << 2)) & np.uint64(0x1249249249249249)
    return v


def normalize_axis_to_u21(a: pd.Series) -> np.ndarray:
    """
    Map physical coordinates in pc from [-200000, +200000] into uint21 grid coords.
    Values outside the cube are clipped.
    """
    v = np.floor((a.to_numpy(dtype=np.float64, copy=False) + HALF_WIDTH_PC) * SCALE)
    v = np.clip(v, 0, GRID_MAX)
    return v.astype(np.uint64)


def morton3d_u64_from_xyz(
    df: pd.DataFrame,
    xcol: str = "x_icrs_pc",
    ycol: str = "y_icrs_pc",
    zcol: str = "z_icrs_pc",
) -> np.ndarray:
    """
    Compute a 64-bit Morton code from normalized integer coordinates.

    Assumes each axis is already normalized to integers in [0, 2^21).
    Uses bit layout:
      x -> bits 0,3,6,...
      y -> bits 1,4,7,...
      z -> bits 2,5,8,...
    """
    ix = normalize_axis_to_u21(df[xcol])
    iy = normalize_axis_to_u21(df[ycol])
    iz = normalize_axis_to_u21(df[zcol])

    return _spread21(ix) | (_spread21(iy) << 1) | (_spread21(iz) << 2)


def add_morton_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 64-bit Morton code to a DataFrame.
    """
    df["morton_code"] = morton3d_u64_from_xyz(df)
    return df
