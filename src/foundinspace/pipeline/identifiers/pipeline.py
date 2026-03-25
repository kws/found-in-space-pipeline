"""Prepare a wide star-identifier sidecar keyed by Hipparcos ID."""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table

IDENTIFIER_COLS = ["hip_source_id", "hd", "bayer", "fl", "cst", "proper_name"]

_BAYER_GREEK_MAP = {
    "alp": "alpha",
    "bet": "beta",
    "gam": "gamma",
    "del": "delta",
    "eps": "epsilon",
    "zet": "zeta",
    "eta": "eta",
    "the": "theta",
    "iot": "iota",
    "kap": "kappa",
    "lam": "lambda",
    "mu": "mu",
    "nu": "nu",
    "xi": "xi",
    "omi": "omicron",
    "pi": "pi",
    "rho": "rho",
    "sig": "sigma",
    "tau": "tau",
    "ups": "upsilon",
    "phi": "phi",
    "chi": "chi",
    "psi": "psi",
    "ome": "omega",
}


def _read_ecsv(input_path: Path) -> pd.DataFrame:
    table = Table.read(input_path, format="ascii.ecsv")
    return table.to_pandas()


def _coerce_positive_int(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    values = numeric.to_numpy(dtype=float, na_value=np.nan, copy=False)
    finite = np.isfinite(values)
    integral = np.equal(np.floor(values), values)
    positive = values > 0
    valid = finite & integral & positive
    out = pd.Series(pd.NA, index=series.index, dtype="Int64")
    if np.any(valid):
        out.loc[valid] = values[valid].astype("int64")
    return out


def _clean_text(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.strip()
    out = out.mask(out == "", pd.NA)
    return out


def _bayer_code_to_display(bayer_code: str | None, constellation: str | None) -> str | None:
    if bayer_code is None or pd.isna(bayer_code):
        return None
    base = str(bayer_code).strip().rstrip(".")
    if not base:
        return None

    match = re.match(r"^([A-Za-z]+)(\d*)$", base)
    if match is None:
        bayer_root = base
    else:
        letters, suffix = match.groups()
        greek = _BAYER_GREEK_MAP.get(letters.lower())
        if greek is None:
            bayer_root = letters
        elif suffix:
            bayer_root = f"{greek}{int(suffix)}"
        else:
            bayer_root = greek

    if constellation is None or pd.isna(constellation):
        cst = None
    else:
        cst = str(constellation).strip()
    if cst:
        return f"{bayer_root} {cst}"
    return bayer_root


def _empty_identifiers() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "hip_source_id": pd.Series(dtype="uint64"),
            "hd": pd.Series(dtype="Int64"),
            "bayer": pd.Series(dtype="string"),
            "fl": pd.Series(dtype="Int64"),
            "cst": pd.Series(dtype="string"),
            "proper_name": pd.Series(dtype="string"),
        }
    )


def _prepare_identifier_sidecar(
    hip_hd_df: pd.DataFrame,
    iv27a_catalog_df: pd.DataFrame,
    iv27a_proper_names_df: pd.DataFrame,
) -> pd.DataFrame:
    hip_hd = hip_hd_df.copy()
    hip_hd.columns = [str(c).strip() for c in hip_hd.columns]
    hip_hd["hip_source_id"] = _coerce_positive_int(hip_hd.get("HIP", pd.Series(pd.NA)))
    hip_hd["hd"] = _coerce_positive_int(hip_hd.get("HD", pd.Series(pd.NA)))
    hip_to_hd = (
        hip_hd.loc[hip_hd["hip_source_id"].notna() & hip_hd["hd"].notna(), ["hip_source_id", "hd"]]
        .drop_duplicates(subset=["hip_source_id"], keep="first")
        .reset_index(drop=True)
    )

    names = iv27a_proper_names_df.copy()
    names.columns = [str(c).strip() for c in names.columns]
    names["hd"] = _coerce_positive_int(names.get("HD", pd.Series(pd.NA)))
    names["proper_name"] = _clean_text(names.get("Name", pd.Series(pd.NA)))
    hd_to_proper = (
        names.loc[names["hd"].notna() & names["proper_name"].notna(), ["hd", "proper_name"]]
        .drop_duplicates(subset=["hd"], keep="first")
        .reset_index(drop=True)
    )

    catalog = iv27a_catalog_df.copy()
    catalog.columns = [str(c).strip() for c in catalog.columns]
    catalog["hip_source_id"] = _coerce_positive_int(catalog.get("HIP", pd.Series(pd.NA)))
    catalog["hd"] = _coerce_positive_int(catalog.get("HD", pd.Series(pd.NA)))
    catalog["fl"] = _coerce_positive_int(catalog.get("Fl", pd.Series(pd.NA)))
    catalog["cst"] = _clean_text(catalog.get("Cst", pd.Series(pd.NA)))
    catalog["bayer_raw"] = _clean_text(catalog.get("Bayer", pd.Series(pd.NA)))
    catalog["bayer"] = [
        _bayer_code_to_display(bayer, cst)
        for bayer, cst in zip(catalog["bayer_raw"], catalog["cst"], strict=True)
    ]
    catalog["bayer"] = pd.Series(catalog["bayer"], index=catalog.index, dtype="string")
    catalog.loc[catalog["bayer"] == "", "bayer"] = pd.NA

    catalog_hip = catalog.loc[
        catalog["hip_source_id"].notna(),
        ["hip_source_id", "hd", "bayer", "fl", "cst"],
    ].drop_duplicates(subset=["hip_source_id"], keep="first")

    merged = catalog_hip.merge(
        hip_to_hd.rename(columns={"hd": "hd_from_hip_main"}),
        on="hip_source_id",
        how="outer",
    )
    merged["hd"] = merged["hd_from_hip_main"].where(
        merged["hd_from_hip_main"].notna(),
        merged["hd"],
    )
    merged = merged.drop(columns=["hd_from_hip_main"])

    merged = merged.merge(hd_to_proper, on="hd", how="left")
    merged = merged.loc[
        merged["bayer"].notna() | merged["proper_name"].notna(),
        IDENTIFIER_COLS,
    ].copy()

    if merged.empty:
        return _empty_identifiers()

    merged["hip_source_id"] = merged["hip_source_id"].astype("uint64")
    merged["hd"] = merged["hd"].astype("Int64")
    merged["fl"] = merged["fl"].astype("Int64")
    merged["bayer"] = merged["bayer"].astype("string")
    merged["cst"] = merged["cst"].astype("string")
    merged["proper_name"] = merged["proper_name"].astype("string")

    merged = merged.sort_values(by="hip_source_id", kind="mergesort", ignore_index=True)
    return merged


def prepare_identifiers_sidecar(
    hip_hd_path: Path,
    iv27a_catalog_path: Path,
    iv27a_proper_names_path: Path,
    output_path: Path,
    *,
    overwrite: bool = False,
) -> Path:
    output_path = Path(output_path).expanduser()
    if output_path.exists() and not overwrite:
        raise FileExistsError(str(output_path))

    for path in (hip_hd_path, iv27a_catalog_path, iv27a_proper_names_path):
        if not Path(path).is_file():
            raise FileNotFoundError(
                f"Missing required identifier catalog input: {path}."
                " Run `fis-pipeline identifiers download` first."
            )

    sidecar = _prepare_identifier_sidecar(
        _read_ecsv(Path(hip_hd_path)),
        _read_ecsv(Path(iv27a_catalog_path)),
        _read_ecsv(Path(iv27a_proper_names_path)),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(sidecar[IDENTIFIER_COLS], preserve_index=False)
    pq.write_table(table, str(output_path), compression="zstd")
    return output_path
