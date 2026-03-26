"""Streaming Gaia/Hipparcos merge into HEALPix-partitioned Parquet output."""

from __future__ import annotations

import json
import logging
import math
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm.auto import tqdm

from foundinspace.pipeline.constants import OUTPUT_COLS

_LOG = logging.getLogger(__name__)
MERGE_POLICY_VERSION = "v1"
MERGE_BATCH_SIZE = 1_000_000

MERGED_OUTPUT_COLS = [
    *OUTPUT_COLS,
    "gaia_source_id",
    "hip_source_id",
    "catalog_source",
    "ra_deg",
    "dec_deg",
    "r_pc",
]

DECISION_COLS = [
    "decision_type",
    "gaia_source_id",
    "hip_source_id",
    "winner_catalog",
    "winner_source_id",
    "gaia_score",
    "hip_score",
    "tie_break_reason",
    "override_id",
    "override_action",
    "override_reason",
    "override_policy_version",
    "note",
]

DROP_OVERRIDE_PAYLOAD_COLS = [col for col in OUTPUT_COLS if col not in {"source", "source_id"}]


@dataclass
class MergeReport:
    """Small aggregate report for one merge run."""

    merge_policy_version: str
    healpix_order: int
    healpix_nside: int
    gaia_dir: str
    hip_path: str
    crossmatch_path: str
    overrides_path: str
    gaia_files: list[str]
    gaia_rows_total: int
    rows_emitted_total: int
    unmatched_gaia: int
    unmatched_hip: int
    matched_pairs_scored: int
    matched_winner_gaia: int
    matched_winner_hip: int
    override_add_applied: int
    override_replace_applied: int
    override_drop_applied: int
    override_no_effect: int
    decisions_rows: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _normalize_source(source: Any) -> str:
    return str(source).strip().lower()


def _normalize_key(source: Any, source_id: Any) -> tuple[str, int | str]:
    src = _normalize_source(source)
    sid = str(source_id).strip()
    if src in {"gaia", "hip"}:
        return src, int(sid)
    return src, sid


def _safe_score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return math.inf
    return score if math.isfinite(score) else math.inf


def _choose_matched_winner(gaia_row: dict[str, Any], hip_row: dict[str, Any]) -> tuple[str, str]:
    """Return (winner_catalog, tie_break_reason)."""
    gaia_score = _safe_score(gaia_row.get("astrometry_quality"))
    hip_score = _safe_score(hip_row.get("astrometry_quality"))
    if gaia_score < hip_score:
        return "gaia", ""
    if hip_score < gaia_score:
        return "hip", ""
    # Spec tie-break: prefer Gaia, then lower source_id.
    return "gaia", "prefer_gaia"


def _xyz_to_radec(df: pd.DataFrame) -> pd.DataFrame:
    """Add r_pc, ra_deg, dec_deg from Cartesian coordinates."""
    x = df["x_icrs_pc"].to_numpy(dtype=float)
    y = df["y_icrs_pc"].to_numpy(dtype=float)
    z = df["z_icrs_pc"].to_numpy(dtype=float)
    r = np.sqrt(x * x + y * y + z * z)

    ra = (np.degrees(np.arctan2(y, x)) + 360.0) % 360.0
    dec = np.zeros_like(r)
    nonzero = r > 0.0
    dec[nonzero] = np.degrees(np.arcsin(np.clip(z[nonzero] / r[nonzero], -1.0, 1.0)))
    dec[~nonzero] = 0.0
    ra[~nonzero] = 0.0

    df["r_pc"] = r
    df["ra_deg"] = ra
    df["dec_deg"] = dec
    return df


def _output_row(
    payload: dict[str, Any],
    *,
    catalog_source: str,
    gaia_source_id: int | None,
    hip_source_id: int | None,
) -> dict[str, Any]:
    row = {col: payload[col] for col in OUTPUT_COLS}
    row["source"] = str(row["source"])
    row["source_id"] = str(row["source_id"])
    row["gaia_source_id"] = str(gaia_source_id) if gaia_source_id is not None else pd.NA
    row["hip_source_id"] = str(hip_source_id) if hip_source_id is not None else pd.NA
    row["catalog_source"] = catalog_source
    x = float(row["x_icrs_pc"])
    y = float(row["y_icrs_pc"])
    z = float(row["z_icrs_pc"])
    r = float(math.sqrt(x * x + y * y + z * z))
    if r > 0.0:
        ra = (math.degrees(math.atan2(y, x)) + 360.0) % 360.0
        dec = math.degrees(math.asin(max(-1.0, min(1.0, z / r))))
    else:
        ra = 0.0
        dec = 0.0
    row["r_pc"] = r
    row["ra_deg"] = ra
    row["dec_deg"] = dec
    return row


def _prepare_gaia_unmatched(df: pd.DataFrame) -> pd.DataFrame:
    out = df[OUTPUT_COLS].copy()
    out["source"] = "gaia"
    out["source_id"] = out["source_id"].astype("uint64").astype("string")
    out["gaia_source_id"] = out["source_id"]
    out["hip_source_id"] = pd.NA
    out["catalog_source"] = "gaia"
    out = _xyz_to_radec(out)
    return out[MERGED_OUTPUT_COLS]


def _build_healpix(order: int):
    try:
        from astropy_healpix import HEALPix
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "astropy-healpix is required for merge HEALPix sharding. "
            "Install dependencies (pip install -e .) and retry."
        ) from exc
    return HEALPix(nside=2**order, order="nested")


def _healpix_pixels(
    hp: Any,
    ra_deg: pd.Series | np.ndarray,
    dec_deg: pd.Series | np.ndarray,
) -> np.ndarray:
    from astropy import units as u

    ra_arr = np.asarray(ra_deg, dtype=float)
    dec_arr = np.asarray(dec_deg, dtype=float)
    return np.asarray(hp.lonlat_to_healpix(ra_arr * u.deg, dec_arr * u.deg), dtype=np.int64)


def _write_shards(
    df: pd.DataFrame,
    *,
    hp: Any,
    shards_root: Path,
    phase_tag: str,
    seq_by_pixel: dict[int, int],
) -> int:
    if df.empty:
        return 0
    pixels = _healpix_pixels(hp, df["ra_deg"], df["dec_deg"])
    rows_written = 0
    for pixel in sorted(np.unique(pixels)):
        pixel_i = int(pixel)
        pixel_dir = shards_root / str(pixel_i)
        pixel_dir.mkdir(parents=True, exist_ok=True)
        next_seq = seq_by_pixel.get(pixel_i, 0) + 1
        seq_by_pixel[pixel_i] = next_seq
        out_path = pixel_dir / f"{next_seq:06d}_{phase_tag}.parquet"
        part = df.loc[pixels == pixel_i, MERGED_OUTPUT_COLS]
        table = pa.Table.from_pandas(
            part,
            preserve_index=False,
        )
        pq.write_table(table, str(out_path), compression="zstd")
        rows_written += len(part)
    return rows_written


def _read_required_parquet(path: Path, required_cols: list[str]) -> pd.DataFrame:
    df = pq.read_table(path).to_pandas()
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {path}: {missing}")
    return df


def _validate_parquet_columns(path: Path, required_cols: list[str]) -> None:
    schema = pq.ParquetFile(path).schema_arrow
    file_cols = set(schema.names)
    missing = [c for c in required_cols if c not in file_cols]
    if missing:
        raise ValueError(f"Missing required columns in {path}: {missing}")


def _build_crossmatch_maps(df: pd.DataFrame) -> tuple[dict[int, int], dict[int, int]]:
    gaia_to_hip: dict[int, int] = {}
    hip_to_gaia: dict[int, int] = {}
    for rec in df[["gaia_source_id", "hip_source_id"]].itertuples(index=False):
        gaia_id = int(rec.gaia_source_id)
        hip_id = int(rec.hip_source_id)
        prev_hip = gaia_to_hip.get(gaia_id)
        if prev_hip is not None and prev_hip != hip_id:
            raise ValueError(
                f"Crossmatch is not one-to-one: Gaia {gaia_id} maps to {prev_hip} and {hip_id}"
            )
        prev_gaia = hip_to_gaia.get(hip_id)
        if prev_gaia is not None and prev_gaia != gaia_id:
            raise ValueError(
                f"Crossmatch is not one-to-one: HIP {hip_id} maps to {prev_gaia} and {gaia_id}"
            )
        gaia_to_hip[gaia_id] = hip_id
        hip_to_gaia[hip_id] = gaia_id
    return gaia_to_hip, hip_to_gaia


def _find_pair_override(
    overrides_by_key: dict[tuple[str, int | str], dict[str, Any]],
    *,
    gaia_id: int | None,
    hip_id: int | None,
) -> dict[str, Any] | None:
    hits: list[dict[str, Any]] = []
    if gaia_id is not None:
        ov = overrides_by_key.get(("gaia", gaia_id))
        if ov is not None:
            hits.append(ov)
    if hip_id is not None:
        ov = overrides_by_key.get(("hip", hip_id))
        if ov is not None:
            hits.append(ov)
    if not hits:
        return None
    unique_ids = {str(h["override_id"]) for h in hits}
    if len(unique_ids) > 1:
        raise ValueError(
            f"Conflicting overrides for pair gaia={gaia_id} hip={hip_id}: {sorted(unique_ids)}"
        )
    return hits[0]


def _validate_drop_override_payload(override: dict[str, Any]) -> None:
    bad_cols = [col for col in DROP_OVERRIDE_PAYLOAD_COLS if not pd.isna(override.get(col))]
    if bad_cols:
        raise ValueError(
            "Drop override "
            f"{override.get('override_id')} must not include payload columns; found values in {bad_cols}"
        )


def run_merge(
    *,
    gaia_dir: Path,
    hip_path: Path,
    crossmatch_path: Path,
    overrides_path: Path,
    output_dir: Path,
    healpix_order: int = 3,
    force: bool = False,
) -> MergeReport:
    """Run streaming merge into HEALPix-partitioned output directories."""
    if healpix_order < 0:
        raise ValueError("healpix_order must be >= 0")

    gaia_dir = Path(gaia_dir).expanduser()
    hip_path = Path(hip_path).expanduser()
    crossmatch_path = Path(crossmatch_path).expanduser()
    overrides_path = Path(overrides_path).expanduser()
    output_dir = Path(output_dir).expanduser()
    shards_root = output_dir / "healpix"
    decisions_path = output_dir / "merge_decisions.parquet"
    report_path = output_dir / "merge_report.json"

    if not gaia_dir.is_dir():
        raise FileNotFoundError(str(gaia_dir))
    for path in (hip_path, crossmatch_path, overrides_path):
        if not path.is_file():
            raise FileNotFoundError(str(path))

    if output_dir.exists():
        has_content = any(output_dir.iterdir())
        if has_content and not force:
            raise FileExistsError(str(output_dir))
        if force:
            shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    shards_root.mkdir(parents=True, exist_ok=True)

    nside = 2**healpix_order
    hp = _build_healpix(healpix_order)

    # Load small lookup tables once.
    hip_df = _read_required_parquet(hip_path, OUTPUT_COLS)
    hip_df["source_id"] = pd.to_numeric(hip_df["source_id"], errors="raise").astype("uint64")
    hip_by_id: dict[int, dict[str, Any]] = {}
    for rec in hip_df[OUTPUT_COLS].to_dict(orient="records"):
        hip_by_id[int(rec["source_id"])] = rec

    cross_df = _read_required_parquet(crossmatch_path, ["gaia_source_id", "hip_source_id"])
    gaia_to_hip, hip_to_gaia = _build_crossmatch_maps(cross_df)

    overrides_df = _read_required_parquet(
        overrides_path,
        [
            *OUTPUT_COLS,
            "override_id",
            "action",
            "override_reason",
            "override_policy_version",
        ],
    )
    overrides_by_key: dict[tuple[str, int | str], dict[str, Any]] = {}
    add_overrides: list[dict[str, Any]] = []
    for ov in overrides_df.to_dict(orient="records"):
        action = str(ov["action"]).strip().lower()
        source = _normalize_source(ov["source"])
        key = _normalize_key(source, ov["source_id"])
        ov["action"] = action
        ov["source"] = source
        ov["source_id"] = str(ov["source_id"]).strip()
        if action == "add":
            add_overrides.append(ov)
            continue
        if action not in {"replace", "drop"}:
            raise ValueError(f"Unsupported override action: {action}")
        if action == "drop":
            _validate_drop_override_payload(ov)
        if key in overrides_by_key:
            raise ValueError(f"Duplicate override target key: {key}")
        overrides_by_key[key] = ov

    # Small state only (no per-row unmatched storage).
    write_seq_by_pixel: dict[int, int] = {}
    resolved_hip_ids: set[int] = set()
    processed_override_ids: set[str] = set()
    decisions: list[dict[str, Any]] = []

    gaia_files = sorted(gaia_dir.glob("*.parquet"))
    gaia_files_for_report = [str(p) for p in gaia_files]

    gaia_special_ids: set[int] = set(gaia_to_hip.keys())
    for (src, sid), _ov in overrides_by_key.items():
        if src == "gaia":
            gaia_special_ids.add(int(sid))
        elif src == "hip":
            partner_gaia = hip_to_gaia.get(int(sid))
            if partner_gaia is not None:
                gaia_special_ids.add(partner_gaia)

    report = MergeReport(
        merge_policy_version=MERGE_POLICY_VERSION,
        healpix_order=healpix_order,
        healpix_nside=nside,
        gaia_dir=str(gaia_dir),
        hip_path=str(hip_path),
        crossmatch_path=str(crossmatch_path),
        overrides_path=str(overrides_path),
        gaia_files=gaia_files_for_report,
        gaia_rows_total=0,
        rows_emitted_total=0,
        unmatched_gaia=0,
        unmatched_hip=0,
        matched_pairs_scored=0,
        matched_winner_gaia=0,
        matched_winner_hip=0,
        override_add_applied=0,
        override_replace_applied=0,
        override_drop_applied=0,
        override_no_effect=0,
        decisions_rows=0,
    )

    for gaia_file in tqdm(
        gaia_files,
        total=len(gaia_files),
        desc="Merging Gaia batches",
        unit="file",
        dynamic_ncols=True,
    ):
        special_out_rows: list[dict[str, Any]] = []
        _validate_parquet_columns(gaia_file, OUTPUT_COLS)
        parquet_file = pq.ParquetFile(gaia_file)
        gaia_num_rows = parquet_file.metadata.num_rows
        batch_bar_total = (
            (gaia_num_rows + MERGE_BATCH_SIZE - 1) // MERGE_BATCH_SIZE
            if gaia_num_rows > 0
            else None
        )
        for batch in tqdm(
            parquet_file.iter_batches(
                batch_size=MERGE_BATCH_SIZE,
                columns=OUTPUT_COLS,
            ),
            desc=f"Batches ({gaia_file.stem})",
            unit="batch",
            total=batch_bar_total,
            leave=False,
            dynamic_ncols=True,
        ):
            gaia_df = batch.to_pandas()
            report.gaia_rows_total += len(gaia_df)
            gaia_df["source_id"] = pd.to_numeric(gaia_df["source_id"], errors="raise").astype(
                "uint64"
            )

            special_mask = gaia_df["source_id"].isin(gaia_special_ids)
            gaia_unmatched_df = gaia_df.loc[~special_mask]
            if not gaia_unmatched_df.empty:
                out = _prepare_gaia_unmatched(gaia_unmatched_df)
                written = _write_shards(
                    out,
                    hp=hp,
                    shards_root=shards_root,
                    phase_tag=f"gaia_{gaia_file.stem}",
                    seq_by_pixel=write_seq_by_pixel,
                )
                report.rows_emitted_total += written
                report.unmatched_gaia += written

            special_rows = gaia_df.loc[special_mask, OUTPUT_COLS].to_dict(orient="records")
            for gaia_rec in special_rows:
                gaia_id = int(gaia_rec["source_id"])
                hip_id = gaia_to_hip.get(gaia_id)
                hip_rec = hip_by_id.get(hip_id) if hip_id is not None else None

                override = _find_pair_override(overrides_by_key, gaia_id=gaia_id, hip_id=hip_id)
                if override is not None:
                    override_id = str(override["override_id"])
                    if override_id in processed_override_ids:
                        # Pair was already resolved by the counterpart route.
                        if hip_id is not None:
                            resolved_hip_ids.add(hip_id)
                        continue
                    processed_override_ids.add(override_id)
                    if hip_id is not None:
                        resolved_hip_ids.add(hip_id)
                    action = str(override["action"])
                    note = ""
                    winner_catalog = ""
                    winner_source_id = ""
                    if action == "replace":
                        special_out_rows.append(
                            _output_row(
                                override,
                                catalog_source="manual",
                                gaia_source_id=gaia_id if gaia_id is not None else None,
                                hip_source_id=hip_id if hip_id is not None else None,
                            )
                        )
                        report.override_replace_applied += 1
                        winner_catalog = "manual"
                        winner_source_id = str(override["source_id"])
                    elif action == "drop":
                        report.override_drop_applied += 1
                    else:
                        raise ValueError(f"Unsupported override action for pair path: {action}")

                    if hip_id is None:
                        note = "partner_missing"
                    decisions.append(
                        {
                            "decision_type": "override",
                            "gaia_source_id": str(gaia_id),
                            "hip_source_id": str(hip_id) if hip_id is not None else pd.NA,
                            "winner_catalog": winner_catalog or pd.NA,
                            "winner_source_id": winner_source_id or pd.NA,
                            "gaia_score": _safe_score(gaia_rec.get("astrometry_quality")),
                            "hip_score": _safe_score(
                                hip_rec.get("astrometry_quality") if hip_rec is not None else np.nan
                            ),
                            "tie_break_reason": pd.NA,
                            "override_id": override_id,
                            "override_action": action,
                            "override_reason": override.get("override_reason", pd.NA),
                            "override_policy_version": override.get(
                                "override_policy_version", pd.NA
                            ),
                            "note": note or pd.NA,
                        }
                    )
                    continue

                if hip_rec is None or hip_id in resolved_hip_ids:
                    special_out_rows.append(
                        _output_row(
                            gaia_rec,
                            catalog_source="gaia",
                            gaia_source_id=gaia_id,
                            hip_source_id=hip_id,
                        )
                    )
                    report.unmatched_gaia += 1
                    continue

                winner_catalog, tie_break_reason = _choose_matched_winner(gaia_rec, hip_rec)
                if winner_catalog == "gaia":
                    winner_row = _output_row(
                        gaia_rec,
                        catalog_source="gaia",
                        gaia_source_id=gaia_id,
                        hip_source_id=hip_id,
                    )
                    report.matched_winner_gaia += 1
                else:
                    winner_row = _output_row(
                        hip_rec,
                        catalog_source="hip",
                        gaia_source_id=gaia_id,
                        hip_source_id=hip_id,
                    )
                    report.matched_winner_hip += 1
                special_out_rows.append(winner_row)
                resolved_hip_ids.add(int(hip_id))
                report.matched_pairs_scored += 1
                decisions.append(
                    {
                        "decision_type": "score",
                        "gaia_source_id": str(gaia_id),
                        "hip_source_id": str(hip_id),
                        "winner_catalog": winner_catalog,
                        "winner_source_id": str(
                            gaia_id if winner_catalog == "gaia" else int(hip_id)
                        ),
                        "gaia_score": _safe_score(gaia_rec.get("astrometry_quality")),
                        "hip_score": _safe_score(hip_rec.get("astrometry_quality")),
                        "tie_break_reason": tie_break_reason or pd.NA,
                        "override_id": pd.NA,
                        "override_action": pd.NA,
                        "override_reason": pd.NA,
                        "override_policy_version": pd.NA,
                        "note": pd.NA,
                    }
                )

        if special_out_rows:
            special_df = pd.DataFrame(special_out_rows, columns=MERGED_OUTPUT_COLS)
            written = _write_shards(
                special_df,
                hp=hp,
                shards_root=shards_root,
                phase_tag=f"gaia_special_{gaia_file.stem}",
                seq_by_pixel=write_seq_by_pixel,
            )
            report.rows_emitted_total += written

    # Flush HIP side, including Gaia-targeted overrides where Gaia row is absent.
    hip_out_rows: list[dict[str, Any]] = []
    for hip_id in tqdm(
        sorted(hip_by_id),
        total=len(hip_by_id),
        desc="Flushing HIP rows",
        unit="row",
        dynamic_ncols=True,
    ):
        if hip_id in resolved_hip_ids:
            continue
        hip_rec = hip_by_id[hip_id]
        gaia_id = hip_to_gaia.get(hip_id)
        override = _find_pair_override(overrides_by_key, gaia_id=gaia_id, hip_id=hip_id)
        if override is not None:
            override_id = str(override["override_id"])
            if override_id in processed_override_ids:
                resolved_hip_ids.add(hip_id)
                continue
            processed_override_ids.add(override_id)
            resolved_hip_ids.add(hip_id)
            action = str(override["action"])
            if action == "replace":
                hip_out_rows.append(
                    _output_row(
                        override,
                        catalog_source="manual",
                        gaia_source_id=gaia_id if gaia_id is not None else None,
                        hip_source_id=hip_id,
                    )
                )
                report.override_replace_applied += 1
            elif action == "drop":
                report.override_drop_applied += 1
            else:
                raise ValueError(f"Unsupported override action for HIP flush path: {action}")
            decisions.append(
                {
                    "decision_type": "override",
                    "gaia_source_id": str(gaia_id) if gaia_id is not None else pd.NA,
                    "hip_source_id": str(hip_id),
                    "winner_catalog": "manual" if action == "replace" else pd.NA,
                    "winner_source_id": str(override["source_id"])
                    if action == "replace"
                    else pd.NA,
                    "gaia_score": pd.NA,
                    "hip_score": _safe_score(hip_rec.get("astrometry_quality")),
                    "tie_break_reason": pd.NA,
                    "override_id": override_id,
                    "override_action": action,
                    "override_reason": override.get("override_reason", pd.NA),
                    "override_policy_version": override.get("override_policy_version", pd.NA),
                    "note": "resolved_in_hip_flush"
                    if gaia_id is not None and ("gaia", gaia_id) == _normalize_key(
                        override["source"], override["source_id"]
                    )
                    else pd.NA,
                }
            )
            continue

        hip_out_rows.append(
            _output_row(
                hip_rec,
                catalog_source="hip",
                gaia_source_id=gaia_id if gaia_id is not None else None,
                hip_source_id=hip_id,
            )
        )
        report.unmatched_hip += 1

    if hip_out_rows:
        hip_out_df = pd.DataFrame(hip_out_rows, columns=MERGED_OUTPUT_COLS)
        written = _write_shards(
            hip_out_df,
            hp=hp,
            shards_root=shards_root,
            phase_tag="hip_flush",
            seq_by_pixel=write_seq_by_pixel,
        )
        report.rows_emitted_total += written

    # Add-only overrides.
    add_rows: list[dict[str, Any]] = []
    for ov in tqdm(
        add_overrides,
        total=len(add_overrides),
        desc="Applying add overrides",
        unit="row",
        dynamic_ncols=True,
    ):
        override_id = str(ov["override_id"])
        if override_id in processed_override_ids:
            continue
        processed_override_ids.add(override_id)
        add_rows.append(
            _output_row(
                ov,
                catalog_source="manual",
                gaia_source_id=None,
                hip_source_id=None,
            )
        )
        report.override_add_applied += 1
        decisions.append(
            {
                "decision_type": "override_add",
                "gaia_source_id": pd.NA,
                "hip_source_id": pd.NA,
                "winner_catalog": "manual",
                "winner_source_id": str(ov["source_id"]),
                "gaia_score": pd.NA,
                "hip_score": pd.NA,
                "tie_break_reason": pd.NA,
                "override_id": override_id,
                "override_action": "add",
                "override_reason": ov.get("override_reason", pd.NA),
                "override_policy_version": ov.get("override_policy_version", pd.NA),
                "note": pd.NA,
            }
        )

    if add_rows:
        add_df = pd.DataFrame(add_rows, columns=MERGED_OUTPUT_COLS)
        written = _write_shards(
            add_df,
            hp=hp,
            shards_root=shards_root,
            phase_tag="override_add",
            seq_by_pixel=write_seq_by_pixel,
        )
        report.rows_emitted_total += written

    # Unapplied replace/drop overrides: neither side present.
    for ov in overrides_by_key.values():
        override_id = str(ov["override_id"])
        if override_id in processed_override_ids:
            continue
        report.override_no_effect += 1
        _LOG.warning(
            "Override %s (%s:%s) has no effect: target and crossmatch partner are absent",
            override_id,
            ov.get("source"),
            ov.get("source_id"),
        )
        decisions.append(
            {
                "decision_type": "override_no_effect",
                "gaia_source_id": pd.NA,
                "hip_source_id": pd.NA,
                "winner_catalog": pd.NA,
                "winner_source_id": pd.NA,
                "gaia_score": pd.NA,
                "hip_score": pd.NA,
                "tie_break_reason": pd.NA,
                "override_id": override_id,
                "override_action": ov.get("action", pd.NA),
                "override_reason": ov.get("override_reason", pd.NA),
                "override_policy_version": ov.get("override_policy_version", pd.NA),
                "note": "target_and_partner_absent",
            }
        )

    expected_rows_emitted = (
        report.unmatched_gaia
        + report.unmatched_hip
        + report.matched_pairs_scored
        + report.override_replace_applied
        + report.override_add_applied
    )
    if report.rows_emitted_total != expected_rows_emitted:
        raise RuntimeError(
            "Row count consistency check failed: "
            f"rows_emitted_total={report.rows_emitted_total} "
            f"expected={expected_rows_emitted}"
        )

    decisions_df = pd.DataFrame(decisions, columns=DECISION_COLS)
    report.decisions_rows = len(decisions_df)
    if decisions_df.empty:
        decisions_df = pd.DataFrame(columns=DECISION_COLS)
    pq.write_table(
        pa.Table.from_pandas(decisions_df, preserve_index=False),
        str(decisions_path),
        compression="zstd",
    )

    with report_path.open("w", encoding="utf-8") as fp:
        json.dump(report.to_dict(), fp, indent=2, sort_keys=True)
        fp.write("\n")

    return report

