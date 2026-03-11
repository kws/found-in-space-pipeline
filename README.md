# Found in Space - Pipeline

A pipeline for reducing **Gaia** and **Hipparcos** data to produce a 3D map of stars. It ingests catalogue data (VOTables), applies astrometry and photometry selection, propagates positions to a common epoch, and outputs **sun-centric Cartesian coordinates** (ICRS, in parsecs) plus derived quantities suitable for visualization or further processing.

## Output

The pipeline produces Parquet tables with a fixed schema. Each row is a star with:

- **Sun-centric Cartesian coordinates** (ICRS frame, J2016.0): `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` (parsecs). The origin is the Sun; axes follow the ICRS convention.
- **Identifiers and source**: `source` (e.g. `"gaia"`), `source_id`, `morton_code` (3D spatial index).
- **Photometry**: `mag_abs` (absolute magnitude), `teff` (effective temperature, K), `photometry_quality` (magnitude uncertainty).
- **Provenance and quality**: `quality_flags` (packed uint16: distance source, Teff source, photometry source, validity and review bits), `astrometry_quality` (e.g. fractional parallax error or Bailer-Jones interval width; finite, no inf).

See `OUTPUT_COLS` in `foundinspace.pipeline.constants` for the full list.

## Installation

Requires Python ≥3.13. From the project root:

```bash
pip install -e .
```

Dependencies include `astropy`, `pandas`, `pyarrow`, and `votpipe` (see `pyproject.toml`).

## CLI

Entry point: **`fis-pipeline`** (or `python -m foundinspace.pipeline`).

| Command | Description |
|--------|--------------|
| `fis-pipeline gaia import INPUT [INPUT ...]` | Read Gaia VOTable(s) (`.vot`, `.vot.gz`, `.vot.xz`), run the Gaia pipeline per batch, and write `{stem}.parquet` next to each input (or under `--output-dir`). |

**Options for `gaia import`:**

- `--output-dir`, `-o` — Directory for output Parquet files (default: same as input).
- `--force`, `-f` — Overwrite existing output files.
- `--limit`, `-l` — Stop after this many output rows (for testing).

## Pipeline stages (Gaia)

For each batch of Gaia data the pipeline:

1. **Astrometry** (`gaia.astrometry.select_astrometry_gaia`) — Multi-tier distance: Tier A = best of DR3 / BJ geometric / BJ photogeometric (quality-tested); Tier B = weak catalog fallback; Tier C = photometric (M_G + A_G); Tier D = synthetic prior. Sets `distance_use_pc`, `quality_flags`, `*_use_*` columns. No row dropped for bad astrometry alone.
2. **Photometry** (`gaia.photometry.assign_photometry_gaia`, `compute_mag_abs_gaia`, `compute_teff_gaia`) — Apparent magnitude (G), absolute magnitude (GSP-Phot / extinction-corrected / distance-modulus cascade), and effective temperature (spectroscopic → BP–RP → B–V → default).
3. **Coordinates** (`common.coords.calculate_coordinates_fast`) — Propagates positions to J2016.0 using proper motions (no radial velocity) and computes `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, plus `ra_deg`, `dec_deg`, `r_pc`.
4. **Morton code** (`common.morton.add_morton_code`) — 64-bit 3D Morton code from Cartesian coordinates (cube ±200 000 pc, 21 bits per axis).

Result columns are trimmed to `OUTPUT_COLS` and written as compressed Parquet (zstd).

## Code layout

```
src/foundinspace/
  pipeline/
    cli.py              # Click root; lazy subcommand "gaia"
    constants.py        # OUTPUT_COLS, quality_flags (DIST_SRC_*, TEFF_SRC_*, PHOT_SRC_*, FLAG_*), qf_* accessors
    __main__.py         # python -m entry
    common/
      coords.py         # calculate_coordinates, calculate_coordinates_fast (ICRS → x,y,z at J2016.0)
      morton.py         # morton3d_u64_from_xyz, add_morton_code
      photometry.py     # teff_to_rgb, teff_to_hex, bv_to_teff, bp_rp_to_teff
    gaia/
      cli.py            # gaia import
      pipeline.py      # Stream VOTable → batches → Parquet (main)
      astrometry.py    # select_astrometry_gaia (DR3 / BJ_GEO / BJ_PHOTOGEO)
      photometry.py    # assign_photometry_gaia, compute_mag_abs_gaia, compute_teff_gaia
    hipparcos/
      astrometry.py    # select_astrometry_hip (Hipparcos-only)
      photometry.py    # assign_photometry_hip, compute_mag_abs_hip, compute_teff_hip
```

Hipparcos modules (`hipparcos.astrometry`, `hipparcos.photometry`) provide HIP-only astrometry and photometry for use in a Hipparcos-capable pipeline; the current CLI exposes only the Gaia import path.

## Key functions

- **`calculate_coordinates`** / **`calculate_coordinates_fast`** (`common.coords`) — Propagate ICRS positions to J2016.0 and add `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `ra_deg`, `dec_deg`, `r_pc`. The fast version uses pure NumPy (no Astropy) and assumes no radial velocity.
- **`select_astrometry_gaia`** (`gaia.astrometry`) — Four-tier distance selection (primary → weak catalog → photometric → synthetic prior) with `quality_flags` (uint16) and finite `astrometry_quality`; populates `distance_use_pc` and all `*_use_*` astrometry columns.
- **`assign_photometry_gaia`** / **`compute_mag_abs_gaia`** / **`compute_teff_gaia`** (`gaia.photometry`) — Gaia G mag, absolute magnitude cascade, and Teff cascade (spectroscopic → BP–RP → B–V → 5800 K default).
- **`add_morton_code`** (`common.morton`) — Adds `morton_code` from `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` (cube ±200 000 pc).

## Tests

```bash
pytest
```

Tests live under `tests/` (e.g. `tests/common/test_coords.py` for coordinate propagation).