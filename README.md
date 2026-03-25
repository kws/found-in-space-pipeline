# Found in Space - Pipeline

A pipeline for reducing **Gaia** and **Hipparcos** data to produce a 3D map of stars. It ingests catalogue data (VOTables), applies astrometry and photometry selection, propagates positions to a common epoch, and outputs **sun-centric Cartesian coordinates** (ICRS, in parsecs) plus derived quantities suitable for visualization or further processing.

## Output

The pipeline produces Parquet tables with a fixed schema. Each row is a star with:

- **Sun-centric Cartesian coordinates** (ICRS frame, J2016.0): `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` (parsecs). The origin is the Sun; axes follow the ICRS convention.
- **Identifiers and source**: `source` (e.g. `"gaia"` or `"hip"`), `source_id`.
- **Photometry**: `mag_abs` (absolute magnitude), `teff` (effective temperature, K), `photometry_quality` (magnitude uncertainty).
- **Provenance and quality**: `quality_flags` (packed uint16: distance source, Teff source, photometry source, validity and review bits), `astrometry_quality` (e.g. fractional parallax error or Bailer-Jones interval width; finite, no inf).

**Morton codes and octree statistics are not produced here**; they belong in a downstream indexer. That stage can shard the merged catalog by **2D sky tiles** (e.g. HEALPix) first so 3D structures do not require scanning every file in the dataset.

See `OUTPUT_COLS` in `foundinspace.pipeline.constants` for the full list.

## End-to-end plan (Gaia + Hipparcos + merge)

Per-catalog CLIs produce staging Parquet. A future **merge** step (see `docs/mergers.md`) will:

1. Run **Gaia** and **Hipparcos** pipelines on their respective inputs.
2. Run an **overrides pipeline** that normalizes a versioned **manual overrides** table (e.g. missing objects like the Sun, or replacements where Hipparcos binary solutions are poor).
3. **Merge** using a cross-match table, quality scoring for Gaia-vs-Hip pairs, with **manual overrides taking precedence** over automatic winners.
4. Emit a **dense** merged table suitable for Stage 00, optionally **partitioned by HEALPix** (or similar) for efficient downstream octree or spatial queries.
5. Emit **sparse sidecars** (identifiers, HD/Bayer designations, merge decisions) keyed by canonical `source_id`, so rare columns are not duplicated on every row.

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
| `fis-pipeline gaia import INPUT [INPUT ...]` | Read Gaia VOTable(s) (`.vot`, `.vot.gz`, `.vot.xz`), run the Gaia pipeline per batch, write `{stem}.parquet` under `data/processed/gaia` by default (or under `--output-dir`), and emit one Gaia↔HIP mapping sidecar for the command run. |
| `fis-pipeline hip import INPUT` | Read a Hipparcos ECSV file, run the Hipparcos pipeline, and write a deterministic output file (default: `data/processed/hip_stars.parquet`). |
| `fis-pipeline hip download` | Download Hipparcos New Reduction catalog (`I/311/hip2`) to ECSV (default: `data/catalogs/hip_bright.ecsv`). |
| `fis-pipeline identifiers download` | Download identifier source catalogs (`I/239/hip_main`, `IV/27A/catalog`, `IV/27A/table3`) to ECSV files in `data/catalogs/`. |
| `fis-pipeline identifiers prepare` | Build a wide identifiers sidecar parquet keyed by `hip_source_id` (default: `data/processed/identifiers_map.parquet`). |
| `fis-pipeline identifiers build` | Run `identifiers download` then `identifiers prepare` in one command. |

**Options for `gaia import`:**

- `--output-dir`, `-o` — Directory for Gaia output Parquet files (default: `data/processed/gaia`).
- `--mapping-output` — Run-level Gaia↔HIP mapping sidecar path (default: `data/processed/gaia_hip_map.parquet`).
- `--force`, `-f` — Overwrite existing output files.
- `--limit`, `-l` — Stop after this many output rows (for testing).

**Options for `hip import`:**

- `--output`, `-o` — Hipparcos output path (default: `data/processed/hip_stars.parquet`).
- `--force`, `-f` — Overwrite existing output files.
- `--limit`, `-l` — Stop after this many output rows (for testing).
- Input format is currently ECSV only.

### Default storage layout and `.env` overrides

By default, the pipeline uses:

- `data/catalogs` for downloaded source catalogs (ECSV).
- `data/processed` for derived Parquet outputs and sidecars, including:
  - Gaia stars in `data/processed/gaia/*.parquet`
  - Gaia↔HIP map at `data/processed/gaia_hip_map.parquet`
  - Hipparcos stars at `data/processed/hip_stars.parquet`
  - Identifiers sidecar at `data/processed/identifiers_map.parquet`

Both roots are configurable via environment variables (for example in a `.env` file):

- `FIS_CATALOGS_DIR` (default: `data/catalogs`)
- `FIS_PROCESSED_DIR` (default: `data/processed`)

CLI flags still take precedence. Output directories are auto-created as needed.

**Identifiers sidecar schema (`identifiers prepare`)**

The sidecar is intentionally **wide** and small, with one row per HIP identifier:

- `hip_source_id` (`uint64`)
- `hd` (`Int64`, nullable)
- `bayer` (`string`, nullable display value such as `alpha Cas`)
- `fl` (`Int64`, nullable Flamsteed number from IV/27A)
- `cst` (`string`, nullable constellation abbreviation from IV/27A)
- `proper_name` (`string`, nullable; first proper name by HD from IV/27A/table3)

Rows are emitted only when at least one of `bayer` or `proper_name` is present.

## Pipeline stages (Gaia)

For each batch of Gaia data the pipeline:

1. **Astrometry** (`gaia.astrometry.select_astrometry_gaia`) — Multi-tier distance: Tier A = best of DR3 / BJ geometric / BJ photogeometric (quality-tested); Tier B = weak catalog fallback; Tier C = photometric (M_G + A_G); Tier D = synthetic prior. Sets `distance_use_pc`, `quality_flags`, `*_use_*` columns. No row dropped for bad astrometry alone.
2. **Photometry** (`gaia.photometry.assign_photometry_gaia`, `compute_mag_abs_gaia`, `compute_teff_gaia`) — Apparent magnitude (G), absolute magnitude (GSP-Phot / extinction-corrected / distance-modulus cascade), and effective temperature (spectroscopic → BP–RP → B–V → default).
3. **Coordinates** (`common.coords.calculate_coordinates_fast`) — Propagates positions to J2016.0 using proper motions (no radial velocity) and computes `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, plus `ra_deg`, `dec_deg`, `r_pc`.

Result columns are trimmed to `OUTPUT_COLS` and written as compressed Parquet (zstd).

## Code layout

```
src/foundinspace/
  pipeline/
    cli.py              # Click root; lazy subcommands "gaia", "hip", "identifiers"
    constants.py        # OUTPUT_COLS, quality_flags (DIST_SRC_*, TEFF_SRC_*, PHOT_SRC_*, FLAG_*), qf_* accessors
    __main__.py         # python -m entry
    common/
      coords.py         # calculate_coordinates, calculate_coordinates_fast (ICRS → x,y,z at J2016.0)
      photometry.py     # teff_to_rgb, teff_to_hex, bv_to_teff, bp_rp_to_teff
    gaia/
      cli.py            # gaia import
      pipeline.py      # Stream VOTable → batches → Parquet (main)
      astrometry.py    # select_astrometry_gaia (DR3 / BJ_GEO / BJ_PHOTOGEO)
      photometry.py    # assign_photometry_gaia, compute_mag_abs_gaia, compute_teff_gaia
    hipparcos/
      cli.py           # hip download, hip import
      download.py      # fetch Hipparcos ECSV from Vizier
      pipeline.py      # ECSV -> Hipparcos transforms -> parquet
      astrometry.py    # select_astrometry_hip (Hipparcos-only)
      photometry.py    # assign_photometry_hip, compute_mag_abs_hip, compute_teff_hip
    identifiers/
      cli.py           # identifiers download, prepare, build
      download.py      # fetch HIP/HD + Bayer/Flamsteed + proper-name catalogs
      pipeline.py      # build wide identifiers sidecar parquet
```

Hipparcos pipeline is available via `fis-pipeline hip import` for ECSV inputs.

## Key functions

- **`calculate_coordinates`** / **`calculate_coordinates_fast`** (`common.coords`) — Propagate ICRS positions to J2016.0 and add `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `ra_deg`, `dec_deg`, `r_pc`. The fast version uses pure NumPy (no Astropy) and assumes no radial velocity.
- **`select_astrometry_gaia`** (`gaia.astrometry`) — Four-tier distance selection (primary → weak catalog → photometric → synthetic prior) with `quality_flags` (uint16) and finite `astrometry_quality`; populates `distance_use_pc` and all `*_use_*` astrometry columns.
- **`assign_photometry_gaia`** / **`compute_mag_abs_gaia`** / **`compute_teff_gaia`** (`gaia.photometry`) — Gaia G mag, absolute magnitude cascade, and Teff cascade (spectroscopic → BP–RP → B–V → 5800 K default).

## Tests

```bash
pytest
```

Tests live under `tests/` (e.g. `tests/common/test_coords.py` for coordinate propagation).
