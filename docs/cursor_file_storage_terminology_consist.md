# File storage terminology consistency
_Exported on 25/03/2026 at 18:20:13 CET from Cursor (2.6.21)_

---

**User**

The code is slightly inconsistent in using data vs download for file storage - can you look at what the default output options are?

---

**Cursor**


Searching the codebase for how output paths default to `data` vs `downloads`.




Here is how defaults are wired today.

### Defaults that use `downloads/`

**Hipparcos download** — single file under `downloads/`:

```11:11:src/foundinspace/pipeline/hipparcos/download.py
DEFAULT_OUTPUT = Path("downloads/hip_bright.ecsv")
```

**Identifiers download** — directory `downloads/` and three ECSV paths:

```14:17:src/foundinspace/pipeline/identifiers/download.py
DEFAULT_OUTPUT_DIR = Path("downloads")
DEFAULT_HIP_HD_OUTPUT = DEFAULT_OUTPUT_DIR / "hip_hd.ecsv"
DEFAULT_IV27A_CATALOG_OUTPUT = DEFAULT_OUTPUT_DIR / "iv27a_catalog.ecsv"
DEFAULT_IV27A_PROPER_NAMES_OUTPUT = DEFAULT_OUTPUT_DIR / "iv27a_proper_names.ecsv"
```

**Identifiers prepare / sidecar** — Parquet under `downloads/`:

```13:13:src/foundinspace/pipeline/identifiers/cli.py
DEFAULT_SIDECAR_OUTPUT = Path("downloads/star_identifiers.parquet")
```

The README matches those defaults (see the table around the `fis-pipeline hip download` / `identifiers` rows).

### Defaults that do **not** use `downloads/` (or `data/`)

**Gaia import** — no fixed output root. Parquet names are derived from each input; without `--output-dir`, outputs sit next to the input file. Mapping Parquet is `gaia_hip_map.parquet` in `--output-dir` if set, otherwise the **single** parent directory shared by all inputs:

```62:65:src/foundinspace/pipeline/gaia/cli.py
        if output_dir is not None:
            output_file = output_dir / output_name
        else:
            output_file = input_file.parent / output_name
```

```100:111:src/foundinspace/pipeline/gaia/cli.py
    if mapping_output is not None:
        return mapping_output
    if output_dir is not None:
        return output_dir / "gaia_hip_map.parquet"

    parents = {path.parent for path in input_files}
    if len(parents) != 1:
        raise click.ClickException(
            "Input files are in multiple directories; provide --mapping-output."
        )
    parent = next(iter(parents))
    return parent / "gaia_hip_map.parquet"
```

**Hip import** — same idea: co-locate with inputs unless `--output-dir`:

```39:43:src/foundinspace/pipeline/hipparcos/cli.py
        output_name = _output_path_for(input_file)
        if output_dir is not None:
            output_file = output_dir / output_name
        else:
            output_file = input_file.parent / output_name
```

### The `data/` that appears elsewhere

**Overrides** use a package-local `data/` folder — that is **bundled YAML** next to the loader, not a user download/output convention:

```13:14:src/foundinspace/pipeline/overrides/loader.py
# Packaged default: sibling `data/` next to this module (editable + wheel with packaged YAML).
_PACKAGE_DATA_DIR = Path(__file__).resolve().parent / "data"
```

So in practice: **network fetches default to `downloads/`**; **ECSV/VOT → Parquet import defaults to the input file’s directory** (or an explicit `--output-dir`); **`overrides/.../data/` is packaged source data**, not aligned with the CLI’s `downloads/` defaults.

---

**User**

And no default output folders? Where does gaia write the outputs?

---

**Cursor**


Checking the Gaia pipeline's write path for any defaults beyond the CLI.




For **Gaia**, there is **no** built-in default output directory such as `downloads/`. The CLI only picks paths from your inputs and optional flags.

**Per-input Parquet** (`fis-pipeline gaia import`):

- If you pass **`--output-dir` / `-o`**: each output is  
  `{output_dir}/{stem_from_input}.parquet`  
  (stem strips `.vot.gz` / `.vot.xz` if present).
- If you **omit** `--output-dir`: each output is  
  `{same_directory_as_input}/{stem_from_input}.parquet`.

That logic is in `import_gaia`:

```59:65:src/foundinspace/pipeline/gaia/cli.py
    for input_file in input_files:
        output_name = _output_path_for(input_file)
        if output_dir is not None:
            output_file = output_dir / output_name
        else:
            output_file = input_file.parent / output_name
```

The pipeline’s `main()` just writes to the `output_path` you pass; the docstring matches “same dir as input” only in the sense that **whoever calls it** (here, the CLI) usually puts the path next to the file:

```131:138:src/foundinspace/pipeline/gaia/pipeline.py
def main(
    input_path: Path,
    output_path: Path,
    *,
    skip_if_exists: bool = True,
    limit: int | None = None,
) -> pd.DataFrame:
    """Stream input_path (VOTable), run pipeline per batch, write {input_stem}.parquet to same dir."""
```

**Gaia↔HIP mapping Parquet** (`gaia_hip_map.parquet`):

- **`--mapping-output`**: that exact path.
- Else if **`--output-dir`**: `{output_dir}/gaia_hip_map.parquet`.
- Else: **`gaia_hip_map.parquet` in the parent directory of your inputs**, and all inputs must share one parent (otherwise Click raises and you must set `--mapping-output`).

```94:111:src/foundinspace/pipeline/gaia/cli.py
def _mapping_output_path_for(
    *,
    input_files: list[Path],
    output_dir: Path | None,
    mapping_output: Path | None,
) -> Path:
    if mapping_output is not None:
        return mapping_output
    if output_dir is not None:
        return output_dir / "gaia_hip_map.parquet"

    parents = {path.parent for path in input_files}
    if len(parents) != 1:
        raise click.ClickException(
            "Input files are in multiple directories; provide --mapping-output."
        )
    parent = next(iter(parents))
    return parent / "gaia_hip_map.parquet"
```

So in practice: **Gaia writes next to the VOT inputs unless you redirect with `--output-dir`**, and the mapping file follows that directory or an explicit `--mapping-output`. Nothing in the Gaia path defaults to `downloads/` the way Hip/identifiers downloads do.

---

**User**

I think we should split download catalogs from processed files. So, can I suggest that we update code to use data/catalogs and data/processed as the two default output folders?  These should be possible to override in cli or in .env using decouple.  Does that fit with the code and make sense?

---

**Cursor**

Yes, that direction fits the current structure well; I’ll quickly map where defaults are defined now and call out the minimal set of places to centralize `data/catalogs` and `data/processed` with CLI + `.env` overrides.



Yes — that split is coherent with the current code and a good cleanup.

- `data/catalogs` for raw downloaded catalogs
- `data/processed` for derived parquet outputs / sidecars

It matches how the code already behaves conceptually: download commands and processing commands are separate, they just currently use mixed defaults (`downloads/...` vs “next to input”).

### How it maps cleanly

- **Catalog defaults (`data/catalogs`)**
  - `hip download` default output in `src/foundinspace/pipeline/hipparcos/download.py`
  - `identifiers download` defaults in `src/foundinspace/pipeline/identifiers/download.py`
- **Processed defaults (`data/processed`)**
  - `identifiers prepare` sidecar default in `src/foundinspace/pipeline/identifiers/cli.py`
  - `gaia import` / `hip import` default `--output-dir` in:
    - `src/foundinspace/pipeline/gaia/cli.py`
    - `src/foundinspace/pipeline/hipparcos/cli.py`
  - Gaia mapping output default (`gaia_hip_map.parquet`) should also live under processed by default

### `.env` + CLI override strategy (recommended)

Use precedence: **CLI flag > `.env` > hardcoded fallback**.

Example env keys:
- `FIS_CATALOGS_DIR=data/catalogs`
- `FIS_PROCESSED_DIR=data/processed`

You already have `python-decouple` in dependencies (`pyproject.toml`) but it is not used yet in `src/` today, so this is a natural place to start using it.

### One important behavior decision

Right now `gaia import` and `hip import` default to writing next to input files.  
Switching to `data/processed` as default is cleaner, but changes behavior. That’s fine, just document it in `README.md` and release notes.

### Minor caveat

Current import CLIs check that output directories already exist (and error otherwise). If you adopt fixed defaults like `data/processed`, it’s nicer to auto-create them (like the download code already does for parent dirs).

So overall: **yes, fits the codebase and makes architectural sense**. If you want, I can sketch the exact constants/module layout and env var plumbing before you switch out of Ask mode.

---

**User**

Yes - please implement with auto-create and update readme to reflect. 

---

**Cursor**

I’m implementing this now with a shared env-backed path config, wiring defaults to `data/catalogs` and `data/processed`, then updating import commands to auto-create output directories and refreshing README defaults.



I found all call sites; next I’m adding a small shared paths module (`decouple`-backed) and then rewiring each CLI/download default to use it.



I’ve finished the code path changes; I’m now updating `README.md` so the new defaults and env override behavior are explicit and accurate.



Implemented. Defaults are now split into catalog vs processed roots, both env-configurable, with CLI override and auto-create behavior.

### What changed

- Added shared path config in `src/foundinspace/pipeline/paths.py`:
  - `FIS_CATALOGS_DIR` (default `data/catalogs`)
  - `FIS_PROCESSED_DIR` (default `data/processed`)
- Updated download defaults to `data/catalogs`:
  - `src/foundinspace/pipeline/hipparcos/download.py`
  - `src/foundinspace/pipeline/identifiers/download.py`
- Updated processed output defaults to `data/processed`:
  - `src/foundinspace/pipeline/gaia/cli.py` (`--output-dir` default + mapping default behavior)
  - `src/foundinspace/pipeline/hipparcos/cli.py` (`--output-dir` default)
  - `src/foundinspace/pipeline/identifiers/cli.py` (sidecar default output)
- Added auto-create for output directories in import flows:
  - `gaia import` now creates `--output-dir` and mapping parent dir as needed.
  - `hip import` now creates `--output-dir` as needed.
- Updated docs in `README.md`:
  - New default locations (`data/catalogs`, `data/processed`)
  - Updated `gaia import` / `hip import` option defaults
  - Added `.env` override section and precedence note (CLI still wins)

### Behavior now

- **Catalog downloads** default under `data/catalogs`.
- **Processed parquet + sidecars** default under `data/processed`.
- User can override by:
  - CLI flags (`--output-dir`, `--output`, etc.), or
  - `.env` via `FIS_CATALOGS_DIR` / `FIS_PROCESSED_DIR`.
- Directories are created automatically where needed.

### Validation

- Lint diagnostics: no new issues on edited files.
- Import smoke test succeeded for updated modules.
- Full `pytest` run failed at collection due to an existing test module name collision (`tests/hipparcos/test_pipeline.py` vs `tests/identifiers/test_pipeline.py`), unrelated to these path changes.

I also noticed an unrelated untracked file in the tree: `src/foundinspace/pipeline/overrides/data/binaries.yaml`. I didn’t touch it; tell me if you want me to include or ignore it in any follow-up workflow (e.g. commit prep).

---

**User**

Where does gaia write the output files to? Since these inherit the input filenames, should we either write these to their own folder, or normalise the naming?

---

**Cursor**

Gaia now writes outputs into a **single output directory**, not next to inputs.

```24:30:src/foundinspace/pipeline/gaia/cli.py
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(file_okay=False, path_type=Path),
    default=PROCESSED_DIR,
    show_default=True,
)
```

```50:67:src/foundinspace/pipeline/gaia/cli.py
output_root = Path(output_dir).expanduser()
output_root.mkdir(parents=True, exist_ok=True)

...
for input_file in input_files:
    output_name = _output_path_for(input_file)
    output_file = output_root / output_name
```

And `PROCESSED_DIR` defaults to `data/processed` (overridable via env var):

```14:15:src/foundinspace/pipeline/paths.py
CATALOGS_DIR = Path(config("FIS_CATALOGS_DIR", default="data/catalogs")).expanduser()
PROCESSED_DIR = Path(config("FIS_PROCESSED_DIR", default="data/processed")).expanduser()
```

Filename logic is currently “input stem + `.parquet`” (strips `.vot.gz` / `.vot.xz`):

```80:89:src/foundinspace/pipeline/gaia/cli.py
def _output_path_for(input_path: Path) -> str:
    """Parquet output filename for a given VOTable input (stem from name)."""
    name_lower = input_path.name.lower()
    if name_lower.endswith(".vot.gz"):
        output_base = input_path.name[: -len(".vot.gz")]
    elif name_lower.endswith(".vot.xz"):
        output_base = input_path.name[: -len(".vot.xz")]
    else:
        output_base = input_path.stem
    return f"{output_base}.parquet"
```

You asked whether to use a dedicated folder or normalize names:
- You already have the dedicated folder (`data/processed`) now, which is good.
- I **would** also normalize naming a bit, because two inputs with same stem from different dirs can collide in one output dir.

Practical recommendation:
- Keep readable naming, but prefix by dataset/source, e.g. `gaia_<stem>.parquet`.
- If collisions are still possible, append a short deterministic suffix (e.g., hash of input path or batch label).

So: **yes, own folder + light normalization** is the safest combo.

---

**User**

Yes - but hipparcos and the other write there too - so no way of telling which are the gaia 'package' files and which are the other datasources. 

---

**Cursor**

Exactly — that’s a real ambiguity with the current setup.

Right now all processed outputs share `FIS_PROCESSED_DIR`:
- Gaia import writes `{stem}.parquet` there.
- Hip import writes `{stem}.parquet` there too.
- Identifiers writes `star_identifiers.parquet` there.

So source provenance is mostly implicit in filenames, and collisions are possible if stems overlap.

The clean fix is to keep one processed root but split by source:

- `data/processed/gaia/`
- `data/processed/hipparcos/`
- `data/processed/identifiers/`

Then either:
- keep existing stem naming inside each subdir (simple, low disruption), or
- add normalized prefixes (`gaia_<stem>.parquet`, `hip_<stem>.parquet`) for extra clarity.

Recommended target layout:

- `data/processed/gaia/<input-stem>.parquet`
- `data/processed/gaia/gaia_hip_map.parquet`
- `data/processed/hipparcos/<input-stem>.parquet`
- `data/processed/identifiers/star_identifiers.parquet`

This gives:
- clear source grouping,
- no cross-source filename clashes,
- easier cleanup/reprocessing per source,
- nicer downstream discovery patterns (`data/processed/gaia/*.parquet`).

If you want, I can draft the exact env var/CLI precedence for this next (e.g. `FIS_PROCESSED_DIR` + optional source-specific overrides) before implementing.

---

**User**

it's only gaia that has multiple input files. I therefore suggest we put the gaia pipeline files into the data/processed/gaia output  as you suggested. We should NOT put the gaia_hip_map in that folder, since again it means we can't use a wildcard. It should go at the top level alongside the others. 

We should also rename both hipparcos and identifiers to start with the dataset name, e.g. hip_stars.parquet, overrides_stars.? and identifiers_map.parquet

That way everything is deterministic and we can pick up gaia files with a wildcard.

---

**Cursor**

That's a clean design. Let me confirm the target layout:

```
data/processed/
  gaia/                        # subdirectory for multi-file Gaia outputs
    <input-stem>.parquet       # one per VOT input, wildcard-friendly
  gaia_hip_map.parquet         # top level, not inside gaia/
  hip_stars.parquet            # deterministic, single file
  identifiers_map.parquet      # deterministic, renamed from star_identifiers
  overrides_stars.parquet      # future, deterministic
```

The concrete changes needed:

1. **`paths.py`** — add `PROCESSED_GAIA_DIR = PROCESSED_DIR / "gaia"`.

2. **`gaia/cli.py`** — `--output-dir` defaults to `PROCESSED_GAIA_DIR`. Mapping default changes from `output_dir / "gaia_hip_map.parquet"` to `PROCESSED_DIR / "gaia_hip_map.parquet"` (top-level, decoupled from `--output-dir`).

3. **`hipparcos/cli.py`** — output becomes deterministic `PROCESSED_DIR / "hip_stars.parquet"` instead of deriving from input stem. The `--output-dir` option would become `--output` pointing to a single file (or keep `--output-dir` and hardcode the filename).

4. **`identifiers/cli.py`** — `DEFAULT_SIDECAR_OUTPUT` renamed from `star_identifiers.parquet` to `identifiers_map.parquet`.

5. **README.md** — update the table and options sections to reflect new paths.

One design question on hipparcos: currently `hip import` accepts `nargs=-1` (multiple files), same as gaia. If it's always a single input, should we tighten that to `nargs=1` at the same time, or keep multi-input support and just overwrite/append to `hip_stars.parquet`?

And for the overrides output — you put a `?` on the extension. Is `overrides_stars.parquet` right, or are you thinking of a different format/name? That one doesn't have a CLI yet, so it's just forward-planning for now.

---

**User**

Well, we have the inputs for the overrides, we need to figure out what the output format is. That's why I put the question mark. 

---

**Cursor**

Right, let me look at what the overrides loader currently produces to understand the shape of the data.

That makes sense — the overrides format is genuinely undecided because the YAML data has a richer schema than the existing `OUTPUT_COLS` pipeline output.

Here's what the overrides currently carry vs what the pipeline output schema expects:

**`OUTPUT_COLS`** (what Gaia and Hip pipelines produce):
- `source`, `source_id`, `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `mag_abs`, `teff`, `quality_flags`, `astrometry_quality`, `photometry_quality`

**Override YAML stars** carry all of those core fields (or enough to derive them), plus:
- **Override metadata**: `override_id`, `action`, `override_reason`, `override_policy_version`
- **Provenance refs**: `position_ref`, `distance_ref`, `photometry_ref`, `temperature_ref`
- **Extra photometry**: `photometry_band`, `photometry_value`, `photometry_note`
- **Audit**: `_source_file`
- **Action semantics**: `add` / `replace` / `drop` — where `drop` rows have no payload at all

The question is whether the overrides pipeline should:

1. **Emit the same `OUTPUT_COLS` schema** as Gaia/Hip (so they're directly mergeable as rows) — `overrides_stars.parquet`. Drop rows and metadata get lost or go to a sidecar.

2. **Emit a wider table** that preserves override metadata alongside the star payload — merger needs that metadata to know *how* to apply each row (add vs replace vs drop).

3. **Split into two files**: `overrides_stars.parquet` (payload rows for `add`/`replace`, conforming to `OUTPUT_COLS`) plus `overrides_actions.parquet` or similar (all rows including `drop`, with `override_id`, `action`, target keys).

Option 3 aligns best with the merger design in `docs/mergers.md` — the merger needs to know actions and targets, not just replacement payloads. And `drop` rows have no star payload at all, so they don't fit `OUTPUT_COLS`.

So the natural split might be:
- `data/processed/overrides_actions.parquet` — every override row with `override_id`, `action`, `source`, `source_id`, `override_reason`, `override_policy_version`
- `data/processed/overrides_stars.parquet` — `add`/`replace` rows conforming to `OUTPUT_COLS` (or `OUTPUT_COLS` + `override_id` as a join key)

Or a single wider table if you'd rather keep it simple and let the merger filter by `action`. That's probably the least-friction option: one `overrides.parquet` with `OUTPUT_COLS` + override metadata columns, where `drop` rows have nulls for the payload columns.

What direction feels right?

---

**User**

So, once we have all these files, the next step is for us to build the merger, so we have a lot of power to decide how to do that effectively. The bottleneck is the HUGE number of gaia stars. We have also decided to shard by healpix L3 so we're independent of any upstream stages - but we still need to figure out how to do this accurately without loading all stars into memory. So have a good think about how we use gaia_to_hip + quality metrics and the overrides to create a single, uniform dataset without doing expensive mergers. 

---

**Cursor**

This is the right question to think through carefully. Let me trace the data volumes and shapes, then work through a design.

OK. Here's the full picture and a concrete design.

---

### Data volumes

| Input | Rows | In memory? |
|-------|------|-----------|
| Gaia processed files | ~1.5 billion | **No** — must stream |
| Hipparcos processed | ~118k | Yes, trivially |
| `gaia_hip_map` | ~118k max | Yes, trivially |
| Overrides | Dozens | Yes, trivially |
| Identifiers sidecar | ~4k | Yes, trivially |

The critical insight: **everything except Gaia fits in memory.** The cross-match set is tiny relative to Gaia. So the merge strategy should be: **pre-compute all decisions from the small tables, then stream Gaia once, applying those decisions as set-membership lookups.**

---

### Phase 0: Build the decision table (all in memory, fast)

Load the three small inputs:
1. `hip_stars.parquet` — full dataframe, ~118k rows
2. `gaia_hip_map.parquet` — `gaia_source_id` → `hip_source_id`, ~118k rows
3. Overrides — normalized star dicts, dozens of rows

Then compute:

- **`override_drop_keys`**: `Set[(source, source_id)]` — rows to suppress entirely
- **`override_replace_map`**: `Dict[(source, source_id) → replacement row]` — curated replacements
- **`override_add_rows`**: `List[row]` — new stars to insert (Sun, etc.)
- **`matched_hip_ids`**: the set of `hip_source_id` values that appear in the cross-match
- **`gaia_exclude`**: `Set[gaia_source_id]` — Gaia rows to skip (because Hip wins the quality contest, or override drops them)
- **`hip_exclude`**: `Set[hip_source_id]` — Hip rows to skip (because Gaia wins, or override drops them)

For the quality scoring of matched pairs: you only need the Hip side's quality metrics (already loaded) and the Gaia side's quality metrics for ~118k matched `source_id` values. You have two options:

**Option A — Pre-scan Gaia for matched quality only.** Read only `source_id`, `quality_flags`, `astrometry_quality` columns from the Gaia files, filter to the ~118k matched IDs. This is a columnar scan (fast with PyArrow predicate pushdown), no full deserialization. Then score all pairs and populate `gaia_exclude` / `hip_exclude`.

**Option B — Default "prefer Gaia" rule.** Per your M3 tie-break, Gaia already wins ties. If you adopt a policy of "Gaia wins for all matched pairs unless an override says otherwise," the quality scan becomes unnecessary. The override YAML is already designed to handle exactly the cases where Hipparcos is better (binaries, etc.). This makes the decision table trivially computable from just the map + overrides.

I'd recommend **Option B** for the first pass. It's correct by construction: the overrides *are* the quality exceptions. You can always add Option A scoring later without changing the streaming architecture.

With Option B, the decision sets become:

```python
# Pseudocode for phase 0
gaia_hip_map = read_parquet("gaia_hip_map.parquet")
matched_hip_ids = set(gaia_hip_map["hip_source_id"])

# Overrides
override_drops = {(s["source"], s["source_id"]) for s in overrides if s["action"] == "drop"}
override_replaces = {(s["source"], s["source_id"]): s for s in overrides if s["action"] == "replace"}

# Gaia wins all matched pairs (M3 default)
# So exclude ALL matched Hip rows, unless an override replaces/drops the Gaia side
hip_exclude = matched_hip_ids  # start by excluding all matched Hip rows
# (override replaces targeting hip rows will re-inject curated data anyway)

# Gaia exclusions: only from explicit drop overrides targeting gaia rows
gaia_exclude = {sid for (src, sid) in override_drops if src == "gaia"}
```

---

### Phase 1: Stream Gaia, shard by HEALPix L3

This is the only expensive pass. Process each `data/processed/gaia/*.parquet` file:

1. Read with PyArrow (can batch via `read_table` or `ParquetFile.iter_batches`)
2. Filter out `source_id in gaia_exclude` (tiny set, O(1) lookup)
3. Apply `replace` overrides for any matching `(source="gaia", source_id)` — very few hits per file
4. **Compute HEALPix L3 pixel** from the Cartesian coordinates
5. Append each row group to the appropriate shard

**HEALPix from Cartesian**: The processed files already compute `ra_deg` and `dec_deg` in `calculate_coordinates_fast`, but `OUTPUT_COLS` currently trims them. You have two choices:
- **Add `ra_deg`, `dec_deg`, `r_pc` to `OUTPUT_COLS`** — the mergers doc already says the dense merged table "should include" these. This is the cleaner fix since you need them for sharding anyway, and they're already computed.
- Or derive from Cartesian at merge time: `healpy.vec2pix(nside=8, x, y, z, nest=True)` works directly on the unit vector (healpy normalizes internally). This avoids changing upstream output, but re-derives what was already computed.

I'd suggest adding them to `OUTPUT_COLS` — they're cheap to persist and the mergers doc already requires them.

**Shard output**: HEALPix L3 at nside=8 gives **768 pixels**. You can:
- Keep 768 `ParquetWriter` objects open (768 file handles — fine on Linux)
- Or buffer batches in memory per pixel and flush periodically (memory-bounded by buffer size, not total catalog)

Shards go into e.g. `data/processed/merged/hp3_<pixel>.parquet`.

---

### Phase 2: Add non-Gaia rows to shards

After the Gaia streaming pass:

1. **Hip survivors**: `hip_stars` where `source_id NOT IN hip_exclude` and not override-dropped. Compute their HEALPix pixel, append to the matching shard files.
2. **Override `replace` rows targeting Hip**: already have curated coordinates, compute pixel, append.
3. **Override `add` rows** (Sun, etc.): compute pixel, append.

This is tiny (~118k Hip rows at most, plus dozens of overrides), so it's instant.

---

### Phase 3: Finalize shards

For each of the 768 shard files:
- Sort by `r_pc` ascending (per your shard ordering contract)
- Rewrite as final compressed Parquet (zstd)
- This can be done shard-by-shard, each fits in memory

---

### Why this avoids expensive merges

| Operation | Cost |
|-----------|------|
| Decision table build | ~118k rows, in memory, instant |
| Gaia streaming | Single linear scan, no join — just `set.__contains__` per row |
| HEALPix assignment | Vectorized NumPy, no join |
| Shard writes | Append-only, no cross-file coordination |
| Hip/override injection | ~118k + dozens of rows, instant |
| Per-shard sort | ~2M rows average per shard, in memory, fast |

There is **zero join between Gaia and Hipparcos** at merge time. The cross-match is resolved entirely from the pre-computed decision sets. The Gaia scan is embarrassingly parallel per input file if you want to go there later.

---

### One upstream change needed

`OUTPUT_COLS` should include `ra_deg`, `dec_deg`, `r_pc` — they're already computed in `calculate_coordinates_fast` but currently trimmed. The mergers doc requires them in the dense output anyway, and without them you'd have to re-derive angles from Cartesian at merge time for HEALPix pixel assignment. Adding them now to the per-catalog pipeline output means the merger can assign pixels with a simple vectorized `healpy.ang2pix` call.

---

**User**

Sorry, what's the M3 tie-break?

---

**Cursor**


**M3** is the deterministic tie-break when two candidates for a matched pair have the **same quality score**.

From `docs/mergers.md`:

1. **Prefer Gaia**  
2. If still tied, **lower canonical `source_id`** (under a fixed ordering)

So “Gaia wins on tie” is explicit policy, not an informal default.

