# Mergers specification

## Purpose

Define how Gaia, Hipparcos, and **manual overrides** are merged into one canonical dataset for downstream processing (Stage 00 onward).

The merger resolves duplicates using a cross-match table and quality rules, applies **highest-precedence manual corrections**, then emits Parquet compatible with Stage 00 and Stage 01. **Morton codes and octree construction are not part of this data pipeline**; they belong in a downstream step that can scan **2D spatial shards** (e.g. HEALPix) instead of the full catalog.

## Scope

This specification covers:

- when merge occurs relative to per-catalog pipelines and spatial sharding
- required inputs and outputs (including manual overrides)
- duplicate-resolution rules and override precedence
- schema contract for the **dense** merged star table vs **sparse** sidecars

This specification does not cover:

- reverse/global lookup indices (implementation detail of serving)
- catalog-specific scientific quality policy beyond the tie-break and scoring contract

---

## Stage placement

### Per-catalog preprocessing

1. **Gaia pipeline** — ingest, astrometry/photometry, coordinates → per-batch or staging Parquet.
2. **Hipparcos pipeline** — same role for Hipparcos-only rows.
3. **Overrides pipeline** — normalize the manual overrides table (versioned artifact; see below).

### Merge (pre–Stage 00)

Merge runs **after** those three inputs exist and **before** Stage 00:

`gaia parquet set + hip parquet + crossmatch + manual_overrides → merged canonical parquet → Stage 00 → Stage 01`

### Spatial sharding (after merge, before 3D indexing)

After merge, the combined dataset is written into **2D spatial shards** (recommended: HEALPix at a fixed level, consistent with download partitioning where applicable). Rationale:

- Octree or other 3D structures can be built by reading only tiles that intersect a node, instead of scanning the entire catalog.
- Morton encoding, if used at all, is a concern of that **downstream** indexer, not of the merge/Stage-00 Parquet schema.
- To improve locality for downstream 3D passes at low cost, sort rows **within each 2D shard** by `r_pc` (distance) as the default policy.
- If profiling later shows meaningful gains, replace or extend that with a quantized spherical ordering key (sub-tile angular key interleaved with radial key), while keeping deterministic ordering guarantees.

### Rationale summary

- Merge requires cross-catalog joins that Stage 00 file-local transforms do not perform.
- Quality metrics for Gaia vs Hip winner selection are available from catalog preprocessing.
- Merging before Stage 00 avoids duplicating heavy work on rows that will be dropped or replaced.
- **Sparse identifiers** (HD, Bayer, multiple catalog IDs on one object) are kept in **sidecars** so the main table does not carry mostly-null columns across billions of rows.

---

## Inputs

Required:

- Gaia source parquet set (or equivalent table), after Gaia pipeline preprocessing
- Hipparcos source parquet (or equivalent table), after Hipparcos pipeline preprocessing
- Cross-match table mapping Gaia `source_id` and Hipparcos `HIP` (or equivalent) for matched pairs
- **Manual overrides** table (versioned): explicit `add`, `replace`, and optional `drop` / suppress actions (see below)

Optional:

- Additional identifier tables (HD, Bayer, Flamsteed, etc.) joined **into sidecars**, not necessarily into every row of the dense merge output
- A provisional Gaia-import mapping sidecar (`gaia_source_id` ↔ `hip_source_id`, when Gaia input includes `hip`) to reduce re-scan cost. This artifact is advisory and can be superseded by merge-time crossmatch/override resolution.

---

## File layout and naming conventions

Use a fixed split between raw catalogs and processed artifacts:

- Raw catalog downloads: `data/catalogs/`
- Processed artifacts used by merger and downstream stages: `data/processed/`

Within `data/processed/`, current naming policy is deterministic and source-aware:

- Gaia stars: `data/processed/gaia/*.parquet` (one file per Gaia input, wildcard-friendly)
- Gaia/Hip mapping: `data/processed/gaia_hip_map.parquet` (top level, intentionally **not** under `gaia/`)
- Hipparcos processed stars: `data/processed/hip_stars.parquet`
- Identifier mapping sidecar: `data/processed/identifiers_map.parquet`
- Manual overrides (merger input): `data/processed/overrides.parquet` — single wide table: `OUTPUT_COLS` plus `override_id`, `action`, `override_reason`, `override_policy_version`. Non-drop rows use `DIST_SRC_OVERRIDE` in `quality_flags` (with `FLAG_DIST_VALID`); `astrometry_quality` / `photometry_quality` are `0.0` placeholders. `drop` rows null payload columns in `OUTPUT_COLS` except `source` / `source_id` for targeting.

Rationale:

- Avoid mixing raw downloads with derived artifacts.
- Keep Gaia multi-file outputs discoverable via wildcard without pulling in non-Gaia files.
- Keep single-file products deterministic for stable pipeline wiring and easier auditing.

---

## Manual overrides

Overrides correct or supplement catalog data where automation is wrong or incomplete, for example:

- **Missing objects** — e.g. the Sun, or other bodies not represented as normal catalog rows.
- **Poor binary / composite solutions** — e.g. Hipparcos parameters for some resolved binaries; curated astrometry/photometry replaces the catalog row.

Each override row should carry at least:

- `override_id` (stable string or integer)
- `action`: `add` | `replace` | `drop` (or equivalent)
- `override_reason` / category (e.g. `missing_star`, `binary_quality`, `manual_astrometry`)
- `override_policy_version` (for reproducibility)
- Target key to match existing rows: `source` + `source_id` (crossmatch partner lookup is handled by the merger; see "Pair-aware override resolution")
- For `add` / `replace`: fields required by the merged schema (coordinates, `mag_abs`, etc.) or pointers to a curated row definition

### Override target key

Override targeting uses the compound star key:

- `source`
- `source_id`

where `source` identifies the namespace/catalog (for example `gaia`, `hip`, or `manual`) and `source_id` is only unique within that namespace.

Action-specific minimums:

- `drop`: provide only target identity (`source`, `source_id`) plus audit metadata (`override_id`, `override_reason`, `override_policy_version`, `action`). No astrometry/photometry payload is required.
- `replace`: provide target identity plus replacement payload fields required by merged output.
- `add`: provide new identity (or enough information to mint canonical identity) plus payload fields required by merged output.

### Precedence

**Manual overrides outrank automatic merge decisions:**

1. `drop` / suppress — remove or exclude the targeted catalog row(s) from the merged dense output (or mark excluded per policy).
2. `replace` — substitute curated astrometry/photometry (and identity links) for the matched object; automatic Gaia-vs-Hip scoring does not apply to that conflict.
3. `add` — insert a row with a canonical `source_id` that may not exist in either catalog (namespace rules below).
4. Otherwise — apply **M1–M3** for unmatched and matched pairs.

For `drop`, matching is by exact compound key (`source`, `source_id`), and the resolved row is removed from dense output.

### Pair-aware override resolution

Override YAML targets a **single** row by (`source`, `source_id`). The merger resolves overrides at the **matched-pair** level using the crossmatch table (`gaia_hip_map.parquet`):

- If a `replace` or `drop` override targets one side of a crossmatch-linked pair, the **entire pair** is resolved by the override. The partner row from the other catalog is suppressed — it does not appear as an unmatched row or compete in quality scoring.
- Override authors do **not** need to supply the partner's ID. The crossmatch map already contains the relationship, and requiring manual partner lookups would be error-prone and redundant.
- The merge decisions sidecar (M5) records both the override action and the suppressed partner, e.g.: "HIP 71683 replaced by `manual.alpha_cen_a.replace.v1`; Gaia counterpart `<gaia_source_id>` suppressed via crossmatch."
- For `add` overrides (which target `source=manual`), no crossmatch lookup applies — the row is inserted directly.

### Magnitude-limited Gaia subsets and missing crossmatch partners

Gaia data may be imported with a magnitude limit (`--mag-limit`), so a `gaia_source_id` present in the crossmatch table may have **no corresponding row** in the actual Gaia batch files. The merger must handle this gracefully in both override and non-override paths.

**Non-override path (matched pairs with a missing side):**

- If the crossmatch links Gaia X ↔ HIP Y but Gaia X has no row in the Gaia data, the pair is treated as having only one present side. HIP Y becomes an **effectively unmatched** Hipparcos row (kept per M1). No quality scoring or winner selection occurs — there is no competitor.
- The converse (HIP side missing) is unlikely in practice but follows the same logic: the present Gaia row is kept as unmatched.

**Override path — target present, partner missing:**

- Override targets `source=hip, source_id=Y`. The crossmatch maps Y to Gaia X, but Gaia X was excluded by mag-limit. The override applies normally to the HIP row. There is no Gaia partner to suppress — this is not an error.

**Override path — target missing, partner present:**

- Override targets `source=gaia, source_id=X`, but X was excluded by mag-limit. The crossmatch maps X to HIP Y, which **does** exist. The merger must still resolve the pair: the override payload replaces or drops as specified, and HIP Y is suppressed. The override is not silently skipped just because its literal target catalog row is absent.

**Override path — neither side present:**

- Override targets a row where both the target and any crossmatch partner are absent from the loaded data. The merger emits a **warning** (the override has no effect) and records this in the merge decisions sidecar. This is not a fatal error — it can happen legitimately with aggressive magnitude filtering.

**Implementation requirement:** For every `replace` or `drop` override, the merger must look up the crossmatch table to find a partner on **either** side, then check which (if any) of the two rows actually exist in the loaded Gaia and HIP data. The override resolves whichever rows are present; missing rows are noted but do not block the override.

---

## Canonical identity

Each merged output row must have one canonical `source_id` used by sidecars and forward lookups.

Policy (extend as needed; namespaces must be consistent before Stage 00):

- **Gaia-selected rows**: `source_id = gaia_source_id` (or namespaced equivalent).
- **Hipparcos-selected rows** without a Gaia counterpart: `source_id = hip_source_id` in the Hipparcos namespace.
- **Manual-only rows** (`add`): `source_id` in a dedicated namespace (e.g. prefixed or separate ID range) so collisions with Gaia/HIP numeric IDs cannot occur.
- **Replaced rows**: canonical `source_id` is always the **original target's** `source` / `source_id`. The override swaps the payload (coordinates, photometry, etc.) but does not remap identity.

`catalog_source` on the dense row indicates the **provenance of the chosen astrometry/photometry** for that row: `gaia`, `hip`, or `manual` (or equivalent enum).

---

## Duplicate resolution

### Definitions

- **Matched pair**: one Gaia row and one Hipparcos row linked by cross-match, where **both rows exist** in the loaded data. A crossmatch link where one side was excluded (e.g. by Gaia magnitude filtering) does not form a matched pair — the present row is treated as unmatched.
- **Unmatched Gaia**: Gaia row not present in any matched pair (either no crossmatch link, or the HIP partner is missing from the data).
- **Unmatched Hipparcos**: Hipparcos row not present in any matched pair (either no crossmatch link, or the Gaia partner is missing from the data).

### Rules

#### M1. Keep all unmatched rows

- Keep all unmatched Gaia rows (unless removed by a `drop` override).
- Keep all unmatched Hipparcos rows (unless removed by a `drop` override).

#### M2. Exactly one winner per matched pair

For each matched pair **not** resolved by a `replace` or `drop` override, compute quality scores and select exactly one winner.

#### M3. Deterministic tie-break

If quality scores tie, apply deterministic tie-break in this order:

1. Prefer Gaia.
2. Lower canonical `source_id` (under a fixed ordering).

#### M4. No duplicate canonical IDs

Merged dense output must not contain duplicate canonical `source_id`.

#### M5. Auditable decision record

Persist provenance for:

- Each **matched pair** decision: `gaia_source_id`, `hip_source_id`, winner catalog, score components (or aggregate), tie-break reason if applicable.
- Each **override**: `override_id`, `action`, keys, `override_reason`, `override_policy_version`, and link to canonical `source_id` after merge.

Recommend a dedicated **merge decisions** sidecar (Parquet or JSONL) rather than widening the main table.

---

## Quality scoring contract

### Policy v1: `astrometry_quality` comparison

Both the Gaia and Hipparcos pipelines produce `astrometry_quality` as a **fractional parallax error** (σ/π): `e_Plx / Plx` for Hipparcos (`hipparcos/astrometry.py`), `parallax_error / max(parallax, ε)` for Gaia DR3 (`gaia/astrometry.py`). For Bailer-Jones distances the analogous half-interval width is used. These values are directly comparable across catalogs: **lower is better**.

Gaia fallback tiers already carry sentinel quality values that sort correctly against real measurements:

- Tier A (primary): actual fractional error (typically < 1.0)
- Tier B (weak catalog fallback): `10.0`
- Tier C (photometric distance): `20.0`
- Tier D (synthetic prior): `50.0`

Winner selection for matched pairs (M2):

1. Compare `astrometry_quality` directly. Lower value wins.
2. If values are equal, apply the deterministic tie-break (M3): prefer Gaia, then lower `source_id`.

This means Hipparcos wins for bright nearby stars where its parallax measurement is more precise than Gaia's, and Gaia wins everywhere else — which is the reason both pipelines compute quality metrics.

The merger must expose:

- versioned quality policy identifier (v1 for this policy)
- deterministic numeric score per candidate
- reproducible output for the same input tables

---

## Output schema contract

### Dense merged table (all stars)

Must satisfy Stage 00 requirements:

- `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`
- `mag_abs`
- optional `teff`

Should include identity fields:

- `source_id` (canonical, required)
- `gaia_source_id` (nullable)
- `hip_source_id` (nullable)
- `catalog_source` (`gaia`, `hip`, or `manual`)

Should also include lightweight spatial helpers needed for 2D sharding and cheap within-shard ordering:

- `ra_deg`
- `dec_deg`
- `r_pc`

**Do not** require HD, Bayer, or other rare designations on every row of the dense table. Those belong in **sparse sidecars** (below).

Extra columns needed for Stage 00 must survive unchanged where Stage 00 uses `SELECT *` on shards.

### Shard ordering contract (recommended)

When writing merged HEALPix (or equivalent 2D) shards:

- Primary recommendation: stable sort by `r_pc` ascending within each shard (cheap and effective).
- Optional: compute `spherical_order_key` for closer-to-octree traversal order if benchmarks justify the added complexity.
- If no explicit key is persisted, ordering must still be deterministic for reproducibility.

### Sparse sidecars (small row counts vs full catalog)

Keep optional metadata out of the billion-row table:

- **Identifiers / aliases**: `canonical_source_id`, optional `gaia_source_id`, `hip_source_id`, `override_id`, plus any extra catalog ids.
- **Designations (current policy)**: for small curated designation sets, prefer a **wide sidecar** keyed by star ID (for example `hip_source_id`, `hd`, `bayer`, `fl`, `cst`, `proper_name`) instead of a `kind/value` long table.
- **Merge decisions**: as in M5, keyed by pair or by `canonical_source_id`.

Downstream UIs or APIs join sidecars when needed.

---

## Validation requirements

Before publishing merged parquet:

1. Assert no duplicate canonical `source_id` in the dense output.
2. Assert all matched pairs produce exactly one winner **or** are fully explained by an override (`replace` / `drop`) in the decision record.
3. Assert row count consistency with documented semantics, e.g.  
   `unmatched_gaia + unmatched_hip + matched_winners + override_adds − override_drops`  
   (exact formula must match whether drops remove rows from counts).
4. Assert required geometry/magnitude columns are non-null where mandatory.
5. Emit merge summary counts by category, winner catalog, and override action type.
6. Assert each `drop` override resolves to exactly one existing row by (`source`, `source_id`) and carries no non-audit payload fields.

---

## Operational outputs

Merger run should produce:

- Canonical merged dense Parquet (input to Stage 00), optionally **pre-partitioned by HEALPix** (or equivalent 2D key) for downstream octree/index builds
- Merge report JSON with counts and policy versions (merge policy + override policy)
- Decision / override audit tables (Parquet recommended)
- Sparse sidecar Parquet files for identifiers and designations

---

## Resolved decisions

The following were resolved before the first merger implementation:

1. **Matched-pair winner policy v1** — Compare `astrometry_quality` directly (lower wins). Both pipelines produce comparable fractional parallax error; Gaia fallback-tier sentinels (10/20/50) sort correctly against real measurements. Tie-break: prefer Gaia, then lower `source_id`. See "Quality scoring contract" above for details.

2. **Canonical identity for `replace` actions** — Replacement rows always retain the original target's `source` / `source_id` as canonical identity. The override swaps the payload only; identity is never remapped.

3. **Override pair resolution** — Overrides target a single (`source`, `source_id`) key. When the target is one side of a crossmatch-linked pair, the merger resolves the entire pair via the crossmatch table; the partner is suppressed. Override authors do not supply partner IDs. When Gaia data is magnitude-limited, the merger handles missing crossmatch partners gracefully — an override still applies if either side of the pair exists in the data. See "Pair-aware override resolution" and "Magnitude-limited Gaia subsets" above.

---

## Non-normative notes

- A "merge plan now, apply later" strategy adds pipeline complexity with little benefit; current recommendation is direct merge after catalog preprocessing and before Stage 00.
- Full reverse lookup indices remain serving-layer artifacts; the **sidecars** above are the pipeline’s contract for sparse metadata.
- HEALPix level for **serving shards** may differ from HEALPix used only for **Gaia download batching**; document the level used for merged output.
- Gaia-import sidecars can provide convenient `gaia_source_id` ↔ `hip_source_id` hints, but merge outputs remain the canonical source of truth for identity/provenance after overrides and winner selection.
