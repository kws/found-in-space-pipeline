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

`gaia parquet + hip parquet + crossmatch + manual_overrides → merged canonical parquet → Stage 00 → Stage 01`

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

- Gaia source parquet (or equivalent table), after Gaia pipeline preprocessing
- Hipparcos source parquet (or equivalent table), after Hipparcos pipeline preprocessing
- Cross-match table mapping Gaia `source_id` and Hipparcos `HIP` (or equivalent) for matched pairs
- **Manual overrides** table (versioned): explicit `add`, `replace`, and optional `drop` / suppress actions (see below)

Optional:

- Additional identifier tables (HD, Bayer, Flamsteed, etc.) joined **into sidecars**, not necessarily into every row of the dense merge output
- A provisional Gaia-import mapping sidecar (`gaia_source_id` ↔ `hip_source_id`, when Gaia input includes `hip`) to reduce re-scan cost. This artifact is advisory and can be superseded by merge-time crossmatch/override resolution.

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
- Keys to match existing rows when applicable: `gaia_source_id` and/or `hip_source_id`
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

---

## Canonical identity

Each merged output row must have one canonical `source_id` used by sidecars and forward lookups.

Policy (extend as needed; namespaces must be consistent before Stage 00):

- **Gaia-selected rows**: `source_id = gaia_source_id` (or namespaced equivalent).
- **Hipparcos-selected rows** without a Gaia counterpart: `source_id = hip_source_id` in the Hipparcos namespace.
- **Manual-only rows** (`add`): `source_id` in a dedicated namespace (e.g. prefixed or separate ID range) so collisions with Gaia/HIP numeric IDs cannot occur.
- **Replaced rows**: canonical `source_id` follows policy (typically retain the primary catalog id used for serving; document in override row).

`catalog_source` on the dense row indicates the **provenance of the chosen astrometry/photometry** for that row: `gaia`, `hip`, or `manual` (or equivalent enum).

---

## Duplicate resolution

### Definitions

- **Matched pair**: one Gaia row and one Hipparcos row linked by cross-match.
- **Unmatched Gaia**: Gaia row not present in cross-match.
- **Unmatched Hipparcos**: Hipparcos row not present in cross-match.

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

The exact scoring formula is configurable, but the merger must expose:

- versioned quality policy identifier
- deterministic numeric score per candidate
- reproducible output for the same input tables

Example signals (non-normative):

- parallax uncertainty metrics
- astrometric fit quality
- photometric quality flags

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

## Non-normative notes

- A "merge plan now, apply later" strategy adds pipeline complexity with little benefit; current recommendation is direct merge after catalog preprocessing and before Stage 00.
- Full reverse lookup indices remain serving-layer artifacts; the **sidecars** above are the pipeline’s contract for sparse metadata.
- HEALPix level for **serving shards** may differ from HEALPix used only for **Gaia download batching**; document the level used for merged output.
- Gaia-import sidecars can provide convenient `gaia_source_id` ↔ `hip_source_id` hints, but merge outputs remain the canonical source of truth for identity/provenance after overrides and winner selection.
