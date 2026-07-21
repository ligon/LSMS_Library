# Prior-Art Ledger — GH #323 (Togo)

**Search tier used:** ripgrep + git floor (gitnexus MCP not reachable from this worktree; used `rg` over `lsms_library/` and the validated L2-wave scanner instead).

## §1 Task, restated

`Country._normalize_dataframe_index` collapses a non-unique DECLARED index with
`groupby().first()`. For Togo three cells collide, and they are three DIFFERENT
things:

- **`plot_inputs`** — real, silent data loss. Togo's `_/categorical_mapping.org`
  maps three distinct EHCVM input labels (`Autres semences`,
  `Plants/boutures de tubercules`, `Semences d'autres cereales`) onto the SAME
  `(Seed, Autre crop)` pair via `harmonize_input` + `harmonize_seed_crop`. The raw
  source `s16b_me_tgo2018.dta` has ZERO duplicates on
  `(grappe, menage, s16bq01, s16bq03b)`; the collision is **manufactured by the
  taxonomy**. `first()` then threw away the colliding rows — which carried MORE
  Quantity than the rows it kept.
- **`cluster_features`** — an EXTRACTION bug, not an aggregation one: a `(t, v)`
  cluster table fed the EHCVM *household* cover page (`s00_me_tgo2018.dta`,
  6,171 households / 540 grappes). Value-lossless, but it fired a spurious #323
  warning that camouflaged the real one.
- **`food_acquired`** — already correct (summed). NO ACTION.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders/drops index levels; collapses duplicates | `tests/test_normalize_index_j_preserved.py` | **extend** (reducer becomes declarative) |
| `_ADDITIVE_MEASURE_COLUMNS` | `lsms_library/feature.py:101` (pre-fix) | one-entry dict `{food_acquired: (Quantity, Expenditure)}` — the ENTIRE reducer registry | no | **replace** with declared policy |
| `_collapse_duplicate_index` | `lsms_library/feature.py` | the *second* collapse site (cross-country `Feature`) | no | **extend** (share one resolver) |
| `aggregation:` block | `Togo/_/data_scheme.yml:91` + 9 other countries | declared `visit: first` … **and NEVER READ** — appears only in skip-lists (`country.py:2387`, `diagnostics.py:174`) | no | **make it real** (column keys) |
| grain-aggregation contract | `SkunkWorks/grain_aggregation_policy.org:104-123, 145-159` | specifies the per-COLUMN `aggregation:` block; names both `.first()` collapses as the #323/#325 loss sites; "Implementation order" step 4 | — | **implement step 4** |
| `LSMS_CACHE_SCHEMA` | `lsms_library/local_tools.py:1354` | manual lever for a library-wide change to a **pre-write** transform | `tests/test_cache_hash_invalidation.py` | **bump 1 → 2** |

**Prior art found, so NOT reinvented:** the per-column `aggregation:` block is not
my invention — it is the design already written down in
`grain_aggregation_policy.org` and left unimplemented (its step 4). The `sum` +
re-derive-`Price` reducer already exists and is proven for `food_acquired`; I
generalized it rather than writing a new one.

## §3 Definitions & conventions in force

- Canonical index per table: `lsms_library/data_info.yml` → `Index Info: index_info`.
- EHCVM `v: grappe`, `i: [grappe, menage]` — per `CLAUDE.md` "Gotchas with Teeth".
- Cache tiers + which transforms are pre-write vs post-read: `CLAUDE.md` "Cache Behavior".
  Load-bearing here: `_normalize_dataframe_index` runs **per wave inside
  `load_from_waves` (country.py:2715), BEFORE the L2-country parquet is written** —
  so the collapse is BAKED INTO the cache. (`_finalize_result`'s call at
  country.py:2115 is post-read; the per-wave one is not.)
- `to_parquet` / `get_dataframe` for all IO — `CLAUDE.md` "Data Access".

## §4 Invariants & assumptions

- **The bug hides behind the cache the bug poisoned.** The #323 warning fires only
  on a COLD build; warm reads serve the already-collapsed `var/` parquet. Any
  measurement of #323 MUST use `LSMS_NO_CACHE=1` (or a schema bump) — and must
  read the **L2-WAVE** parquet, never `var/`, for ground truth.
- **The L2-wave parquet is itself evicted** by `_evict_hashless_wave_caches` on
  every rebuild descent (GH #479), so under a cold build it is *also* an unstable
  ground truth. The **raw `.dta` is the only immutable anchor** — this task's
  conservation check is anchored there (`s16b`, gate `s16bq02==1`).
- `groupby(...).first()` is per-column **first-non-null**, so it can weld
  `Quantity` from one seed type onto `Purchased`/`Quantity_purchased` from another
  — synthesizing a row that never existed. Not merely lossy: **wrong**.
- Reducer changes alter **pre-write** content ⇒ `LSMS_CACHE_SCHEMA` must be bumped
  or warm users keep the poisoned cache and the fix is invisible.
- Only cells whose declared index is non-unique can change: the fix lives entirely
  inside `if not df.index.is_unique:`. This is the containment argument for the
  regression proof.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| reducer registry | **extend → declarative** | `_ADDITIVE_MEASURE_COLUMNS` is a one-entry hardcoded dict; the `Aggregation:` block is the already-designed replacement (§2). Fixing only Togo's `plot_inputs` would close the instance and leave the class — the exact mistake that closed #323 the first time. |
| `plot_inputs` reducer | **reuse `food_acquired`'s** (`sum`, + `any` for the bool) | `food_acquired` is the existence proof that INTENDED_AGGREGATION+sum is right for a many-to-one harmonization on a quantity-bearing index. Verified conserved to the cent. |
| `crop_production`, `livestock`, `plot_labor` | **declare `sum`** | Same class, unambiguously additive. `crop_production` collides today (Ethiopia, Niger); the other two do not, so their policy is a no-op now and a guardrail later. |
| `assets` | **NEW: declare NOTHING; ratchet it** | `Quantity` is additive but `Age` / `Value` / `Purchase Price` across two rows for one `(t,i,j)` are genuinely ambiguous. Non-negotiable #4: DROP LOUDLY rather than guess. Keeps `.first()` + the loud warning (class-2), pinned in the test ratchet. |
| `cluster_features` | **fix the EXTRACTION** | Not an aggregation question: a `(t,v)` table was fed a household file. Verified lossless on the source before collapsing (all 540 grappes carry exactly 1 distinct Region and 1 Rural). |

## §6 Open questions for the human

- **`assets` (Niger, Nigeria — 30,992 duplicate rows) is left on `.first()`.** It
  needs a survey-doc decision on how to reduce `Age` / `Value` / `Purchase Price`
  when a household reports the same asset `j` twice. I refused to guess. Blocks:
  closing #323 for `assets`.
- The `cluster_features` household-cover-page shape recurs in **~22 countries / 60
  cells** (Mali alone: 4.7M phantom rows). I fixed Togo's only. It is lossless
  everywhere it was checked, but it is a large, separate sweep.

---
### Phase 3 — verification

- `resolve_aggregation` / `collapse_with_policy` (`feature.py`) — **OK (anchored on §2/§5)**: implements `grain_aggregation_policy.org` step 4; single resolver shared by BOTH collapse sites, so `Feature()` and `Country()` cannot diverge.
- `_normalize_dataframe_index` (`country.py`) — **OK (§4)**: change is confined to the `not df.index.is_unique` branch; the loud warning is preserved for undeclared tables.
- `Aggregation:` block (`data_info.yml`) — **OK (§3)**: canonical cross-country policy lives with the other canonical conventions, per `CLAUDE.md`.
- `LSMS_CACHE_SCHEMA = 2` (`local_tools.py`) — **OK (§4)**: required, not cosmetic; without it every warm cache keeps the lossy collapse.
- `Togo/2018/_/mapping.py::cluster_features` — **OK (§5)**: guards constancy and RAISES rather than silently picking a row, so a future non-constant wave fails loudly.
- `tests/test_grain_aggregation.py` — **OK (§4)**: the ratchet turns "hope the registry is complete" into an enforced invariant (`test_every_additive_table_declares_a_reducer`).
