# Prior-Art Ledger — 859-nigeria-kgfactor

> Per-task ledger (copy of `TEMPLATE.md`). Carries Nigeria GHS-Panel's
> per-row `Kg/L conversion factor` into `crop_production` as the optional
> canonical `KgFactor` column. Inherits `STANDING.md`; cites `CLAUDE.md` and
> `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server did not
connect this session (`CONNECTION_CLOSED`) and this mirror has no `.gitnexus/`
index, so the substitution sanctioned by `CLAUDE.md` §"Code intelligence:
GitNexus is OPTIONAL" was used. Blast radius swept with
`rg -n 'crop_production_for_wave|KgFactor' lsms_library/` — one caller of
`crop_production_for_wave` (`Nigeria/_/crop_production.py`), zero test callers.

## §1 Task, restated

Nigeria `crop_production` is a **script-path** registered table
(`materialize: make`, `Nigeria/_/data_scheme.yml:255`) built by ONE
country-level script, `Nigeria/_/crop_production.py`, which calls
`nigeria.crop_production_for_wave(t, frames, crop_labels)` once per wave.
Grain `(t, i, plot, crop)`; `t = PH_QUARTER[wave]` (the post-harvest round of
a post-planting/post-harvest wave — the Nigeria idiosyncrasy recorded in
`Nigeria/_/CONTENTS.org` §crop_production and §Sampling Design). `Quantity` is
in the native unit `u`, normalised by `nigeria._canon_unit` to a
**size-agnostic** base Preferred Label.

GH #859 asks that the survey's own per-row kilograms-per-unit factor be
carried as `KgFactor`, which `transformations.harvest_kg` already prefers as
its first (`reported`) layer. Nigeria supplied none, so every Nigeria row
reached the `inferred` layer or `none`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `nigeria.crop_production_for_wave` | `Nigeria/_/nigeria.py:316` | assembles one wave from N source frames, each described by a dict of column names | via the build | **extend** — one optional `kg_factor` key |
| `Nigeria/_/crop_production.py` | country script | the only caller; declares the per-wave frame dicts | via the build | **extend** — name the column per frame |
| `nigeria._canon_unit` | `Nigeria/_/nigeria.py` | native unit label → size-agnostic base `u` label | via the build | **reuse, untouched** |
| `transformations.harvest_kg` / `harvest_kg_factors` | `transformations.py:2102` / `:1913` | layered `reported → survey_median → inferred → none`, discloses `attrs['kg_factor_sources']` | `tests/test_crop_kg_factor.py` | **reuse verbatim — NOT touched by this task** |
| `transformations._screen_reported_factors` | `transformations.py:1717` | plausibility screen on a reported factor: kg-unit must weigh 1, `> KG_FACTOR_MAX` (250), integral calendar year on a non-kg unit | `tests/test_crop_kg_factor.py` | **reuse verbatim** |
| `Uganda/_/uganda.py:1358-1384`, `:1423` | Uganda's #849 wiring | reads `a5?q6d`, `where(kgf > 0, nan)`, emits `KgFactor`, coerces `Float64` after concat | Uganda build | **the precedent to copy** |
| `Uganda/_/data_scheme.yml` `KgFactor: {type: float, optional: true}` | | the declaration shape | `test_schema_consistency.py` | **copy the shape** |
| `data_info.yml` `crop_production.KgFactor` | `lsms_library/data_info.yml:545-588` | canonical note: "REPORTED, NEVER CONSTRUCTED"; a RATE, not a weight | `test_schema_consistency.py` | **the contract this task must satisfy** |

## §3 Definitions & conventions in force

- `KgFactor` = kilograms per ONE unit of the row's `u`, **as the instrument
  reports it**; `Quantity * KgFactor` is the row's harvest in kg. "REPORTED,
  NEVER CONSTRUCTED … not a median over sibling rows, not a factor from the
  library's unit tables." — `lsms_library/data_info.yml:545-588`.
- Nigeria `t` is the **round**, never the wave directory: crop harvest is a
  post-harvest module, so each wave maps to a single `t = PH_QUARTER[wave]`
  (`Nigeria/_/data_scheme.yml:228-246`, `CONTENTS.org` §crop_production).
- Nigeria `u` is **size-agnostic** (`_canon_unit` docstring; `CONTENTS.org`
  §community_prices "The C8 SIZE field is recorded by the survey and NOT read
  by the build").
- Sale-side units are NOT the harvest unit (`CONTENTS.org` §"The SALE has its
  own unit code (`sa3q11b`)", GH #824): `u` describes `Quantity`, and is
  applied to `Quantity_sold` by assumption on ~96% of W1/W2 rows.

## §4 Invariants & assumptions

- **A factor belongs to the quantity block it was asked beside.** The W4/W5
  `secta3i` files each carry TWO `*_conv` columns; the second
  (`sa3iq6d_conv` W4, `sa3iq15_conv` W5) sits in the *"How much MORE [CROP]
  does HH expect to harvest"* block with its **own** unit / size / condition
  columns (`sa3iq6d2` / `sa3iq15b`), which the build never reads. Wiring it to
  a row whose `u` came from `sa3iq6ii` / `sa3iq9b` would attach a factor for
  one container to a quantity measured in another. NOT WIRED.
- **`secta3ii` is hh-crop grain and is not in this table.** `sa3iiq1_conv`
  (W4 8 669 / 8 839; W5 6 962 / 7 184) belongs to the household-level total
  harvest question, with no plot linkage — exactly the reason
  `crop_production.py` already leaves W3–W5 `Quantity_sold` / `Value_sold`
  NaN. NOT WIRED.
- **W1, W2 and W3 ship NO `*_conv` column at all** (metadata-verified on
  `secta3_harvestw1`, `secta3_harvestw2`, `secta3i_harvestw3`,
  `secta3ii_harvestw3`). `KgFactor` is legitimately all-NaN in those three
  waves; `optional: true` is what keeps the all-null Site-B guard from firing
  (`CLAUDE.md` §"The Silent All-Null Read").
- **Never construct.** No median, no unit-table lookup, no cross-section
  average — those layers already exist downstream in `harvest_kg` and count
  what they served.
- **Dtype.** `crop_production_for_wave` fills absent canonical columns with
  `pd.NA` (object). Concatenating three all-NA waves with two float waves
  yields `object`; the country script must coerce to float before
  `to_parquet` — the same step Uganda takes at `uganda.py:1423`
  (cf. `.coder/ledger/645-to-parquet-null-coercion.md`).
- **Cache.** Editing `nigeria.py` moves the fingerprint of EVERY Nigeria
  table, not just `crop_production` (`CLAUDE.md` §"Automatic content-hash
  staleness": the country module is hashed into each table's
  `lsms_cache_hash`). Expected, measured, not a bug.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| per-row kg factor carrier | **reuse** `KgFactor` (canonical, `data_info.yml:545`) | the column exists precisely for this |
| per-wave assembly | **extend** `crop_production_for_wave` with an optional `kg_factor` key | positional, same frame, no join, no averaging |
| kg conversion itself | **reuse** `transformations.harvest_kg` untouched | the layering + screen are already tested |
| plausibility screen | **reuse** `_screen_reported_factors` | screening at build time would silently drop a reported number the schema says must be stored |

## §6 Open questions for the human

- `survey_median` now becomes reachable for Nigeria W4/W5 rows that report no
  factor, and it medians over `u` **alone** (Nigeria carries no `condition`
  column) — i.e. across the survey's own `Size` axis, which `_canon_unit`
  collapses. Recorded as a caveat; restoring the size axis would change the
  published `u` vocabulary of a shipped table (the same maintainer decision
  `CONTENTS.org` §community_prices already defers).
- The two unwired blocks (`sa3iq6d_conv` / `sa3iq15_conv`, expected-future
  harvest) would need `crop_production` to carry that second quantity as its
  own row before their factor has a home. Out of scope for #859.

---
### Phase 3 — verification (filled at task end)

- `crop_production_for_wave(..., kg_factor=)` — **OK (anchored on §2/§5)**: one
  optional key, read positionally off the same frame; no join, no aggregation.
- `KgFactor` column in `Nigeria/_/data_scheme.yml` — **OK (anchored on §2)**:
  identical shape to Uganda's post-#849 declaration.
- `sa3iq6d_conv` / `sa3iq15_conv` / `sa3iiq1_conv` left unwired — **OK
  (anchored on §4)**: wiring any of them would contradict the invariant that a
  factor belongs to the quantity block it was asked beside.
- No edit to `transformations.py` — **OK (anchored on §5)**: the Uganda
  baseline in `tests/test_crop_kg_factor.py` must not move, and does not.

### Measured, 2026-09-09 (isolated `LSMS_DATA_DIR`)

Rows unchanged (62 844; 12 752 / 12 777 / 11 829 / 15 054 / 10 432 per `t`).
`KgFactor` non-null 19 678 (2019Q1 11 393, 2024Q1 8 285; W1–W3 zero — no source
column). `harvest_kg` total 9 315 175 → 22 111 818; layers
`reported` 0 → 19 667, `survey_median` 0 → 342, `inferred` 11 817 → 11 635,
`none` 51 027 → 31 200, `reported_implausible` 0 → 11.
All 24 Nigeria `_table_cache_hash` values move (worktree vs. main config tree).

**Operational note — a warm Nigeria cache does NOT pick this up (GH #866).**
Observed here: the edits moved all 24 hashes, and the very next read still
served the OLD frame (no `KgFactor` column); only
`lsms-library cache clear --country Nigeria` produced the rebuild.

**The mechanism is a defect in the REBUILD DESCENT, not the read gate**, and an
earlier version of this note got it wrong (it blamed the `legacy`
trust-once-and-stamp path). Diagnosed by the red team, filed as **GH #866**:

1. The gate works — the read grades `stale` and logs
   `v0.8.0 cache STALE: crop_production …; rebuilding from source`.
2. The rebuild reaches `run_make_target`, which tries `try_make` FIRST
   (`country.py:3538`): `make -s $(VAR_DIR)/crop_production.parquet` in
   `Nigeria/_`.
3. Nigeria's `_/Makefile` has **no rule for that target**. GNU make, handed a
   rule-less target that *exists as a file*, calls it up to date and **exits
   0** (it errors only when the file is absent — which is why `cache clear`
   works).
4. `try_make` reads exit 0 as a successful rebuild and returns the stale file.
   `try_script`, the only path that runs `_/crop_production.py`, is never
   reached.
5. `country.py:3865` then writes that stale content back **with the new hash**,
   so every later read grades `fresh`. The bug erases its own evidence.

It fires on **every** hash move — there is no one-shot window, and no
self-healing. `LSMS_NO_CACHE=1` does **not** escape it (measured: bypasses the
read gate, lands in the same `try_make` exit-0).
`_evict_hashless_wave_caches` (GH #479) does not cover it either: it globs
`<wave>/_/{table}.parquet`, and the country-level `var/{table}.parquet` never
matches. Distinct from #788 / #809, which describe the `legacy` path — the
red team's reproduction stamped a *bogus* hash, so the parquet graded `stale`,
`legacy` was never entered, and the bug fired regardless.

`KgFactor` being `optional: true` (right for the data — W1–W3 have no source
column) is also what removed the last tripwire:
`_assert_built_required_columns` would have raised on a stale frame missing a
*required* declared column.

Corpus exposure measured by the red team: 20 country-level script-path tables
across 13 countries have no country-target rule at all; five of them are
Nigeria's (`crop_production`, `plot_inputs`, `livestock`, `plot_labor`,
`people_last7days`). The remaining 75 are exposed to the weaker form — their
rules' prerequisites never include `_/{country}.py` or `_/data_scheme.yml`,
both of which ARE in the hash.

**So: the post-merge Nigeria re-warm must run
`lsms-library cache clear --country Nigeria` first.**
