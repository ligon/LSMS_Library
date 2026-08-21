# Prior-Art Ledger — GH #323 (Ethiopia config) / unblocking PR #627

**Search tier used:** ripgrep + git floor (gitnexus not consulted). The task is
config-tree only; the blast radius was established directly, by rebuilding every
Ethiopia table before and after against PR #627's core.

## §1 Task, restated

PR **#627** closes site 4 of GH #323 — the `dfs:` outer merge in
`Wave.grab_data` that *manufactures* the duplicate rows every other site then
collapses. It also converts the GH #515 swallowed `KeyError` into a hard
`RuntimeError` when a dropped sub-frame costs a **required** declared column.
#627's body lists **Ethiopia — 3 cells** as an unfixed blocker.

Two questions, therefore:

1. Does `Country('Ethiopia')` build under #627's core against the config now on
   `development`? (The blocker table in #627's body was written 2026-07-13 and
   has not been refreshed since.)
2. Are #627's two Ethiopian **cartesian** cells — 2013-14 (60,221 phantom rows)
   and 2015-16 (52,832) — still present?

**Answer to both: Ethiopia is clear.** Commit `3488b791` (PR #628) landed the
`cluster_features` fix for all five waves and is an ancestor of
`origin/development`. It closes the 3 raising cells *and* both cartesian cells —
the same re-key does both, which is why #627 could not see it coming from its
own census. **No config change was needed for #627.** This branch therefore
carries the verification (tests + this ledger) rather than a re-implementation.

Separately, the verification surfaced **one genuinely unfixed defect** in the
same country — `individual_education` in 2013-14 / 2015-16 — which is *not* a
#627 blocker but is a #323 site-1 true positive of the same shape (a broken
identifier). It is fixed here.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Wave._cartesian_keys` | `country.py:909` (#627 only) | exact many-to-many detection: key values duplicated in **both** sub-frames | yes (#627, 16 tests) | **reuse** — the oracle for the before/after census |
| `Wave._merge_subframes` | `country.py:945` (#627 only) | warns on a cartesian with an exact phantom count; fatal under `LSMS_GRAIN_STRICT=1` | yes | **reuse** |
| `_required_scheme_columns` | `country.py:478` (#627 only) | required vs `optional:` scheme columns; drives the #515 raise | yes | **reuse** |
| `Wave.cluster_features` | `country.py:1373` | GH #161 projection of a household-grain frame onto `(t, v)`; fires **only when `i` is an index level** | yes | **reuse** — this is why `3488b791` puts `i` in `final_index` rather than `drop:`-ing it |
| `_normalize_dataframe_index` | `country.py` | collapses a non-unique **declared** index with `groupby().first()`; audits first (`_audit_index_collapse`) | yes | **reuse as the oracle** — its own warning text quantified the education loss |
| `_join_v_from_sample` | `country.py` | joins `sample.v` at API time | yes | untouched |
| `id_walk` / `Country.panel_ids` | `local_tools.py` / `country.py:3359` | walks wave-native ids back to the panel baseline | yes | untouched — but see §4, it is order-dependent |
| `3488b791` (PR #628) | `countries/Ethiopia/{wave}/_/data_info.yml` | the landed `cluster_features` fix | **not until now** | **verified here, not re-implemented** |

## §3 Definitions & conventions in force

Cited, not paraphrased:

- **D1 — the core never aggregates.** `SkunkWorks/grain_aggregation_policy.org`
  §3a; restated in `CLAUDE.md` §"Grain Collapse". The `aggregation:` key in
  `data_scheme.yml` is **dead config** and is deliberately not honoured. A test
  in this branch pins that Ethiopia declares none.
- **Duplicates on a declared index mean the IDENTIFIER IS BROKEN or a LEVEL IS
  MISSING — fix the index, do not declare a reducer.** `CLAUDE.md` §"Grain
  Collapse". This is the whole argument for the `individual_education` fix.
- `cluster_features` canonical index is `(t, v)`;
  `individual_education` is `(t, i, pid)` — `countries/Ethiopia/_/data_scheme.yml`.
- **Ethiopia's wave-keyed ID scheme**: `household_id` / `ea_id` in W1/W4/W5;
  `household_id2` / `ea_id2` in W2/W3 — `countries/Ethiopia/_/CONTENTS.org`.
- Ethiopia is a PP/PH country (19 script-path tables), but **not at these
  sites**: `cluster_features` and `individual_education` both read the
  *household* cover/section files (`sect_cover_hh_w*`, `sect2_hh_w*`), not a
  `_pp_`/`_ph_` file, and Ethiopia declares no `wave_folder_map`. Verified
  per-`t`: one round per wave here.

## §4 Invariants & assumptions

- **The re-key must be `household_id2` in W2/W3, never `household_id`.**
  `household_id` is the W1 baseline id and is **blank** for households with no
  W1 antecedent, so it is non-unique on the empty value — re-keying to it trades
  an EA cartesian for a **null-key** cartesian, because `pd.merge` matches null
  keys. Measured: `household_id2` is unique and blank-free in both
  `cluster_features` sub-frames (5,262 / 5,287 rows, 0 keys duplicated in both).
- **`i` must reach `final_index`, not be `drop:`-ed.** `Wave.cluster_features`'
  GH #161 collapse to the cluster grain only fires when `i` is an index *level*.
  `3488b791` gets this right; the abandoned `fix/323-ethiopia` branch did not —
  it used `final_index: [t, v]` + `drop: [i]` and leaned on an `aggregation:`
  key that nothing reads, leaving the frame household-grain on a non-unique
  `(t, v)` for site 1 to `first()`.
- **Wave-level assertions only, for row counts.** `_normalize_dataframe_index`
  makes the API index unique **by construction**, so a post-collapse uniqueness
  assertion passes with the bug fully present. (Inherited instrument note from
  `tests/test_gh323_benin_togo.py` / the CotedIvoire tests.)
- **`individual_education`'s country-level row count is ORDER-DEPENDENT and must
  not be asserted on.** `id_walk` is applied only once `panel_ids` has resolved,
  and whether it has depends on cache state and on what else the process built
  first. Reproduced against the *unchanged* `development` config:

  | invocation (same process, same cache) | rows |
  |---|---|
  | `Country('Ethiopia').individual_education()` | 63,139 |
  | `c.panel_ids` first, then the same call | 62,939 |

  The 200-row delta lands on **2011-12 / 2015-16 / 2018-19 / 2021-22** — waves
  this branch does not touch — and reproduces identically in the pre-fix config
  tree. It is **pre-existing and orthogonal**; recorded here so the next person
  does not mistake it for this change. See §6.
- A **stale L2-wave parquet shadows the #627 raise.** Observed while running the
  negative control: three Ethiopian waves that raise on a genuinely cold build
  returned a cached `{wave}/_/cluster_features.parquet` and passed. `#627`'s own
  warning ("the suite is green and that green is a lie") applies to any
  data-backed test here; the config-level assertions in
  `tests/test_gh323_ethiopia_config.py` are the ones that cannot be shadowed.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| Ethiopia `cluster_features` config | **reuse `3488b791` unchanged** | Already on `development`, already correct, and verified here end to end. Re-implementing would layer a second differently-shaped fix on a solved problem. |
| the cartesian census | **reuse #627's `_merge_subframes` warning** | It emits an exact phantom count; no separate oracle needed. |
| the education loss measurement | **reuse `_audit_index_collapse`'s warning** | The framework already reports destroyed and NaN-key-deleted rows by wave. Writing a bespoke counter would have been a second, unvalidated implementation of the same arithmetic. |
| `individual_education` W2/W3 index | **new (config)** — `household_id2` / `individual_id2` | Per §3: fix the identifier, never declare a reducer. No existing mechanism repairs a broken id. |
| `interview_date` W4/W5 datetime coercion | **rejected** | `fix/323-ethiopia` added `mapping.py` `interview_date` hooks. Measured on `development`: the table already returns `datetime64[us]`, 8,236 rows, 0 nulls — `_enforce_canonical_dtypes` honours the declared `Int_t: datetime` at API time. The hooks are now redundant; adding them would be inert code that reads as a fix. |
| the `2013-14` wave-level `panel_ids:` block | **deferred, not touched** | `fix/323-ethiopia` deleted it as a wrong-source (livestock-cover, holder-grain) duplicate. On `development` it now reads `i: household_id2` and the country-level `_/panel_ids.py` (`materialize: make`) is authoritative anyway. Out of #627's path; see §6. |

## §6 Open questions for the human

- **`individual_education`'s country-level row count is order-dependent** (§4).
  The proximate cause is the lazy `self.updated_ids` probe in
  `_aggregate_wave_data` (`country.py:2668`), whose `except (FileNotFoundError,
  KeyError, ValueError): pass` means a first-attempt failure is recorded as
  "attempted" and `id_walk` is then skipped for the rest of the process. It
  affects every Ethiopian wave, in both the old and the new config, so it is not
  this branch's to fix — but it means **the same call can return two different
  row counts depending on what the process touched first**, which is a
  correctness problem, not a performance one. Worth its own issue.
- **Ethiopia's 3 raising cells were, in #627's words, "true positives".** They
  were — but they had already been fixed when #627's table was written. The
  general lesson is #627's own: *a stale blocker table is a stale cache*. Nothing
  in the repo re-derives that table; it is prose.
- **The 2013-14 wave-level `panel_ids:` block still points at
  `sect_cover_ls_w2.dta`**, the livestock-section cover, whose grain is the
  agricultural **holder** (3,812 rows / 3,670 households) and which covers only
  3,670 of the wave's 5,262 households. `data_scheme.yml` declares `panel_ids`
  as `materialize: make`, so the country-level script wins and the block is
  vestigial — but it is a wrong-source claim sitting in config, and it should
  either be deleted or shown to be load-bearing. Not touched here: it is outside
  #627's path and deleting config on a "probably unused" basis is exactly the
  unevidenced move `CLAUDE.md` forbids.

---
### Phase 3 — verification

- `2013-14/_/data_info.yml` + `2015-16/_/data_info.yml` (`individual_education`
  idxvars) — **OK (anchored on §3/§4)**: fixes the *identifier*, per the standing
  rule, rather than declaring a reducer; uses the same wave-native ids
  `household_roster` and `shocks` already use for those waves, so panel linkage
  via `id_walk` + `panel_ids` is unchanged.
- `tests/test_gh323_ethiopia_config.py` — **OK (anchored on §4)**: asserts at the
  **wave** level, upstream of `_normalize_dataframe_index`, and deliberately does
  **not** assert on post-collapse index uniqueness or on the order-dependent
  country-level row count.
- `test_no_dead_aggregation_key_in_ethiopia_config` — **OK (anchored on §3)**:
  pins D1 for this country, so the abandoned branch's `aggregation:` keys cannot
  be revived by a future salvage attempt.
- Ethiopia `cluster_features` config — **REINVENTION AVOIDED (§5)**: the fix
  exists on `development` as `3488b791`; this branch verifies it and changes
  nothing.
- `interview_date` `mapping.py` hooks — **REINVENTION AVOIDED (§5)**: measured
  redundant against `_enforce_canonical_dtypes`, so not salvaged.

---
### Measurements

All against **PR #627's core** (`origin/development` merged with
`origin/fix/323-site4-dfs-merge`, `git merge-tree` → `59b34cc2`), asserted at
runtime via `lsms_library.__file__`, with an **isolated `LSMS_DATA_DIR`** whose
only pre-populated tier is a symlinked `dvc-cache`.

**Negative control** — pre-`3488b791` Ethiopia config, same core:

| wave | result | cartesian cells | phantom rows |
|---|---|---|---|
| 2011-12 | **RuntimeError** — `df_geo` lacks `lat_dd_mod` (file has `LAT_DD_MOD`) | 0 | 0 |
| 2013-14 | 65,508 rows | 1 | **60,221** |
| 2015-16 | 57,786 rows | 1 | **52,832** |
| 2018-19 | **RuntimeError** — `df_geo` lacks `lat_dd_mod` (file has `lat_mod`) | 0 | 0 |
| 2021-22 | **RuntimeError** — geo file has no `ea_id` at all | 0 | 0 |

Reproduces #627's census exactly: 3 raising cells, 2 cartesian cells,
**113,053** phantom rows.

**Current `development` config, same core:**

| wave | result | cartesian cells | phantom rows |
|---|---|---|---|
| 2011-12 | 3,969 rows | 0 | 0 |
| 2013-14 | 5,262 rows | 0 | 0 |
| 2015-16 | 4,954 rows | 0 | 0 |
| 2018-19 | 6,770 rows | 0 | 0 |
| 2021-22 | 4,959 rows | 0 | 0 |

`Country('Ethiopia').cluster_features()` → **2,168 rows on a unique `(t, v)`**,
2,146 of them carrying Latitude. Per wave: 333 / 433 / 432 / 535 / 435 clusters.

**Full-country sweep, all 25 declared tables, same core:** `raised: 0`,
`cartesian: 0`, `dropped sub-dfs: 0`.

**#627's own test file** (`tests/test_gh323_site4_dfs_merge.py`, 16 tests) passes
against this config tree.

**`individual_education`**, identical conditions (`LSMS_NO_CACHE=1`, fresh
process, same script) before → after:

| | before | after |
|---|---|---|
| country rows | 59,092 | **63,181** (+4,089) |
| 2013-14 wave frame, unique `(t, i, pid)`? | **No** | Yes (23,785 / 23,785) |
| 2015-16 wave frame, unique `(t, i, pid)`? | **No** | Yes (23,393 / 23,393) |
| `GrainCollapseWarning`s | 2 | **0** |
| W2 `(i, pid)` pairs joinable to `household_roster` | **0 / 8,505** | 12,583 / 12,583 |
| W3 `(i, pid)` pairs joinable to `household_roster` | **0 / 12,597** | 12,599 / 12,599 |

The framework's own audit quantified the loss before the fix:

> `Ethiopia/individual_education/2013-14: declared index (t, i, pid) is NOT
> UNIQUE. Collapsing it with groupby().first() DESTROYED 5,247 of 23,785 rows
> whose values DISAGREE (1 conflicting index tuples). ... Additionally 5,248
> row(s) carry NaN in a declared index level and are DELETED OUTRIGHT`

> `Ethiopia/individual_education/2015-16: ... DESTROYED 5 of 23,393 rows whose
> values DISAGREE (5 conflicting index tuples).`

**Test negative control:** `tests/test_gh323_ethiopia_config.py` — **21/21 pass**
against the current config; **17/21 fail** against the pre-`3488b791` config tree
on the same core. (Of the 4 that still passed, 3 were the data-backed
`cluster_features` cases in waves whose raise was shadowed by a stale L2-wave
parquet — see §4; the config-level assertions failed 5/5 in every wave and cannot
be shadowed.)
