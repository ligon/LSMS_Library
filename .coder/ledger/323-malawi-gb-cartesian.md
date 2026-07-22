# Prior-Art Ledger — GH #323 site 4 / #627 (Malawi + Guinea-Bissau cartesians) and #637 (Malawi `.first()` sites)

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`; cites
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git floor. gitnexus not consulted — the config
half touches no symbol, and the `malawi.py` half is a comment + one `dropna`
inside a function whose only callers are the four wave scripts that import it
by name (enumerated by grep).

## §1 Task, restated

Three of the eight cartesian `dfs:` cells from PR #627's 40-country census were
unowned after Mali's went in PR #641:

| cell | L x R | merged | phantom |
|---|---|---|---|
| Malawi 2010-11 | 12,271 x 12,271 | 196,083 | 183,812 |
| Malawi 2019-20 | 14,612 x 11,434 | 185,842 | 171,230 |
| Guinea-Bissau 2018-19 | 5,351 x 450 | 5,410 | 59 |

Plus: review the three `groupby().first()` sites in `Malawi/_/malawi.py` for
KEY SOUNDNESS (#637). Config/script only, no `aggregation:` key, fix the merge
never the aftermath.

## §2 Existing machinery (this task's area)

| symbol | path | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `Wave._cartesian_keys` / `_merge_subframes` | `origin/fix/323-site4-dfs-merge` `country.py:909/945` | exact many-to-many detector + phantom count | 16 tests on that branch | **measuring instrument only**; core untouched |
| `Wave.grab_data` `dfs:` block | `country.py:~1112-1215` | outer-merges sub-frames on `merge_on ∪ {t}` | yes | untouched |
| sub-frame `df_edit` dispatch | `country.py:802` + `:997` — `column_mapping(<sub-frame name>, ...)` then `mapping_info.pop('df_edit')` | runs a country/wave-module function named after the SUB-FRAME on that sub-frame, **before** the merge | no direct test before this PR | **used** for Guinea-Bissau; now pinned by a test |
| `_collapse_to_cluster_grain` (site 2) | `country.py:~4490` | projects household-grain `cluster_features` onto `(t, v)` | yes | not touched — it is why `Wave.cluster_features()` is blind here |
| PR #641 `Mali/2021-22` | `Mali/2021-22/_/data_info.yml` | deleted the `dfs:` block for a single-file extraction | yes | **template**; not applicable to either country here (see §5) |
| PR #639 `fix/323-malawi-config` | `Malawi/{2004-05,2016-17}/_/data_info.yml` | fixed 2004-05's cluster key and 2016-17's `df_geo` merge key | yes | **this branch is stacked on it**; its two "deliberately not fixed" comments are replaced |
| `tests/conftest.py::requires_s3` | `tests/conftest.py:71` | data-free-CI skip marker | yes | reused |

## §3 Definitions & conventions in force

- **D1, fix the merge, never reduce afterwards** — `CLAUDE.md` "Grain Collapse";
  `SkunkWorks/grain_aggregation_policy.org` §3a. `aggregation:` is dead config.
- **`dfs:` merges exist to be collapsed, not grown** — `CLAUDE.md` "Gotchas with
  Teeth": *"Existing `dfs:` merges are grandfathered but should be collapsed
  when touched"*.
- **The published cluster GPS is a displaced cluster fix stamped on every
  household**, not household GPS — `CLAUDE.md` site-2 note (GH #161). Verified
  here for both Malawi waves (0 of 768 / 0 of 717 EAs carry two coordinates).
- **`.first()` skips NA per column**, so a conflicting group yields a composite
  belonging to no real row — `CLAUDE.md` "Grain Collapse". Wrong only when the
  duplicate rows are DIFFERENT ENTITIES; `skipna=False` was drafted and
  abandoned repo-wide.
- **EHCVM cluster identity**: `v: grappe`, `i: [grappe, menage]` — `CLAUDE.md`
  "EHCVM countries".
- **Only 2016-17's cross-sectional half is `cs-17-` prefixed in the wave
  scripts; every other half emits the raw wave hhid and relies on `id_walk` /
  `panel_ids` chaining** — `Malawi/_/CONTENTS.org`, "plot_features (GH #167)".
  Read *before* concluding anything about 2019-20's `i_prefix=''`; it is the
  documented decision, not a defect (see §4, trap 3).
- **IHS4's GPS displacement is "nearly but not exactly EA-constant"** — 7 of
  779 EAs carry more than one `lat_modified` — `Malawi/_/CONTENTS.org`,
  "GH #323: 2016-17 had no GPS". So the EA-constant-coordinate invariant this
  PR asserts holds for 2010-11 and 2019-20 and is *documented not to* for
  2016-17; the test is parametrized accordingly and says why.

## §4 Invariants & assumptions

- **A merge key duplicated in BOTH sub-frames is a cartesian by construction** —
  `_cartesian_keys` docstring; sound *and* complete.
- **The warm cache hides all of this.** L2-country is written post-collapse.
  Every number here was measured cold, in an isolated `LSMS_DATA_DIR` with only
  `dvc-cache` symlinked, with `LSMS_NO_CACHE=1`, against #627's core on
  `PYTHONPATH` (asserted in-process, not assumed).
- **`Wave.cluster_features()` cannot see a cartesian for Malawi** — site 2
  projects to `(t, v)` first, giving 768 / 819 rows with or without the bug.
  Tests must call `grab_data('cluster_features')`. (Guinea-Bissau declares no
  `i`, so site 2 does not fire and its wave frame is the merged frame.)
- **An in-process `.first()` patch cannot see a `materialize: make` build** —
  hence `runpy` (PR #646's method). All three #637 tables are `materialize:
  make`.
- **"Exact duplicates" is not reassurance and invariance can be missingness** —
  PR #646 / the Tanzania key. Disposed of here by there being no duplicate
  groups at all on a non-null key, which is a stronger statement than "the
  duplicates agreed".
- **A BROKEN KEY CAN PRODUCE ZERO DUPLICATES** (#637 trap 3, the Tanzania
  `shocks` inversion). A key from the wrong namespace makes every replicated
  row its own distinct "household", so the duplicate-count instrument reads
  clean on a thoroughly broken key. **"0 duplicates" is therefore necessary,
  not sufficient**; the discriminating check is a **per-wave** overlap of the
  table's `i` against the roster's — per wave, because clean waves otherwise
  carry broken ones through an aggregate check. Run for all three Malawi
  sites: **100% in every wave**, matching literally (`101011000014` on both
  sides), with no row-count inflation relative to source households ×
  strategies. Worth running rather than waving away: Malawi 2019-20 has the
  surface shape of a namespace split — `cs_i` (`'cs-19-' + format_id`) in
  `data_info.yml` for `sample` / `household_roster` / `cluster_features` vs.
  `i_prefix=''` in all three wave scripts. **`CONTENTS.org` already records
  that asymmetry as deliberate** (§3 above), and the measurement confirms it:
  the prefix survives to the API on neither side (0 of 14,612 ids carry it, in
  any table), so both are in the raw `case_id` namespace and they agree.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| Malawi `df_geo` key | **re-key** `v: ea_id` → `i: case_id` (`cs_i` in 2019-20), `merge_on: [i]` | files are exactly 1:1 on `case_id`; #627's first named remedy |
| Malawi: delete the `dfs:` block (Mali's cure) | **rejected** | lat/lon exist only in the geovariables file; the cover page has none |
| Guinea-Bissau: re-key on `i` | **rejected** | the GPS file is genuinely grappe-grain and carries nothing finer; both copies of every duplicate agree on `vague` |
| Guinea-Bissau: single-file cure | **rejected** | cover page has no GPS columns at all (checked) |
| Guinea-Bissau: `df_geo` hook + `drop_duplicates()` | **taken** | #627's *second* named remedy — reduce the sub-frame to merge-key grain BEFORE the merge. Lossless: the 5 extra rows are byte-identical incl. GPS timestamp |
| Guinea-Bissau: `groupby().first()` / `keep='first'` | **rejected** | would silently choose between two disagreeing fixes if one ever ships; `drop_duplicates()` lets the guard fire instead |
| Guinea-Bissau: convert to a `materialize: make` script | **rejected** | far heavier than 59 rows warrant, and moves a YAML-expressible table onto the script path against the repo's stated preference |
| `.first(skipna=False)` at any #637 site | **rejected** | no multi-row groups exist; the repo abandoned that approach |
| `aggregation:` key anywhere | **rejected** | dead config, D1 |

## §6 Open questions for the human

- **The Guinea-Bissau hook is dispatched BY NAME off the sub-frame key.** This
  is real, current behaviour on both `development` and #627's branch, but it is
  undocumented and there was **no prior art for it** (`rg '^def df_geo'` across
  all 40 countries: zero hits). It is heavily documented in three places and
  pinned by a test, but a reviewer may prefer a different mechanism. If so, the
  alternatives are ranked in §5.
- **2019-20 at HOUSEHOLD grain**: 8 IHS5 geo rows have a NULL coordinate while
  their 15 EA siblings carry the EA's fix. Under the `v`-merge those 8
  households borrowed a sibling's value; under the `i`-merge they get their own
  NULL. The returned `(t, v)` table is unchanged (`.first()` skips NA), and the
  household-grain intermediate is now honest — but it *is* a behavioural
  difference, and it is the only one.
- **`food_coping` / `months_food_inadequate` build only the cross-sectional
  half in 2016-17 and 2019-20** (12,447 and 11,434 households; the IHPS panel
  halves are absent). Observed while auditing #637; a coverage gap, not a key
  defect, and out of scope here.

---
### Phase 3 — verification

- `Malawi/{2010-11,2019-20}/_/data_info.yml` — **OK (§3, §5)**: `merge_on: [i]`,
  no `aggregation:`, cured at the merge. Phantom 183,812 → 0 and 171,230 → 0.
- `Guinea-Bissau/2018-19/_/{data_info.yml,mapping.py}` — **OK (§3, §5)**:
  reduced before the merge, de-duplication not aggregation. Phantom 59 → 0.
- Value preservation — **OK (§4)**: country-level `cluster_features`
  bit-for-bit identical for both countries (`DataFrame.equals` True, index
  equal, dtypes equal; Malawi 3,235 x 5, Guinea-Bissau 450 x 4).
- `tests/test_gh323_malawi_gb_cartesian.py` — **OK (§4)**: asserts on
  `grab_data`, not `Wave.cluster_features()`. **Negative control run** (pre-fix
  configs restored, fresh isolated data root): `6 failed, 10 passed` — the three
  cartesian tests report 5410 / 196083 / 185842 and the three structural tests
  fail, while the 10 source-invariant and cluster-count tests pass *with the bug
  fully present*, which is exactly the blindness the module docstring warns
  about. After: `16 passed`.
- `Malawi/_/malawi.py` — **OK (§3)**: no `skipna=False`, no reducer added; one
  `dropna(subset=['plot_id'])` that makes an existing silent deletion explicit
  and is a provable no-op (`groupby(dropna=True)` already removed those rows).
