# Prior-Art Ledger — GH #323 site 4 (the `dfs:` merge)

**Search tier used:** ripgrep + git floor over `lsms_library/country.py`, every
`dfs:` block in `countries/*/*/_/data_info.yml` (**76** at the branch's base
`45aee170`; **80** after merging `development` — see §6 and §12; an earlier draft
of this line said 47, which was neither count), and the 17 core patches on the
`fix/323-*` branches. The affected surface is one function, so the floor is
complete.

## §1 Task, restated
`Wave.grab_data`'s `dfs:` sub-dataframe merge did `pd.merge(..., how='outer')`
with no cardinality guard. When `merge_on` is non-unique in **both** sub-frames
the merge is many-to-many and emits a **cartesian product within each key
group** — it MANUFACTURES the duplicate rows that sites 1–3 of #323 then
silently collapse. This is upstream of the whole class. Fix the merge; do not
reduce the explosion afterwards.

Second, smaller defect in the same block: the GH #515 optional-sub-df fallback
swallows a `KeyError` and drops the sub-df. Where that sub-df was the sole
supplier of a **required** declared column, the table is served with the column
100% absent, behind a warning.

## §2 Existing machinery (this task's area)

| symbol | path | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `Wave.grab_data` `dfs:` merge | `country.py:~1032` | `pd.merge(..., how='outer')` on `merge_on` | no | **extend** — cardinality guard + `merge_how:` |
| GH #515 optional-sub-df fallback | `country.py:~1000-1022` | swallows KeyError, drops sub-df with a warning | no | **extend** — hard error when the drop costs a required column |
| `Country._assert_built_required_columns` | `country.py:2355` | required-vs-optional scheme-column parsing (`_skip` set + `optional:`) | yes | **reuse** — factored out to module-level `_required_scheme_columns` and shared |
| `_merge_subframes` / `_required_scheme_columns` | `fix/323-ethiopia` | the same guard, entangled with a rejected Design-A core patch | its own tests | **salvage** — guard kept; the `aggregation:` reader discarded per D1 |
| `_grain_strict()` / `LSMS_GRAIN_STRICT` | `country.py:~4409` (PR #614, site 1) | warn-by-default / fatal-under-env escalation lever | yes (in #614) | **reuse the SYMBOL** — see the correction below |
| `_normalize_dataframe_index` | `country.py:~4190` | site 1 — the generic collapse | — | **do not touch** (PR #614) |
| `Wave.cluster_features` | `country.py:~1177` | site 2 — the hardcoded collapse | — | **do not touch** (PR #617) |

## §3 Definitions & conventions in force
- **Core does not aggregate** — D1 of `DESIGN_grain_collapse_sites_2026-07-13.org`,
  upholding `SkunkWorks/grain_aggregation_policy.org`. No `aggregation:` key is
  wired up here. A cartesian is fixed by making the merge correct, never by
  reducing afterwards.
- Required vs optional declared columns: a `data_scheme.yml` entry key is
  REQUIRED unless `optional: true` (`country.py`, `_SCHEME_NON_COLUMN_KEYS`).
- `cluster_features` owns `v`; no other feature puts `v` in its index — CLAUDE.md.
- class-2 (silently MISSING) is safer than class-1 (silently WRONG) — but a
  *required* column silently missing from a served table is not a tolerable
  class-2; nothing downstream will ever notice it.

## §4 Invariants & assumptions
- **The cartesian test is exact, not a heuristic.** A join on `keys` is
  many-to-many *iff* some key value is duplicated in BOTH frames. Sound and
  complete. The row-count ceiling `len(out) <= len(left) + len(right)` that the
  Ethiopia branch's docstring claimed as a "proof" is sound but **not
  complete** — two 3-row frames sharing one 2×2 key and one 1×1 key yield 5
  rows and never breach the 6-row ceiling. Rejected.
- **Null keys count.** `pd.merge` MATCHES null keys to each other, so a null key
  duplicated on both sides explodes like any other. `dropna=False` in the
  groupby is load-bearing. (Distinct from site 3, where `groupby().first()`
  *deletes* NaN-keyed rows; here they *multiply*.)
- **Severity is split, deliberately.** The cartesian guard WARNS (fatal under
  `LSMS_GRAIN_STRICT`, read via `_grain_strict()`) because raising would break
  countries whose configs this PR may not touch, and because it changes no
  returned data — it reports. The required-column check RAISES, because it is
  the one failure that no downstream mechanism will ever catch. *(An earlier
  draft of this bullet said its blast radius was "a single country whose config
  fix is already written". It was **three** countries and ten cells — see §6 —
  all now fixed and merged, §10.)*
- **What the required-column check actually asserts.** "A mis-named column in a
  `dfs:` sub-df is fatal" — NOT "a required column is never absent". It fires
  only when a sub-df was dropped, so a wave with no `dfs:` block is outside its
  reach (Niger 2014-15, §11.3). And its escape hatch `optional: true` is
  **country**-grain while the check is per-wave.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| cartesian detection | **salvage + correct** Ethiopia's `_merge_subframes` | keep the both-sides-duplicated test; replace the row-count-ceiling justification, which is unsound as stated |
| `merge_how:` YAML key | **keep** | not dead config — core reads it (`data_info.get('merge_how', 'outer')`), all five Ethiopia waves set `merge_how: left`, and it is tested end-to-end here |
| `aggregation:` YAML key | **discard** | D1. It stays in `_SCHEME_NON_COLUMN_KEYS` only so an old config carrying one is not mistaken for a required column |
| required-vs-optional parsing | **reuse** — factored to module-level `_required_scheme_columns`, shared with `Country._assert_built_required_columns` | two guards that disagree on "required" mean one of them lies |
| `LSMS_GRAIN_STRICT` reader | **CORRECTED after review** — call `_grain_strict()`, do not re-read the env | see §11.1: the private copy was not bit-identical, and the divergence was measurable |

## §6 The 40-country census (measured, `LSMS_NO_CACHE=1`, config tree at `45aee170`)

> **Correction (post-review).** This section originally opened *"67 `dfs:`
> merges exercised across 19 countries (the other 21 declare none)."* The
> declaration count is wrong and is checkable statically: parsing every
> `countries/*/*/_/data_info.yml` at `45aee170` (the branch's base) yields
> **76 `dfs:` blocks across 20 countries** — the reviewer's independent count,
> reproduced here. 67 was the number of merges the sweep actually *executed*,
> which is smaller (Nepal's 2 blocks cannot load; some declared wave dirs are
> not in `Country.waves`). Two different quantities were reported as one. The
> per-cell table below is unaffected and was reproduced to the row by the
> reviewer.

**8 cartesian cells / 5 countries / 4,907,774 phantom rows.** Every one is
`cluster_features` merged on the CLUSTER key `v` when both sub-frames are
household-grain.

| country | wave | left | right | merged | phantom |
|---|---|---:|---:|---:|---:|
| Mali | 2021-22 | 393,480 | 6,143 | 4,718,148 | **4,324,668** |
| Malawi | 2010-11 | 12,271 | 12,271 | 196,083 | 183,812 |
| Malawi | 2019-20 | 14,612 | 11,434 | 185,842 | 171,230 |
| Ethiopia | 2013-14 | 5,262 | 5,287 | 65,508 | 60,221 |
| Nigeria | 2012Q3 | 4,859 | 4,802 | 62,538 | 57,476 |
| Nigeria | 2013Q1 | 4,859 | 4,802 | 62,538 | 57,476 |
| Ethiopia | 2015-16 | 4,954 | 4,954 | 57,786 | 52,832 |
| Guinea-Bissau | 2018-19 | 5,351 | 450 | 5,410 | 59 |

Ethiopia — the only case the branches knew about — is **2.3%** of the total.
Mali 2021-22 alone is 88%: a 393,480-row (individual-grain) frame joined to a
6,143-row frame on `v`.

**10 required-column errors / 3 countries.** All true positives; the data is
usually sitting in the file under a different name:

| country | waves | cause | fix |
|---|---|---|---|
| Nigeria | 2010Q3, 2011Q1 | YAML asks `LAT_DD_MOD`; file has `lat_dd_mod` | casing |
| Nigeria | 2015Q3, 2016Q1 | YAML asks `lat_dd_mod`; file has `LAT_DD_MOD` | casing |
| Nigeria | 2018Q3, 2019Q1 | geo file is keyed `hhid`, YAML asks `ea` | re-key to `i: hhid` |
| Ethiopia | 2011-12, 2018-19, 2021-22 | casing / missing `ea_id` | fixed on `fix/323-ethiopia` → merged as #628 |
| Niger | 2011-12 | **the YAML pointed `df_geo` at the wrong file** | point it at the sibling `NER_EA_Offsets.dta` — merged in `3488b791` |

Nigeria has been served **without GPS in six waves** for as long as the #515
fallback has existed, because a `KeyError` on a case-mismatched column name was
swallowed and the table reported clean.

> **Correction (post-review).** The Niger row originally read *"geo file has
> **no** lat/lon column at all"*, prescribing `optional: true`. Half right and
> wholly the wrong conclusion. `NER_HouseholdGeovars_Y1.dta` (4051 × 43) indeed
> has no coordinate column — re-verified here from source, derived raster
> covariates only. But the coordinates ship in a **sibling file in the same,
> already-DVC-tracked directory**: `NER_EA_Offsets.dta`, `271 × ['grappe',
> 'LAT_DD_MOD', 'LON_DD_MOD']`, 270 unique non-null grappes, latitudes
> 11.876–18.747 N — measured here, not quoted. `optional: true` would have
> deleted a real column *and*, because `data_scheme.yml` is country-grain,
> disarmed this PR's own guard for the three Niger waves that do carry
> coordinates. The fix that landed (`3488b791`) re-points the file; Niger
> 2011-12 now serves 270 clusters with 270 coordinates. **This is the exact
> failure mode CLAUDE.md warns about under "Adjudicating `absent` cells": never
> write an unevidenced "no module here" claim.**

## §7 PP/PH round structure (coordinator's addendum) — measured, not assumed

The hypothesis was that a sub-frame keyed without the round dimension could
generate a cartesian *across* rounds. **Empirically empty at this site:**

- **0** `dfs:` sub-frame library-wide is drawn from a file whose **name** carries
  a `_pp_` / `_ph_` round marker. PP/PH round-splitting happens exclusively on
  the *script* path (`materialize: make`), which never enters
  `Wave.grab_data`'s `dfs:` merge.

  > **Correction (post-review).** As originally written ("0 of the 67 sub-frames
  > is drawn from a `_pp_`/`_ph_` round file") this overstated a **filename**
  > test into a claim about provenance. Nigeria's `df_main` *is* a post-planting
  > file — `Post Planting Wave 1/.../secta_plantingw1.dta` — it just does not
  > carry the marker in its basename. The substantive conclusion is unchanged
  > and rests on the two bullets below, which are about round *structure*, not
  > filenames: `t` is constant within every `dfs:` merge in the corpus, so no
  > merge here can cross rounds.
- **Tanzania** declares no `dfs:` block at all, so its `2008-15/` multi-round
  folder cannot reach this code.
- **Nigeria** is PP/PH *and* has a `wave_folder_map`, and `Country.waves`
  already returns ROUND labels (`2012Q3`, `2013Q1`, …) — so the census is
  natively per-round for the one PP/PH country that has `dfs:` blocks. The
  identical figures for 2012Q3 and 2013Q1 are the same folder read under two
  `t` values, each independently cartesian.
- **Ethiopia** `cluster_features` sources the household **cover** file
  (`sect_cover_hh_w2.dta`), not a sectional PP/PH file, and `wave_folder_map`
  is empty. `t` is constant within the merge; there is no round dimension.

## §8 Correction to the brief: the re-key is `household_id2`, not `household_id`

The task brief said the Ethiopia fix re-keys `v: ea_id` → `i: household_id`.
**Measured, `household_id` is NOT unique** in either sub-frame of W2/W3 — it is
the *W1* id, blank for households new to the panel, so it repeats on the empty
value. Re-keying to it would have traded an EA-grain cartesian for a **null-key**
cartesian (`pd.merge` matches nulls), which is precisely what the `dropna=False`
in `_cartesian_keys` exists to catch. The unique key is **`household_id2`**, and
`fix/323-ethiopia` correctly uses it.

Verified end-to-end: **this core + `fix/323-ethiopia`'s config = all 5 waves
clean**, 0 cartesians, 0 required-column errors, and `merge_how: left` honoured
(merged rows == left rows exactly, in all five).

## §9 The suite is green, and the green is NOT proof

> **Status note (post-review).** The three raises below are **fixed** — see
> §10. This section is kept because its *lesson* is what the reviewer, the
> Ethiopia agent and the Malawi agent each independently re-derived: a green
> suite was not evidence here, and neither is a `sane` coverage cell. The row
> counts and mtimes are a snapshot of `a54568f9`, not of the current branch.

Full suite at `a54568f9`: **3 failed, 3546 passed, 128 skipped** — and the 3 are
exactly the 3 pre-existing failures (`test_currency::test_feature_ghana_per_wave`,
`test_table_structure::*[CotedIvoire/cluster_features]` ×2). **Zero new
failures.**

That green does not mean what it appears to mean, and the reason is #323's own
pathology. Verified at the time by direct API call on a warm cache:

```
Country('Ethiopia').cluster_features()  -> RuntimeError
Country('Nigeria').cluster_features()   -> RuntimeError
Country('Niger').cluster_features()     -> RuntimeError
```

The suite does not see it, for two different reasons:

- **Ethiopia, Niger** — `test_table_structure` enumerates `var/*.parquet` and
  reads them with `pd.read_parquet`, by its own stated contract ("only test what
  is already cached"). It **never rebuilds and never checks the cache hash**.
  Both countries still carry a `cluster_features.parquet` written *yesterday by
  the pre-change code* (mtime 2026-07-12 21:16). The test reads the old file and
  passes.
- **Nigeria** — has **no** cached `cluster_features.parquet` at all, so the cell
  is simply absent from `_find_cached_parquets()` and drops out of the test
  matrix silently. Not tested, not reported.

This is the same sentence the design note opens with: *the bug hid behind the
cache that the bug poisoned.* Here it is hiding my own change's blast radius.
**Do not read the green tick as clearance for these three countries.**

One correction to how this was framed, from re-deriving it under review: the
mechanism above is `test_table_structure`'s stated read-only contract, **not**
the v0.8.0 hash failing to notice a core change. The hash does notice — see
§11.5. Both facts are needed: a cache-reading test is blind to a build-path
guard *even though* the build-path guard correctly invalidates the cache, because
the test never asks for a build.

## §10 Sequencing — RESOLVED (was "open questions for the human")

The blocker is discharged. Every config named below is now an ancestor of
`origin/development`, and this branch has been merged with `development` so the
PR carries them.

| country | fix | status | re-verified here, cold |
|---|---|---|---|
| Ethiopia | #628 (`3488b791`) + #644 | merged | ✅ 2,168 rows, 5/5 waves, Latitude on 2,146 |
| Niger | #628 (`3488b791`) | merged | ✅ 1,599 rows, 4/4 waves, 2011-12 now 270/270 coords |
| Nigeria | #625 (`194b55d0`) | merged | ✅ 5,248 rows, Latitude on 5,200, 8 round-waves |

Method for that column: this branch merged with `origin/development`, imported
with `lsms_library.__file__` **asserted** into the worktree, config via
`LSMS_COUNTRIES_ROOT`, and an **isolated `LSMS_DATA_DIR`** wiped before the run
with only `dvc-cache` symlinked. `Country(c).cluster_features()` for all three:
**0 raises, 0 site-4 cartesian warnings.**

**Still open:**

- **When to flip `LSMS_GRAIN_STRICT` on in CI.** Corrected framing: this is
  *not* site 4's decision to make. The variable is now read by sites 1, 2 and 4
  through one predicate, so turning it on makes **all three** fatal. Measured
  today: with `LSMS_GRAIN_STRICT=1`, Ethiopia / Niger / Nigeria
  `cluster_features()` all raise `GrainCollapseError` — from **site 2**, the
  household→cluster projection, not from this site. Site 4's own census being
  clean is necessary and nowhere near sufficient.
- **The remaining cartesian cells.** Mali 2021-22 (4,324,668 phantom) is fixed
  and merged (#641). Ethiopia's two are cleared by #628. Malawi 2010-11 /
  2019-20 and Guinea-Bissau 2018-19 are written but **open** in #653. Nigeria
  2012Q3 / 2013Q1 (57,476 each) — check against #625, which re-keyed `v`.
  §12 records the census as re-measured on the merged tree.

## §11 Response to the adversarial review (2026-07-22)

The review returned **FIX_FIRST** with the code judged correct — every headline
number reproduced to the row and all eight mutants were killed by the tests.
What follows is what changed and what is disputed. Where a finding is disputed
it is disputed **with a measurement**, not an argument.

### §11.1 Reuse: `LSMS_GRAIN_STRICT` must be read through `_grain_strict()` — FIXED

§2 of this ledger asserted the lever was *"not yet on `development`; defined
independently here, no symbol conflict."* That was true when written and is
false now: PR #614 merged, and `_grain_strict()` lives in this same module. The
private copy was **not** bit-identical — `os.environ.get('LSMS_GRAIN_STRICT')`
is truthy for `"0"` and `"false"`, `_grain_strict()` is not. Measured:
`LSMS_GRAIN_STRICT=0` made site 4 **raise** while site 1 stayed in warn mode.
One lever with two readers is a future defect; now there is one reader.
Discriminating test: `test_grain_strict_is_read_through_the_one_shared_predicate`
(fails on the old implementation for `0`/`false`/`no`/`off`).

### §11.2 The required-column check judged "present" too early — FIXED

It ran between `set_index` and `apply_derived`, while its country-level twin
`_assert_built_required_columns` runs **post-**`_finalize_result`. Two guards
sharing `_required_scheme_columns` but disagreeing on *when* presence is judged
is the same trap the shared helper was factored out to avoid. Moved below
`derived:`, `drop:` and the `df_edit` hook. Discriminating test:
`test_required_column_supplied_by_the_wave_hook_is_not_reported_absent` (raises
`RuntimeError` with the check in its old position). Latent when written — no
live config depended on it — fixed anyway, because latent is a property of
today's configs, not of the code.

### §11.3 The escape hatch is country-wide, the invariant is not — DOCUMENTED

`data_scheme.yml` is one file per **country**, so `optional: true` disarms the
column for every wave and every script-path build, while the check is per-wave.
The error message said *"if they are genuinely unavailable for this wave"*,
which reads as if the hatch were per-wave; it now says so plainly and points at
the column name as the real fix. Pinned by
`test_hard_error_says_optional_true_is_country_wide`.

The reviewer's sharper point is recorded in CLAUDE.md and the skill: **read this
guard as "a mis-named column in a `dfs:` sub-df is fatal", not "a required
column is never absent."** It fires only on a *dropped sub-df*, so a wave with
no `dfs:` block can serve a declared column 100% absent and never trip it.
Verified: Niger **2014-15** declares `Latitude: float` and serves 270 clusters
with **0** populated, no raise, because that wave's `cluster_features` is a
single-file extraction.

### §11.4 `merge_how:` prose overstated what `left` prevents — CORRECTED

Three places (CLAUDE.md, the skill, the `merge_how` comment in `country.py`)
claimed the `outer` orphans *"collapse together into one phantom null-keyed
row"*. Re-measured on Ethiopia 2013-14 under the current config, two isolated
cold processes differing only in whether `merge_how: left` is present:

| | wave rows | null-`v` rows | delivered clusters | ΣLatitude | `District` dtype |
|---|---:|---:|---:|---:|---|
| `outer` | 5,287 | 25 | 433 | 4070.3702 | float64 |
| `left`  | 5,262 | 0 | 433 | 4070.3702 | int8 |

The orphans do not survive to the delivered table: the cluster-grain collapse
**deletes** them, because `groupby` drops null keys. `DataFrame.equals` is
`False` between the two, but every *value* is identical — the only difference is
the widened dtype. So `merge_how: left` is not a data fix; it is worth declaring
because it stops manufacturing null-keyed rows for site 1 to delete and stops
the merge widening an integer column, and its cost is a lost diagnostic (under
`outer` the site-1 report *told* you 25 geo households had no cover page).

**One refinement the review did not make, found while re-deriving it.** The
"phantom 434th EA" story is not false — it is about a *different config*. Before
#628, Ethiopia's `df_geo` declared `v: ea_id2`, so the geo file's orphan EA
arrived as a real, non-null 434th cluster. Since #628 `df_geo` is keyed on `i`
only, so orphans have a **null** `v` instead. `Ethiopia/_/CONTENTS.org` (~L531)
and `Ethiopia/2013-14/_/data_info.yml`'s `merge_how` comment still tell the
old-config story next to the new config. Not edited here (those files belong to
#628, and this PR touches no country config); flagged on the PR thread.

### §11.5 "The hard error is cache-state-dependent" — DISPUTED as stated

The finding's premise was: *"`lsms_library/country.py` is not part of the v0.8.0
content hash, so a warm L2-country parquet is served without ever entering
`grab_data`."* **The premise is false, and it is measurable.** `Wave.grab_data`
carries `@build_transform()`, and `Wave._input_hash` folds
`btf=build_transforms_fingerprint(table)` — which walks the tagged closures'
ASTs — into every wave hash, which `Country._table_cache_hash` composes.
Measured, holding the config tree fixed and varying only the core:

```
origin/development  btf(cluster_features) = 16e4e8c6c84e6a55b0...
this branch         btf(cluster_features) = 9c475fa1150055b86c...
```

and mutating *only* `_merge_subframes`'s body moves it again
(`7438a585…`). Nigeria's real warm parquet at
`~/.local/share/lsms_library/Nigeria/var/cluster_features.parquet` grades
**`stale`** under this branch's core. So landing the guard does invalidate;
users are not silently served the pre-guard table on the ordinary path.

**Two narrower bypasses are real** and the conclusion (verify cold) survives:

1. A **hashless** pre-v0.8.0 parquet grades `legacy` → trust-once **and
   re-stamp**, so it is served unguarded once and then looks `fresh` forever.
2. `assume_cache_fresh=True` skips the check outright.

Recorded in CLAUDE.md on that narrower, correct basis.

### §11.6 Merge order — DISCHARGED, see §10

The review's one HIGH finding. It named #625 as the last open leg; #625 merged
(`194b55d0`). All three countries re-verified cold above.

### §11.7 Not accepted as a defect: "the fixtures might convert exceptions into skips"


Checked, since it would make the end-to-end tests vacuous. It does not happen
here. `tests/test_gh323_site4_dfs_merge.py` builds a synthetic one-wave country
in `tmp_path` from CSVs it writes itself — no microdata, no S3, no DVC, no
`requires_s3` marker and no `pytest.skip` anywhere in the module. Every one of
the 28 tests executes on every run, and two of them are proved discriminating by
reverting the change (§11.1, §11.2).

## §12 The census, RE-MEASURED on the merged tree (2026-07-22)

The §6 census was taken at `45aee170` and is a historical record. This branch is
now merged with `origin/development` (`4c236d11`), which carries #625 (Nigeria),
#628 + #644 (Ethiopia, Niger) and #641 (Mali). Re-swept from scratch, in an
isolated `LSMS_DATA_DIR` (wiped, only `dvc-cache` symlinked), with this
branch's core asserted on import:

```
DECLARED blocks: 80 | country/wave dirs: 48 | countries: 20
EXERCISED merges: 90        # > 80: Nigeria's wave_folder_map replays one
                            # wave dir under two round labels
CARTESIAN cells: 3
```

| cell | merged rows | phantom | owner |
|---|---:|---:|---|
| Malawi 2010-11 / `cluster_features` | 196,083 | 183,812 | **#653, open** |
| Malawi 2019-20 / `cluster_features` | 185,842 | 171,230 | **#653, open** |
| Guinea-Bissau 2018-19 / `cluster_features` | 5,410 | 59 | **#653, open** |

Those three phantom counts are **identical to the ones §6 recorded**, measured
by a different sweep on a different day — the census reproduces.

Cleared since §6: Mali 2021-22 (4,324,668 — #641 deleted the merge outright,
the cover page carried the geography), Ethiopia 2013-14 + 2015-16 (60,221 +
52,832 — #628's re-key to `household_id2`), Nigeria 2012Q3 + 2013Q1 (57,476
each — #625's `v` re-key). **4,552,673 of 4,907,774 phantom rows gone, 92.8%;
355,101 remain, all three in one open PR.**

Only failures in the sweep: Nepal 1995-96 and 2003-04 `sample`,
`PathMissingError` — no data in the repository, pre-existing and unrelated.
**0 required-column raises anywhere in the library.**
