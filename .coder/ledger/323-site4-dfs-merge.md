# Prior-Art Ledger — GH #323 site 4 (the `dfs:` merge)

**Search tier used:** ripgrep + git floor over `lsms_library/country.py`, the 47
`dfs:` blocks in `countries/*/*/_/data_info.yml`, and the 17 core patches on the
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
| `LSMS_GRAIN_STRICT` | PR #614 (site 1) | warn-by-default / fatal-under-env escalation lever | yes (in #614) | **reuse the convention** (not yet on `development`; defined independently here, no symbol conflict) |
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
  `LSMS_GRAIN_STRICT=1`) because raising would break countries whose configs
  this PR may not touch, and because it changes no returned data — it reports.
  The required-column check RAISES, because it is the one failure that no
  downstream mechanism will ever catch, and its blast radius is a single
  country whose config fix is already written (`fix/323-ethiopia`).

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| cartesian detection | **salvage + correct** Ethiopia's `_merge_subframes` | keep the both-sides-duplicated test; replace the row-count-ceiling justification, which is unsound as stated |
| `merge_how:` YAML key | **keep** | not dead config — core reads it (`data_info.get('merge_how', 'outer')`), all five Ethiopia waves set `merge_how: left`, and it is tested end-to-end here |
| `aggregation:` YAML key | **discard** | D1. It stays in `_SCHEME_NON_COLUMN_KEYS` only so an old config carrying one is not mistaken for a required column |
| required-vs-optional parsing | **reuse** — factored to module-level `_required_scheme_columns`, shared with `Country._assert_built_required_columns` | two guards that disagree on "required" mean one of them lies |

## §6 The 40-country census (measured, `LSMS_NO_CACHE=1`)

67 `dfs:` merges exercised across 19 countries (the other 21 declare none).

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
| Ethiopia | 2011-12, 2018-19, 2021-22 | casing / missing `ea_id` | fixed on `fix/323-ethiopia` |
| Niger | 2011-12 | geo file has **no** lat/lon column at all | `optional: true` |

Nigeria has been served **without GPS in six waves** for as long as the #515
fallback has existed, because a `KeyError` on a case-mismatched column name was
swallowed and the table reported clean.

## §7 PP/PH round structure (coordinator's addendum) — measured, not assumed

The hypothesis was that a sub-frame keyed without the round dimension could
generate a cartesian *across* rounds. **Empirically empty at this site:**

- **0** of the 67 `dfs:` sub-frames library-wide is drawn from a `_pp_` / `_ph_`
  round file. PP/PH round-splitting happens exclusively on the *script* path
  (`materialize: make`), which never enters `Wave.grab_data`'s `dfs:` merge.
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

## §9 Open questions for the human
- **Sequencing.** The required-column hard error makes `cluster_features()` raise
  for **Ethiopia, Nigeria and Niger** until their configs are fixed. All ten are
  true positives on real, pre-existing bugs, and the fixes are one-liners — but
  only Ethiopia's is written. **This PR should land with, or after,** config
  fixes on `fix/323-nigeria` and `fix/323-niger`.
- **When to flip `LSMS_GRAIN_STRICT=1` in CI.** Once the 8 cartesian cells are
  re-keyed, the cartesian guard should become fatal by default.
- **Mali 2021-22** (4.3M phantom rows) is unowned and by far the largest cell.
