# Prior-Art Ledger — GH #323 (Uganda): silent duplicate-index collapse

**Search tier used:** ripgrep + git, plus the consolidation note
`slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org` (branch
`origin/docs/323-grain-collapse-sites`) and the rescued diagnosis at
`rescue/2026-07-21/323-uganda`. Every measurement below is a COLD build
(`LSMS_NO_CACHE=1`) against the worktree config tree
(`LSMS_COUNTRIES_ROOT=<worktree>/lsms_library/countries`).

## §1 Task, restated

Uganda is hit by the #323 grain collapse two independent ways.

* **`people_last7days` (2018-19, 2019-20)** — INDEX_INCOMPLETE, silently
  **wrong**. Source `GSEC15A.dta` is LONG from 2018-19 on: two rows per `hhid`
  keyed by `CEA01` ∈ {`Household members`, `Visitors`}, `CEA01A-D` being the
  counts *for the selected category*. The declared index is `(i, t)` and
  `CEA01` was left undeclared (the YAML commented it out), so both rows
  collided and `first()` kept whichever the file lists first — a coin flip that
  served VISITOR counts to 50.5% / 49.0% of households.
* **`cluster_features` (all 8 waves)** — the household→cluster reduction is
  *intended* (`final_index: [t, v]`, GH #161) but was **undeclared**, so it ran
  through the same `first()`. Three real defects rode on it: `v` is not a
  cluster key in 2018-19 / 2019-20; 2009-10's 565 out-of-frame households got
  no cluster at all; `Rural` (and GPS) are not parish attributes.

**Scope constraint (the thing that changed since the rescue snapshot):** this
is a CONFIG-ONLY PR. Decision D1 of the consolidation note — *core does not
aggregate* — puts every file under `lsms_library/*.py` off limits, and makes
`aggregation:` YAML keys dead config that must not be added.

## §2 Existing machinery (this task's area)

| symbol | path | what it does | tested? | reuse / extend / new |
|--------|------|--------------|---------|----------------------|
| `reduce_to_agreed` | `build_transforms.py:422` | lossless-or-loud grain reducer; agree → value, NaN = absence, disagree → raise or `<NA>`+warn | yes (`tests/test_gh323_explicit_reducers.py`) | **reuse verbatim** — landed as PR #618 for exactly this |
| `collapse_to_cluster_grain` | `build_transforms.py:514` | the named household→cluster case of the above | yes | reuse (via `reduce_to_agreed`; Uganda needs a `v`-drop step first) |
| `fill_v_with_coord_bin` | `build_transforms.py:218` | blank `v` → synthetic `@lat,lon` bin | yes | **reuse** — and reach it DECLARATIVELY, via the existing `derived:` block |
| `apply_derived` / `derived:` | `build_transforms.py:311` | YAML transformer dispatch; runs *after* `set_index(final_index)` | yes | reuse (the "after set_index" detail dictates the YAML shape — see §6.2) |
| `Wave.column_mapping` → `final_mapping['df_edit']` | `country.py:802` | dispatches a function named after the table as that table's frame hook, country module first then wave `mapping.py` | — | reuse: the sanctioned country-level extension point |
| `uganda.v` | `Uganda/_/uganda.py` | country-level scalar `v` = `format_id` | no | **override** per-wave (a list-valued `v` hands the formatter a whole row) |
| `_collapse_to_cluster_grain` | `country.py:4490` | Site 2's audit-then-`.first()` | yes | **do not touch** (core; PR #617) |
| `_normalize_dataframe_index` | `country.py:4540` | Site 1's audit-then-`.first()` | yes | **do not touch** (core; PR #614) |
| `aggregation:` block in `data_scheme.yml` | 9 countries | **dead config — nothing reads it** | n/a | **forbidden** (D1); the rescue snapshot's version of this task tried to make it load-bearing |

## §3 Definitions & conventions in force

- `sample()` is the single source of truth for a household's cluster; `v` is
  joined from it at API time — `CLAUDE.md` "`sample()` and Cluster Identity",
  `_join_v_from_sample`. ⇒ **`sample.v` and `cluster_features.v` must be the
  same key**, or the join matches nothing. (Verified after the change: 0
  `cluster_features` `(t, v)` pairs unknown to `sample` in any wave.)
- `cluster_features` owns `v`; index `(t, v)` — `Uganda/_/data_scheme.yml`.
- "NO AGGREGATION IN CORE" — `SkunkWorks/grain_aggregation_policy.org`, and
  D1 of `slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org`.
- D2 of the same note: NaN-key rows are DELETED and REPORTED, never retained.
- `format_id` is auto-applied to `idxvars`, not `myvars` — `CLAUDE.md`.

## §4 Invariants & assumptions

- **The #323 audit fires only on a COLD build.** The collapse is baked into the
  L2-country cache, so the bug hides behind the cache it poisoned. Every
  measurement here ran under `LSMS_NO_CACHE=1`.
- `LSMS_COUNTRIES_ROOT` (not `PYTHONPATH`) is the correct lever for a
  config-only worktree; asserted before trusting any run.
- `comm` (2005-06 … 2011-12) is the **2005-06 EA of origin**, so its
  multi-district groups in later waves are panel **movers**, not a code
  collision.
- Parish **names** are not unique in Uganda (`CENTRAL` occurs in 10 districts);
  parish **codes** in 2018-19 / 2019-20 are broken (19 distinct values).
- `_audit_index_collapse` counts a NaN-vs-value difference as destruction. That
  is deliberately conservative and it dominates Uganda's headline numbers:
  2005-06 reports 2,551 destroyed rows but has **zero** clusters whose
  households actually disagree on `Region`/`Rural`/`District` — they differ
  only in whether the geovar carried a GPS fix. `reduce_to_agreed`'s default
  (`na_is_conflict=False`) reads that as absence, so those completions are
  lossless and silent.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| member/visitor row filter | **new, but no new primitive**: extract `CEA01` as a `_category` myvar + a country-level `people_last7days(df)` hook | the rescue snapshot added a `where:` row-filter primitive to `local_tools.df_data_grabber` — that is CORE, forbidden by D1 (and the same category as Mali's `({const: value})`, which the consolidation note puts on its own PR). The existing `df_edit` hook expresses the filter with no framework change. |
| duplicate-collapse policy | **reuse** `reduce_to_agreed(on_conflict='na')` from a country hook | PR #618 landed exactly this helper for country modules to call BY NAME. A second mechanism (`aggregation:`) would fork the vocabulary AND contradict D1. |
| cluster-attribute reducer | `reduce_to_agreed` default: agree → value, disagree → `<NA>` + warn | `first` is arbitrary (class-1 wrong). `mode` was tried on the rescue branch and **rejected on evidence** — see §6.3. |
| GPS reducer | **none** — `<NA>` on disagreement | the rescue argued `median`. Measured: where within-cluster GPS varies it varies by a MEDIAN of 4.6–42.9 km and up to 584 km. That is a broken key, not a scatter about a centroid — the identical argument that retired core's `.mean()` on 2026-07-13. Averaging would smear two places together and hide the evidence. |
| 2009-10 synthetic cluster | **reuse** `fill_v_with_coord_bin`, reached through the *existing* `derived:` YAML block | `sample` already builds exactly this label; a second implementation would drift, and the two `v`s **must** agree. Declarative beats a hand-written hook. |
| cluster key 2018-19 / 19-20 | `(district, parish)` composite | eliminates all 20 / 23 multi-district groups; **not** finer — see §6.4 |

## §6 Decisions that went against the rescue snapshot (with the evidence)

1. **No `where:` primitive, no `aggregation:` keys, no `country.py` /
   `local_tools.py` patch.** The rescue snapshot carried +155 lines of
   `country.py` and +60 of `local_tools.py`. D1 supersedes all of it. What the
   patches were *for* is delivered by machinery that has since landed centrally
   (`reduce_to_agreed`, PR #618) or by the `df_edit` hook that already existed.

2. **2009-10's coord-bin fallback is declarative, not a hand-written hook.**
   The rescue wrote a `cluster_features(df)` in the wave `mapping.py` that
   called `fill_v_with_coord_bin` itself, because `derived:` runs *after*
   `set_index(final_index)` and `v` was an idxvar by then. The cheaper fix is
   to move `v` to a *myvar* and set `final_index: [t, i]`, so `v` is still a
   COLUMN when the dispatcher runs — then the existing `derived:` block does
   the work and the wave needs no Python at all.

3. **`mode` rejected as the categorical reducer** (carried over from the rescue
   diagnosis, which measured it). Validated against the only independent ground
   truth — the 2005-06 frame, where comm→district is 1:1 — `mode` recovered the
   right district for 78% of the *ambiguous* groups but only **84% of the
   unambiguous control** groups, because Uganda split its districts between
   2005 and 2011. An estimator that misses 16% of the cases whose answer is
   already known cannot certify the cases whose answer is not.

4. **Key is `(district, parish)`, not the finest available.** Adding
   `subcounty` splits 10 further groups in 2018-19 (25 in 2019-20), but the
   ones inspected are spelling artefacts of a single subcounty — `NYENGA` /
   `NYENGA DIVISION` (centroids 0.0 km apart), `LUBAGA` / `RUBAGA DIVISION`
   (5.3 km), `'KAGADI  TOWN COUNCIL'` / `'KAGADI TOWN COUNCIL'` (a doubled
   space). Keying finer would fragment real parishes on data-entry noise.

5. **`comm` was NOT re-keyed** (carried over from the rescue diagnosis).
   `comm` is a structured code with zero district collisions in its frame year,
   and its multi-district groups show a *dispersal* signature, whereas
   `parish_name`'s `CENTRAL` shows a *collision* signature. Splitting `comm` by
   district would fracture real EAs and desynchronise `v` from `sample` and
   from the 2005-06 baseline.

6. **2005-06's 368 phantom `outer`-merge rows left to GH #606.** They carry
   `v = NaN`; the hook now deletes them *with a count* instead of letting
   `groupby(dropna=True)` do it silently. Changing `how='outer'` at
   `country.py` is library-wide and would risk 40 countries for zero Uganda
   gain. Documented in `CONTENTS.org`, not touched.

## §7 Measured effect (cold build, `LSMS_NO_CACHE=1`)

`people_last7days` — 6,185 destroyed → **0**:

| wave | destroyed | served visitor counts | zero-people HH | mean people/HH |
|------|-----------|----------------------|----------------|----------------|
| 2018-19 | 3,147 → 0 | 1,636 / 3,242 (50.5%) → 0 | 1,053 → 64 | 2.81 → 4.51 |
| 2019-20 | 3,038 → 0 | 1,507 / 3,078 (49.0%) → 0 | 1,011 → 16 | 2.77 → 4.53 |

(counts at source; through the API after the panel-id walk the means are
4.60 / 4.55 and the zero-people households are 3 / 4.)

`cluster_features` — 10,837 destroyed → **0**; 935 NaN-key deletions → 394,
all of them now announced; 4,409 → 4,808 clusters. Per-wave table in
`Uganda/_/CONTENTS.org`.

What replaces the destruction is a `GrainConflictWarning` per wave naming the
cells that could not be determined — 0 `Region`/`District` conflicts remain in
2018-19 / 2019-20 (the composite key fixed those), and what is left is panel
dispersal (`comm` waves) and genuine sub-parish variation in `Rural` / GPS.

## §8 Verification

- `sample.v` ≡ `cluster_features.v` for every wave: 0 `cluster_features`
  `(t, v)` pairs unknown to `sample`; 0 orphaned `sample` clusters except 4 in
  2010-11 whose every attribute conflicts (documented).
- `tests/test_uganda_323_grain.py` — 24 tests, all config-level; exercises no
  core aggregation.
- `tests/test_uganda_v_grain_invariants.py`, `test_uganda_tables.py`,
  `test_uganda_api_vs_replication.py`, `test_uganda_invariance.py` pass.
- `tests/fixtures/uganda_baseline.json` refreshed for exactly two entries
  (`var/cluster_features.parquet`, `var/sample.parquet`); a surgical update
  that would have reported — and refused — any unrelated drift.
- Pre-existing failure, NOT caused by this branch and NOT fixed by it (core is
  out of scope): `tests/test_gh323_explicit_reducers.py::
  test_core_does_not_dispatch_the_reducers` fails on pristine
  `origin/development` because PR #617's private `country._collapse_to_cluster_grain`
  contains PR #618's banned substring `collapse_to_cluster_grain`.

## §9 Deferred (needs core, or another PR)

- The `where:` row-filter primitive for `df_data_grabber`. Not needed by
  Uganda any more, but if it is ever built it must: evaluate a pandas `query`
  string against the raw source frame BEFORE `set_index`, RAISE on zero matches
  (a silently-empty table is the failure mode it exists to prevent) and RAISE
  on an unresolvable column name. Own PR; same category as Mali's
  `({const: value})`.
- GH #606: the `how='outer'` sub-df merge that manufactures 2005-06's 368
  phantom rows (Site 4).
- 2019-20 has no `df_geo`, so its 827 clusters have no coordinates at all.
  Pre-existing and unrelated to #323.
