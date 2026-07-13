# Ledger — GH #323 (Malawi): the silent `groupby().first()` collapse

Branch `fix/323-malawi`, based on `origin/development` (006cdb3b).

## §1 What the bug is

`cluster_features` is **extracted** at household grain `(t, v, i)` but
**declared** at `(t, v)`. `_normalize_dataframe_index` therefore drops the
undeclared `i` and reduces with `groupby(['t','v']).first()`. That reduction is
*invisible*: the #323 RuntimeWarning fires only on a **cold** build, and by then
the collapse is already baked into the L2 cache — **the bug hides behind the
cache the bug poisoned.**

Malawi is not one bug. It is four, stacked on that mechanism.

## §2 Instrument (validated BEFORE use)

Scanner counts duplicate rows on the **full declared index** from
`data_scheme.yml`, reading **L2-WAVE** parquets (L2-country `var/` is written
POST-collapse and lies — a previous red-teamer got zero hits from it, including
for Guyana, a known positive).

Validated on the known positives before trusting any number:

| probe | expected | got |
|---|---|---|
| `Mali/2014-15/household_roster` | 32,026 | 32,026 ✅ |
| `Guyana/1992/housing` | 311 | 311 ✅ |

## §3 The four defects (each verified against the RAW source, not the parquet)

1. **2004-05 cluster key — INDEX_INCOMPLETE, silently WRONG (class-1).**
   `v: ea`. In `sec_a.dta`, `ea` is a 1–3 digit EA sequence *within a Traditional
   Authority* → only **110** distinct values. 79 of those buckets span >1
   district; **66 span >1 REGION**; **9,940 of 11,280 households (88%)** sit in a
   region-straddling bucket. `.first()` gives each bucket one arbitrary
   household's Region/District.
   The real key is in the file and unused: `psu` — **564 distinct, exactly 20 HH
   each, `psu == case_id[:8]` for all 11,280 rows, one district/region/reside
   apiece.** Because **`sample` owns `v`** and `_join_v_from_sample` puts it on
   every household-level table, the broken key leaked into **every** Malawi
   2004-05 table. → `v: psu` in *both* `cluster_features.idxvars` and
   `sample.myvars`.

2. **2004-05 shocks — label collision.** `ab02` codes **117 and 118 both carry
   the Stata label "Other"** (the questionnaire's two separate "Other (specify)"
   roster slots). Reading labels merged them → `(t,i,Shock)` non-unique for
   11,077 rows, of which **10,831 were pure artefact** (the distinguishing level
   is in the file and was thrown away). → `converted_categoricals: False` + a
   code→label decoder keeping the two slots apart. Dups **11,077 → 246**;
   labels **18 → 19**.

3. **2010-11 + 2019-20 — cartesian merge (EXTRACTION_BUG).** `merge_on: [v]`
   against a **household-grain** geo file ⇒ within-EA cartesian:
   `sum(n_hh_per_ea²)` = **196,083** rows for 12,271 households (2010-11) and
   **185,842** for 14,612 (2019-20). *Honest caveat:* value-neutral — lat is
   EA-constant, so `.first()` happened to land right. It is a ~16× row
   fabrication that masked the grain problem. → `merge_on: [i]`.

4. **2016-17 — keyspace mismatch (GH #606).** `df_main` rewrites `case_id` to
   `'cs-17-'+…` via `cs_i`; `df_geo` keyed on the **raw** `case_id` ⇒ the merge
   matched **nothing**. Latitude non-null on **0 of 14,955** real rows (100% of
   the wave's geovariables lost) and 12,447 unmatched geo rows became phantom
   NaN-`v` rows. → apply `mapping: cs_i` to `df_geo` too. **0 → 772** clusters
   with coordinates.

## §4 What the handed-down diagnosis got WRONG (found by re-deriving, not trusting)

- **"2013-14 is the clean case / INTENDED_AGGREGATION" — FALSE.** It is class-1.
  The IHPS **tracks movers**: `ea_id` is the *baseline* EA but
  `region`/`district`/`reside` are where the household was *found in 2013*.
  Region disagrees within `ea_id` in **93 of 204** EAs, district in **165**,
  reside in **131**, lat/lon in **188**.
  Proof it is exactly the mover effect: **100%** of the households that differ
  from their EA's modal value have `dist_to_IHS3location > 0` (177/177 region,
  519/519 district, 300/300 reside), and restricting to non-movers makes `ea_id`
  **perfectly** consistent (0 of 161 EAs vary on any attribute).
  The **same** contamination is in the **panel half** of 2016-17 (reside 75/102
  EAs) and 2019-20 (region 66, district 98, reside 86 of 102) — the diagnosis
  missed this entirely.

- **"`.first()` keeps the empty No row and discards the real one" — FALSE.**
  pandas `GroupBy.first()` skips NaN **per column**, so it returns the *first
  non-null* value in each column. Tested directly. The payload of the residual
  shock duplicates is therefore preserved for 210 of 243 groups; only 33 have
  genuinely conflicting non-null values (15 conflict on `ab04` itself). This
  removed the need for a wave-script rewrite of `shocks`.

- **"Declare the reduction with `aggregation: {i: first}`" — would be a NO-OP.**
  Nothing in the library reads that key: it appears only in the `_skip` sets of
  `country.py`/`diagnostics.py`, and `SkunkWorks/grain_aggregation_policy.org`
  says the mechanism "is not yet [implemented]". Malawi's existing
  `interview_date: aggregation: {visit: first}` is already prose that does
  nothing. **Prose is not enforcement** — so I did not use it.

## §5 The fix — and the CLASS, not just the instances

Cluster geography now comes from the **EA CODE**, which encodes it and is
**mover-immune**: `ea_id[:1]` → region, `ea_id[:3]` → district. Verified unique
in *every* Malawi file with a sound cluster key (2004-05 `psu` 564 EAs; 2010-11
768; 2016-17 CS 779; 2019-20 CS 717) and against 2013-14's own non-movers. One
shared decoder in `_/malawi.py`. (**Not** applied to 2004-05: IHS2 uses a
*different, combined* district coding — its `105` is "Mzimba/Mzuzu City" where
the modern `105` is Mzimba and `107` Mzuzu City — so 2004-05 keeps its own
`region`/`dist`, which are sound once `v` is keyed on `psu`.)

Where the source **cannot** determine a cluster value, we **emit NA rather than
guess** (class-2 beats class-1):
- 2013-14 lat/lon — per-household jittered coordinates; a mover's can be **699 km**
  from the baseline EA. Declaration removed for that wave.
- 2016-17 / 2019-20 **panel** `Rural` — no `baseline_rural` flag exists in those
  files. Household-level rural status is untouched and still correct in `sample()`.

**The class fix:** a `cluster_features(df)` **df_edit guard** in `malawi.py`
blanks — *loudly*, with a `RuntimeWarning` — any attribute that disagrees inside
its own cluster. This makes the `(t,v)` collapse **lossless by construction**: a
future broken key can no longer silently publish an arbitrary household's value;
it becomes loudly-missing instead. With the config correct it fires on **exactly
the 7** of 2016-17's 779 EAs whose source coordinates differ (by up to 107 km)
and nothing else.

## §6 Evidence

**The invariant** — clusters whose attributes disagree within `(t,v)`:
**1,257 → 0.**

| metric | BEFORE | AFTER |
|---|---|---|
| 2004-05 clusters (`cluster_features`) | 110 | **564** |
| 2004-05 clusters (`sample.v` → leaks everywhere) | 110 | **564** |
| 2016-17 rows with a coordinate | **0** of 14,955 | 772 clusters |
| 2004-05 shock labels | 18 | **19** |
| 2004-05 shocks duplicate `(t,i,Shock)` | 11,077 | **246** (genuine source repeats) |
| 2010-11 wave rows | 196,083 | **12,271** (= households) |
| 2019-20 wave rows | 185,842 | **14,612** (= households) |
| 2016-17 wave rows | 27,402 | **14,955** (= households) |
| #323 warning | 11,077 dups | 246 dups |

`tests/test_malawi_gh323.py`: **12 of 16 fail on the parent commit**, all 16 pass
after. The class-level test (`test_cluster_attributes_constant_within_cluster`)
fails on 4 of the 5 waves pre-fix.

## §7 Known residuals (NOT closed — stated plainly)

- **246 genuine `(case_id, ab02-code)` repeats** in 2004-05 shocks survive and are
  still collapsed by `groupby().first()` (the #323 warning correctly reports them
  on a cold build). Because `first()` skips NaN per column, the payload is
  correctly recovered for **210 of 243** groups; **33** have conflicting non-null
  values (15 conflict on `ab04`) and get an arbitrary pick. Closing these needs an
  explicit declared reducer — i.e. a **library** change (`aggregation:` made real)
  or a script-path rewrite of `shocks` across all 5 waves. Out of scope for a
  Malawi config fix; deliberately left loud rather than papered over.
- **`function:` is not a key `country.py` understands** (only `mapping:`/
  `mappings:`). 2019-20 spells all six of its `cs_i` hooks `function: cs_i`, so
  **cs_i never fires** there and its cross-sectional households keep a bare
  `case_id`. The wave is internally *consistent* (its scripts assume and document
  exactly that), so this is dead config, not corruption — but it is a latent trap:
  "fixing" the spelling would silently re-key the whole wave. Left alone; flagged
  here and in the YAML.

## §8 Process notes

- **Never `git stash` in this repo.** The stash stack is **repository-global**,
  not per-worktree: another agent (#323-CotedIvoire) popped my stash mid-run.
  Use an exported baseline tree (`git archive`) + `LSMS_COUNTRIES_ROOT` instead.
- The main checkout was sitting on **another agent's unmerged branch**
  (`fix/602-spellings`), so measuring "BEFORE" against it silently compared
  against someone else's in-progress work. Baseline must come from
  `origin/development`, explicitly.
- Config-only edits verified via `LSMS_COUNTRIES_ROOT=<worktree>/…/countries`
  (the `.pth` pins `lsms_library` to the main checkout, so `PYTHONPATH` alone
  would not have redirected the config tree).
