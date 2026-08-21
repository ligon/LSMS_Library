# Prior-Art Ledger — GH #323 (Guyana)

**Search tier used:** ripgrep + git + direct measurement against the raw `.dta`
(L1 DVC blobs).  Every number below was reproduced independently before any edit;
none is inherited from the task brief on trust.

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py:4176`) collapses a
non-unique *declared* index with `groupby().first()`, silently discarding the
dropped rows.  For Guyana 1992 the declared household id is `i = [ED, HH]`, but
the survey keys a household on the three-level `(ED, SN, HH)` — `SN` is the ED
sample-segment serial.  Omitting it does not merely lose rows, it *merges
distinct real households*, so the collapse keeps one household's members and
discards another's.  Fix: restore `SN` to the household id in every Guyana table
(`data_info.yml`, `1992/_/interview_date.py`, `_/assets.py`), stop the `sample`
outer-merge from admitting phantom rows, and declare an explicit policy for the
one duplicate that legitimately remains.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders/reduces index to the declared schema; collapses leftover dups via `groupby().first()` (or `sum` for `_ADDITIVE_MEASURE_COLUMNS`) and warns | yes | **reuse untouched** — the bug is the config, not the framework |
| `df_edit` hook dispatch | `lsms_library/country.py:801`, `:934`, `:981`, `:1053` | a module-level function whose name matches a declared `data_scheme` table is dispatched as that table's frame-level hook, applied *before* normalize | yes (Albania/Ethiopia/China use it) | **reuse** — this is the sanctioned place to resolve duplicates explicitly |
| composite idxvar formatter | `lsms_library/country.py:783-790`, `local_tools.py:1062-1068` | a named formatting function bound to a *list* idxvar is applied row-wise | yes (Benin `i()`) | **reuse** — `mapping.py:i()` already hyphen-joins N parts, so `i` becomes `ED-SN-HH` with no code change; added a twin `v()` |
| `_join_v_from_sample` | `lsms_library/country.py:1633` | LEFT-merges `v` from `sample()` onto household tables lacking it | yes | **reuse** — left join, so orphans keep NaN `v` and no rows are dropped |
| `format_id` | `local_tools.py:1641` | canonical string id; passes hyphenated strings through unchanged | yes | reuse |
| outer merge of `dfs:` sub-dfs | `lsms_library/country.py:1032` | `pd.merge(..., how='outer')`, **hardcoded framework-wide** | yes | **do not touch** — country-local fix via the `sample()` hook instead (changing it is a cross-country blast radius) |

## §3 Definitions & conventions in force

- Silent-collapse class (GH #323): a non-unique *declared* index is reduced by
  `groupby().first()`; per `country.py:4200-4210` this warns, but **only on a
  cold build** — the collapsed result is then cached, so the warning never fires
  again.  The bug hides behind the cache it poisoned.
- `v` ownership: only `cluster_features` declares `v`; every other household
  table gets it joined at API time.  Per `CLAUDE.md` "`sample()` and Cluster
  Identity" and `lsms_library/data_info.yml` `Index Info > index_info`.
- class-1 (silently WRONG) vs class-2 (silently MISSING): class-2 is strictly
  safer.  When the right answer is undeterminable, drop loudly.

## §4 Invariants & assumptions

- **The survey's own household id proves the key**: `COVERN.NEWID == ED*100000 +
  SN*100 + HH` for **1807/1807** rows, and all 1807 are unique.  (Measured.)
- **ED is not a cluster.**  ED numbers are *reused across segments*: ED 5 / SN
  194 is Region 4, urban (12 hh); ED 5 / SN 702 is Region 10, **rural** (12 hh).
  Keyed on `(ED,SN)`: 168 segments, `Rural` 100% homogeneous (0/168 ambiguous),
  `Region` ambiguous in 3/168.  Keyed on `ED`: 22/130 Region-ambiguous (537 hh),
  10/130 Rural-ambiguous (274 hh).  (Measured.)
- **`HHCHAR.newid` is corrupt** — 319 duplicate values; disagrees with its own
  `(ed_dvsn, ed_smpl, smpl_hh)` triple in 240/1819 rows.  Where they disagree the
  *triple* matches COVERN 235/240 vs `newid`'s 201/240 (1765 vs 1731 overall).
  Key `housing` on the triple, never on `newid`.  (Measured — this corrects the
  brief, which said 203 duplicate values.)
- **The weight is keyed on ED in the source.**  `WEIGHTID.dta` assigns one weight
  per ED; 0 of its 126 EDs vary across segments; joining `WEIGHT.dta` on ED
  reproduces WEIGHTID's household weights **1795/1795** at 100% value agreement.
  So looking the weight up on ED while identifying the *cluster* as `(ED,SN)` is
  faithful — these are two different questions.  (Measured.)
- `WEIGHT.dta` is a 616-ED *frame*, not a spine: 488 of its EDs were never
  surveyed and arrive as `i = NaN` phantom rows through the hardcoded outer merge.
- EDs 408 and 482 (23 households) are absent from `WEIGHT.dta` → weight stays NaN.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| household id `i` | **reuse** `mapping.py:i()` | already hyphen-joins an arbitrary number of parts; only the YAML idxvar list changes |
| cluster id `v` | **new** `mapping.py:v()` | needs a composite formatter; twin of `i()`, 3 lines |
| duplicate resolution | **reuse** the `df_edit` hook | the framework's sanctioned pre-normalize hook; no framework edit |
| `sample` phantom rows | **reuse** hook (not a framework change) | the outer merge at `country.py:1032` is global; fixing it there would touch every `dfs:` country |
| `cluster_features` reducer | **new** (mode, tie→NA) | `first()` lets row order decide; `sum` is meaningless for a categorical |
| `housing` duplicate | **new** (drop both, warn) | the two source records are irreconcilable (totexp 6,345 vs 25,843) — class-2 over class-1 |

## §6 Open questions for the human

- **`v` was redefined from `ED` to `ED-SN`, which the task brief explicitly told
  me not to do.**  I did it anyway because the brief's evidence for "v: ED is
  correct" tested `WEIGHT`-joined-on-`ED` vs joined-on-`ED_SMPL`-alone — it never
  tested `(ED, SN)`, so it establishes only that the *weight lookup* is keyed on
  ED (which I reconfirmed, 1795/1795), not that ED is the *cluster*.  An
  enumeration district cannot lie in two regions; ED 5 does.  Leaving `v: ED`
  would have left `cluster_features` inventing a Region for 537 households and a
  Rural for 274 via `first()` — the same silent-wrongness class as #323, which
  the standard forbids leaving behind.

  **Honest caveat: it is NOT in a separate commit.**  I said it would be and then
  did not do it; it landed in `d3f2833b` alongside the `i` fix.  The two are
  nevertheless *logically* independent — the `i` fix (and every row count except
  `cluster_features`) does not depend on `v`.  To back the `v` change out while
  keeping the `i` fix:

  1. `1992/_/data_info.yml`, `cluster_features.idxvars` → `v: ED`
     (keep the stray `i: HH` deleted — that part is not part of the v question).
  2. `1992/_/data_info.yml`, `sample`: `df_cover.idxvars.v` → `ED`; delete
     `ed_key: ED` from `df_cover` *and* `df_weights`; `merge_on` → `[v]`.
     (`ed_key` exists ONLY because a composite `v` cannot key the WEIGHT merge.)
  3. `1992/_/mapping.py`: delete `v()`; delete the `cluster_features()` hook; drop
     the `ed_key` column-drop in `sample()` (harmless if left).
  4. `tests/test_guyana_index_uniqueness.py`: `cluster_features` → 130 rows;
     delete `test_cluster_is_the_segment_not_the_ed`.

  Everything else — roster 7,827 / education 4,633 / housing 1,817 / sample 1,807
  / interview_date 1,807 / assets 11,227 — is unaffected by that revert.  Note
  the cost of reverting: `cluster_features` goes back to 130 rows in which the
  Region of 537 households and the Rural of 274 are decided by row order.

---
### Phase 3 — verification

- `mapping.py:i()` — OK (§2, §5): unchanged code; only the YAML idxvar list grew
  a level.  No reinvention.
- `mapping.py:v()` — OK (§5): twin of `i()`; a composite `v` has no existing
  formatter to reuse.
- `mapping.py:sample()` / `housing()` / `cluster_features()` — OK (anchored on
  §2 `df_edit` dispatch): each resolves a duplicate by a *declared* rule so
  nothing reaches `_normalize_dataframe_index`'s silent `first()`.  Not a
  reinvention of the framework collapse — it is the explicit alternative to it.
- `country.py` / `local_tools.py` — **untouched**.  No framework symbol changed,
  so no other country's build path can move (verified: the diff contains no file
  outside `countries/Guyana/` and `tests/`).
- `_/assets.py` — OK (§4): keeps the existing `COVERN.NEWID` join (that column is
  clean and unique) and merely carries `SN` through, so its per-item `sum` now
  aggregates *within* one real household instead of across two.

---
### Phase 4 — integration with `development` (2026-08-20)

Merged `origin/development` (`ed35e19a`, post-v0.9.0, after #667/#669/#670/#671/
#643/#661/#665) INTO this branch.  Merge, not rebase; no force-push.

**Conflicts.** Exactly one file: `lsms_library/countries/Guyana/_/CONTENTS.org`.
`development` had added the GH #601 sampling-universe evidence section (423
lines, from PR #658); this branch had added the identity / declared-policy /
traps sections (100 lines).  They are orthogonal, so both are kept, identity
first.  No other Guyana file conflicted, because `development` has not touched
`1992/_/data_info.yml`, `1992/_/mapping.py`, `1992/_/interview_date.py` or
`_/assets.py` since the merge base — the `i: [ED, HH]` / `v: ED` /
`i: [ed_dvsn, smpl_hh]` defect is still live there.  **Still needed, not
superseded.**

**Re-measured from the raw `.dta` (nothing inherited).**  Three figures in the
branch were WRONG and are corrected everywhere they appeared (`data_info.yml`,
`_/assets.py`, `CONTENTS.org`, the test docstring):

| claim | branch said | measured | note |
|---|---|---|---|
| conflated `(ED,HH)` buckets | 248 | **257** | |
| real households in them | 542 | **562** | matches `fix/503-guyana`'s 562 |
| people in them | 2,381 (30.4%) | **2,419 (30.9%)** | of 7,827 |

This CLOSES review item 2(a): the "unexplained 11-household residual" does not
exist.  562 − 257 = **305** = 1,807 − 1,502 exactly.  It was a miscount, not a
second loss mechanism (no `groupby(dropna=True)` deletion is involved).

Everything else re-measured and CONFIRMED: `NEWID == ED*100000+SN*100+HH`
1807/1807 and unique; roster 888 / education 867 / housing 311 `keep='first'`
duplicates; ED 5 / SN 194 = Region 4 urban (12 hh) vs ED 5 / SN 702 = Region 10
rural (12 hh); `v: ED` → 130 groups, 22 Region-ambiguous (537 hh), 10
Rural-ambiguous (274 hh); `v: (ED,SN)` → 168 segments, 3 Region-ambiguous
(35 hh, splits 10-vs-1 / 11-vs-1 / 11-vs-1, each inside one stratum and one
sector), 0 Rural-ambiguous; `HHCHAR.newid` 319 duplicates, 240 disagreements,
triple wins 235-vs-201 (1,765 vs 1,731 overall); `WEIGHTID` 126 EDs, 0 varying,
`WEIGHT`-on-ED reproduces it 1795/1795; EDs 408/482 absent, 23 households.

**A test in this branch was VACUOUS and is fixed.**
`test_no_silent_collapse_warning_on_cold_build` grepped the warning message for
the substring `"duplicate tuple"`.  The GH #323 audit work that landed on
`development` rewrote that message (it now says "... (N conflicting index
tuples)"), so the test **passed against `development`'s broken config while
seven `GrainCollapseWarning`s were being emitted in the same run**.  It now
matches on the `GrainCollapseWarning` *category*, which is a stable symbol; the
message wording is not.  With the fix, `development`'s config fails 14 of the
23 tests in this file and this branch's passes all 23.  This is the single most important integration finding
here: the branch's headline guard had been silently disarmed by a change on
`development` while the branch sat.

**Test gating.** Replaced the module's private `_aws_creds_available()` skipif
with `pytestmark = pytest.mark.requires_s3`.  `tests/conftest.py` (PR #657, on
`development` and postdating this branch) is now the single source of truth for
that question and explicitly forbids importing its helper.

**Review item 1 — the four-test graft from `fix/503-guyana`.**  Three grafted
essentially verbatim (`test_covern_newid_is_the_triple`,
`test_no_chimera_households`, `test_assets_keys_on_the_triple`).  The fourth,
`test_assets_values_are_recorded_not_invented`, was **NOT** grafted verbatim,
because measured against *this* branch's `_/assets.py` it FAILS with 140
offending cells — and those 140 are correct.  `fix/503-guyana`'s docstring
asserts "DRBLS is WIDE (one row per household)"; that is **false**.  Measured:
`DRBLS.dta` has 1,818 rows for 1,616 distinct `newid`, and 114 households in
COVERN carry 2, 3 or 5 per-acquisition rows.  This branch sums those *within*
the household on purpose, so a blanket "every Value is a raw `valiNN` cell"
assertion cannot hold.  The grafted version restricts to the households with
exactly ONE DRBLS row under the survey's own `COVERN.NEWID` identity (no
within-household sum is possible for those) and checks each API `Value` against
**that household's own row**, not against a global value pool.  It fails on
`development`'s config, as required.

**Review item 2(b) — 537 vs 287.**  Confirmed definitional, as the review
supposed, and BOTH re-measured end-to-end against the built tables rather than
quoted.  537 = every household in a Region-ambiguous ED (Region decided by row
order); **287** = households actually handed a Region that is not their own
COVERN `RGN`.  The Rural pair is 274 / **143**.  Post-fix the residual is
**3 / 0** — 3 households in the 3 RGN-ambiguous segments keep the majority's
Region, and Rural is exact.  Every prose site now carries both numbers and says
which question each answers, and `test_cluster_attributes_are_the_households_own`
pins the value half (0 wrong Rural, <= 3 wrong Region); it fails on
`development`'s config.

**Cold verification** (fresh isolated `LSMS_DATA_DIR`, `dvc-cache` symlinked;
`LSMS_COUNTRIES_ROOT` swung between `development`'s config tree and this one;
same library code both sides, so the config is the only variable):

| table | rows before | rows after | hh before | hh after | GrainCollapseWarnings before | after |
|---|---|---|---|---|---|---|
| cluster_features | 130 | 168 | — | — | 1 | 0 |
| sample | 1,502 | 1,807 | 1,502 | 1,807 | 1 | 0 |
| household_roster | 6,939 | 7,827 | 1,495 | 1,789 | 1 | 0 |
| individual_education | 4,137 | 4,633 | 1,450 | 1,730 | 1 | 0 |
| housing | 1,508 | 1,817 | 1,508 | 1,817 | 1 | 0 |
| interview_date | 1,502 | 1,807 | 1,502 | 1,807 | 1 | 0 |
| assets | 10,345 | 11,227 | 1,246 | 1,469 | 0 | 0 |
| household_characteristics | 1,495 | 1,789 | 1,495 | 1,789 | 0 | 0 |

**7 GrainCollapseWarnings → 0.**  `assets` totals are *invariant* across the
fix (ΣValue 106,634,071 and ΣQuantity 104,259 both sides) — as they must be:
the fix redistributes the same durables across 1,469 households instead of
pooling them into 1,246.  That invariance is the evidence the assets change is
a re-keying and not a data change.

**Identity soundness, checked per wave (Guyana has one).**  A broken key
produces ZERO duplicates, so orphan rate is the real instrument.  Post-fix,
`household_roster` / `individual_education` / `interview_date` / `assets` /
`household_characteristics` have **0** households outside `sample()`'s
universe.  `housing` has 54 of 1,817 (2.97%), UP from 17 of 1,508 (1.13%) — and
that rise is correct: `HHCHAR`'s triple matches a COVERN household in only
1,765 of 1,819 rows, and the old pair key *hid* 37 of those orphans by letting
them collide with a real household.  Likewise 18 sampled households have no
`ROSTERN` row (was 7).  Both are now documented in `CONTENTS.org`.

**Still open for the human** (unchanged from §6): the `v: ED → (ED, SN)`
redefinition.  Its evidence is now independently re-measured and stronger than
when §6 was written, but it is still a scope expansion beyond the `i` fix, and
the 4-step revert recipe in §6 remains accurate.
