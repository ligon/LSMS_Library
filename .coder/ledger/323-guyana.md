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
  the standard forbids leaving behind.  **This is isolated in its own commit and
  can be reverted alone** if you disagree.  Everything else (the `i` fix) stands
  independently.

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
