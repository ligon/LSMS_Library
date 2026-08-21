# Prior-Art Ledger — GH #645: `to_parquet` destroys values that spell like nulls

**Search tier used:** ripgrep + git (floor), plus a **live cross-country cold
sweep**: 28 data-bearing countries × `individual_education`, each in its own
process with its own empty `LSMS_DATA_DIR` (only `dvc-cache` symlinked to the
shared L1), plus targeted before/after cold builds of `Nigeria/food_acquired`
and `Guyana`/`South Africa` `housing`.  gitnexus not used.
**Inherits:** `.coder/ledger/STANDING.md` §2/§3/§4 — cited, not restated.
**Base commit:** rebased onto `4c236d11`.  The branch started at `45aee170`; the
`origin/development` ref moved **twice** mid-task (`eadafe75` India #611, then
`4c236d11` through #644) — see §6, and note that #644 changes Ethiopia's
baseline numbers, so every Ethiopia figure here was **re-measured** on
`4c236d11`.

---

## §1 Task, restated

`local_tools.to_parquet` serializes every `dtype == object` column as
`astype(str).astype('string[pyarrow]').replace({'nan': None, 'None': None,
'<NA>': None})`.  It stringifies **first** and then tries to recover the nulls
by matching the resulting characters — but after `astype(str)` a genuine
missing and a legitimate value spelling `'None'` are the same characters, so
the recovery is wrong in principle, not merely in practice.

`None` was the canonical library label for *"no education"*
(`categorical_mapping/harmonize_education.org`), so the write path nulled every
never-schooled person, and `Country._finalize_result`'s `dropna(how='all')`
(`country.py`) then **deleted the row** — `individual_education` has exactly one
column.  Fix the write path, rename the hostile label, force one cache rebuild.

## §2 Existing machinery (reused, did NOT rebuild)

| symbol | path | what it does | tested? | decision |
|---|---|---|---|---|
| `to_parquet` | `local_tools.py` (STANDING §2) | the only sanctioned writer | yes | **extend in place** — 3-line change inside the existing coercion loop; no new writer, no new call site, no signature change |
| `LSMS_CACHE_SCHEMA` | `local_tools.py` | the manual library-version lever folded into `Wave._input_hash` / `Country._table_cache_hash` | cache tests | **reuse** — `4 -> 5`.  This is exactly the lever's documented purpose (a library-wide change to the *pre-write* extraction logic whose per-table input hashes are unchanged); no new invalidation mechanism |
| `_enforce_canonical_spellings` | `country.py` (STANDING §2) | variant → canonical at API time, on columns AND index levels | `test_schema_consistency.py` | **reuse** — the `None -> No education` back-compat mapping is a `spellings` entry, not migration code.  Historical caches resolve forward with no rebuild |
| `spellings` block in `data_info.yml` | STANDING §3 | the sanctioned place to declare accepted values | `test_declared_spellings.py` | **reuse** — followed the `plot_features.Tenure` / `TenureSystem` pattern (full vocabulary, empty variant lists) |
| `harmonize_education` org tables | `categorical_mapping/` + 25 country `categorical_mapping.org` | raw label → canonical level | via country builds | **edit data, not code** — the rename is 38 table rows; no mapping code changed |
| `tests/conftest.py` `requires_s3` | added on `development` in #648 | skips data-dependent tests in the data-free CI job | — | **reuse** — the three cold integration cases carry it |

**Did NOT reinvent:**
- *A migration script for existing caches.*  `LSMS_CACHE_SCHEMA` already forces
  exactly one rebuild everywhere; a bespoke rewriter would be a second,
  untested invalidation path.
- *A null-sentinel scrubber on the read path.*  `transformations.py`
  `_na_strings`, `country.py` `_NA_SENTINELS` and `conversion.py` already do
  targeted, deliberate mop-ups on `Relationship` / `sex` / `age` / dates.  Those
  are *correct* (they operate on columns whose vocabulary genuinely excludes
  these strings) and are left alone — see §4.3.  Adding a general one would
  re-create this bug one layer up.
- *A reducer or an `aggregation:` key.*  GH #323 D1 forbids it; nothing here
  needed one (`test_gh323_explicit_reducers.py` and
  `test_gh323_grain_contract.py` both green, 280 assertions).

## §3 Definitions & conventions in force (cited)

- **Canonical schema** = `lsms_library/data_info.yml` (STANDING §3).  The
  education vocabulary is now declared there, so
  `diagnostics._check_declared_vocabularies` enforces it.
- **Canonical education ladder** = `categorical_mapping/canonical_education_labels.org`
  — 14 ordered levels + the non-ordinal `Unknown` sentinel.  Level 0 renamed
  `None` → **`No education`**.
- **Cache tiers / staleness** = `CLAUDE.md` §"Cache Behavior (v0.7.0+)" and the
  `LSMS_CACHE_SCHEMA` comment block in `local_tools.py`.
- **`sane` is not `blessed`** = `CLAUDE.md` §"Coverage Matrix".  Directly
  load-bearing here: `Guatemala,individual_education,2000,sane,declared,20678`
  certified a table missing 30% of its rows.
- **GH #323 D1 — no aggregation in core** = `SkunkWorks/grain_aggregation_policy.org` §3a.

## §4 Invariants & assumptions (the landmines)

1. **The pyarrow guard is still load-bearing.**  Measured in this venv
   (pandas 3.0.2 / pyarrow 23.0.1, `future.infer_string=True`): a genuinely
   mixed object column still raises `ArrowTypeError` — `str`+`int`, `str`+`float`,
   `str`+`Timestamp`, `str`+`list`/`tuple`/`dict`, `bool`+`str`, `str`+`Decimal`
   all fail.  The block may NOT be deleted.  Pinned by
   `test_mixed_type_object_column_is_still_stringified`.
2. **But it fires arbitrarily.**  Under pandas 3.0 a *pure string* column is
   inferred as `str`, not `object`, so it skips the block entirely.  Only
   whatever still lands as `object` (Stata reads: 16 of 32 columns in Ethiopia
   2011-12 `sect2_hh_w1.dta`) is affected.  That is why exactly 3 of 25
   at-risk countries were hit — the other 22 were correct *by accident of dtype
   inference*, not by design.  **Narrowing the block to "genuinely mixed"
   columns was considered and rejected** — see §5.
3. **`LSMS_NO_CACHE=1` MASKS this bug.**  It skips the L2-wave write where the
   rebinding coercion happens, so it returns the *right* answer and hides the
   defect.  Every measurement here used a genuinely fresh `LSMS_DATA_DIR`; the
   integration tests do the same, in a subprocess, and say so.
4. **The two `to_parquet` call sites disagree.**  `country.py` `Wave.grab_data`
   **rebinds** the return value; the country-level writer **discards** it.  That
   is the whole mechanism of the cold≠warm signature, and why Guatemala (whose
   loss is entirely at the wave level) is cold == warm == wrong and therefore
   invisible to any A/B comparison.  Left as-is — with a value-preserving write
   the difference is unobservable, and `cold == warm` is now a *tested*
   invariant rather than an accident (`test_cold_and_warm_builds_agree`).
5. **Declaring a `spellings` vocabulary is an assertion that FAILS a country.**
   `diagnostics._check_declared_vocabularies` returns `Check(..., "fail")` for
   any value outside `spellings.keys()`.  Declaring only `No education` would
   have turned every other education level into a violation in 24 countries.
   Verified empirically instead of assumed — §5.
6. **`groupby(dropna=True)` deletes NaN index keys** (GH #323 §3b, open).
   Relevant because the pre-fix code was nulling an *index level* in Nigeria
   `food_acquired` (`u`).  Measured: no rows were actually lost there — but the
   combination is a live hazard, and one fewer NaN key is a strict improvement.

## §5 Reuse decision & the two judgement calls

| quantity | decision | reason |
|---|---|---|
| null-preserving write | **extend `to_parquet`** | capture `isna()` before the cast, `.mask(na)` after.  Smallest change that makes the recovery well-posed |
| cache invalidation | **reuse `LSMS_CACHE_SCHEMA`** | documented lever, exact fit |
| back-compat for the old label | **reuse `spellings`** | applies at API time to columns *and* index levels; no rebuild needed |
| keep vs. narrow the object-dtype block | **KEEP** | narrowing changes stored dtypes for homogeneous non-str object columns (an object column of ints would be stored `int64` instead of the strings `'1'`,`'2'`), i.e. it would silently change returned data across the corpus — the exact class of failure this issue is about.  The premise ("pyarrow can't take mixed object") is **still true**; what has changed is only that fewer columns reach it.  Recorded, not acted on |
| declaring the full education vocabulary | **DO IT, on evidence** | swept 28 data-bearing countries cold at the API level, then audited **107 freshly cold-built parquets** (wave + country level) with the real `_check_declared_spellings`: **zero** off-vocabulary values after the one leak below.  Cross-checked statically against all 20 `harmonize_education` tables (`test_harmonize_education_targets_stay_inside_the_declared_vocabulary`) |
| Ethiopia 2013-14 `76.0` | **fix at the wave `df_edit` hook**, not in the org table | the leak (1 row of 12,583, a bare float that never got a Stata value label) CANNOT be fixed in `categorical_mapping.org`: the table's `Original Label` column is mixed text, so `df_from_orgfile` reads every key as a **string** and a float source value can never match one — verified empirically after an org-row attempt failed (contrast Guyana, whose all-numeric table parses keys as floats).  The hook reuses the shape of Uganda's tested `_/_education_helpers.py` and reads the vocabulary from `data_info.yml` (via the already-tested `diagnostics._DECLARED_VOCABULARIES`) rather than re-listing it.  `Unknown` is the vocabulary's own definition for "unmappable", so this is not a guess.  Zero effect on the API output (Ethiopia 59,092 before and after) |

## §6 Notes for the human

- `origin/development` advanced twice **during** this task (`45aee170` →
  `eadafe75` India #611 → `4c236d11` through #644).  A
  `git checkout origin/development -- <path>` for the negative-control revert
  therefore pulled in unrelated India work; caught and reverted by pinning the
  base SHA.  Worth knowing: in a shared-object-store worktree, `origin/*` refs
  are not stable within a session — **pin the SHA for any A/B measurement.**
- **#644 interacts with this fix on Ethiopia, benignly but visibly.**  It
  re-keys 2013-14 `individual_education` onto the wave-native ids, which stops
  5,247 people collapsing onto one blank tuple.  A side effect is that the
  stray unlabelled `76.0` code **now survives to the API frame**, where before
  it was collapsed away — so the `df_edit` hook added here is doing real work
  on the current base, not just tidying a wave parquet.  All Ethiopia numbers
  in this ledger, the tests and the PR are measured on `4c236d11`.
- **`Nigeria/*/_/food_acquired.py` writes `fillna('None')` into the `u` index
  level** (4 waves, 1,151 rows) — the same hostile-sentinel pattern, one table
  over.  Pre-fix those became NaN index keys; post-fix they are the literal
  `'None'`.  Not renamed here (out of scope, and `u` feeds the unit-conversion
  tables), but it should be renamed to something like `Not recorded`.

---

### Phase 3 — verification (anchored on this ledger)

- `to_parquet` (`local_tools.py`) — **OK (§2, §4.1, §5)**: extended in place, no
  new writer; the pyarrow guard retained and pinned by a test that asserts raw
  `df.to_parquet` still raises on a mixed column.
- `LSMS_CACHE_SCHEMA = 5` — **OK (§2, §5)**: the documented lever, with a
  comment stating the reason (content change, unchanged input hashes).
- `Columns.individual_education.Educational Attainment.spellings` — **OK (§3,
  §4.5)**: full vocabulary, `Tenure`-pattern, verified against a 28-country cold
  sweep before being declared.  Not a REINVENTION of `_check_value_constraints`
  (which reads the *country's* `data_scheme.yml` and only acts on list
  declarations) — it is data for the existing canonical checker.
- 38 `harmonize_education` rows + `canonical_education_labels.org` — **OK
  (§3)**: data edits onto the vocabulary of record; no mapping code touched.
- `tests/test_gh645_to_parquet_null_coercion.py` — **OK (§2)**: uses
  `requires_s3` from `tests/conftest.py` (#648) rather than a private
  creds helper; cold isolation per §4.3 rather than `LSMS_NO_CACHE`.
- `Ethiopia/2013-14/_/mapping.py::individual_education` — **OK (§2, §5)**: a
  wave `df_edit` hook, the mechanism Uganda already uses for exactly this
  ("unlabelled float junk code"); reads the vocabulary from the schema of record
  instead of hardcoding a second copy of it.
- `country.py` `dropna(how='all')` log line — **OK (§1)**: reports, changes
  nothing.  Anchored on the fact that this step is where the value corruption
  became a deletion; deliberately INFO, and the escalation policy is left to the
  maintainer rather than invented here.
- **No CONTRADICTION found** with GH #323 D1: no `aggregation:` key, no reducer;
  `test_gh323_explicit_reducers.py` + `test_gh323_grain_contract.py` green.
