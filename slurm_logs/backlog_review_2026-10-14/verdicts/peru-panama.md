VERDICT: pass-with-fixes -- both fixes are correct and every claimed number reproduces on a cold private build, but the branch introduces two NEW red cells in `tests/test_sample.py` (Peru is newly parametrised) and adds no test that pins either fix, so it must not merge as-is.

Reviewer: red-team, job 39923129, clone of `fix/peru-panama` @ e887e163b; merge-base with development 2ed85b49b; base worktree at origin/development 88707ddb0 (differs from merge-base only by unrelated review/Uganda commits -- checked). Private `LSMS_DATA_DIR` seeded with pre-fix warm Peru/Panama parquets; both countries `cache clear`ed and rebuilt cold on the branch. GitNexus unavailable; blast radius by `rg`/`git diff` (declared substitution).

## 1. Refutations (claim -> what I ran -> result)

- **Raw-data mechanism for #777** (ga106a==7.7 on 10,798 of 10,847 unit-77 purchase rows; ga110a==7.7 on 175 of 180 unit-77 obtained rows). Ran `get_dataframe('../Data/GAST-A.DTA', convert_categoricals=False)` from `Panama/1997/_` and crosstabbed. **Held exactly** (10,847/10,800/10,798; 180/175/175; q-dist on unit 77 = {7.7: 10798, NaN: 43, 77.0: 3, 6.0: 2, 1.0: 1}; ga106c on unit 77: 10,614 real values < 9999, median 2.59, 27 NaN, 206 >= 9999).
- **"Only ONE 7.7 exists under any other unit (code 75)"** (commit + CONTENTS.org). Raw: units of ga106a==7.7 are {77: 10798, 75: 1, NaN: 1}. **Off by one**: a second stray 7.7 sits under a NaN unit code. The served-table statement "2 stray 7.7 kept" is correct (measured 2 before and after); the raw-data sentence is not.
- **"Obtained unit-77 rows fall to the existing no-amount drop"**. Read the script: `obtained = df.loc[df['ga110a'].notna() & (df['ga110a'] > 0)]` excludes them at construction once ga110a is NaN; they never reach the drop. Same outcome (179 rows gone), wrong mechanism sentence. Only the 233 purchase rows go through the drop.
- **Before/after Panama table** (rows, value rows, Quantity counts, expenditure sums, derived-table row counts, 2003/2008 byte-identical). Ran my own `measure.py` on base code against the pre-fix warm parquets, then cold on the branch. **All held** (table below). 2003/2008 `hash_pandas_object` sums identical before/after.
- **grain / null_read / null_index reports unchanged**: `grain_reports()` [] both; `null_read_reports('Panama')` the same two pre-existing 1997 weight/panel_weight null-content reports; `null_index_reports('Panama')` the same 443 NaN `u` keys (1997: 437, 2003: 1, 2008: 5). Index unique before and after. **Held**; no laundering.
- **#700 invariants** (`set(sample.v)==set(cluster_features.v)=={i.split('-')[0]}`, 364==364==364; non-joining 60 -> 0; roster index `(i,t,v,pid)`, v null 0/19,285; sample 3,623 rows unique on `(i,t)`; Rural {Urban 2287, Rural 1336}; household_characteristics 3,621 rows). **All held** on a cold build. Before (base code): cf.v = {'1','10','100',...}, roster index `(i,t,pid)`, 60 non-joining segmentos.
- **"cluster_features mode reduction unchanged"**. Checked by a stronger test than the warning text: base-vs-branch outer merge on `(t, zfill(v))` -> 364 both/0 left/0 right, `Region` and `Rural` equal on every cluster. Only `v`'s padding changed. **Held**.
- **Dispatch rule** ("framework binds a same-named function only to myvars and list idxvars; scalar idxvar -> bare format_id"). `country.py:1161` `if not (format_id_function and isinstance(value, str))`. **Held**; the `_pad_v_level` re-key in the hook is necessary, not belt-and-braces.
- **Cache cost** ("full Panama re-warm; full Peru re-warm; no other country"). Measured `Country._table_cache_hash(t, waves)` at merge-base 2ed85b49b vs branch: Panama food_acquired/sample/nutrition MOVED, Peru cluster_features/household_roster MOVED, Albania sample/household_roster UNMOVED, Uganda sample UNMOVED. **Held**.
- **Two Peru `test_sample.py` reds, nothing else**. Ran the named test files in full (983 tests, nothing deselected) under the Makefile recipe: exactly `test_has_weight_column[Peru]` and `test_covers_all_waves[Peru]` fail. Applied the drafted `test_sample_peru.patch` in scratch: weight cell -> XFAIL, `test_covers_all_waves[Peru]` still FAILED. Reverted. **Held**, with the consequence stated in §4.
- **Fix agent's side observation** (`get_data_file('../Documentation/x.pdf')` materialises OUTSIDE the data root): the fix stage shows a `Documentation/` dir beside its `data/`. Not reproduced here (I fetched no PDF), but the artefact is there. Pre-existing framework behaviour, not this branch; worth its own issue.

## 2. Claimed vs measured

| quantity | claimed before | claimed after | measured before (base code, warm) | measured after (branch, cold) |
|---|---|---|---|---|
| Panama fa rows 1997 | 156,154 | 155,742 | 156,154 | 155,742 |
| u=='value' rows / with Quantity / ==7.70 | 11,026 / 10,983 / 10,973 | 10,614 / 0 / 0 | 11,026 / 10,983 / 10,973 | 10,614 / 0 / 0 |
| 'Value' rows (capital) | 46 | 46 | 46 | 46 |
| 7.7 under other u | 2 | 2 | 2 | 2 |
| Expenditure sum 1997 | 356,079.56 | 356,079.56 | 356,079.56 | 356,079.56 |
| food_expenditures rows | 501,480 | 501,480 | 501,480 | 501,480 |
| food_prices kgvalue / kgprice | 483,661 | 483,661 | 483,661 / 483,661 | 483,661 / 483,661 |
| food_prices unitvalue / unitprice | 501,027 | 490,456 | 501,027 / 501,027 | 490,456 / 490,456 |
| food_quantities(units='kgs') rows | 570,803 | 559,820 | 570,803 | 559,820 |
| 2003 / 2008 fa | byte-identical | | hash equal | hash equal |
| 'value' rows by s (before) | 10,847 purchased + 179 obtained | all purchased | {purchased 10847, inkind 126, produced 39, other 14} | {purchased 10614} |
| script mask counts | ~10,804 / ~179 | | | 10,804 / 179 |
| Peru segmentos sample==cf==i | 364==364==364 | | n/a (no sample) | True, 364 |
| Peru non-joining segmentos | 60 -> 0 | | 60 | 0 |
| Peru roster v null | 0 of 19,285 | | no v level | 0 of 19,285 |
| Peru cf.v | '1','10' -> '001','010' | | ['1','10','100',...] | ['001','002',...] |
| Peru hc rows / index | 3,621 (t,v,i) | | 3,621 (t,i) | 3,621 (t,v,i) |

## 3. Regression

- `pytest tests/test_table_structure.py tests/test_sample.py tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py tests/test_currency_denominated_units.py tests/test_kinship_other_relative.py tests/test_null_read_guard.py -n 8 --dist=loadfile` (OPENBLAS/OMP/MKL=1, LSMS_BUILD_WORKERS=1): `2 failed, 983 passed, 85 skipped, 3 xfailed, 135 warnings in 543.97s` -- the two Peru `test_sample.py` cells only.
- `bench/feature_audit/scan.py --features sample cluster_features food_acquired --countries Peru Panama`: `records: 50 ... by status: {'warn': 48, 'error': 1, 'fail': 1}`. Every finding is pre-existing on development: Peru `food_acquired` (also `individual_education`, `interview_date`) is declared in `Peru/_/data_scheme.yml` but has no 1994 wiring and fails to materialise on base and branch alike; Panama 1997 weight/panel_weight 100% null; 443 NaN `u` keys; `labels='Aggregate'` unavailable for Panama; file-handle ResourceWarnings.
- `Feature('sample')(['Peru','Panama'])`: 21,976 rows, unique on (country,i,t); Peru's weight/panel_weight fill NA (3,623) as the concat rule says. `Feature('household_roster')` index `(country,t,v,i,pid)` unique.
- `find lsms_library/countries -name '*.parquet'` -> 0; `git status --short` empty; nothing written outside the private root by my runs.

## 4. Scope findings

- `git diff --stat origin/development...HEAD`: 7 files, 330+/36-, exactly the named paths plus one moved pin. No baseline/golden/blessed/lockfile/dependency edit. The moved pin (`unitvalue` 501_027 -> 490_456) has before/after in the commit and matches measurement. No skip/xfail added.
- **Blocker: the branch turns the suite red.** On development Peru is not in `SAMPLE_COUNTRIES` (no `sample` in its scheme), so the two failing cells did not exist. The branch creates them and leaves them red. `test_has_weight_column[Peru]` is resolved by the drafted one-line patch (verified XFAIL). `test_covers_all_waves[Peru]` has no exemption mechanism: Peru is the first country whose `sample` genuinely covers a strict subset of `Country.waves` (GhanaAHIES/Nigeria/Tanzania also have wave dirs without `_/`, but those are `wave_folder_map` round names already covered by `t`). Needs a design decision (§5).
- **No new tests.** The only test change is the moved pin. The #777 invariant lives in a build-time `assert` inside the wave script; the #700 set-equality lives only in the fix stage's `verify_peru.py`. Neither fix is pinned in `tests/`.
- Two documentation inaccuracies (raw-data "only one 7.7 under another unit" -- there are two; "obtained rows fall to the no-amount drop" -- they are excluded at construction). Outcomes correct, sentences not.
- CONTENTS.org: both countries name the idiosyncrasy (Peru: v/i padding + `_weight_` SPSS artefact; Panama: unit-77 echo, two 'value' populations); claims re-measured and true apart from the off-by-one above. Residual issue drafts present in the report (#700 none beyond pre-existing "wire 1985/1990/1991"; #777 resolved) -- consistent with what I measured.

## 5. Residual risks / what a human must decide

1. **`test_covers_all_waves[Peru]`**: accept a permanently red cell, add a per-country exemption set, or make the test compare against waves that have a `_/` dir. Recommend the second (a per-country exemption naming the unwired waves, see §6.2); the wiring-status variant would silently stop checking the `wave_folder_map` round dirs of GhanaAHIES/Nigeria/Tanzania.
2. **179 obtained-in-valor rows are DELETED, not served expenditure-only.** The triage's option A said rows "stay as expenditure-only observations"; obtained rows have no expenditure by construction, so the fact that a household received item j as gift/own-produce (126 inkind, 39 produced, 14 other) is gone from the table. Defensible and disclosed as part of the 412 in the commit, but it is a row deletion with no registry entry; the human is ratifying it.
3. **Peru has no weight column by design** -- weighted analysis of Peru is impossible from the library until the seven domain expansion factors are acquired (acquisition gap, correctly not papered over with ones).
4. `cluster_features.v` served values change ('1' -> '001'); any downstream code with the unpadded strings hard-coded breaks. Disclosed.
5. Pre-existing, not this branch: Peru declares `food_acquired`, `individual_education`, `interview_date` with no 1994 wiring (hard RuntimeError on both base and branch); `get_data_file` relative-path materialisation outside the data root.

## 6. Recommended follow-up commits (before merge)

1. `tests/test_sample.py`: apply `$STAGE/../../backlog_fix_39921399/peru-panama/test_sample_peru.patch` (add "Peru" to `NO_WEIGHT_COUNTRIES` with the REG01 `_weight_` reason). Verified: cell -> XFAIL.
2. `tests/test_sample.py::test_covers_all_waves`: add a per-country exemption `PARTIALLY_WIRED_SAMPLE = {"Peru": {"1985", "1990", "1991"}}` (unwired waves, with the reason) and subtract it from `expected_waves`. Do NOT switch the test to "waves whose `_/` dir exists": GhanaAHIES, Nigeria and Tanzania have `_`-less `wave_folder_map` round dirs that the test currently checks via `t` and would silently stop checking. Verify the other 36 countries still pass.
3. New `tests/test_panama_777.py` (cache-gated like `TestDelivered`): on `Country('Panama').food_acquired()` 1997, assert `Quantity` is all-NA on `u=='value'` rows and `(Quantity==7.7)&(u=='value')` count == 0; Expenditure sum 1997 == 356,079.56 +- 0.01. Fails on development (10,973).
4. New `tests/test_peru_700.py`: `set(sample.v) == set(cluster_features.v) == {i.split('-')[0]}` and `household_roster` carries `v` with 0 nulls. Fails on development (no `sample`).
5. `Panama/1997/_/food_acquired.py` comment + `Panama/_/CONTENTS.org`: "one stray 7.7 under unit 75" -> "two stray 7.7 rows, one under unit 75 and one with no unit code"; "obtained rows fall to the no-amount drop" -> "obtained rows are excluded by the `ga110a.notna()` filter when the canonical rows are built". Commit message cannot be amended post-push; CONTENTS.org carries the correction.
