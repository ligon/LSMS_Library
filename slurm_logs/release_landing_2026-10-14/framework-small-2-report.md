# framework-small-2 -- REPORT (2026-10-14)

## SCOPE DEVIATIONS
- `tests/test_gh905_vectorised_min_count_sum.py`: §6.2 said "amend nothing else", but the module docstring said "a couple of ULP" and the §5.2 ruling is "make every comment say so"; the docstring now reads "<= 4, max 2 measured". Docstring ONLY -- the `<= 4` assertion at line 67 is untouched. Declared here rather than discovered.
- Nothing else. Files touched (all named in VERDICT.md section 6 / the task): `tests/test_dvc_caching.py`, `tests/test_countries_root_override.py`, `lsms_library/build_transforms.py`, `lsms_library/countries/Panama/{1997,2003,2008}/_/food_acquired.py`, `tests/test_gh905_vectorised_min_count_sum.py` (module docstring only), `lsms_library/country.py` (docstring only), `tests/test_categorical_mapping_resolution.py` (assertion message only). No stop-list match. No CONTENTS.org edit was needed (no new idiosyncrasy found).

## 1. Commits, branch, push
Branch `fix/framework-small-2`, pushed to `origin` (shared mirror); `git status --short` empty; `find lsms_library/countries -name '*.parquet'` -> 0.

| SHA | what |
|---|---|
| `f080a5b27` | merge of `origin/development` (clean, no conflicts) |
| `1e6dc1825` | §6.1 -- `mock_country_structure` creates a real config tree at `tmp_path/config` and sets `LSMS_COUNTRIES_ROOT` to it; `test_subprocess_override_routes_all_layers` creates `ov/"Uganda"`. The #915 raise is unchanged. |
| `2d78cfc3f` | §6.2 -- "1 ULP" -> "a few ULP (<= 4, pinned by tests/test_gh905...; max 2 measured)" at `build_transforms.py:203`, the three Panama scripts, and the #905 test's module docstring |
| `2d78cfc3f` ERRATUM | its message says "0 of 55 ... values moved"; that figure is from a DISCARDED probe (see §2) and is not a valid measurement. Valid: Mali 0 of 24, Uganda 0 of 28, 3 fingerprints unchanged, Panama 5 of 5 moved. Cannot amend (pushed; no force-push); recorded in `fd3ebb1c5` (empty errata commit) and here. |
| `89d11fa95` | §6.4 -- `_strip_string_values` docstring and `test_gh694_numeric_values_survive_a_same_named_table` message say "mixed object column" |
| `fd3ebb1c5` | empty errata commit for `2d78cfc3f`'s figure |

## 2. Verification
- Identity: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert ...'` -> `identity ok /local/job39924156/clone/lsms_library/__init__.py`
- FULL suite BEFORE touching anything (recipe, `$STAGE/pytest_baseline.log`):
  `15 failed, 4530 passed, 2803 skipped, 3 xfailed, 1 xpassed, 802 warnings in 851.82s (0:14:11)`
  = the 14 named (13 `test_dvc_caching.py::TestDVCCaching::*` + `test_subprocess_override_routes_all_layers`) + `tests/test_sample.py::TestSample::test_covers_all_waves[Albania]` (see §4).
- `pytest tests/test_dvc_caching.py tests/test_countries_root_override.py -n 8 --dist=loadfile` after §6.1: `50 passed in 66.47s (0:01:06)` (`$STAGE/pytest_targeted1.log`)
- `pytest tests/test_gh905_vectorised_min_count_sum.py tests/test_categorical_mapping_resolution.py -n 4 --dist=loadfile` after §6.2/§6.4: `38 passed, 1 skipped in 8.76s` (`$STAGE/pytest_targeted2.log`)
- FULL suite AFTER (same recipe, `$STAGE/pytest_after.log`):
  `5595 passed, 1753 skipped, 3 xfailed, 1 xpassed, 841 warnings in 755.20s (0:12:35)`
  0 failed. (More passed / fewer skipped than baseline because the private root is warmer after the first run -- cold cells skip with a reason under #703.)
- Stash-probe `Country._table_cache_hash(t, C.waves)` for every table of Mali, Uganda, Panama + `build_transforms_fingerprint` x3, merge base `f080a5b27` (worktree, `PYTHONPATH`+`LSMS_COUNTRIES_ROOT` to it, identity asserted) vs HEAD (`$STAGE/probe/hash_{before,after}.json`): **moved 5 of 60** -- `Panama/food_acquired`, `Panama/food_expenditures`, `Panama/food_prices`, `Panama/food_quantities`, `Panama/sample`. **Mali 0 of 24, Uganda 0 of 28, all three fingerprints unchanged.** (A first probe attempt reported "0 of 55" and was WRONG -- the call omitted `waves` and a try/except turned every entry into the same error string; discarded and re-run without the except.)
- Albania on `origin/development` (worktree, `PYTHONPATH`/`LSMS_COUNTRIES_ROOT`/identity assert): `Country('Albania').sample()` -> 21,357 rows, missing waves `[]`. On the branch with `LSMS_NO_CACHE=1`: 21,357 rows, 6 of 6 waves. `git diff origin/development HEAD --stat -- lsms_library/countries/Albania` -> empty.

## 3. Cache cost (stated, not paid)
- §6.1 / §6.4: zero (tests + a docstring in `country.py`, which the probe shows reaches no hash).
- §6.2: the comment in `build_transforms.py` moves nothing (fingerprint is not text-sensitive to comments: 0 of 3 fingerprints and 0 Mali/Uganda hashes moved). The three Panama wave-script comments DO move Panama's `food_acquired` L2-country hash (wave `_/*.py` are hashed by text in the `binp:` loop), and `sample` + the three derived food tables with it: **Panama re-warms once** (5 tables). No other country.
- Inherited from the base branch, unchanged here: #905's edit moved Panama `food_acquired` once already; #915/#694/#909/#703/#734/#792 are read-path/tests.

## 4. Surprises / CONTENTS.org
- **The 15th baseline red was NOT a branch defect.** `test_covers_all_waves[Albania]` reported wave 2003 missing on the cold private root; in the SAME baseline run `test_index_is_i_t[Albania]` saw `Rural` null in `[2004]` only (2003 absent) while a later read saw `[2003, 2004]` (2003 present) -- two reads of one table in one session disagreeing. 2003 is a script-path wave (`Albania/2003/_/sample.py`, `materialize: make` in `Albania/_/data_scheme.yml`; there is NO Makefile at wave or country level -- the baseline log's `Makefile not found in .../Albania/_` warnings -- so it is run through the script fallback) and the baseline run built Albania cold under 40 xdist workers; a sequential build on either branch gives 6/6 waves, and the after-run (root now warm) is green. So: a cold-root concurrent-build race on a script-path wave, not red on development as such and not caused by this branch. Not fixed (outside the named paths); worth its own issue if it recurs -- it is the same class as the `--dist=load` hazard CLAUDE.md records under scrum-master addendum 3b.
- Nothing contradicts `Albania/_/CONTENTS.org` or `Panama/_/CONTENTS.org`; no edits.

## 5. Decisions
- **Fixture lever (§5.1 / advisor):** a SEPARATE config tree (`tmp_path/config`) rather than pointing `LSMS_COUNTRIES_ROOT` at the data tree. With `countries_root() == data_root()` every parquet the tests write is "in-tree" and the GH #803 guard (`country.py:1739`, `:4104`) would warn on every cache read; before #915 `file_path` pointed at a non-existent dir under the real root and nothing read it, which the split preserves. Both `countries_root` and `catalog._country_dirs` are `lru_cache`d and are cleared on entry and in teardown (`--dist=loadfile` shares a worker across files).
- **ULP contract (§5.2):** `<= 4` as pinned; max 2 measured (e95f8d792). Comments and the test docstring say so; no assertion moved.
- **#905 residual (§6.3):** not swept, per ruling. Drafted below.
- **VERDICT corrections (§6.5):** adopted. R3: "byte-identical" -> **data-identical (`DataFrame.equals` True; the parquet FOOTER differs on `lsms_cache_hash`, which #905 moves by design)**. R6: the prior report's "seeded warm Mali roster parquet has NO manifest -> (False, 0, 4)" is **withdrawn**: re-measured here, `read_parquet_cache_waves(Mali/var/household_roster.parquet)` = `['2014-15','2017-18','2018-19','2021-22']` -- but my root had been through a full suite run by then, so it cannot distinguish "seeded with a manifest" from "re-stamped", and the reviewer measured `(True, 4, 4)` on a fresh seed. The sentence is dropped rather than re-asserted.
- **Claim check on `89d11fa95`'s message** ("an int/float column was never at risk -- it has no .str accessor path"): verified against `git show 4ae96f103`. The removed code was `if hasattr(df[col], 'str'): df[col] = df[col].str.strip()`; on an int64 Series `.str` raises `AttributeError` (pandas 3.0.2: "Can only use .str accessor with string values"), so `hasattr` was False and the strip was skipped. "Never at risk" holds; the mechanism was the `hasattr` guard, which is the precise wording.
- Human decision still open (VERDICT §5.3, #703 CI): the `data-tests` job's in-tree-vs-`data_root()` question is unverifiable from here and unchanged by this element; name it when merging.

## #905 residual (drafted issue text)
`_finalize_canonical_food_acquired` and the three Panama wave scripts now use the cython `groupby.sum(min_count=1)`; every other `lambda s: s.sum(min_count=1)` reducer survives. Each is one Python call per group plus a deep-copy of `df.attrs` per call (the GH #871 cost). Convert with a cold before/after `equals` check (<= 4 ULP tolerance on summed floats, as `tests/test_gh905_vectorised_min_count_sum.py` pins) and restore column order explicitly.

food_acquired sites (the issue's stated scope):
- `lsms_library/countries/GhanaSPS/2009-10/_/food_acquired.py:109-110`
- `lsms_library/countries/GhanaSPS/2013-14/_/food_acquired.py:81-82`
- `lsms_library/countries/GhanaSPS/2017-18/_/food_acquired.py:80-81`
- `lsms_library/countries/Cambodia/2019-20/_/food_acquired.py:102-103`
- `lsms_library/countries/Serbia/2007/_/food_acquired.py:128-129` (`lambda x`)
- `lsms_library/countries/Guatemala/2000/_/food_acquired.py:152-153` (`lambda x`; not in the VERDICT list)
- `lsms_library/countries/GhanaLSS/_/ghanalss.py:898` (`reduce_duplicate_food_rows`, `_sum` used at :899)

Same pattern, other tables (correction to VERDICT R5: `ghanasps.py:837` is in `sum_duplicate_harvest_records`, i.e. crop_production, not food_acquired):
- `lsms_library/countries/GhanaSPS/_/ghanasps.py:837` (crop_production)
- `lsms_library/countries/Mali/2014-15/_/plot_inputs.py:163`, `Mali/2017-18/_/plot_inputs.py:169`
- `lsms_library/countries/Iraq/2012/_/assets.py:44-46`
- `lsms_library/countries/Albania/2002/_/assets.py:41`
- `lsms_library/countries/Tanzania/_/tanzania.py:1664-1666`
- `lsms_library/countries/Malawi/_/malawi.py:2968`

Cache cost of a sweep: each converted wave script moves that wave's country hashes (text-hashed); each country module moves every table of that country.