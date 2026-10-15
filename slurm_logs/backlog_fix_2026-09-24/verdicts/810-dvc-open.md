VERDICT: pass-with-fixes -- the fix does what it claims (symptom reproduced on development, gone on the branch, every number re-measured), data is byte-identical on cold builds, and the two suite failures are pre-existing order-dependent flakes; one code comment / commit-message claim about the WB fallback is false and should be corrected in a follow-up.

Reviewer: independent red-team, Slurm job 39199803, own clone of `fix/810-dvc-open` (`bebe68a51`), base worktree at `origin/development` (`b5103b64f`, merge-base `242573697`), mid worktree at `4d4d16d64`, private `LSMS_DATA_DIR` (warm for no country at start). Package identity asserted for every run (`assert realpath(lsms_library.__file__).startswith(<tree>)`). GitNexus MCP did not connect; `rg`/grep call-site sweeps substituted (declared).

## 1. Refutations

1. **"Symptom: YAMLValidationError / cwd walk on a missing source" -- HELD (reproduced and gone).**
   Unstubbed, from a fresh tmp cwd holding a malformed `foo.dta.dvc`, `get_dataframe("Nowhere/2000/Data/absent.dta")`:
   - development: `dvc.utils.strictyaml.YAMLSyntaxError: unable to read: 'foo.dta.dvc'` -- 55.0 s cold, 0.0 s warm.
   - branch: `FileNotFoundError [Errno 2] source is not on disk, has no DVC sidecar under .../countries, and could not be fetched from the World Bank Microdata Library` -- 56.0 s cold, 0.0 s warm.
   (`scratch/repro.py`, run with `PYTHONSAFEPATH=1` and `PYTHONPATH=<tree>` from inside each tree.) The 55 s is `file_system_path`'s correctly-rooted `DVCFS.open` index walk, paid identically on both trees, as the commit message says.

2. **"Nepal BrokenProcessPool route removed" -- HELD by construction and by cost.** `Country('Nepal').sample()` from a tmp cwd: development `PathMissingError` after **234.1 s** (clean cwd) / `YAMLSyntaxError` after **255.7 s** (cwd with malformed sidecar); branch `FileNotFoundError` after **67.9 s**, caught by `grab_data` ("DVC unavailable for sample: [Errno 2] ... Falling back to manual aggregation"). Armenia `household_roster`: branch `FileNotFoundError` after 60.8 s. The pool death itself was not reproduced (a tiny cwd cannot produce a 300 s walk); the cwd-rooted call is gone from the package (`grep -rn 'dvc.api' lsms_library/*.py` -> only comments and `from dvc.api import DVCFileSystem`).

3. **"The old branch also ended in a re-raised FileNotFoundError" / "Genuine WB fetch failures propagate from `_try_get_data_file` as before" -- REFUTED as a premise, conclusion held.**
   On development the else-branch raised `dvc.exceptions.PathMissingError` (a `DvcException`, outside the caught `(OSError, ValueError, KeyError, ImportError)` tuple), so `_try_get_data_file` was **never reached** for a plain missing source: 0 "No SOURCE.org / No Stata zip" log lines on development vs 6 on the branch for the same Nepal call. And `get_data_file` swallows network/HTTP/timeout errors to `None` (`data_access.py:1031`, `:1053`), so a WB fetch failure now surfaces as the same `FileNotFoundError`, not as a propagated network error. What the report actually needs is true: every downstream catch site takes `FileNotFoundError` beside the DVC classes (`country.py:1435, 1510, 2695, 4703, 4745`), so the #515 soft-degrade still works (observed above). Fix the comment (see §6).

4. **"4 of 5 new tests fail on development with the issue's signature" -- HELD.** Base worktree: `4 failed, 1 passed in 12.10s`; three of the four fail with `dvc.utils.strictyaml.YAMLSyntaxError: unable to read: 'foo.dta.dvc'`, the fourth (`never_imports_dvc_api_open`) with the intended `AssertionError: dvc.api.open reached`. Branch: `5 passed in 9.37s` (slowest 1.16 s, the two timed tests 0.22 s and 0.02 s). All five also passed inside the full `-n 8` suite on a load-32 node, so the `elapsed < 1.0` bound did not flake here.

5. **"Cache cost 0/3, 0/81 for commit 1; 3/3, 81/81 for commit 2" -- HELD exactly** (probe over Uganda, GhanaLSS, Ethiopia, Kosovo; `scratch/hashprobe.py`): base->mid 0/3 fingerprints, 0/81 table hashes; mid->branch 3/3, 81/81; base->branch 3/3, 81/81. The re-warm scope claim (corpus-wide, because `df_data_grabber` closes over `get_dataframe`) is right.

6. **"`local_tools.change_id` has zero in-corpus callers" -- HELD.** The four call sites (GhanaLSS `household_roster.py`, Ethiopia `food_acquired.py`) import `change_id` from `ghanalss.py` / `ethiopia.py`. `from_dta` and `get_dataframe` share the `convert_categoricals=True` default, so the happy path is unchanged for the one public caller class that could exist.

7. **"No data changes" -- HELD by cold builds on both trees.** Kosovo, all 9 tables, cold into separate private roots: identical row counts, index uniqueness, NA counts, numeric column sums, warning classes, and identical `grain_reports()` (1 report: Kosovo/housing 2000, 4,182 destroyed), `null_read_reports()` (0), `null_index_reports()` (0). GhanaLSS, cold on both trees: `food_acquired` (5,340,606 x 4) and `sample` (56,330 x 5) parquets `assert_frame_equal(check_exact=True)` IDENTICAL, `lsms_grain_audit` metadata identical; only `lsms_cache_hash` differs (expected).

## 2. Claimed vs measured

| claim (report / commits) | measured here |
|---|---|
| before: YAMLSyntaxError on `foo.dta.dvc` | YAMLSyntaxError, 55.0 s cold, 0.0 s warm |
| after: FileNotFoundError, 51.6 s cold, 0.0 s warm | FileNotFoundError, 56.0 s cold, 0.0 s warm |
| new tests on unfixed code: 4 failed, 1 passed | 4 failed, 1 passed in 12.10 s |
| new tests on branch: 5 passed | 5 passed in 9.37 s (also 5/5 inside the full suite) |
| commit 1 moves 0/3 fingerprints, 0/81 hashes | 0/3, 0/81 |
| commit 2 moves 3/3, 81/81 | 3/3, 81/81 |
| full suite: 4244 passed, 203 skipped, 3 xfailed, 1 xpassed | 4235 passed + 1 failed + 8 errors = 4244; 203 skipped, 3 xfailed, 1 xpassed (see §3) |
| `find lsms_library/countries -name '*.parquet'` = 0 | 0 in clone, base and mid, before and after the suite |
| "old branch re-raised FileNotFoundError"; WB failures propagate "as before" | old branch raised `PathMissingError`; WB fallback was unreachable before, reached now; network errors become `FileNotFoundError`, not propagated |
| `change_id`: 0 in-corpus callers | 0 |

## 3. Regression

Full suite on the branch, verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 pytest tests -n 8 --dist=loadfile -q`, cold private root):

```
1 failed, 4235 passed, 203 skipped, 3 xfailed, 1 xpassed, 808 warnings, 8 errors in 1979.62s (0:32:59)
FAILED tests/test_ghanalss_aggregate.py::TestExpenditures::test_expenditures_sum_levels_without_losing_households_or_sources[total]
ERROR tests/test_uganda_crop_condition.py::{8 tests}  -- RuntimeError: Could not materialize Uganda/crop_production ... no cached parquet was found at .../pytest-2/popen-gw1/test_the_helper_is_a_noop_on_a0/Uganda/var/crop_production.parquet
```

Both adjudicated as pre-existing, order-dependent, not attributable to the branch:

- **Uganda (8 errors)**: the fixture's data root is `test_gh866_stale_country_parquet_not_laundered.py::test_the_helper_is_a_noop_on_an_absent_parquet`'s `tmp_path` -- that test does `monkeypatch.setenv("LSMS_DATA_DIR", tmp_path)` and `paths.data_root()` is `@lru_cache` (`paths.py:91-92`), so the poisoned root outlives the test in that xdist worker. Running the two files in one process reproduced the stall at exactly the first crop_production-fixture test; `tests/test_uganda_crop_condition.py` alone on the branch: `13 passed in 96.37s`. `paths.py`, `country.py` and both test files are untouched by this branch.
- **GhanaLSS `[total]` (1 failure)**: `default` vs `labels='Preferred'` shapes (1,604,073 vs 1,603,947). The same node id alone: branch (warm) `2 passed in 328.91s`; development (cold, `cmp_base` root) `2 passed in 694.72s`. The parquets the two trees built are byte-identical (§1.7), so the difference is on the read path under some sibling test's state, not in this diff. Not reproducible in isolation on either tree; no open issue names it. Recommend a separate flake ticket.

New-test file: 5/5 on branch, 4 fail / 1 pass on development (§1.4). `bench/feature_audit/scan.py` not run: no feature is touched (the only reachable behaviour change is the missing-source branch, which no built table enters -- confirmed by the identical GhanaLSS/Kosovo builds).

## 4. Scope findings

- `git diff --stat origin/development...HEAD` (three-dot; the two-dot form the brief quotes drags in 22 unrelated deletions that live on `development`): exactly `lsms_library/local_tools.py` (+35/-20) and `tests/test_gh810_no_cwd_rooted_dvc.py` (+117). No baseline, golden, blessed, lockfile, dependency or CONTENTS.org edit. No weakened, skipped or xfailed assertion; no moved pin.
- The `import errno` addition and the `import dvc.api` removal are the only import changes; `DVCFileSystem` import kept; `_sync_legacy_dvc_creds` (now serving no caller) left in place, correctly named as a follow-up.
- `git status --short` empty in clone, base and mid; nothing written outside the private root (`~/.local/share/lsms_library` -> shared cache untouched; `~/.cache/lsms_library/issues.log` mtime unchanged).
- CONTENTS.org: framework-only change, no country idiosyncrasy in play; the report's "n/a" is correct.

## 5. Residual risks and what a human must decide

1. **Land beside the #959 re-warm.** Confirmed corpus-wide: every L2-country hash and every build fingerprint moves (§1.5). Merging this alone triggers a full cold re-warm on the next read of every table.
2. **New behaviour: every missing source now makes WB Microdata API calls.** Development never reached the WB fallback from this branch (§1.3). Online, the cost was a few seconds per missing source (Nepal, 3 waves: 67.9 s total, ~50 s of it the DVCFS walk). Offline / firewalled: bounded by `urlopen(timeout=30)` in `_get_catalog_idno` and `_find_stata_zip_url` (~60 s per missing source) plus one 15 s key validation per process (`permissions()` and `_validate_wb_api_key` are cached). Nepal/Armenia (no microdata, but WB-URL `SOURCE.org`) pay this on every cold read. Acceptable, but it is a cost the report did not state.
3. **Newly exposed pre-existing defect, not this branch's:** the branch's log shows `No SOURCE.org found for ../..` -- one of `grab_data`'s relative candidate paths (`../Data/...`) is parsed by `get_data_file` into `country='..'`. Harmless (returns None) but noisy; worth its own small issue.
4. The residual ~50-93 s `DVCFS.open` index walk for a genuinely absent source is unchanged and correctly disclaimed.

## 6. Recommended follow-up commits

1. `lsms_library/local_tools.py` (~line 1405, the else-branch comment): replace "Genuine WB fetch failures (network, auth) propagate from `_try_get_data_file` as before" with a true statement: the WB route was previously unreachable from this branch (the old call raised `PathMissingError`, outside the caught tuple); `get_data_file` returns `None` on network/HTTP/timeout errors, so those surface as this `FileNotFoundError`, and `grab_data` / `df_data_grabber(missing_ok=)` soft-degrade on it. Same correction in the docstring at the top of `tests/test_gh810_no_cwd_rooted_dvc.py` if it is echoed there (it is not; only the code comment and commit message carry it).
2. Optional, same PR or next: remove `_sync_legacy_dvc_creds` (`lsms_library/__init__.py:316-330`, `data_access.py:788-917`) now that no `dvc.api.open` caller exists in the package; retire the CLAUDE.md gotcha about `get_dataframe('../Data/..')` from a chdir'd cwd.
3. Separate issues, not this branch: (a) `paths.data_root` lru_cache poisoned by `monkeypatch.setenv` in `test_gh866_...` (add `data_root.cache_clear()` in that test's teardown or a conftest autouse); (b) order-dependent `test_ghanalss_aggregate[total]` flake; (c) the `../..` path-parse in `get_data_file`.
