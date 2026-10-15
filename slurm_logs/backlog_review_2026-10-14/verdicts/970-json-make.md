VERDICT: pass -- the branch removes the only `make` shell-out the issue names, the Design-B cold crosswalk is byte-identical before and after, every claimed number reproduces, and the one suite failure is a pre-existing `ci.yml` check already fixed on `development` after this branch's merge-base.

Reviewer: red-team element `970-json-make`, branch `fix/970-json-make` @ `f4ad88885` (merge-base `340e30f1e`; `origin/development` has since moved to `88707ddb0`). Private root: `$STAGE/data` (Malawi, Nigeria warm). Scratch base worktree at `/local/job39923122/base` (origin/development tip), removed at the end. Import identity asserted (`/local/job39923122/clone/lsms_library/__init__.py`). GitNexus unavailable; `rg`/`grep` sweeps used instead.

## 1. Refutations

| claim (report / commits) | what I ran | result |
|---|---|---|
| Cold Design-B `panel_ids` on development shells out to `make -s ... panel_ids.json`, fails, logs "Makefile build failed ... Falling back" | `repro.py` (spies `subprocess.run`, captures `logging` WARNING + Python warnings) on the base worktree with `$LSMS_DATA_DIR/{C}/_/panel_ids.*` removed, Malawi and Nigeria | **held**: base ran `['make','-s','-j20','panel_ids.json']` with `env` absent, cwd `countries/{C}/_`, make printed `No rule to make target`, warning logged |
| Branch makes no subprocess call and logs no warning, still returns the dict | same script on the branch | **held**: `subprocess.run calls: []`, `log warnings: []`, Malawi `panel_ids` 5,686 keys / `updated_ids` 3, Nigeria 14,251 / 4 |
| Output unchanged by the fix | sha256 of sorted items, base vs branch | **held**: Malawi `c7acf31874071a09` / `600ca1cefbac09ad` both trees; Nigeria `6a187812cfb5ce16` / `97ed9ee58c2ac2bf` both trees. The rebuilt `_/panel_ids.parquet` `.equals()` the pre-fix warm parquet seeded in the private root (Malawi 9,686x1, Nigeria 19,110x1) |
| `make` absent (GH #968) takes the no-make path cleanly | fully COLD root (`/local/job39923122/coldroot`, only the dvc-cache symlink) with a PATH whose `/usr/bin` was replaced by a symlink farm lacking `make`/`gmake` | **held**: branch derived the crosswalk from the three wave `data_info.yml`s with zero subprocess calls and no warning; base on the same PATH logged `Makefile build failed ... [Errno 2] No such file or directory: 'make'`. Cold root ended with exactly `Malawi/_/panel_ids.parquet` + three `{wave}/_/panel_ids.parquet`; nothing in-tree |
| New tests fail pre-fix | copied both new test files into the base worktree, `-p no:xdist` | **held**: `3 failed, 10 passed` -- the two `test_gh970_no_json_make.py` tests and `test_every_subprocess_call_in_country_py_passes_env` (`subprocess.run at line 4365`); the old function-level guard passed on the same code, as the issue says |
| New tests + `test_panel_ids_persistence.py` pass on branch | serial run on branch | **held**: `69 passed in 13.88s` |
| `else: run_make_target(...)` branch only reachable via successful make, so removable | read `load_json_cache` on branch; `grep` of all 21 `countries/*/_/Makefile` for `json` | **held**: every `.json` mention is an `all:` prerequisite, a comment, or `make clean` echo; no rule has a `.json` LHS. `run_make_target`'s own `JSON_CACHE_METHODS` branches are untouched |
| `LSMS_BUILD_BACKEND=make` route still works for JSON methods | branch, Malawi, `_/` cold, `LSMS_BUILD_BACKEND=make` | **held**: same 5,686-key dict, no subprocess, nothing written (expected for the make backend) |
| Cache cost: 5/5 `btf`, 5/5 `tch` for Malawi, Nigeria, Uganda | agent's `hash_probe.py` on base worktree and on branch | **held**: 5/5 on all four rows. My `btf` values equal the agent's before/after byte-for-byte; my branch `tch` equals the agent's after. My base `tch` differs from the agent's before because my base is `88707ddb0` (78 files, Uganda `data_scheme.yml`/scripts, Burkina/Senegal `.org`) not `340e30f1e` -- explained, not a discrepancy |
| Merge with #969: `country.py` auto-merges, test file conflicts | `git fetch` GitHub `feature/964-windows-script-builds`; `git merge-tree --write-tree HEAD FETCH_HEAD` | **held**: `CONFLICT (content)` in `tests/test_build_subprocess_env.py` only. Additionally: the merged tree's `country.py` has no bare `subprocess.run(make_cmd, cwd=self.file_path ...)` and the branch's AST guard reports `[]` offenders on it, so the deletion survives either merge order |
| `test_every_poetry_install_passes_no_root` failure is pre-existing | ran it on branch and on base worktree | **held, with nuance**: fails on the branch (its base `340e30f1e`), **passes on `origin/development` tip** -- fixed upstream by `24cd065b0` (`ci: windows-smoke poetry install --no-root`). Disappears on rebase/merge |
| No in-tree parquet, no writes outside private root | `find lsms_library/countries -name '*.parquet'` after every run incl. the suite | **held**: empty; `git status --short` empty |

Commit message says `-j3`; I saw `-j20`. That is `_make_jobs_flag()` reading the CPU count, not a discrepancy.

## 2. Claimed vs measured

| figure | claimed | measured |
|---|---|---|
| pre-fix new tests | 3 failed, 10 passed | 3 failed, 10 passed |
| targeted post-fix (3 files) | 69 passed | 69 passed |
| `btf` moved (5 tables) | 5/5 | 5/5 |
| `tch` moved Malawi / Nigeria / Uganda | 5/5 each | 5/5 each |
| Malawi `panel_ids` size | (not stated) | 5,686 keys; parquet 9,686 rows, equal to pre-fix |
| Nigeria `panel_ids` size | (not stated) | 14,251 keys; parquet 19,110 rows, equal to pre-fix |
| full suite failures attributable to branch | 0 | 0 |

## 3. Regression

Full suite, branch, verbatim recipe (`OPENBLAS/OMP/MKL=1 LSMS_BUILD_WORKERS=1 -n 40 --dist=loadfile`), `/local/job39923122/pytest.log`:

```
FAILED tests/test_no_editable_install.py::test_every_poetry_install_passes_no_root
1 failed, 4603 passed, 187 skipped, 3 xfailed, 1 xpassed, 888 warnings in 1019.15s (0:16:59)
```

Passed count is lower than the agent's 5,594 because my private root holds only Malawi/Nigeria warm, so more cache-gated tests skip (187 vs 156). The agent's order-dependent `test_food_prices_dtype.py` failures did not recur at `-n 40`. The "Makefile execution failed for Nigeria/_ plot_inputs/plot_labor" warnings in the log are the three deliberate GH #866 warnings, not this branch.

`bench/feature_audit/scan.py --features panel_ids`: **N/A** -- `panel_ids` is a dict property; `scan.py:287` records the load and skips the frame checks, so there is nothing a scan would add beyond the dict-equality above. No DataFrame-serving table is touched, so `grain_reports()` / `null_index_reports()` / `null_read_reports()` are unaffected by construction (and the 87 such warnings in the suite log are the corpus's known ones).

## 4. Scope findings

- Diffstat matches the task: `lsms_library/country.py` (-14/+11), `tests/test_build_subprocess_env.py` (+70, additive only), new `tests/test_gh970_no_json_make.py` (+149). Report lists the new file as its one "deviation"; the task asked for that test.
- No baseline/golden/blessed/lockfile/dependency edit; no `xfail`/`skip`/pin moved (checked the diff).
- The old function-level guard is kept and the AST guard added beside it, as the task permitted.
- CONTENTS.org untouched; correct -- the defect is framework-level, and Malawi/Nigeria CONTENTS.org already describe the Design-B `panel_ids` derivation (`Malawi:78,1247`, `Nigeria:776`). Nothing it claims contradicts them.
- Residual-draft rule: the branch resolves #970 in full (option (a) + the AST guard), so no residual issue is owed. The report does name two pre-existing defects it found without drafting them (see §6).

## 5. Residual risks and what a human must decide

1. **Merge order with #969** (`feature/964-windows-script-builds`): `tests/test_build_subprocess_env.py` conflicts (both append at EOF; #969 also renames the parametrized guard's `Wave.grab_data` target). Trivial resolution -- keep both appended blocks -- but a human merges it. `country.py` is clean in either order and the AST guard passes on the merged tree.
2. **Corpus-wide cold re-warm** is real and derived, not separately probed: every L2-country hash moves (5/5 measured on three countries), and the L2-wave invalidation follows from `btf` moving (it is folded into `Wave._input_hash`). Accepted per the 2026-10-14 ruling, bundled with #810/#868.
3. **AST guard blind spot**: it recognises `subprocess.<call>` and bare `run(`/`call(`/... names; `import subprocess as sp` would evade it. No such import exists in `country.py` today. Not blocking.
4. `test_every_poetry_install_passes_no_root` fails on this branch's base and is already fixed on `development` (`24cd065b0`); a rebase makes the suite fully green.

## 6. Recommended follow-up commits (none required for merge)

- File the two pre-existing defects the report describes but did not draft: (a) `tests/test_gh866_stale_country_parquet_not_laundered.py::test_the_helper_is_a_noop_on_an_absent_parquet` sets `LSMS_DATA_DIR` via `monkeypatch.setenv` without `data_root.cache_clear()`, leaking a tmp root to later files on its xdist worker (the agent saw it fail `test_food_prices_dtype.py`); (b) the general absence of a `conftest.py`-level `data_root` cache reset. Both one-line issues, no code on this branch.
- Optional hardening of `_subprocess_calls_without_env` in `tests/test_build_subprocess_env.py` to resolve `import subprocess as <alias>`.
