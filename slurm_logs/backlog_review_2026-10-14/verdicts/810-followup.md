VERDICT: pass-with-fixes -- the follow-up commit is comment-only, every code claim I re-ran held (symptom reproduced on development and gone on the branch, 0/36 hashes moved by the follow-up, 36/36 by the whole branch as ruled, new tests 4-fail/1-pass on development and 5/5 on the branch, full suite green but for a failure development has already fixed); what needs fixing is one remaining false clause in the very comment the task was about, a report number that does not reproduce, a moot candidate, and the undrafted residual for the stopped step 2.

Reviewer: independent red-team, Slurm job 39922541, own clone of `fix/810-followup` @ `0014e7332` (merge-base with `origin/development` = `340e30f1e`; development tip now `c7377eca4`, 20 commits ahead of the branch). Base worktree at `origin/development`, pre-merge worktree at `9ffefeb76`, private `LSMS_DATA_DIR` (cold for every country at start, 198 MB after the suite). Package identity asserted for every run. GitNexus MCP did not connect; grep call-site sweeps substituted (declared).

## 1. Refutations

1. **"Comment-only change" (0014e7332) -- HELD.** `git diff 9ffefeb76..0014e7332 | grep '^[+-]' | grep -v '^[+-]\s*#'` → no non-comment lines (17+/3- in `local_tools.py`, all `#`).
2. **"0 of 37 `_table_cache_hash` values moved (Kosovo, Uganda)" -- HELD (0 of 36 here; I excluded `panel_ids`/`updated_ids`).** `scratch/hashprobe.py` run inside each tree with `PYTHONPATH=<tree>`: premerge→HEAD 0/36 moved; development→HEAD 36/36 moved (the base branch's corpus-wide re-warm, accepted by ruling, @ligon 2026-10-14; not paid here -- my private root re-warmed as a side effect of the suite, nothing outside it touched: `~/.cache/lsms_library/issues.log` mtime still 2026-07-13, `find lsms_library/countries -name '*.parquet'` = 0 before and after).
3. **The new comment's claims, checked against code:**
   - `PathMissingError` MRO = `DvcException → Exception`; `issubclass` vs `OSError/ValueError/KeyError/ImportError` all False -- HELD (ran it).
   - `get_data_file` helpers return `None` on `URLError/OSError/TimeoutError` with `urlopen(timeout=30)` at `data_access.py:1043/1067`, catches at `:1047/:1069/:1149`; `permissions()` memoised via `_cached_permissions` (`:945-992`) -- HELD.
   - **"`grab_data` and `df_data_grabber(missing_ok=)` catch that class" -- half REFUTED.** `grab_data` does (`country.py:1455, 1530`, inside its nested `get_data`). `df_data_grabber(missing_ok=True)` does NOT: `missing_ok` governs missing *columns* (`local_tools.py:1478-1495`); its `get_dataframe` call (`:1513`) is unwrapped. Measured with `DVCFS` and the WB fallback stubbed: `df_data_grabber("Nowhere/2000/Data/absent.dta", {"i":"hhid"}, missing_ok=True)` raises `FileNotFoundError`. This clause is in the paragraph the base commit wrote and the follow-up left untouched; the task was "make the comment true", so it is in scope. One-line comment fix (§6.1).
4. **Symptom reproduced on development, gone on branch -- HELD.** Unstubbed, tmp cwd with malformed `foo.dta.dvc`, `get_dataframe("Nowhere/2000/Data/absent.dta")`: development `dvc.utils.strictyaml.YAMLSyntaxError: unable to read: 'foo.dta.dvc'` 51.7 s cold / 0.0 s warm; branch `FileNotFoundError [Errno 2] source is not on disk, has no DVC sidecar under …/countries, and could not be fetched from the World Bank Microdata Library` 50.2 s cold / 0.0 s warm. The ~50 s is the correctly-rooted `DVCFS` index walk on both trees, as disclaimed.
5. **New tests meaningful -- HELD.** Copied `tests/test_gh810_no_cwd_rooted_dvc.py` into the development worktree: `4 failed, 1 passed in 7.29s` (three with `YAMLSyntaxError: unable to read: 'foo.dta.dvc'`, `never_imports_dvc_api_open` with its own AssertionError); `change_id_reads_a_local_crosswalk` is the one that passes on development (a happy path, intended). Branch: `5 passed in 6.32s`.
6. **Step-2 STOP: "`_sync_legacy_dvc_creds` removal reaches beyond the named sites" -- HELD, with one number refuted.** In-tree `.dvc/s3_creds` consumers: `tests/conftest.py:37-74` (`aws_creds_available` reads the in-tree path and gates `requires_s3`), `tests/test_s3_unlock.py:82` monkeypatches the symbol (default `raising=True`), `scripts/dvc_remote_reachability.py:58`, and `data_access.py:883,935` call it from `_auto_unlock_s3`/the writer path. **"47 test files" gated on `requires_s3`: measured 36** (`grep -rl requires_s3 tests | wc -l`; 15 for `aws_creds_available`). The conclusion stands at 36 as at 47. Note `DVCFS` never reads the in-tree file: `_build_dvcfs` overrides `credentialpath` to `config.s3_creds_path()` (`local_tools.py:180-201`), which resolves to `$LSMS_S3_CREDS` or `<config_dir>/s3_creds` with NO in-tree fallback (`config.py:120-135`). The sync only ever copies user-config creds in-tree, so the one case it changes (user-config creds present) is exactly the case where every reader already uses user-config; the removal is a test-harness/script coordination, not a reader-path risk.
7. **"The CLAUDE.md gotcha does not exist" -- HELD.** `grep -rn "chdir'd"` over `*.md/*.org/*.py/*.yml` excluding `slurm_logs/`: no hits; `git log -S"chdir'd"` (no path filter): only the four backlog commits that quote the 09-24 verdict/report. The sentence originates in issue #810's own body ("Related: the CLAUDE.md gotcha …") and was never in `CLAUDE.md`/`AGENTS.md` (symlink). `.claude/skills/add-wave/SKILL.md:223` ("Some older library functions … still use `dvc.api.open` internally -- that's fine") is now false, as the report says.
8. **"`test_no_editable_install` is red on `development` itself" -- REFUTED (stale).** On the branch and on the pre-merge commit: `1 failed, 3 passed`. On current `origin/development` (`c7377eca4`): `4 passed in 0.04s` -- fixed by `24cd065b0` ("ci: windows-smoke poetry install --no-root"), which post-dates the agent's merge. Candidate #5 in the report is moot. Trial merge of `origin/development` into HEAD (scratch worktree, `--no-commit`, then aborted): clean, no conflicts.
9. **Residual-draft rule.** #810 is fully resolved by this branch: both `dvc.api.open` sites are gone (`grep -rn 'dvc\.api' lsms_library/*.py` → comments + `from dvc.api import DVCFileSystem` only), the test the issue asks for exists, and the "CLAUDE.md gotcha" the issue names never existed -- nothing to draft on #810. The stopped step 2 is not an issue the branch partially resolves, but the report gives it only a one-line candidate rather than a drafted body; see §6.3.

## 2. Claimed vs measured

| claim | measured |
|---|---|
| follow-up commit is comment-only | held: 0 non-comment changed lines |
| 0/37 `_table_cache_hash` moved by the comment commit | 0/36 (panel_ids/updated_ids excluded) |
| base branch moves 81/81 (corpus-wide re-warm) | development→HEAD 36/36 on Kosovo+Uganda; scope claim right |
| before: `YAMLSyntaxError` on `foo.dta.dvc`; after: FNF 51.6 s cold / 0.0 warm | 51.7 s / 0.0 s (dev); 50.2 s / 0.0 s (branch) |
| new tests: 4 fail / 1 pass on unfixed code; 5/5 on branch | 4 failed, 1 passed in 7.29 s; 5 passed in 6.32 s |
| `PathMissingError` outside the old except tuple | held |
| `df_data_grabber(missing_ok=)` catches `FileNotFoundError` | **false** -- raises `FileNotFoundError` with `missing_ok=True` |
| 47 test files gated on `requires_s3` | **36** |
| `test_no_editable_install` red on `development` | **passes** on `origin/development` c7377eca4 (fixed by 24cd065b0); fails on the branch only because it is 20 commits behind |
| targeted: `1 failed, 76 passed in 16.19s` | not re-run as a block; the one failure is the item above |
| full suite: `1 failed, 5158 passed, 173 skipped, 3 xfailed, 1 xpassed` (-n 6) | `1 failed, 4411 passed, 206 skipped, 3 xfailed, 1 xpassed` (-n 40; same single failure) |
| `find lsms_library/countries -name '*.parquet'` = 0 | 0 before and after |

## 3. Regression

Full suite on the branch, verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest tests -n 40 --dist=loadfile -q`, cold private root, node load 5.5):

```
FAILED tests/test_no_editable_install.py::test_every_poetry_install_passes_no_root
1 failed, 4411 passed, 206 skipped, 3 xfailed, 1 xpassed, 830 warnings in 1005.64s (0:16:45)
```

The failure is `.github/workflows/ci.yml:96: run: poetry install` -- already fixed on `origin/development` (`24cd065b0`); it vanishes on re-merge. Neither order-dependent flake from the 09-24 review (Uganda `crop_condition` x8, GhanaLSS `[total]`) recurred. `bench/feature_audit/scan.py` not run: no feature touched (comment-only follow-up; base-branch builds were shown byte-identical in the 09-24 verdict).

## 4. Scope findings

- `git diff --stat 340e30f1e..HEAD` (merge-base; the two-dot form against today's development drags in 68 unrelated files that live on development): exactly `lsms_library/local_tools.py` (+49/-20) and `tests/test_gh810_no_cwd_rooted_dvc.py` (+117). No baseline/golden/blessed/lockfile/dependency edit, no weakened/skipped/xfailed assertion, no moved pin, no CONTENTS.org edit (framework-only; n/a is correct).
- Step 2 correctly STOPPED per the brief's own rule; nothing left half-done in the tree.
- `git status --short` empty in the clone; worktrees removed; nothing written outside the private root.

## 5. Residual risks and what a human must decide

1. **Re-merge before merging.** The branch is 20 commits behind `development`; the trial merge is clean, and the single suite failure disappears with it.
2. **`_sync_legacy_dvc_creds` still runs at import and writes into `countries_root()`** (`lsms_library/countries/.dvc/s3_creds`, mtime = first import). That is a runtime write into the config tree -- the same invariant as #803/#914/#831 ("`countries_root()` is READ-ONLY at runtime"); on a shared install it fails non-fatally (`__init__.py:325`). Its only remaining justification is the test-harness gate in `conftest.py`. Decision: file the coordinated removal (§6.3) and accept that `requires_s3` must then read `config.s3_creds_path()` instead.
3. **New behaviour (base branch, unchanged here):** every genuinely missing source now makes WB catalog calls; Nepal/Armenia pay it on every cold read. The comment now discloses it; nothing further to decide.
4. The corpus-wide re-warm is accepted by ruling and bundled with #868/#970; this element did not pay it.

## 6. Recommended follow-up commits

1. `lsms_library/local_tools.py` ~line 1404-1406: change "``grab_data`` and ``df_data_grabber(missing_ok=)`` catch that class" to "``grab_data`` catches that class (``country.py`` ``get_data``); ``df_data_grabber``'s ``missing_ok`` covers missing *columns* only and lets a missing file propagate". Comment-only; 0 hashes move.
2. `.claude/skills/add-wave/SKILL.md:223`: delete the sentence claiming older library functions still use `dvc.api.open`; none do.
3. File (do not fix here) **"Remove `_sync_legacy_dvc_creds`; it writes into `countries_root()` at import and serves no reader"** with this body: touch points `lsms_library/__init__.py:316-330` (import-time call), `lsms_library/data_access.py:801-917` (function) and `:883,:935` (callers in `_auto_unlock_s3`), `tests/conftest.py:37-74` (`aws_creds_available` → read `config.s3_creds_path()` with the in-tree path as fallback), `tests/test_s3_unlock.py:82` (drop the monkeypatch), `scripts/dvc_remote_reachability.py:58` (resolve creds the way `_build_dvcfs` does); `DVCFS` reads only the user-config path (`local_tools.py:180-201`, `config.py:120-135`, no in-tree fallback), so no reader depends on the in-tree copy whenever user-config creds exist, which is the only case in which the sync writes anything; CI writes the file by hand (`ci.yml:162,289`) so CI will not detect a regression -- a test that `requires_s3` is not skipped when only user-config creds exist is the guard to add.
4. Report-only corrections if the report is archived: "47 test files" → 36; candidate #5 (`ci.yml:96`) is already fixed on development by `24cd065b0`.
5. The three separate issues the 09-24 verdict named (data_root `lru_cache` poisoning in `test_gh866_*`, the GhanaLSS `[total]` order-dependent flake, the `../..` parse in `get_data_file`) remain unfiled candidates; the report lists them, as instructed.
