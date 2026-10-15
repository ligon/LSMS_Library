VERDICT: pass-with-fixes -- the branch does what the task asked, is hash-neutral and regression-free, but its tracked-file exclusion misreports a pip install inside any user git repo, its stated `check-ignore` rationale is false, and its suffix set misses five source types that carry `.dvc` sidecars.

Reviewer: red-team element `831-cache-clear`, job 39199808, clone of `fix/831-cache-clear` at `a12690068` (one commit on merge-base `242573697`; the 24-file `git diff --stat origin/development..HEAD` is development having moved ahead by five handoff/backlog commits, none touching `cli.py`). Package identity asserted for every run (`lsms_library.__file__` under the clone, or under the base worktree for base runs). Private `LSMS_DATA_DIR` only; nothing pushed, nothing committed.

## 1. Refutations

**Held -- the symptom reproduces on development and is gone on the branch.** Planted a countries root (`Uganda/_` copied from the real tree, sidecar-less `Uganda/2019-20/Data/AGSEC5A.dta`, sidecar-beside `Uganda/2013-14/Data/GSEC1.dta`, root-level `2015-16/Data/HH_SEC_H.dta`, an empty `Brazil/`), then `LSMS_COUNTRIES_ROOT=<planted> python -m lsms_library.cli cache clear --dry-run` with the REAL `Country` (no stubs), for both `--country Uganda` and `--all`, on a base worktree of `origin/development` and on the branch. Base: `No cached files matched the provided filters.` and nothing else, both invocations. Branch: three blocks (1 stray dir, 2 sidecar-less, 1 sidecar-beside), sidecar-less before sidecar-beside, both invocations, exit 0. The `--all` run constructs `Country('2015-16')` and `Country('Brazil')` before the report and neither raises -- the agent's "surprise" claim holds. The non-dry-run `cache clear --country Uganda` prints the same report and deletes nothing in the planted tree (all four planted files still present afterwards).

**Held -- hash-neutral.** `build_transforms_fingerprint` for `food_acquired`/`household_roster`/`sample` and `Country('Uganda')._table_cache_hash(t, c.waves)` for the same three, measured on the base worktree and the branch: 6 of 6 lines byte-identical (`diff` empty). `rg` confirms `cli` is a leaf: the only non-test reference is a string in `util/run_stage.py:78` (a subprocess argv), not an import.

**Held -- "62 passed".** The agent's five-file selection under the verbatim recipe (`-n 8 --dist=loadfile`, BLAS pinned, `LSMS_BUILD_WORKERS=1`): `62 passed in 35.92s`.

**Held -- real tree clean, cost nil.** On this checkout `_intree_source_files` over all 40 country dirs: 0 / 0 / 0, `git_consulted=True`, 11,695 tracked paths (claimed 11,695); `git ls-files` 0.01 s, sweep 0.07 s; `cache clear --all --dry-run` end-to-end 6.0 s wall against my private root, one line of output, no `GH #8` line. `git status --short` empty afterwards, `find lsms_library/countries -name '*.parquet'` = 0, `find lsms_library/countries -newer <marker>` empty, nothing newer than the marker under the private `LSMS_DATA_DIR` outside `dvc-cache/`. With `tracked=None` exactly one false positive, `Panama/1997/Data/unittable.csv` (claimed 1).

**Held -- "nine tracked .csv", "nine country dirs without `_/`".** `git ls-files lsms_library/countries | grep -iE '\.csv$'` = 9 (8 under `_/`, 1 not); dirs without `_/` = the nine named (Bosnia-Herzegovina, Brazil, Bulgaria, Harmonized_LSMS-ISA_Ag, KenyaLPS, Kyrgyz Republic, Nicaragua, Rwanda, Tanzania_Kegera).

**REFUTED -- "six of the nine match the `*.csv` ignore and are tracked anyway, so check-ignore would call them strays" (commit message, `cli.py` docstring of `_git_tracked_files`, and the test docstring of `test_git_tracked_and_underscore_dir_files_are_sources_not_strays`).** Ran `git ls-files lsms_library/countries | grep -iE '\.csv$' | git check-ignore --no-index --stdin`: prints nothing, exit 1 -- git considers NONE of the nine ignored. With `-v`, every one of the nine resolves to a NEGATION rule at `.gitignore:99-106` (`!lsms_library/countries/*/_/fct_*.csv`, `!.../*/_/unit_labels.csv`, `!.../*/_/central_american_fct*.csv`, `!.../*/*/_/food_label.csv`, `!.../*/*/_/ihs3_conversions.csv`, and **`!lsms_library/countries/Panama/1997/Data/unittable.csv` by name**). The commit's further claim that `.gitignore` "un-ignores only `fct_*.csv`/`unit_labels.csv` by name" is likewise false. The CHOICE of `ls-files` is fine (it asks the right question directly); the recorded reason for it is wrong in three places and should not be left in the tree, because the next reader will trust it. It also means the one non-`_/` tracked file is already named, by path, in `.gitignore:106`, which makes the fix below a one-liner.

**REFUTED (new finding, not in the report) -- `git_consulted=True` is reported for a countries root that git does not track.** `git -C <root> ls-files -- .` succeeds with EMPTY output when `<root>` lies inside some other git repository (a pip install under `.venv/` inside a user's project repo -- the common deployment). Reproduced with a tmp `git init` repo, `.gitignore` = `venv/`, countries root at `venv/lsms_library/countries/` holding `Panama/1997/Data/unittable.csv`: `_git_tracked_files` -> `set()` (not `None`), `git_consulted` -> `True`, and the report prints `unittable.csv` under "NO .dvc sidecar ... get_dataframe SERVES these ... delete each unless it is new data awaiting dvc add" WITH the "git could not be consulted" hint suppressed. So on every pip install -- in a foreign repo or outside any repo -- the command tells the user to delete a shipped, git-tracked lookup table, and in the foreign-repo case says so with full confidence. Harm today is bounded (the file's only reference is a comment in `Panama/_/units.py:108`), but the report's own contract is "sidecar-less = dangerous, delete", so a false positive in that class is the worst kind it can produce. Fix: treat an empty tracked set as not-consulted, and exclude `unittable.csv` (see §6).

**Partially held -- "the real tree reports nothing (0 strays)".** True for `.dta/.sav/.csv`, but the sidecar census of this checkout is 6,813 `.dta`, 535 `.dat`, 531 `.dct`, 413 `.csv`, 134 `.sav`, 38 `.tab`, 36 `.ssp`, 218 `.xlsx`, 29 `.xls`. `get_dataframe` reads `.DAT` (GhanaLSS) and `.SSP` (Peru) through the same local -> DVC -> WB chain, so a sidecar-less in-tree `.DAT` shadows the blob exactly as a `.dta` does and the report says "clean". Planted five sidecar-less `Y12A.DAT`, `Y12A.DCT`, `N00A.SSP`, `x.tab`, `x.xlsx`: reported 0 / 0. Not a scope violation -- the task named three suffixes -- but "0 strays" is a weaker statement than it reads.

**Not re-measured, by decision:** the wording "get_dataframe SERVES these in preference to the DVC blob" is reader behaviour the branch does not touch (`local_tools.py:1195-1214` unchanged, confirmed by the diff); the mechanism is visible in `local_file()` (any readable path without a sidecar returns True). The read-policy escalation on #831 stays open, as the commit says.

## 2. Claimed vs measured

| claim (report / commit) | measured | status |
|---|---|---|
| 5-file selection: 62 passed | 62 passed in 35.92 s | held |
| 6/6 fingerprint + cache-hash lines identical | 6/6 identical, base worktree vs branch | held |
| real tree: 0 sidecar-less, 0 sidecar-beside, 0 stray dirs, `git_consulted=True`, 11,695 tracked paths | 0 / 0 / 0 / True / 11,695 | held |
| `tracked=None` -> exactly 1 false positive (`Panama/1997/Data/unittable.csv`) | 1, that file | held |
| planted tree: three blocks, sidecar-less first | three blocks, that order, `--country` and `--all`, real `Country` | held |
| `Country('Brazil')`/`Country('2015-16')` `.clear_cache(dry_run=True)` do not raise | `--all` on the planted root with both present: exit 0 | held |
| nine tracked csvs; nine dirs without `_/` | 9; 9 | held |
| "six of the nine match the `*.csv` ignore" / check-ignore would call them strays | `check-ignore` ignores 0 of 9; all 9 hit negation rules at `.gitignore:99-106` | **refuted** |
| `.gitignore` un-ignores only `fct_*.csv`/`unit_labels.csv` by name | eight negation patterns incl. `Panama/1997/Data/unittable.csv` by full path | **refuted** |
| `find lsms_library/countries -name '*.parquet'`: 0 | 0 (before and after `--all --dry-run`) | held |
| cache cost nil | no table re-warm; 0 of 6 hashes moved | held |
| (implicit) `git_consulted` means git answered for THIS tree | `True` with `tracked=set()` inside a foreign repo | **refuted** |

## 3. Regression

- Full suite, verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 pytest tests -n 8 --dist=loadfile -q`, private `LSMS_DATA_DIR`, log `/local/job39199808/pytest.log`):
  `4237 passed, 203 skipped, 3 xfailed, 1 xpassed, 755 warnings, 10 errors in 1015.24s (0:16:55)` -- **0 failed**. The 10 errors are all `FileNotFoundError: .../tmp/pytest-of-ligon/pytest-2/popen-gwN` at SETUP in three files (`test_build_subprocess_env.py` x2, `test_s3_unlock.py` x2, `test_canonical_shape_via_cache_miss.py` x6): the reviewer's own scratch cleanup (`rm -rf /local/job39199808/tmp*/`) deleted pytest's basetemp mid-run. Self-inflicted, not the branch. Re-ran exactly those three files under the same recipe afterwards: `23 passed, 60 warnings in 648.31s (0:10:48)`, 0 errors. `grep -cE 'OpenBLAS blas_thread_init|node down|INTERNALERROR|Killed'` over both finished logs: 0 and 0. Collected here: 4,237 + 203 + 3 + 1 + 10 = 4,454 tests, against the 5,666 CLAUDE.md records for the 2026-09-11 run; the full suite was not run on the base worktree so the difference is not attributed, but a leaf-module edit plus one new 8-test file cannot change what other files collect, so the gap belongs to development, not to this branch.
- Five-file selection (branch): `62 passed in 35.92s`.
- New test file on `origin/development` worktree: `7 failed, 1 passed in 5.79s` -- the seven fail on the missing `cli._intree_source_files` (AttributeError), so they are real regression tests; `test_no_cli_symbol_reaches_any_build_fingerprint` PASSES on development, which is expected of a structural pin and is not a defect, but note it does not test the branch.
- No feature touched, so no `bench/feature_audit/scan.py` run; no table, row count, index or `grain_reports()` can change (no build-path symbol edited, hash-neutral as measured).

## 4. Scope findings

- Diff from merge-base: `lsms_library/cli.py` (+132/-1) and `tests/test_gh831_cache_clear_intree_sources.py` (+181). No baseline, golden, blessed, lockfile or dependency edit; no assertion weakened, skipped or xfailed; `local_tools.py` untouched.
- The new test is what the task's Verify line asked for; not a deviation.
- Docs not updated (`docs/guide/caching.md`, CLAUDE.md's "`cache clear` now says so"): acceptable, but the commit's false `check-ignore` claim IS in tree, in a docstring and a test docstring -- that is the scope item to fix.
- CONTENTS.org: n/a (framework change); nothing claimed, nothing to verify.

## 5. Residual risks and what a human must decide

1. **The read policy is still the real hole.** This branch is a report; a sidecar-less in-tree file is still served silently by `get_dataframe`. The report is only seen by someone who runs `cache clear`. Whether to refuse/warn at read time (as #803 does for parquets) is the escalation the triage named and it is unchanged.
2. **A shipped file is listed for deletion on every pip install** (both no-git and foreign-git cases) until §6 items 1-2 land. Bounded today because `unittable.csv` is referenced only in a comment, but the message says "delete each".
3. **Suffix coverage is narrower than the sidecar census** (`.dat/.dct/.ssp/.tab/.xls/.xlsx`). A human should decide whether `_INTREE_SOURCE_SUFFIXES` is "what the task said" or "every suffix with a sidecar in the corpus"; the second is the one that matches the hazard.
4. `_report_intree_source_files` runs on every `cache clear`, including non-dry-run, and is cheap (0.07 s here; Lustre `rglob` over ~40 dirs). No decision needed, noted for the record.

## 6. Recommended follow-up commits

1. `lsms_library/cli.py` `_git_tracked_files`: return `None` when `ls-files` returns an empty set (a countries root that git tracks nothing under is not "consulted"; on this repo the set is never empty). Add a test with a tmp `git init` repo whose countries root is under an ignored `venv/` dir, asserting `git_consulted is False` and the hint line prints.
2. Move `Panama/1997/Data/unittable.csv` under `Panama/_/` (adjust `.gitignore:106` and the comment at `Panama/_/units.py:108`), so the existing `_/` rule covers it on every install with no list to maintain -- the repo's stated stance is against allowlists. Fallback only if the move is refused: exclude it by name in `lsms_library/cli.py` (a one-entry frozenset checked against `rel.as_posix()`; the path is already un-ignored by full path at `.gitignore:106`). Then extend `test_git_tracked_and_underscore_dir_files_are_sources_not_strays` so the `tracked=None` branch expects `[stray]` only.
3. `lsms_library/cli.py` docstring of `_git_tracked_files`, the module comment, and the docstring of `test_git_tracked_and_underscore_dir_files_are_sources_not_strays`: replace the "six of nine match `*.csv`" / "check-ignore would call them strays" rationale with the true one (all nine are un-ignored by negation rules at `.gitignore:99-106`; `ls-files` is used because tracked-ness is the question, not ignore status).
4. `lsms_library/cli.py` `_INTREE_SOURCE_SUFFIXES`: add `.dat .dct .ssp .tab .xls .xlsx` (every suffix with a `.dvc` sidecar in the tree that `get_dataframe`/`get_data_file` reads), with a `.DAT` case in `test_sidecarless_and_sidecar_beside_are_told_apart`. Excluding `.pdf/.doc/.zip` is fine -- they are not on the tabular read chain.
5. One sentence in `docs/guide/caching.md` beside the #803 parquet note, and in CLAUDE.md's "`cache clear` now says so" sentence, that the command also names in-tree `.dta/.sav/.csv`.
