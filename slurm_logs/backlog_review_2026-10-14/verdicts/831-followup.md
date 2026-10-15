VERDICT: pass-with-fixes -- every code and number claim in the report reproduces (hash cost Panama 5/5, Uganda 0/28, Malawi 0/26 against HEAD~1; three of the nine tests discriminate against the parent; the CLI reports what it says it does with no in-tree writes), but the 2026-10-14 rule that a partially resolved issue must come with a DRAFTED residual was not met: the report only *names* the read-policy half, #831 has no comment and no PR exists for either branch.

Reviewed: `fix/831-followup` @ 858c41b47. NOTE the branch carries BOTH #831 commits (a12690068 `cache clear names in-tree .dta/.sav/.csv` and 858c41b47 the follow-ups); neither is on `origin/development`, so merging brings the combined diff. The three-dot diff vs `origin/development` is exactly the task's 8 files; `git merge-tree --write-tree origin/development HEAD` succeeds (no conflicts) although development has advanced past the merge-base 340e30f1e with unrelated Uganda edits.

## 1. Refutations (claim -> what I ran -> result)

- **"Does it fix the issue"** -- planted `Uganda/2019-20/Data/ZZ_STRAY.dta` (no sidecar), `ZZ_BESIDE.dta` (+ `.dvc`) and `GhanaLSS/1998-99/Data/ZZ_STRAY.DAT` in each checkout's real tree, ran `python -m lsms_library.cli cache clear --country Uganda --country GhanaLSS --dry-run` with `LSMS_COUNTRIES_ROOT` at that tree and my private `LSMS_DATA_DIR`. `origin/development`: prints nothing about them (cli.py has no `_intree_source_files`). Parent a12690068: names `.dta` stray and the beside copy, MISSES `.DAT`. Branch: names all three, sidecar-less block first. **Held.** Planted files removed; all three trees `git status --short` empty.
- **Follow-up 1, empty `ls-files`** -- tmp `git init` repo, `.gitignore`=`venv/`, countries root under `venv/`, one `Panama/1997/Data/lookup.csv`. Parent: `_git_tracked_files -> set()`, `git_consulted=True` (the false confidence). Branch: `None`, `git_consulted=False`, file still listed, hint line printed. **Held.**
- **Follow-up 2, Panama move** -- `git ls-files` shows `Panama/_/unittable.csv`; `git check-ignore -v` on the new path exits 1 (not ignored), old path would be ignored by `*.csv`. `rg unittable` over the tree: `.gitignore:106`, two cli.py docstrings, the `units.py` comment, CONTENTS.org and tests -- nothing reads it by path. **Held.** The `units.py` comment `get_dataframe('unittable.csv')` is cwd-relative (`local_tools.py:1229`), true when run from `_/` as the legacy recipe did. Held.
- **Follow-up 3, rationale** -- the nine tracked data files under `countries/` are all `.csv`, all under a `_/` dir, each un-ignored by a negation rule (verified `git ls-files | grep csv`). New docstrings say exactly that. **Held.** The report's own correction (the "module comment" never carried the "six of nine" claim) is accurate.
- **Follow-up 4, suffixes** -- `_INTREE_SOURCE_SUFFIXES` is the brief's list exactly; `.DAT`/`.DCT`/`.xlsx`/`.pdf`/`.zip` cases are in the test; `.DAT` end-to-end shown above. **Held.**
- **Follow-up 5, docs** -- `grep 803 docs/guide/caching.md` on `origin/development`: no hit, so the report's "no #803 note to sit beside" is true; sentence added in the `cache clear` paragraph. `CLAUDE.md -> AGENTS.md` symlink, edit landed in AGENTS.md. **Held.**
- **"refused by get_dataframe" wording** -- `local_tools.py:1195-1214`: a copy beside its sidecar warns and returns `local_file=False`, falling through to the DVC cache; a sidecar-less copy is served. The CLI/docs text matches the code. **Held.**
- **Hash neutrality** -- `Country._table_cache_hash(t, c.waves)` over every `data_scheme` table, run in-checkout with `PYTHONPATH`+`LSMS_COUNTRIES_ROOT` pinned and `lsms_library.__file__` asserted, vs HEAD~1 (438cb8060, the merge commit = the follow-up's true baseline): Panama 5/5 moved (sample, food_acquired + 3 derived), Uganda 0/28, Malawi 0/26. **Held.** Caveat for the merger: against the CURRENT `origin/development` tip Uganda is 28/28 moved and against the parent a12690068 Malawi is 26/26 moved -- both from unrelated development commits (Uganda `uganda.py`/`data_scheme.yml`/`crop_production.py` edits after the merge-base; Malawi edits between a12690068 and the merge), not from this branch.
- **Full-tree sweep** -- 48 country dirs (49 `iterdir` entries incl. `.dvc`): git `0/0/0 consulted=True 0.25 s`, `tracked=None` `0/0/0 consulted=False 0.09 s`, 11,695 tracked paths. **Held.**
- **Cache cost nil elsewhere** -- `test_no_cli_symbol_reaches_any_build_fingerprint` passes; the hash probe above is the empirical twin. Held.

## 2. Claimed vs measured

| claim | measured | |
|---|---|---|
| Panama 5/5 hashes move; Uganda 28/28, Malawi 26/26 identical | 5/5; 0/28; 0/26 vs HEAD~1 | held |
| 11,695 tracked paths under countries/ | 11,695 | held |
| sweep 48 dirs: 0/0/0 both ways, 0.17 s / 0.09 s | 0/0/0 both ways, 0.25 s / 0.09 s | held |
| `rg unittable` -> comment only (code) | comment + docstrings + CONTENTS + tests; no reader | held |
| `cache clear --country Panama --dry-run`: 8 parquet paths, no GH #831 line | 4 paths in my root (only `sample` was built here), no #831/WARNING line, rc 0 | held |
| `--all --dry-run`: one line on cold root | warm root: 662 lines, 34 `would remove` blocks, 0 WARNING/GH #8 lines, rc 0 | held |
| full suite: 1 failed (ci.yml `--no-root`), 5285 passed, 164 skipped | 1 failed (same test), 4416 passed, 205 skipped, 3 xfailed, 1 xpassed, 17:39 on cold root; `test_table_structure.py` rerun warm: 1121 passed, 4 skipped | held (the count gap is cold-root collection: that file parametrizes over existing parquets) |
| new tests fail on the pre-fix code | 3 of 9 fail on parent a12690068 (`told_apart`, `tracking_nothing`, `real_tree`); 6 pass there (they date from a12690068 or, for `sources_not_strays`, test behaviour the parent already had); on `origin/development` 8 of 9 fail (`cli` has no `_intree_source_files`) and `test_no_cli_symbol_reaches_any_build_fingerprint` passes (it only inspects `_build_registry` closures) | held, with the non-discriminating six noted |
| Panama `find lsms_library/countries -name '*.parquet'` = 0 | 0 in all three trees | held |

## 3. Regression

- Full suite, branch, recipe verbatim (`-n 40 --dist=loadfile`, BLAS pinned, `LSMS_BUILD_WORKERS=1`), cold private root:
  `1 failed, 4416 passed, 205 skipped, 3 xfailed, 1 xpassed, 812 warnings in 1059.86s (0:17:39)`
  `FAILED tests/test_no_editable_install.py::test_every_poetry_install_passes_no_root`
  Pre-existing: the branch's `.github/workflows/ci.yml:96` is `poetry install` (from mirror merge 796386fa7, not touched here); `origin/development` tip already has `--no-root` on all five lines, so the failure disappears on merge.
- `tests/test_table_structure.py tests/test_no_editable_install.py` warm: `1 failed, 1121 passed, 4 skipped in 111.69s` (same lone failure).
- Targeted set (`test_gh831_cache_clear_intree_sources.py test_cli.py test_gh803_intree_parquet_never_read.py test_gh831_no_runtime_writes_into_config_tree.py test_build_transform_hash.py`): `63 passed in 86.10s`.
- Data loss / laundering, Panama (the one country whose caches move): all five `data_scheme` tables built cold on the branch and, with `LSMS_COUNTRIES_ROOT` at HEAD~1 and a separate private sub-root (`$LSMS_DATA_DIR/mb_probe`, deleted afterwards), from HEAD~1. Byte-identical dicts (rows, index uniqueness, columns, waves, every numeric column sum): sample 18,353; food_acquired 571,354; food_expenditures 501,480; food_prices 483,661; food_quantities 570,803; all indexes unique; `grain_reports()` 0, `null_index_reports()` 1, `null_read_reports()` 2 on both sides. The only build-relevant change is a file nothing reads. No other table build path is touched (cli is a leaf; hash probe confirms). In-tree writes after my CLI runs and the suite: only three `__pycache__/*.pyc` (gitignored, from wave scripts the suite ran); no parquet, no raw file; nothing written outside the private root.
- Reviewer's note on method: my own first suite launch overlapped a targeted run that was writing parquets, which made `test_table_structure` collection differ across xdist workers ("Different tests were collected") in two later launches; those launches were discarded, the figures above are from the uncontended run.

## 4. Scope findings

- Three-dot diff vs `origin/development` = exactly the 8 named files (+ the `git mv`). No baseline/golden/blessed/lockfile/dependency edit, no skipped/xfailed/weakened assertion. `AGENTS.md` instead of `CLAUDE.md` is the symlink target, declared in the report.
- `Panama/_/CONTENTS.org` entry: content verified true (position of the file, the no-git false positive, the one-country re-warm).
- `.gitignore:106` still needs a by-name negation for `Panama/_/unittable.csv` (no generic `_/*.csv` negation exists); the report states this.

## 5. Residual risks / what a human must decide

1. **The residual was not drafted.** Rule of 2026-10-14: a branch that resolves part of an issue must DRAFT the remaining work. The report §5 names the read-policy half (`local_tools.py` serves a sidecar-less in-tree file) but drafts nothing; `gh issue view 831` has 0 comments, and `gh pr list --head fix/831-followup` / `--head fix/831-cache-clear` are both empty. Someone must write the residual (see §6) before or at merge.
2. `test_the_real_tree_consults_git_and_excludes_its_tracked_csvs` skips entirely when `tracked is None` -- i.e. on the pip install where the `_/` position rule is the only guard; its second half (`tracked=None` sweep) needs no git and could run unconditionally. Coverage gap, not a defect.
3. `_git_tracked_files` docstring hardcodes "11,695 tracked paths"; will drift. Cosmetic.
4. The branch pins its own `ci.yml:96` without `--no-root`; the tip is already fixed, so merge order resolves it. If the branch is rebased rather than merged, re-check.
5. The `--all --dry-run` sweep walks each country dir with `rglob`; measured 0.25 s here, but on a Lustre checkout with 433 stray files (the 2026-09-08 state) it is one stat per candidate -- acceptable, just not free.

## 6. Recommended follow-up commits

1. Residual issue text (post as a comment on #831 or a new issue, and reference from the PR body): "Read policy for a sidecar-less file inside `countries_root()`: `local_tools.get_dataframe.local_file` (local_tools.py:1195-1214) serves it in preference to the DVC blob by design ('new data being prepped for dvc add'). Decide: (a) warn once per read naming the path, (b) refuse unless `LSMS_ALLOW_INTREE_SOURCE=1`, or (c) keep as-is and rely on `cache clear`'s report. #917 fixed the write side; #831's `cache clear` report (this branch) is the mechanical half; nothing changes what is served."
2. `tests/test_gh831_cache_clear_intree_sources.py::test_the_real_tree_consults_git_and_excludes_its_tracked_csvs`: move the `tracked=None` assertions above the `pytest.skip` so the position rule is pinned on a no-git install too.
3. `lsms_library/cli.py` `_git_tracked_files` docstring: drop the hardcoded "11,695" (or say "thousands").
