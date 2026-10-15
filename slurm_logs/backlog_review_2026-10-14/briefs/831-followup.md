# RED-TEAM review of branch `fix/831-followup` @ 858c41b47 (element `831-followup`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/831-followup` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of no country (framework change); `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/831-followup/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

## Mandate: REFUTE
Your job is to find the reason this branch must NOT be merged. Default to `verdict: fail` where you are uncertain. A fix agent wrote this branch and its report; the report is a self-report and its "SCOPE DEVIATIONS: none" is a claim, not a proof. Check, by running, not by reading:
1. **Does it fix the issue?** Reproduce the issue's symptom on `origin/development` code (a second clone: `git worktree add /local/job$SLURM_JOB_ID/base origin/development` is allowed -- that is your own scratch, not the shared mirror) and show it gone on the branch, in your private root, cold where the change is a build-path change (`.venv/bin/python -m lsms_library.cli cache clear --country X` against YOUR root; NEVER against the live one).
2. **Do the claimed numbers reproduce?** Every before/after figure in the report and commit messages: re-measure and tabulate claimed vs measured. A number you cannot reproduce is a finding.
3. **Data loss / laundering.** Row counts, index uniqueness, `grain_reports()`, `null_index_reports()`, `null_read_reports()` on every touched table before vs after; any `find lsms_library/countries -name '*.parquet'` hit; any write outside the private root.
4. **Healthy countries intact.** For a framework change: the full suite under the verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest tests -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > /local/job$SLURM_JOB_ID/pytest.log 2>&1`, then read the file), and `bench/feature_audit/scan.py --features <touched feature> --countries <2-3 warm ones>` where a feature is touched. For a country change: that country's tests plus `tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py`.
5. **Scope and stop-list.** `git diff --stat origin/development..HEAD` versus the task's named files; any baseline/golden/blessed/lockfile/dependency edit; any weakened, skipped or xfailed assertion (a moved pin is acceptable ONLY with before/after numbers in the commit message -- check they are there and that they match your measurement).
6. **Are the new tests meaningful?** Would each new test fail on `origin/development`? Run them there (the worktree) and say which do not.
7. **CONTENTS.org**: did the agent name the idiosyncrasy, and is what it wrote true? Anything it claims contradicts CONTENTS.org -- verify against the raw data.
8. **Cache cost claim**: is the stated re-warm scope right? (A `_/*.org`, `_/*.py`, `data_scheme.yml`, wave-module edit moves that country; `get_dataframe`/`_normalize_dataframe_index`-reachable framework edits move the corpus.)

Environment: `PYTHONPATH` is your clone; use `PYTHONSAFEPATH=1` for `-c`/`-m` runs only. Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`. `GH_REPO` is set; `gh` reads only, no writes. Read `lsms_library/countries/<C>/_/CONTENTS.org` for each country concerned.

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- You commit NOTHING and push NOTHING: this is a review. `git status --short` must be empty at the end (delete any scratch you made inside the clone).
- Write the report to `$STAGE/VERDICT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.


## Verdict format (write to `$STAGE/VERDICT.md` AND repeat as your final message)
Begin with one line: `VERDICT: pass | pass-with-fixes | fail | needs-human` and one sentence why. Then:
1. **Refutations** -- each a claim from the report/commit, what you ran, what you got (a claim that survives is listed as "held", with the command).
2. **Claimed vs measured** table.
3. **Regression** -- suite/scan summary lines verbatim.
4. **Scope findings**.
5. **Residual risks and what a human must decide** (the branch may be right and still need a decision).
6. **Recommended follow-up commits** (concrete, with paths), if `pass-with-fixes`.

## Rules the fix agent was under (2026-10-14): an open issue must describe exactly the remaining work -- if the branch resolves part of an issue, the report must DRAFT the residual; check that it did.

## The task the fix agent was given
## The task

Apply the reviewer's five follow-ups on `fix/831-cache-clear` (verdict: pass-with-fixes):
1. `lsms_library/cli.py` `_git_tracked_files`: return `None` when `ls-files` yields an empty set (a countries root under which git tracks nothing was not "consulted"). Add a test with a tmp `git init` repo whose countries root sits under an ignored `venv/` dir.
2. RULING (@ligon 2026-10-14): MOVE `lsms_library/countries/Panama/1997/Data/unittable.csv` under `lsms_library/countries/Panama/_/` (`git mv`), adjusting `.gitignore` (~line 106) and the comment at `Panama/_/units.py` (~line 108), so the existing `_/` rule covers it with no allowlist. Confirm nothing reads it by path (`rg unittable`).
3. Fix the false rationale in the `_git_tracked_files` docstring, the module comment, and the docstring of `test_git_tracked_and_underscore_dir_files_are_sources_not_strays` (the nine files are un-ignored tracked sources, not "six of nine match *.csv").
4. `_INTREE_SOURCE_SUFFIXES`: add `.dat .dct .ssp .tab .xls .xlsx` (every suffix with a `.dvc` sidecar that `get_dataframe`/`get_data_file` reads; `.pdf/.doc/.zip` stay excluded), with a `.DAT` case in `test_sidecarless_and_sidecar_beside_are_told_apart`.
5. One sentence in `docs/guide/caching.md` beside the #803 parquet note, and in CLAUDE.md's "`cache clear` now says so" sentence, that the command also names in-tree `.dta/.sav/.csv` (and the new suffixes).
The read-policy half (refuse/warn at read time for a sidecar-less in-tree file) stays OUT of scope; name it in the report.
**Verify:** the branch's cli tests + `tests/test_gh803_*` + full suite via the recipe; `lsms-library cache clear --dry-run` (against your PRIVATE root only) output pasted; hash-neutrality re-asserted (`Country._table_cache_hash(t, c.waves)` on two countries before/after the Panama move -- Panama's own hashes will move because `_/*.csv` is a hashed build input; state that).


## The branch: diffstat (vs development)
```
 .gitignore                                         |   2 +-
 AGENTS.md                                          |   2 +-
 docs/guide/caching.md                              |   8 +-
 lsms_library/cli.py                                | 154 ++++++++++++-
 lsms_library/countries/Panama/_/CONTENTS.org       |  13 ++
 lsms_library/countries/Panama/_/units.py           |   3 +-
 .../Panama/{1997/Data => _}/unittable.csv          |   0
 tests/test_gh831_cache_clear_intree_sources.py     | 239 +++++++++++++++++++++
 8 files changed, 416 insertions(+), 5 deletions(-)
```
## The branch: commit messages
--- 858c41b47 cli: `cache clear` in-tree audit follow-ups -- empty ls-files is not consulted, wider suffix set, Panama unittable.csv under _/ (GH #831)
Reviewer follow-ups on fix/831-cache-clear (verdict pass-with-fixes, §6):

1. `_git_tracked_files` returns None when `ls-files` yields an empty set.
   A package installed under an ignored `venv/` inside the user's own
   repo makes `git -C <root> ls-files -- .` succeed with empty output,
   which says nothing about the files there; it was reported as
   `git_consulted=True` and the hint line was suppressed.  New test
   `test_git_tracking_nothing_under_the_root_is_not_consulted` builds
   that repo with a tmp `git init` and drives the CLI through it.

2. RULING (@ligon 2026-10-14): `git mv Panama/1997/Data/unittable.csv
   Panama/_/unittable.csv` -- the one tracked data file outside a `_/`
   dir, so the existing position rule covers it with no allowlist.
   `.gitignore:106` negation and the `units.py` legacy-recipe comment
   follow it; nothing reads it by path (`rg unittable`: comment only).
   The no-git fallback now has zero false positives on the shipped
   tree (asserted in `test_the_real_tree_consults_git_and_excludes_its_tracked_csvs`).

3. The "six of nine match *.csv / check-ignore would call them strays"
   rationale (refuted by the review: git considers none of the nine
   ignored; all are un-ignored by negation rules) is replaced in the
   `_git_tracked_files` docstring, the module comment and the test
   docstring with the true one: ls-files because tracked-ness is the
   question.

4. `_INTREE_SOURCE_SUFFIXES` += .dat .dct .ssp .tab .xls .xlsx (every
   sidecar-bearing suffix on the get_dataframe/get_data_file read
   chain; .pdf/.doc/.zip/.sps/.txt/.wk1/.fmt/.xml stay out).  `.DAT`,
   `.DCT`, `.xlsx`, `.pdf` and `.zip` cases added to
   `test_sidecarless_and_sidecar_beside_are_told_apart`.

5. One sentence each in docs/guide/caching.md (beside the `cache clear`
   paragraph -- that file had no #803 parquet note to sit beside) and
   CLAUDE.md's "`cache clear` now says so" sentence.

Measured: full-tree sweep over 48 country dirs under the wider suffix
set, with git (11,695 tracked paths) and with tracked=None: 0 / 0 / 0
both ways, 0.17 s / 0.09 s.  `Country._table_cache_hash(t, c.waves)`
before/after: Panama 5 of 5 moved (`_/*.csv` and `_/units.py` are
hashed build inputs -- one Panama re-warm, not paid here); Uganda 28/28
and Malawi 26/26 byte-identical.  The read-policy half of #831 (what
`get_dataframe` serves for a sidecar-less in-tree file) stays out of
scope.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- a12690068 cli: `cache clear` names in-tree .dta/.sav/.csv, sidecar-less first (GH #831)
The mechanical half of #831.  #917 stopped the runtime writing raw files
into countries_root(), but a checkout can still hold them and the reader
is unchanged: a copy BESIDE its .dvc sidecar is refused by get_dataframe
(disk + one warning per read), a copy WITHOUT one is served in preference
to the DVC blob, silently.  `cache clear` already names in-tree parquets
(#803); it now names both source-file classes, told apart, and the
digit-named root-level dirs (`countries/2015-16/Data/...`) that the stray
extraction produced and that no --country can reach.

Not strays, and excluded: git-tracked files (nine .csv lookup tables live
in-tree; `git ls-files`, not `check-ignore`, because six of the nine match
the *.csv ignore and are tracked anyway) and anything under a `_/`
component (config by definition -- _BUILD_INPUT_SUFFIXES hashes its .csv
as build inputs).  The `_/` rule is what keeps a pip install, which has no
.git, from listing the shipped tables on every run; there the one stated
false positive is Panama/1997/Data/unittable.csv, and the report says git
was not consulted.

Read policy is NOT changed: what get_dataframe serves is the open
escalation on #831 (local_tools.py:1195-1214 untouched).

Cache cost: nil, measured.  cli is a leaf module nothing in the library
imports; build_transforms_fingerprint for food_acquired/household_roster/
sample and Country('Uganda')._table_cache_hash for the same three are
byte-identical with HEAD's cli.py and this one.  Pinned structurally by
test_no_cli_symbol_reaches_any_build_fingerprint.

Verification: tests/test_gh831_cache_clear_intree_sources.py (8 tests)
plus test_cli, test_gh803_intree_parquet_never_read,
test_gh831_no_runtime_writes_into_config_tree, test_build_transform_hash:
62 passed.  The real tree reports nothing (0 strays; the tracked csvs are
excluded through git).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


