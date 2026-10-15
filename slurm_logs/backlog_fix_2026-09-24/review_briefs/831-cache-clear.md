# RED-TEAM review of branch `fix/831-cache-clear` (element `831-cache-clear`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/831-cache-clear` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of no country (framework change) built from `development`; `dvc-cache` in it is a read-only symlink to the shared blob store. `development` is available as `origin/development` in your clone (`git diff --stat origin/development..HEAD`).

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

## The task the fix agent was given
## The task (from the triage's dispatch plan)
**What:** Extend `cache clear` (cli.py:228-240) to list in-tree .dta/.sav/.csv with and without sidecars. The read-policy half (refuse / warn-but-serve sidecar-less in-tree files, local_tools.py:1195-1214) waits on the escalation.

**Verify:** Unit test on a tmp countries_root with a sidecar-less .dta; hash-neutral (no cli/data_access symbol in a build fingerprint, per #917). Cost: nil.

**Blocked by (as triaged):** read-policy decision for the other half.

## Triage records for the issues in scope

### #831 -- tier B, human, dedup partial
- country: framework
- approach: Decision needed from @ligon: may a sidecar-less data file inside countries_root() be READ at all? Refusing it (as #803 does for parquets) closes the shadowing hole but breaks the add_wave workflow, where a maintainer develops wave config against files extracted before `dvc add`. A middle path: warn (InTreeDataWarning) but serve, and extend `cache clear` to list in-tree .dta/.sav/.csv with and without sidecars -- that half is mechanical and hash-neutral (no data_access/cli symbol is in a build fingerprint, per #917).
- verify cost: none
- CONTENTS.org: n/a
- evidence: lsms_library/data_access.py:84-120 (_fetch_destination); lsms_library/local_tools.py:1195-1214 (sidecar-less in-tree file accepted); lsms_library/cli.py:228-240 (parquet-only stray report); PR #917 body 'Not fixed here'; PR #922; merged_prs.json 917/922/918 closes lists; find lsms_library/countries -name '*.parquet' -> 0; ls -d lsms_library/countries/[0-9]* -> none; git ls-files shows Panama/1997/Data/unittable.csv tracked
- summary: The write side is fixed: PR #917 (6e4dc5b68, merged 2026-09-14) routes every read-path fetch through data_access._fetch_destination (data_access.py:84-120), which lands under data_root() unless populate_cache=True (the add_wave/populate_and_push acquisition branch), with a static guard and tests/test_gh831_no_runtime_writes_into_config_tree.py; PR #922 (575413801) added the sidecar fast path so get_data_file no longer walks the DVC index. At HEAD the clone has 0 stray countries/<wave>/ dirs and 0 in-tree parquets; the one in-tree data file (Panama/1997/Data/unittable.csv) is git-tracked, not a stray. Two of the issue's three bullets remain, and #917's body names them under 'Not fixed here': (a) get_dataframe still serves a sidecar-less file inside countries_root() -- local_tools.py:1195-1214 deliberately allows it ('new data being prepped for dvc add, user scratch data'); (b) `cache clear` reports only in-tree parquets (cli.py:228-240), not .dta/.sav. Severity has dropped because the runtime no longer creates the dangerous class, but the reader behaviour that let a stray shadow the DVC blob is unchanged.

Read the full issue and its comments first: `gh issue view 831 --comments`.


## The branch: diffstat (vs development)
```
 lsms_library/cli.py                            | 133 +++++++++++++++++-
 tests/test_gh831_cache_clear_intree_sources.py | 181 +++++++++++++++++++++++++
 2 files changed, 313 insertions(+), 1 deletion(-)
```
## The branch: commit messages
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


## The fix agent's report (a self-report -- refute it)
**SCOPE DEVIATIONS**
- `tests/test_gh831_cache_clear_intree_sources.py` (new). This is the unit test the task's Verify line asks for, not named by path in the brief.
- No stop-list matches. `lsms_library/cli.py` is the only other file touched. `local_tools.py:1195-1214` (read policy) is untouched.

**1. Commit and branch**
- `a12690068` on `fix/831-cache-clear`, pushed to `origin` (the mirror). Confirmed with `git ls-remote origin refs/heads/fix/831-cache-clear` matching HEAD.

**2. Verification**
- Package identity assert: `ok /local/job39198802/clone/lsms_library/__init__.py`.
- `pytest tests/test_gh831_cache_clear_intree_sources.py tests/test_cli.py tests/test_gh803_intree_parquet_never_read.py tests/test_gh831_no_runtime_writes_into_config_tree.py tests/test_build_transform_hash.py` (verbatim recipe, output to `/tmp/pytest_831_sha.log`, run on the committed SHA): `62 passed in 43.20s`.
- Hash neutrality, measured: `build_transforms_fingerprint` for `food_acquired`/`household_roster`/`sample` and `Country('Uganda')._table_cache_hash` for the same three tables are byte-identical with HEAD's `cli.py` and the new one (6 of 6 lines identical). Also pinned structurally by the new `test_no_cli_symbol_reaches_any_build_fingerprint`.
- Real tree on this checkout: `_intree_source_files` over Uganda/Panama/GhanaLSS/Malawi reports 0 sidecar-less, 0 sidecar-beside, 0 stray dirs, `git_consulted=True` (11,695 tracked paths under `countries/`). With `tracked=None` (the no-git fallback) it reports exactly 1 false positive, `Panama/1997/Data/unittable.csv`.
- Planted tree with one sidecar-less `.dta`, one sidecar-beside `.dta` and one `2015-16/Data/` dir: all three reported, in three distinct blocks, sidecar-less first.
- `lsms-library cache clear --country Panama --dry-run` against my private `LSMS_DATA_DIR`: `No cached files matched the provided filters.` and no #831 warning (tree is clean).
- `find lsms_library/countries -name '*.parquet'`: 0.

**3. Cache cost**
- Nil. `cli.py` is a leaf module nothing in the library imports, so no build fingerprint reaches it; the measurement above confirms 0 of 6 probed hashes moved. No table re-warms.

**4. Surprises**
- The assumption that a digit-named stray dir (or a country dir without `_/`, like Brazil) would crash `cache clear --all` at `Country(name)` is **false, measured**: both `Country('Brazil', preload_panel_ids=False).clear_cache(dry_run=True)` and `Country('2015-16', ...)` under a planted `LSMS_COUNTRIES_ROOT` return `[]` without raising. So `--all` already tolerates them; my root-level sweep is what makes them *visible*.
- Nine country dirs have no `_/` (Bosnia-Herzegovina, Brazil, Bulgaria, Harmonized_LSMS-ISA_Ag, KenyaLPS, Kyrgyz Republic, Nicaragua, Rwanda, Tanzania_Kegera), so "no `_/`" cannot identify a stray wave dir.
- Nine git-tracked `.csv` lookup tables live in-tree; six match the `*.csv` ignore pattern yet are tracked (`.gitignore` un-ignores only `fct_*.csv`/`unit_labels.csv` by name).
- CONTENTS.org: n/a, framework change, no country idiosyncrasy found or contradicted.
- Docs not updated: CLAUDE.md's "`cache clear` now says so" sentence is about parquets and stays true; that sentence and `docs/guide/caching.md` could be extended to mention the source-file report. Left for the reviewer since neither file was named.

**5. Decisions**
- **Tracked-file exclusion uses `git ls-files`, not `check-ignore`**, because six of the nine tracked csvs are ignore-pattern matches. Git is consulted via subprocess (precedent: `_dvc_cmd` in the same file).
- **Files under any `_/` path component are always excluded** as config by position (`_BUILD_INPUT_SUFFIXES` already treats `_/*.csv` as build inputs). This is what keeps a pip install, which has no `.git`, from listing the shipped tables on every run. The one stated false positive in the no-git case is `Panama/1997/Data/unittable.csv`, and the report prints a line saying git was not consulted.
- **Digit-prefixed root dir name is the stray-dir discriminator**, the same one the issue used (`ls -d countries/[0-9]*`). These are swept regardless of `--country`.
- **Sidecar test reuses `local_tools._is_polluted_workspace_copy`** so there is one definition of "has a sidecar" shared with the reader.
- **Report is silent on a clean tree** and prints the sidecar-less class before the sidecar-beside class, with wording that states current reader behaviour accurately ("SERVES these in preference to the DVC blob" vs "refused by get_dataframe").
- **Read policy deliberately unchanged**: what `get_dataframe` serves remains the open escalation.
