# RED-TEAM review of branch `fix/810-followup` @ 0014e7332 (element `810-followup`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/810-followup` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of no country (framework change); `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/810-followup/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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

Apply the reviewer's follow-ups on `fix/810-dvc-open` (verdict: pass-with-fixes):
1. `lsms_library/local_tools.py` (~line 1405, the else-branch comment): replace "Genuine WB fetch failures (network, auth) propagate from `_try_get_data_file` as before" with the TRUE statement: the WB route was previously unreachable from this branch (the old `dvc.api.open` call raised `PathMissingError` outside it); online, a missing source now costs a few seconds per source; offline it is bounded by the urlopen timeout.
2. Remove `_sync_legacy_dvc_creds` (`lsms_library/__init__.py` ~316-330 and `data_access.py` ~788-917) now that no `dvc.api.open` caller remains in the package, and retire the CLAUDE.md gotcha about `get_dataframe('../Data/..')` from a chdir'd cwd. If removal touches anything beyond those sites, STOP and report instead.
3. Do NOT fix the three separate issues the reviewer named (data_root lru_cache poisoning, the order-dependent GhanaLSS flake, the `../..` path parse) -- list them in your report as candidates to file.
RULING (@ligon 2026-10-14): the corpus-wide re-warm this branch costs is ACCEPTED and will be paid once, bundled with #868 and #970; say so in your report and do not pay it here.
**Verify:** `tests/test_gh810_*` (or whatever the branch added) plus `tests/test_build_subprocess_env.py tests/test_no_editable_install.py tests/test_windows_portability.py`, then the full suite with the recipe above (write to a file; paste the summary line).


## The branch: diffstat (vs development)
```
 lsms_library/local_tools.py           |  69 ++++++++++++++------
 tests/test_gh810_no_cwd_rooted_dvc.py | 117 ++++++++++++++++++++++++++++++++++
 2 files changed, 166 insertions(+), 20 deletions(-)
```
## The branch: commit messages
--- 0014e7332 gh810 follow-up: the else-branch comment states what is NEW about the WB route, not "as before"
Red-team verdict (slurm_logs/backlog_fix_2026-09-24/verdicts/810-dvc-open.md
§1.3, §6.1): the comment claimed "Genuine WB fetch failures (network, auth)
propagate from _try_get_data_file as before".  Both halves were false.

1. The WB route was previously UNREACHABLE from this branch.  The old
   `dvc.api.open(fn, mode='rb')` raised `dvc.exceptions.PathMissingError`,
   a `DvcException` outside the caught `(OSError, ValueError, KeyError,
   ImportError)` tuple (verified: issubclass against all three is False),
   so `_try_get_data_file` never ran for a plain missing source.
2. `get_data_file` swallows its own network/HTTP/timeout errors to None
   (`_get_catalog_idno`, `_find_stata_zip_url`, `_download_and_extract`),
   so a WB fetch failure surfaces as this `FileNotFoundError`, never as a
   propagated network error.

The comment now says so, and states the cost the verdict measured: online a
few seconds of WB catalog calls per missing source; offline bounded by the
`urlopen(timeout=30)` calls plus one memoised key validation per process;
Nepal/Armenia pay it on every cold read.

Comment-only change.  Cache cost measured: 0 of 37 `Country._table_cache_hash`
values (Kosovo, Uganda) moved; the base branch's own 81/81 re-warm is
accepted by ruling (@ligon 2026-10-14), to be paid once with #868 and #970.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- bebe68a51 gh810: get_dataframe's last-resort branch goes straight to the WB fallback, never through a cwd-rooted dvc.api.open
The else-branch of `get_dataframe` -- reached only when `file_system_path`
found no sidecar at cwd or `countries_root()` AND `DVCFS.open` (rooted at
`countries_root()`) raised FileNotFoundError -- opened with
`dvc.api.open(fn, mode='rb')` and no `repo=`.  DVC then resolved the
repository from the CURRENT WORKING DIRECTORY and walked every `*.dvc`
beneath it: 300-450 s per missing source from `/tmp`, ending in a
`YAMLValidationError` on an unrelated malformed pytest fixture
(Armenia/household_roster, 457 s), and a `BrokenProcessPool` when the
walk ran inside a forked build worker (Nepal).  Both branches that reach
it are for a source that is genuinely absent from the DVC tree (the one
directory-level sidecar in the corpus, `EthiopiaRHS/_dataverse_archive.dvc`,
is read by no script or YAML), so the branch now calls
`_try_get_data_file(fn)` directly and raises `FileNotFoundError(ENOENT,
<why>, fn)` when that returns None -- the class `grab_data` and
`df_data_grabber(missing_ok=)` already catch.  WB fetch failures still
propagate.  `import dvc.api` is dropped; only `DVCFileSystem` remains.

Measured, unstubbed, from a tmp cwd holding a malformed `foo.dta.dvc`:

  before: dvc.utils.strictyaml.YAMLSyntaxError: unable to read:
          'foo.dta.dvc', YAML file structure is corrupted
  after:  FileNotFoundError ... 51.6 s cold, 0.0 s warm

The 51.6 s is `file_system_path`'s own correctly-rooted `DVCFS.open` index
walk over 9,986 sidecars (~93 s on Lustre), paid before and after this
change for a genuinely absent source; it is not the cwd walk and is not
fixed here.  With `DVCFS` stubbed (tests/test_gh810_no_cwd_rooted_dvc.py)
the branch under test fails in well under a second: 5 passed.

Cache cost, measured: 3 of 3 build_transforms_fingerprint values and
81 of 81 Country._table_cache_hash values (Uganda, GhanaLSS, Ethiopia,
Kosovo) move -- `df_data_grabber` is @build_transform-tagged and closes
over `get_dataframe`, so ANY edit to it is a corpus-wide re-warm.  Not
paid here; land beside the GH #959 re-warm decision so the corpus
re-warms once.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 4d4d16d64 gh810: change_id reads its crosswalk through get_dataframe, not a cwd-rooted dvc.api.open
`local_tools.change_id` fell back from `open(fn)` to `dvc.api.open(fn,
mode='rb')` with no `repo=`, so DVC resolved the repository from the
current working directory and walked every `*.dvc` beneath it (GH #810).
It now calls `get_dataframe(fn)` -- local -> DVC blob cache rooted at
`countries_root()` -> WB fallback -- which is exactly what the three
country-level copies of the function (`ghanalss.py`, `ghanasps.py`,
`ethiopia.py`) already do; those copies are the ones the corpus's scripts
import, so `local_tools.change_id` has zero in-corpus callers and this
brings the public one into line with them.

Two behaviour changes, both sanctioned: a polluted in-tree copy of a
DVC-tracked crosswalk is now refused (`_is_polluted_workspace_copy`), and
Site R (`check_read`) now reports on the crosswalk file.

Also adds tests/test_gh810_no_cwd_rooted_dvc.py: five tests run from a
tmp_path cwd holding a malformed `foo.dta.dvc` with `DVCFS` and the WB
fallback stubbed.  Against the unfixed code four of them fail with the
issue's own signature (`dvc.utils.strictyaml.YAMLSyntaxError: unable to
read: 'foo.dta.dvc'`); the `change_id` happy path passes.  The
`get_dataframe` else-branch is fixed in the next commit.

Cache cost, measured: 0 of 3 build_transforms_fingerprint values and
0 of 81 Country._table_cache_hash values (Uganda, GhanaLSS, Ethiopia,
Kosovo) moved -- `change_id` is not in any build fingerprint.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


