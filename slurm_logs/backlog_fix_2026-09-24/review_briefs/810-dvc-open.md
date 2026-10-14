# RED-TEAM review of branch `fix/810-dvc-open` (element `810-dvc-open`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/810-dvc-open` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of no country (framework change) built from `development`; `dvc-cache` in it is a read-only symlink to the shared blob store. `development` is available as `origin/development` in your clone (`git diff --stat origin/development..HEAD`).

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
**What:** #810: replace both dvc.api.open sites (local_tools.py:1394 get_dataframe, :1671 change_id) with the countries_root-rooted path (DVCFS at :198 / _try_get_data_file), fail fast; add a tmp_path-cwd test with a malformed foo.dta.dvc. Editing get_dataframe moves 37/37 build fingerprints, so SCHEDULE IT BESIDE the #959 paired re-warm decision so the corpus re-warms once, not twice (soft dependency, not a blocker).

**Verify:** New test: no YAMLValidationError, sub-second failure from a tmp cwd; existing suite via `make test` recipe (-n, --dist=loadfile, LSMS_BUILD_WORKERS=1, BLAS threads pinned). Cost: one CORPUS re-warm (shared with #959 if executed).

**Blocked by (as triaged):** soft: coordinate with #959 decision (execute re-warm vs accept mixed-writer corpus).

## Triage records for the issues in scope

### #810 -- tier B, auto, dedup open
- country: framework
- approach: Replace both dvc.api.open sites with the countries_root-rooted path (DVCFS.open or straight to _try_get_data_file, fail fast with the WB-fallback error) and add a test that calls get_dataframe on a nonexistent source from a tmp_path cwd holding a malformed foo.dta.dvc, asserting no YAMLValidationError and sub-second failure. Note: editing get_dataframe moves every build fingerprint (37/37), so budget a corpus re-warm as with any get_dataframe edit.
- verify cost: none
- CONTENTS.org: n/a
- evidence: lsms_library/local_tools.py:1392-1405 (dvc.api.open with no repo=), :1667-1672 (change_id), :198 DVCFS = DVCFileSystem(_COUNTRIES_DIR), :1219-1231 file_system_path candidates; `git log --oneline -S'dvc.api.open' -- lsms_library/local_tools.py` -> d82c1044f, 6ffb118db, 96577be50 (all pre-issue); grep tests/ for YAMLValidationError/dvc.api.open -> no cwd test; #810 comment 2026-09-07T22:40 (Nepal BrokenProcessPool); CLAUDE.md anti-pattern table names dvc.api.open
- summary: get_dataframe's last-resort branch calls `dvc.api.open(fn, mode='rb')` with no repo=, so DVC resolves the repository from CWD and walks every *.dvc under it; from /tmp that means 300-450 s per missing source and a YAMLValidationError on a malformed pytest fixture, and (second comment) a BrokenProcessPool on Nepal from the walk running inside a forked build worker. Nothing has moved: both sites are unchanged at local_tools.py:1394 (get_dataframe) and :1671 (change_id, 4 callers), `git log -S'dvc.api.open'` shows only pre-issue commits, and no test runs get_dataframe from a tmp_path cwd. The branch is reached only when file_system_path finds no sidecar at cwd or _COUNTRIES_DIR (:1219-1231), i.e. for genuinely absent sources, so the right fix is to drop dvc.api.open there and go straight to _try_get_data_file / DVCFS (rooted at _COUNTRIES_DIR, :198), not to pass repo= (which still pays the sidecar walk). Armenia/Nepal are where the symptom surfaced, not the locus.

Read the full issue and its comments first: `gh issue view 810 --comments`.


## The branch: diffstat (vs development)
```
 lsms_library/local_tools.py           |  55 ++++++++++------
 tests/test_gh810_no_cwd_rooted_dvc.py | 117 ++++++++++++++++++++++++++++++++++
 2 files changed, 152 insertions(+), 20 deletions(-)
```
## The branch: commit messages
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


## The fix agent's report (a self-report -- refute it)
## SCOPE DEVIATIONS

`git diff --stat development..HEAD` shows exactly two files:

- `lsms_library/local_tools.py` (named by the task)
- `tests/test_gh810_no_cwd_rooted_dvc.py` (new; implied by the task's "add a tmp_path-cwd test", not named by path)

No stop-list match. No `CONTENTS.org`, `CLAUDE.md`, or country file touched.

## 1. Commits and branch

Branch `fix/810-dvc-open`, pushed to `origin` (the shared mirror) as a new branch. Two commits:

- `4d4d16d64` — `change_id` reads its crosswalk through `get_dataframe`, plus the new test file.
- `bebe68a51` — `get_dataframe`'s else-branch goes straight to `_try_get_data_file` and raises `FileNotFoundError(ENOENT, why, fn)` when that returns None. `import dvc.api` dropped; only `DVCFileSystem` remains imported from `dvc.api`.

## 2. Verification

| command | result |
|---|---|
| package-identity assert (`PYTHONSAFEPATH=1 … assert realpath startswith cwd`) | ok, `/local/job39198803/clone/lsms_library/__init__.py` |
| new test file against unfixed code | `5 failed in 16.55s`. Four fail with the issue's own signature: `dvc.utils.strictyaml.YAMLSyntaxError: unable to read: 'foo.dta.dvc', YAML file structure is corrupted`. The fifth was a bug in my test (it used level `i`; `change_id` maps `j`). |
| new test file after commit 1 only | `4 failed, 1 passed in 12.41s` (the four cwd-trap tests still reach the old else-branch) |
| new test file after commit 2 | `5 passed in 8.20s` |
| full suite, Makefile recipe (`-n 8 --dist=loadfile`, `LSMS_BUILD_WORKERS=1`, BLAS pinned, output to file) | `4244 passed, 203 skipped, 3 xfailed, 1 xpassed, 814 warnings in 1867.34s (0:31:07)`, `exit=0` |
| `find lsms_library/countries -name '*.parquet'` | 0 before and 0 after the suite |

The suite count is not comparable to the 5,666 in CLAUDE.md. This private root is warm for four countries only, so cache-gated modules skip. That is coverage the root cannot reach, not lost tests.

Unstubbed end-to-end, from a tmp cwd holding a malformed `foo.dta.dvc`, requesting a nonexistent source:

| | before | after |
|---|---|---|
| exception | `YAMLSyntaxError` on `foo.dta.dvc` (cwd walk) | `FileNotFoundError`, message names the countries root and the WB fallback |
| cold | 0.01 s at the `dvc.api.open` step here (tiny cwd; 300–450 s from `/tmp` per the issue) | 51.6 s |
| warm | — | 0.0 s |

Be clear about what the test discriminates. In this fixture the tmp cwd is tiny, so the cwd walk is fast and the `elapsed < 1.0` bound is not what catches the bug. The exception-class assertion is the load-bearing one (`YAML*` pre-fix, `FileNotFoundError` post-fix). The bound is kept because the task asked for it, with `DVCFS` and `_try_get_data_file` stubbed so the branch under test is what is timed. If it ever flakes on a loaded node, loosen the bound, never the class check.

The 51.6 s cold cost is the correctly-rooted `DVCFS.open` index walk over 9,986 sidecars in `file_system_path`, plus `get_data_file`'s `fs.exists`. It was paid before this change too, it is not the cwd walk, and it is not fixed here (~93 s on Lustre per the existing comments).

## 3. Cache cost (stated, not paid)

Probed `build_transforms_fingerprint` (3 values) and `Country._table_cache_hash` for every table of Uganda, GhanaLSS, Ethiopia and Kosovo (81 values), before and after each commit:

| commit | fingerprints moved | country hashes moved |
|---|---|---|
| `4d4d16d64` (`change_id`) | 0 of 3 | 0 of 81 |
| `bebe68a51` (`get_dataframe`) | 3 of 3 | 81 of 81 |

So the second commit is a corpus-wide re-warm: `df_data_grabber` is `@build_transform`-tagged and closes over `get_dataframe`, so any edit to that function moves every table's build fingerprint. Not paid in this job. Landing this pairs with the GH #959 decision (execute the re-warm vs. accept a mixed-writer corpus) so the corpus re-warms once, not twice.

## 4. Surprises

- **Triage said `change_id` has 4 callers. `local_tools.change_id` has zero in-corpus callers.** The four call sites (GhanaLSS `household_roster.py`, Ethiopia `food_acquired.py`) import country-local copies in `ghanalss.py`, `ghanasps.py` and `ethiopia.py`, and all three already read the crosswalk via `get_dataframe(fn)`. My edit brings the public function into line with them and changes no built table.
- **The issue names `YAMLValidationError`; a malformed sidecar raises `YAMLSyntaxError`.** Same DVC family, class depends on the malformation. The test guards on the `YAML` name prefix rather than one class.
- **One directory-level sidecar exists** (`EthiopiaRHS/_dataverse_archive.dvc`, a `.dir` entry). It is read by no script or YAML, so the else-branch is provably the "genuinely absent source" case and removal is safe.
- GitNexus timed out, so I substituted `rg` call-site sweeps (9 `change_id` references, all country-local copies; 951 `get_dataframe` call sites, signature unchanged) for the impact analysis.
- CONTENTS.org: nothing contradicted; this is framework-only, no country file touched.

## 5. Decisions

- **Fail with `FileNotFoundError`, not a new class.** The old branch also ended in a re-raised `FileNotFoundError`, and `grab_data` (`country.py:1435`, `:1510`) and `df_data_grabber(missing_ok=)` catch that class, so the #515 soft-degrade and multi-file candidate loops keep working.
- **Go straight to `_try_get_data_file`, not `DVCFS.open` first.** `file_system_path` already tried `DVCFS.open` on the correct root and got `FileNotFoundError`; a second attempt would be a redundant walk.
- **`change_id` via `get_dataframe`** rather than `DVCFS.open`: `DVCFS` resolves relative paths against the repo root, so the callers' `../{t}/Data/{f}` form would break; `get_dataframe` resolves it via its `Path.cwd()/fn` sidecar candidate. Two sanctioned side effects: a polluted in-tree crosswalk copy is now refused, and Site R reports on the crosswalk read.
- **`dvc.api.open` made unreachable, not merely unreached.** One test patches `dvc.api.open` to raise and asserts it is never hit.

Follow-ups named, not done (outside named paths): `_sync_legacy_dvc_creds` in `__init__.py` and `data_access.py` exists solely for `dvc.api.open` callers, and none remain anywhere in the package, so it is a removal candidate. The CLAUDE.md gotcha about `get_dataframe('../Data/..')` from a chdir'd cwd falling through to `dvc.api.open` describes this same bug and can be retired once this lands. The residual cold `DVCFS` index walk for a genuinely absent source is its own issue.
