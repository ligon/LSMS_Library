# Backlog fix element `810-dvc-open` -- dispatch-plan step 2: issues #810

You are a worker agent in a Slurm job with its OWN clone of `development` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) into which the warm parquets of your countries were copied; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else, and the live cache must not be touched.

## Guardrails
**Out of scope -- STOP and report if the task would require these**:
- Modifying any file under `tests/fixtures/`, `.coder/coverage/`, or any file whose name contains `baseline`, `golden`, `expected`, `snapshot` or `blessed`
- Modifying `pyproject.toml`, `poetry.lock`, `requirements*.txt`, `dvc.lock`, any `.dvc` sidecar, or any dependency pin
- Weakening, skipping, or xfailing any existing test assertion (moving a pinned baseline the fix legitimately moves is allowed ONLY with the before/after numbers in the commit message)
- Running any "regenerate baseline"/"update snapshot"/coverage-matrix script, or `lsms-library cache clear` against any root other than your private `LSMS_DATA_DIR`
- Any `gh` WRITE (issue close/comment/label, pr create/merge). `gh` READS are fine (`GH_REPO` is set).
- Any git push to a branch other than your own `fix/<key>` on `origin` (the shared mirror); never push to `development`; never force-push; never run checkout/reset/stash/clean on any checkout but your own clone
- Touching files outside the paths the task names, and any DVC CLI invocation (read via `get_dataframe` / `get_data_file` only)
If any of these become necessary, STOP, leave the clone in a committable state, and put the conflict at the top of your report.

## Environment facts
- `PYTHONPATH` is your clone. For `python -c` / `-m` runs add `PYTHONSAFEPATH=1` so the clone is the imported package; do NOT export `PYTHONSAFEPATH` into `make`/wave-script subprocesses (338 wave scripts import a sibling by bare name; commit 6df53a5c6). Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`.
- Cores: `$SLURM_CPUS_ON_NODE` (8). Test recipe, verbatim: `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest <targets> -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > <logfile> 2>&1` -- write pytest output to a file, never through a pipe. Never `-n auto`.
- A rebuild of one of your countries' tables is allowed and lands only in your private root. `find lsms_library/countries -name '*.parquet'` must stay empty (GH #803).
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/810-dvc-open` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Scope note
Verification is a tmp-cwd unit test; NO corpus re-warm in this job. Your PR text MUST state that editing get_dataframe moves every table's build fingerprint (a corpus re-warm) and that landing it pairs with the GH #959 decision.

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

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- Commit and push (`git push origin fix/<key>`) BEFORE writing your final message, and `git status --short` must be empty when you do.
- Write the report to `$STAGE/REPORT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
