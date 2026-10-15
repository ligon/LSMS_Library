# Backlog follow-up element `868-followup` -- base `868-reduce-agreed`, issues #868

You are a worker agent in a Slurm job with your OWN clone of the base branch (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the warm parquets of your countries; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else; the live cache must not be touched.

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/868-followup` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## First step, always
`git merge --no-edit origin/development` into your branch (trial-merged clean on 2026-10-14; if it conflicts, STOP and report). Then run the identity assert. Your PR text later goes against `development` as it now stands.

## Review context
The red-team verdict for your base branch is at `slurm_logs/backlog_fix_2026-09-24/verdicts/868-reduce-agreed.md` (section 6 lists the follow-up commits; section 5 the residual risks) and the original element report at `slurm_logs/backlog_fix_2026-09-24/reports/868-reduce-agreed.md`. Read both before touching anything.

## The task

RULING (@ligon 2026-10-14): the corpus-wide, both-tier re-warm is ACCEPTED, bundled with #810 and #970 (paid once, not here). Apply the reviewer's follow-ups on `fix/868-reduce-agreed` (verdict: needs-human, now resolved):
1. `lsms_library/country.py` `_replay_grain_audit` docstring and `_resolve_build_context` in `build_transforms.py`: one sentence each stating that under the framework context a report's `wave` is the wave FOLDER (`self.year`); a two-round folder (Nigeria PP/PH) must pass `wave=` explicitly to name the round.
2. `lsms_library/build_transforms.py` `_cells_blanked_report`: rename `dropped` to `collapsed_rows` (it is the projection/de-dup count, never a loss) so it cannot be read as Site 1's `dropped`; update `tests/test_gh868_reduce_to_agreed_ledger.py` accordingly and any formatter that prints it.
3. `Uganda/_/CONTENTS.org` and the commit message: replace "the cold-path signal is unchanged" with the measured statement: in-process hook unchanged; a cold `make` build emits the child's `GrainConflictWarning` on stderr and the parent's `GrainCollapseWarning` replay in-process.
4. In your report, restate the cache cost in the merge-note form the reviewer asked for: corpus-wide, both tiers, so the next person to warm a root does not attribute the cold builds to something else.
**Verify:** `tests/test_gh868_*`, `tests/test_gh323_*`, `tests/test_uganda_323*` if present, then the full suite via the recipe. Confirm with a stash-probe that YOUR edits (docstrings, rename) move no ADDITIONAL hashes beyond what the base branch already moves (the base moves all of them; say so).

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
