# Backlog follow-up element `nigeria-824-followup` -- base `nigeria-824`, issues #824 #867

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/nigeria-824-followup` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## First step, always
`git merge --no-edit origin/development` into your branch (trial-merged clean on 2026-10-14; if it conflicts, STOP and report). Then run the identity assert. Your PR text later goes against `development` as it now stands.

## Review context
The red-team verdict for your base branch is at `slurm_logs/backlog_fix_2026-09-24/verdicts/nigeria-824.md` (section 6 lists the follow-up commits; section 5 the residual risks) and the original element report at `slurm_logs/backlog_fix_2026-09-24/reports/nigeria-824.md`. Read both before touching anything.

## The task

Apply the rulings and follow-ups on `fix/nigeria-824` (verdict: pass-with-fixes):
1. RULING (@ligon 2026-10-14): do NOT sign the 0.20 nitrogen share for Mali "Other inorganic" or Nigeria W1-W3 "other (specify)". REMOVE the `'other inorganic fertilizer': 0.20` alias (and any `_normalise_input_label` change that only exists to reach it) from `lsms_library/transformations.py` so the branch is #824 only; #867 carries the nitrogen vocabulary with its own disclosure. Measure and report Mali `plot_inputs` kg N before/after your removal (should return to the development figure; the reviewer measured +122,434 kg N / +13.5% from the alias) and Nigeria's (+4,402 kg N).
2. `Nigeria/_/CONTENTS.org` "DONE Unit_sold": replace the "built-table counts are lower than raw because..." sentence with the reviewer's measured decomposition (0 unresolved crop labels; rows dropped by the `(t,i,plot_id,crop)` dedup; 159 positive-sale rows discarded by the country script's `drop_duplicates(keep='first')` in W1/W2, 40 of them the only sale on their key -- take the exact numbers from the verdict §5.3 / §6.2).
3. `Nigeria/_/nigeria.py` `crop_production_for_wave` docstring: NA means the frame names no sold-unit column, the row records no sale, OR the sale's unit was not recorded (41 / 6 positive-sale rows).
4. `tests/test_nigeria_sold_unit.py::TestOtherInorganicSpelling`: invert it -- pin that `'Other inorganic'` (Mali) and `'other (specify)'` do NOT resolve to a nitrogen share (so #867 cannot land the alias by accident without a test moving). Optionally make `TestBuiltTable` assert on the API frame rather than the raw parquet.
5. Report (do not fix) the two pre-existing findings for filing: the silent `drop_duplicates` grain collapse, and the unread other-buyer sale columns `sa3q16a/b`, `sa3q17`.
**Verify:** `tests/test_nigeria_sold_unit.py`, `tests/test_gh867*` if any, full suite via the recipe; Nigeria `crop_production` cold-rebuilt in your PRIVATE root with `Unit_sold` counts per wave pasted; the Nigeria re-warm (24 tables) is the branch's known cost -- state, do not pay on the live root.

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
