# Backlog follow-up element `niger-followup-2` -- base `niger-followup`, issues #679 #865 #824 #811 #833

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/niger-followup-2` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## First step, always
`git merge --no-edit origin/development` into your branch (trial-merged clean on 2026-10-14; if it conflicts, STOP and report). Then run the identity assert. Your PR text later goes against `development` as it now stands.

## Review context
The red-team verdict for your base branch is at `slurm_logs/backlog_fix_2026-09-24/verdicts/niger-followup.md` (section 6 lists the follow-up commits; section 5 the residual risks) and the original element report at `slurm_logs/backlog_fix_2026-09-24/reports/niger-followup.md`. Read both before touching anything.

## The task

Apply the reviewer's four follow-ups on `fix/niger-followup` (verdict: pass-with-fixes). Read `Niger/_/CONTENTS.org` FIRST (the NPS-style extension-key design, the #679 and #833 sections) and name the idiosyncrasy you found in your report.
a. `Niger/_/CONTENTS.org:469-470` and `Niger/2021-22/_/crop_production.py:58-62`, plus `CONTENTS.org:1080-1084`: state 280 / 367 as all-raw-row counts, with 17 / 50 on the kept rows and the served 35-row / 29-group `plot_inputs` residual (exact wording per verdict §6a).
b. `tests/test_niger_2014_15_extension_key.py::TestServedKey::test_every_wave_keys_into_the_sample_universe`: add `KNOWN_STRAY = {("crop_production", "2011-12"): {"27010"}}` and assert `stray == KNOWN_STRAY.get((table, t), set())`, with the CONTENTS reference in the message -- the test must go green by NAMING hid 27010, not by weakening.
c. `Niger/_/panel_ids.py` docstring and the CONTENTS #679 section: name the five links the re-key drops (three ext-2-only pairs (89,5) (95,1) (128,11) by decision; two newly guard-skipped) beside the "3,251 -> 3,529" figure.
d. CONTENTS #679 "served rows before -> after" list and the #833 section: add household_characteristics 2014-15 3,558 -> 3,616 and the three 2021-22 derived-food-table moves (98,249 -> 98,231; 98,247 -> 98,244; 131,549 -> 131,522).
Line numbers are from the verdict and may have shifted after the merge; locate by content.
**Verify:** `tests/test_niger_*` + full suite via the recipe; the panel_ids regeneration (`make -C lsms_library/countries/Niger/_ panel-ids`) must produce NO diff to the committed JSON (the crosswalk is a committed source, GH #914); the Niger re-warm (18 tables) is the known cost -- state, do not pay on the live root.

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
