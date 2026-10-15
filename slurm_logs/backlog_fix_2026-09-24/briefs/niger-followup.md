# Backlog fix element `niger-followup` -- complete the Niger bundle (branch `fix/niger-bundle`)

You are a worker agent in a Slurm job with its OWN clone of the branch `fix/niger-bundle` (your cwd, checked out on a new branch `fix/niger-followup` on top of it), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) into which Niger's warm parquets -- built from `development`, i.e. BEFORE the bundle -- were copied; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else.

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/niger-followup` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- Commit and push (`git push origin fix/<key>`) BEFORE writing your final message, and `git status --short` must be empty when you do.
- Write the report to `$STAGE/REPORT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.

## What happened, and what you must finish
A previous agent landed five commits on `fix/niger-bundle` (read them: `git log development..HEAD` with full messages) for GH #679, #865, #824, #811 and #833. Its turn ended while a cold Niger rebuild was running in the background, and its clone was destroyed with these left UNCOMMITTED and now lost:
- `tests/test_niger_region_labels.py` (cited by commit b502bb28d as pinning served Region 16 -> 8 labels)
- `tests/test_niger_sold_unit.py` (for commit 04c1efbf9: Unit_sold carried; crop-level sale keyed on it; s16d duplicate lines dropped -- Value_sold 78,934,620 -> 73,206,995 FCFA on single-plot pairs)
- `tests/test_niger_food_label_drift.py` (for commit d36054864: Chou/Choux, Sucre, 'Aubergine,' folds; code 166 stays)
- an edit to `lsms_library/countries/Niger/_/CONTENTS.org` correcting the 2021-22 sold-unit agreement figure (63.0% was a spelling artefact; through the `u` table 89.9% -- see 04c1efbf9's message)
Committed tests that DO exist: `tests/test_niger_2014_15_extension_key.py`, `tests/test_niger_value_sentinels.py`.

Your task, in order:
1. Read `lsms_library/countries/Niger/_/CONTENTS.org` (prose AND LOGBOOK/WAITING/TODO) and the five commit messages; NAME in your report the idiosyncrasies that bear on each (EHCVM grappe/vague design, EXTENSION, the crop-level sale block in 2014-15/2021-22, the s16d repetition).
2. Run the COLD Niger rebuild the previous agent never finished: `lsms-library cache clear --country Niger` against YOUR private root (via `.venv/bin/python -m lsms_library.cli ...`), then build every Niger table with `Country('Niger')` in the FOREGROUND with `timeout`, output to a file. Report any warning, error or `_rebuild_failure_error`.
3. Re-measure, on the rebuilt private root, every number the five commit messages claim (list them in a table: claimed vs measured). A discrepancy is a finding to report, not to paper over.
4. Recreate the three tests as data-gated tests in the repo's existing style (look at `tests/test_niger_value_sentinels.py` and `tests/test_uganda_sold_unit.py` for the pattern; gate on the cache being warm the way they do), and the CONTENTS.org correction. Run them green in the foreground; paste the pytest summary line.
5. Run the wider regression selection: `tests/test_niger*.py tests/test_ehcvm_7day.py tests/test_schema_consistency.py tests/test_panel_ids_persistence.py tests/test_feature_canonical_index.py` with the verbatim recipe; paste the summary line.
6. Commit (one commit per recreated artefact is fine), `git push origin fix/niger-followup`, write REPORT.md, then your final message.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
