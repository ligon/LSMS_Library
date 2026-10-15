# Backlog follow-up element `framework-small-2` -- base `fix/framework-small` (red-team verdict: FAIL), issues #694 #915 #905 #909 #703 #734 #792

You are a worker agent in a Slurm job with your OWN clone of the base branch (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with Mali's warm parquets; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else; the live cache must not be touched.

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
- Cores: `$SLURM_CPUS_ON_NODE` (a whole node). Test recipe, verbatim: `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest <targets> -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > <logfile> 2>&1` -- write pytest output to a file, never through a pipe. Never `-n auto`.
- A rebuild of one of your countries' tables is allowed and lands only in your private root. `find lsms_library/countries -name '*.parquet'` must stay empty (GH #803).
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/framework-small-2` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## First step, always
`git merge --no-edit origin/development` into your branch (if it conflicts, STOP and report). Run the identity assert. **Then run the FULL suite with the recipe above BEFORE touching anything and paste its summary line**: the previous agent on this branch never ran it and shipped 14 red tests.

## Why you exist
The red-team reviewer failed `fix/framework-small` at /global/scratch/fsa/fc_jevons/ligon/venv_validate/backlog_review2_39923207/framework-small/VERDICT.md: the GH #915 `ValueError` in `Country.__init__` turns 14 existing tests red (13 `tests/test_dvc_caching.py::TestDVCCaching::*` and `tests/test_countries_root_override.py::test_subprocess_override_routes_all_layers`). Every other claim held. Your job is to apply the reviewer's section 6 exactly, then prove the suite is green.

## Rulings (scrum master, 2026-10-14)
- §5.1: the TESTS give, not the raise -- make the mock trees real `countries_root()` overrides (reviewer's preferred remedy); keep #915's raise. If you find a test that cannot be made consistent that way, STOP and report it rather than softening the raise.
- §5.2: accept <= 4 ULP for #905 as the tests pin; make every comment say so.
- §6.3: do NOT sweep the other food_acquired copies; write the `#905 residual` list (every surviving site, with path:line) into your report as a drafted residual issue.

## 5. Residual risks / human decisions
1. **Which side of #915 gives?** Either the 14 tests change (point `LSMS_COUNTRIES_ROOT` / `monkeypatch` `countries_root` at the mock tree *before* constructing, or construct with a real name and patch `file_path`), or `Country.__init__` keeps raising only when the name is also absent from the registered list *and* no override is active -- the override test shows `Country('Uganda')` under a bare `LSMS_COUNTRIES_ROOT` is an intended use that now dies. Note `0 are available` in that message: the suggestion list is computed from the override root, which is correct but reads oddly.
2. **ULP contract for #905**: accept <= 4 ULP (the test's bound) or require bitwise equality (then the vectorised path cannot be used). The code comments must match whichever is chosen.
3. **#703 CI**: the claim that `.github/workflows/ci.yml` `data-tests` (master/separate_data pushes only; `poetry install --no-root` then `make -j2` in `Uganda/_`) leaves `CACHED` non-empty is unverifiable here; if those parquets land in-tree rather than in `data_root()` (the write-side `.pth`/package-identity trap, GH #803), that job's previously-silent skip becomes a hard failure. Name it when merging.
4. **#909 API-path cost**: pre-#891 or partial roster parquets now send `check_panel_consistency` through the API path; on a cold root that triggers a full roster build (observed on Mali: 4 waves rebuilt). Correct, but a behaviour change for anyone running diagnostics on a cold root.
5. #694: `None` vs `NaN` representation and the 1.9x per-column cost on huge object columns (read-path, small tables in practice).


## 6. Recommended follow-up commits
1. `tests/test_dvc_caching.py`, `tests/test_countries_root_override.py` (or `lsms_library/country.py:2000`): make the 14 tests and the #915 raise consistent -- preferred: in `mock_country_structure` set `LSMS_COUNTRIES_ROOT`/`countries_root.cache_clear()` to `tmp_path/"countries"` before `Country("TestCountry")`, and in `test_subprocess_override_routes_all_layers` create `ov/"Uganda"` (an empty dir is enough). Re-run the full suite and quote the summary line.
2. `lsms_library/build_transforms.py:203`, `lsms_library/countries/Panama/{1997,2003,2008}/_/food_acquired.py`: "1 ULP" -> "a few ULP (<= 4 pinned by tests/test_gh905...)"; amend nothing else.
3. Either sweep the remaining `food_acquired` copies (GhanaSPS x3 + `ghanasps.py:837`, Cambodia 2019-20, Serbia 2007) with cold before/after `equals` checks, or add a `#905 residual` to the element report listing every surviving site (section 1, R5) so the issue stays open with exact remaining work.
4. `lsms_library/country.py` `_strip_string_values` docstring, commit-message-level wording, and `tests/test_categorical_mapping_resolution.py::test_gh694_numeric_values_survive_a_same_named_table` message: say "mixed object column"; keep the test as a guard.
5. Report corrections: "byte-identical" -> "data-identical (footer hash differs)"; drop the seeded-roster "no manifest" sentence or re-measure it.


**Verify:** the 14 named tests + `tests/test_gh905*` + `tests/test_categorical_mapping_resolution.py`, then the FULL suite via the recipe (summary line pasted verbatim; the only acceptable failures are ones you show are red on origin/development too, by name). Stash-probe `Country._table_cache_hash` on Mali and Uganda before/after your commits and report which moved.

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
