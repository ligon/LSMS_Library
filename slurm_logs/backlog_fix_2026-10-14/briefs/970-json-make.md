# Backlog follow-up element `970-json-make` -- base `development`, issues #970

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/970-json-make` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## First step, always
Your clone is `development` as of 2026-10-14 (post-merge with GitHub). Run the identity assert.

## The task

GH #970 (read it: `gh issue view 970`): `Country._aggregate_wave_data` -> nested `load_json_cache(method_name)` (`lsms_library/country.py`, `def load_json_cache` ~line 4337, the `make_cmd.append(f"{method_name}.json")` ~line 4364) shells out to `make <method>.json` with no `env=`. Since GH #914 no country Makefile has a `.json` target, so the call can only fail (Design-B countries Malawi/Nigeria on a cold cache get a `CalledProcessError`, a misleading "Makefile build failed ... Falling back" warning, then `load_from_waves`). Fix option (a), chosen by @ligon 2026-10-14:
1. Delete the `make` attempt: when no cached JSON/parquet candidate exists, go straight to `load_from_waves(waves)`. Remove the now-dead `else: run_make_target(...)` branch if it is unreachable. Keep the four cached-candidate lookups exactly as they are.
2. Tighten `tests/test_build_subprocess_env.py::test_shell_out_sites_do_not_build_their_own_env` (or add a sibling test) so it walks `country.py` with `ast` and asserts EVERY `subprocess.run(` / `subprocess.Popen(` / `check_output(` call passes an `env=` keyword -- not merely that the enclosing function mentions `_script_subprocess_env`. It must fail on the pre-fix code (show that in the report: run it once before your edit).
3. Regression test: on a tmp `LSMS_DATA_DIR` with no cache, `Country('Malawi').panel_ids` (a property) must NOT emit the "Makefile build failed" warning and must still return the dict (Design B derives it from per-wave `data_info.yml`). Mock/monkeypatch `make` away if the tmp build is too heavy; `shutil.which('make')` returning None must also take the no-make path cleanly (GH #968).
4. Measure the cache cost: `build_transforms_fingerprint(t)` for 5 tables and `Country._table_cache_hash(t, c.waves)` for Malawi, Nigeria, Uganda before and after (git stash the edit for the "before"). `_aggregate_wave_data` is on the build path, so expect every hash to move; report the numbers. RULING: that corpus re-warm is accepted, bundled with #810 and #868 -- state, do not pay.
Do not touch #969's Windows work (open PR, `feature/964-windows-script-builds`) -- note in the report if your edit will conflict with it (`git merge-tree --write-tree HEAD origin/feature/964-windows-script-builds` after fetching GitHub: `git fetch https://github.com/ligon/LSMS_Library.git feature/964-windows-script-builds` is allowed, read-only).
**Verify:** the three tests above + `tests/test_panel_ids_persistence.py` + `tests/test_build_subprocess_env.py`, then the full suite via the recipe.

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
