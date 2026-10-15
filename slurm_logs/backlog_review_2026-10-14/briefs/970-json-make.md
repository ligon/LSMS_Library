# RED-TEAM review of branch `fix/970-json-make` @ f4ad88885 (element `970-json-make`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/970-json-make` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Malawi,Nigeria; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/970-json-make/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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

GH #970 (read it: `gh issue view 970`): `Country._aggregate_wave_data` -> nested `load_json_cache(method_name)` (`lsms_library/country.py`, `def load_json_cache` ~line 4337, the `make_cmd.append(f"{method_name}.json")` ~line 4364) shells out to `make <method>.json` with no `env=`. Since GH #914 no country Makefile has a `.json` target, so the call can only fail (Design-B countries Malawi/Nigeria on a cold cache get a `CalledProcessError`, a misleading "Makefile build failed ... Falling back" warning, then `load_from_waves`). Fix option (a), chosen by @ligon 2026-10-14:
1. Delete the `make` attempt: when no cached JSON/parquet candidate exists, go straight to `load_from_waves(waves)`. Remove the now-dead `else: run_make_target(...)` branch if it is unreachable. Keep the four cached-candidate lookups exactly as they are.
2. Tighten `tests/test_build_subprocess_env.py::test_shell_out_sites_do_not_build_their_own_env` (or add a sibling test) so it walks `country.py` with `ast` and asserts EVERY `subprocess.run(` / `subprocess.Popen(` / `check_output(` call passes an `env=` keyword -- not merely that the enclosing function mentions `_script_subprocess_env`. It must fail on the pre-fix code (show that in the report: run it once before your edit).
3. Regression test: on a tmp `LSMS_DATA_DIR` with no cache, `Country('Malawi').panel_ids` (a property) must NOT emit the "Makefile build failed" warning and must still return the dict (Design B derives it from per-wave `data_info.yml`). Mock/monkeypatch `make` away if the tmp build is too heavy; `shutil.which('make')` returning None must also take the no-make path cleanly (GH #968).
4. Measure the cache cost: `build_transforms_fingerprint(t)` for 5 tables and `Country._table_cache_hash(t, c.waves)` for Malawi, Nigeria, Uganda before and after (git stash the edit for the "before"). `_aggregate_wave_data` is on the build path, so expect every hash to move; report the numbers. RULING: that corpus re-warm is accepted, bundled with #810 and #868 -- state, do not pay.
Do not touch #969's Windows work (open PR, `feature/964-windows-script-builds`) -- note in the report if your edit will conflict with it (`git merge-tree --write-tree HEAD origin/feature/964-windows-script-builds` after fetching GitHub: `git fetch https://github.com/ligon/LSMS_Library.git feature/964-windows-script-builds` is allowed, read-only).
**Verify:** the three tests above + `tests/test_panel_ids_persistence.py` + `tests/test_build_subprocess_env.py`, then the full suite via the recipe.


## The branch: diffstat (vs development)
```
 lsms_library/country.py            |  25 +++----
 tests/test_build_subprocess_env.py |  70 +++++++++++++++++
 tests/test_gh970_no_json_make.py   | 149 +++++++++++++++++++++++++++++++++++++
 3 files changed, 230 insertions(+), 14 deletions(-)
```
## The branch: commit messages
--- f4ad88885 tests(gh970): restore the data_root cache on fixture teardown
The cold-root fixture clears paths.data_root's lru_cache and resolves it to
tmp_path; monkeypatch restores the env at teardown but not the cache, so a
later test file on the same xdist worker would have seen data_root() pointing
at a deleted tmp dir.  Yield + finally-clear, the GH #803 fixture pattern.

Serial ordered check (this file, then test_gh891_wave_subset_cache, then
test_panel_ids_persistence, -p no:xdist): green.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- b374c7d7c tests(gh970): clear the lru-cached data_root in the cold-root fixture
In the full suite (-n 6 --dist=loadfile) the regression test's worker had
already resolved paths.data_root() for an earlier file, so the fixture's
LSMS_DATA_DIR setenv was inert: the test read the warm private
panel_ids.parquet instead of deriving from the waves, and "the derived
crosswalk must be cached under LSMS_DATA_DIR" failed with an empty glob.
Mirror the GH #803 tests: data_root.cache_clear() /
countries_root.cache_clear() and assert data_root() == tmp root.

Verified serially with a primed cache (test_panel_ids_persistence +
test_gh891_wave_subset_cache + this file, -p no:xdist): 78 passed.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- fde45bd66 country: load_json_cache never shells out to make (GH #970)
`Country._aggregate_wave_data` -> nested `load_json_cache` ran
`make -s <method>.json` in `countries/{C}/_` whenever none of its four
cached candidates existed, with the parent's environment (the one
subprocess call in country.py not routed through _script_subprocess_env).
Since GH #914 no country Makefile has a .json target, so the call could
only fail: Malawi/Nigeria (Design B) on a cold cache got a
CalledProcessError, a misleading "Makefile build failed ... Falling back
to wave aggregation" warning, then the wave aggregation that always ran
anyway; where `make` is absent it raised FileNotFoundError first
(GH #964/#968).  Option (a) per @ligon 2026-10-14: go straight to
load_from_waves(waves).  The `else: run_make_target(...)` branch was only
reachable through a successful make and is removed with it.  The four
cached-candidate lookups and the cache-write block are unchanged.

Tests:
- tests/test_build_subprocess_env.py: an `ast` walk over country.py that
  asserts EVERY subprocess.run/Popen/check_output/check_call/call passes
  env=.  Pre-fix it failed on `subprocess.run at line 4365`; the old
  function-level guard passed on the same code because `build_env` in the
  enclosing method satisfied it.
- tests/test_gh970_no_json_make.py: on an empty private LSMS_DATA_DIR,
  with Wave.grab_data monkeypatched to a 5-row crosswalk frame,
  Country('Malawi').panel_ids returns the mapping, logs no "Makefile
  build failed" warning, attempts no subprocess at all (also with
  shutil.which('make') -> None and subprocess raising FileNotFoundError),
  and caches under the private root, never in-tree.  Pre-fix: the
  forbidden `['make', '-s', '-j3', 'panel_ids.json']` call fired.

Cache cost: _aggregate_wave_data is on the build path, so every
L2-country hash moves; measured numbers in the branch report.  Re-warm
accepted and bundled with #810/#868 per the 2026-10-14 ruling.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


