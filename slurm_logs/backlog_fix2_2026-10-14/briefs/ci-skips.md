# Backlog fix element `ci-skips` -- issues #907 #941

You are a worker agent in a Slurm job with your OWN clone of `development` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the warm parquets of your countries; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else; the live cache must not be touched.

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
- `PYTHONPATH` is your clone. For `python -c` / `-m` runs add `PYTHONSAFEPATH=1`; do NOT export it into `make`/wave-script subprocesses. Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`.
- Cores: `$SLURM_CPUS_ON_NODE` (a whole node). Test recipe, verbatim: `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest <targets> -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > <logfile> 2>&1` -- output to a file, never a pipe. Never `-n auto`.
- `find lsms_library/countries -name '*.parquet'` must stay empty (GH #803).
- Commit as you go; end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/ci-skips` (origin is the shared mirror; no GitHub push, no PR from here).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org`.
- Read `CLAUDE.md` section headings and the sections your files touch; read `.claude/skills/backlog-workflow/SKILL.md` for the rule that an open issue must describe exactly the remaining work: if your fix resolves PART of an issue, DRAFT the residual (title + body + tier) in your report.

## Scope note from the scrum master
#907: add module-level `pytestmark = pytest.mark.requires_s3` to the 56 files the record names where every test builds a Country/Feature; prove the skip count is unchanged and the wall time of a credential-less run drops (measure). #941 step 1 ONLY: `workflow_dispatch` on ci.yml so data-tests can be re-run without a release; step 2 (the diagnosis of the sequential cold session) is out of scope -- say what you would need.

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #907 -- 80% of CI's skipped tests take the expensive skip path: 56 files reach a skip via a network timeout instead of a requires_s3 mark
```json
{
 "tier": "C",
 "type": "test",
 "tractability": "auto",
 "dedup_status": "open (0 of the 56 files are marked today; #906 landed parallelism/durations only)",
 "priority": 3,
 "summary": "Still true in full: all 56 files named in the issue contain no 'requires_s3' string on development (checked by grep); 36 other files are marked. tests/conftest.py:84-165 still has both paths -- the collection-time requires_s3 skipif and the pytest_runtest_makereport net whose message asks for the mark. The @ligon comment's 43 s band is the per-test timeout cost. Mechanical, but a module-level mark on a file that mixes data-free tests would drop coverage, so the passed count is the invariant.",
 "evidence": "grep -c requires_s3 over the 56 files (all 0); tests/conftest.py:84-93 (marker), :143-165 (net); GH actions run 34761645883 durations in the issue",
 "approach": "For each of the 56 files: if every test builds a Country/Feature, add `pytestmark = pytest.mark.requires_s3` at module level; otherwise mark the data-dependent tests/fixtures only. Then flip the makereport net (tests/conftest.py:143) to emit a UserWarning naming the unmarked file so the list cannot regrow. Confirm the 43 s mechanism on one file first (--durations=5 with LSMS_SKIP_AUTH=1).",
 "verify": "Two credential-free runs (LSMS_SKIP_AUTH=1): passed count identical before/after (2,644 on the 09-13 run), skipped identical, wall time down. CACHE COST: none (tests only).",
 "contents_org": "n/a (framework)",
 "blocked_by": "",
 "decision_for_human": ""
}
```

### #941 -- master data-tests: EHCVM food_acquired served WITHOUT Derivation / inkind rows in the sequential cold CI session (52 failures in test_ehcvm_7day.py); not reproducible cold locally
```json
{
 "tier": "B",
 "type": "test",
 "tractability": "diagnose-then-auto",
 "dedup_status": "open",
 "priority": "2 -- a served EHCVM food_acquired without Derivation/with valued produced rows is wrong data if it can happen outside the CI session; the cold path is verified correct, so it is a test-session hazard until proven otherwise",
 "summary": "Still open and NOT re-exercised since it was filed. `.github/workflows/ci.yml:274` gates `data-tests` on `event_name == 'push'`; every master run after the 09-17 failure (35602082022, 36432622626, 37327210465, 38270795197) is a `schedule` event at the SAME sha 1528fa937 with `data-tests skipped` (verified with `gh run view --json jobs`). So 'green master' says nothing. Two threads in the issue are dead on today's code: (1) the body's derivations.yml-outside-the-hash mechanism (already refuted in-thread); (2) the reviewer's surviving lead -- 'if Country.formatting_functions is a shared/cached dict, wave hooks leak' -- is not live: `country.py:2007-2014` is a plain `@property` that builds a NEW dict on every access, so the in-place `.update` at `country.py:1067-1074` cannot leak across waves. The set-literal `{f'{wave_folder}.py', 'mapping.py'}` at :1068 has nondeterministic order, but no EHCVM wave ships a `{wave_folder}.py` (ls of all 12 wave `_/` dirs), so it is inert here. A third thing the thread has not noticed: the failing assertion `assert 10798 == 10802` is `tests/test_ehcvm_7day.py:322` `len(inkind) == n` -- the served frame HAD 10,798 `s='inkind'` rows, and the only emitter of inkind rows in the package's top-level modules or in any EHCVM country's code is `ehcvm._inkind_rows` (`ehcvm.py:352`; `grep -rl inkind` over `lsms_library/*.py` and `countries/{Senegal,Togo,Benin,Mali,Niger,Guinea-Bissau,CotedIvoire,Burkina_Faso}/**/*.py` finds only `Mali/2014-15/_/mapping.py`, a non-EHCVM wave; the pre-#876 reshape `build_transforms.food_acquired_to_canonical` emits purchased+produced only, :124-138). So the reviewer's 'the wave df_edit hook did not run at all' is contradicted too: some version of `food_acquired_ehcvm` ran, and the frame then lost `Derivation`, gained `Expenditure` on produced rows and lost the NA arm. That points at (a) a different raw read (CI shadow-test covered pandas/numpy/pyarrow but NOT pyreadstat, which decides Stata missing/label handling and could move 4 inkind rows), and/or (b) something after the hook in a single sequential process. `get_formatting_functions` (`local_tools.py:2900-2916`) exec's each wave module under the name `formatting_<wave_folder>` without registering it in sys.modules; `Senegal/2018-19/_/food_acquired.py` and `Burkina_Faso/2018-19|2021-22/_/food_acquired.py` scripts coexist with the mapping.py hook (ls). One live stale-parquet route to read next: Senegal 2018-19 and Burkina 2018-19/2021-22 keep a LEGACY wave script `_/food_acquired.py` (wide shape, 'last expenditure' columns) beside the YAML+hook build, and the country Makefiles still carry the pattern rule `../%/_/food_acquired.parquet: $(food_acquired)` -> `python food_acquired.py` (`Senegal/_/Makefile:36-40`, `Burkina_Faso/_/Makefile:34-37`), reachable from `run_make_target` and from the `food_*` derived-parquet rule (:33/:31). A hashless L2-wave parquet written that way grades `legacy` -> trust-once + re-stamp at the L2-wave read (CLAUDE.md GH #809). Its shape does not match the CI frame on its own, so it is a candidate, not the answer.",
 "evidence": ".github/workflows/ci.yml:272-300; gh run view 38270795197/35174026106 --json jobs (schedule: data-tests skipped; push: data-tests failure); lsms_library/country.py:1066-1074,1173,1428,1475,1520,2007-2014; lsms_library/local_tools.py:2900-2916; lsms_library/ehcvm.py:303,310-314,352; lsms_library/build_transforms.py:95-138; tests/test_ehcvm_7day.py:84,245-260,264-325; ls lsms_library/countries/{Togo/2018,Benin/2018-19,Mali/2018-19,...}/_/*.py; grep -rn \"'inkind'\" lsms_library/*.py; gh pr list --state open (none touch it); merged_prs_since_0918.tsv (#944 only MENTIONS #941 as a merge note)",
 "approach": "Two steps, neither a data fix. (1) Make the job re-runnable without a release: add `workflow_dispatch` to `ci.yml` so `data-tests` can be triggered on a chosen ref (zero cache cost; pure CI config; land after PR #973, which is open on ci.yml), then run it once on current development to learn whether the 52 failures persist post-#947/#952/#963. (2) Reproduce sequentially in a private LSMS_DATA_DIR in CI file order with `-p no:xdist` and `LSMS_BUILD_WORKERS` UNSET, with CI's full resolved stack including pyreadstat (not only pandas/numpy/pyarrow), stopping at the first file after which `Country('Senegal').food_acquired()` loses `Derivation`; the candidate list from the reviewer stands (test_add_market_index_dedup, test_albania_cluster_features, test_burkina_faso_2014_passages, test_coverage_matrix, test_currency*, test_derivations). Also instrument `Wave.grab_data` to log `type(df_edit_function).__module__`/`__file__` for food_acquired at dispatch. A fix is written only once the mechanism is known.",
 "verify": "CI: a `workflow_dispatch` run of data-tests on development; local: the sequential cold run above (cost: cold builds of every country those test files touch -- hours on one node; L1 blobs shared, nothing is written to the warm cache if run in a private root). No cache invalidation until a code fix is known; a fix in `local_tools.get_formatting_functions` or `Wave.grab_data` would move `build_transforms_fingerprint` for every table (grab_data carries @build_transform, CLAUDE.md 'Landing a guard does invalidate') -- corpus-wide re-warm.",
 "contents_org": "n/a (framework/CI). Togo/_/CONTENTS.org carries the #876 'one row, two windows' note that the config-tier test asserts; nothing in it bears on the session ordering.",
 "blocked_by": "a sequential cold reproduction (or a data-tests run) nobody has executed since 2026-09-17; step 1 edits ci.yml, so sequence after PR #973 (open on the same file; it does not touch the data-tests job)",
 "decision_for_human": "Add `workflow_dispatch` to ci.yml so data-tests can run without a master push: yes/no?"
}
```

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
