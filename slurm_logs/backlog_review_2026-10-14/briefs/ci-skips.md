# RED-TEAM review of branch `fix/ci-skips` @ 77cb428e6 (element `ci-skips`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/ci-skips` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of no country (framework change); `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/ci-skips/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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


## The branch: diffstat (vs development)
```
 .github/workflows/ci.yml                       | 19 ++++++++++++--
 tests/conftest.py                              | 34 +++++++++++++++++++++++++-
 tests/test_863_transforms.py                   |  8 ++++++
 tests/test_add_market_index_dedup.py           |  2 ++
 tests/test_age_dtype_consistency.py            |  2 ++
 tests/test_canonical_shape_via_cache_miss.py   | 17 +++++++------
 tests/test_china_index_uniqueness.py           |  7 ++++++
 tests/test_conversion.py                       |  2 ++
 tests/test_currency.py                         |  5 ++++
 tests/test_currency_denominated_units.py       |  4 +++
 tests/test_declared_spellings.py               |  1 +
 tests/test_derivations.py                      |  1 +
 tests/test_ehcvm_7day.py                       |  1 +
 tests/test_feature_canonical_index.py          |  1 +
 tests/test_food_labels.py                      |  4 +++
 tests/test_food_prices_dtype.py                |  2 ++
 tests/test_gh323_benin_cluster_features.py     |  4 +++
 tests/test_gh323_benin_togo.py                 |  2 ++
 tests/test_gh323_ethiopia_config.py            |  3 +++
 tests/test_gh323_malawi_cluster_key.py         |  7 ++++++
 tests/test_gh323_mali_cartesian.py             |  4 +++
 tests/test_gh323_tanzania_cluster_key.py       | 10 ++++++++
 tests/test_gh842_uganda_harvest_unit.py        |  6 +++++
 tests/test_ghanalss_community_prices.py        |  2 +-
 tests/test_ghanalss_food_label_canonical.py    |  1 +
 tests/test_ghanalss_nutrition.py               |  8 ++++++
 tests/test_ghanalss_visit_alignment.py         |  2 ++
 tests/test_guatemala_psu_grain.py              |  2 ++
 tests/test_guinea_bissau_cluster_features.py   |  2 ++
 tests/test_id_format_id_canonical.py           |  1 +
 tests/test_india_employment_activity_index.py  |  6 +++++
 tests/test_locality_deprecation.py             |  1 +
 tests/test_niger_labor_hours.py                |  1 +
 tests/test_niger_u_sentinel.py                 |  1 +
 tests/test_nigeria_cluster_identity.py         |  1 +
 tests/test_nigeria_food_acquired_sources.py    |  5 +++-
 tests/test_nigeria_kg_factor.py                |  3 +++
 tests/test_panel_attrition.py                  |  2 ++
 tests/test_panel_id_collisions.py              |  2 ++
 tests/test_quantity_kg.py                      |  3 +++
 tests/test_quantity_screen.py                  |  1 +
 tests/test_rcsi.py                             |  1 +
 tests/test_recall_record.py                    |  1 +
 tests/test_sample.py                           |  2 ++
 tests/test_table_structure.py                  |  4 +++
 tests/test_tanzania_community_cluster_xwalk.py |  2 ++
 tests/test_tanzania_grain_gh323.py             |  2 ++
 tests/test_togo_nonfood_expenditures.py        |  5 ++++
 tests/test_u_code_leak.py                      |  1 +
 tests/test_uganda_99999.py                     |  1 +
 tests/test_uganda_api_vs_replication.py        |  5 +++-
 tests/test_uganda_crop_condition.py            |  9 +++++++
 tests/test_uganda_intercropped.py              |  3 +++
 tests/test_uganda_invariance.py                |  2 ++
 tests/test_uganda_sold_unit.py                 |  7 ++++++
 tests/test_uganda_tables.py                    |  2 ++
 tests/test_uganda_v_grain_invariants.py        |  2 ++
 tests/test_visualizations.py                   |  4 +++
 58 files changed, 230 insertions(+), 13 deletions(-)
```
## The branch: commit messages
--- 77cb428e6 tests/ci: harden the net's warning against filterwarnings=error; dvc-lock-check is push-only (GH #907, #941)
conftest: pytest's recorder wraps the whole runtest protocol, makereport
included, and applies the item's filterwarnings inside it -- so under a
`filterwarnings("error")` the new UnmarkedS3TestWarning would have RAISED
in the hookwrapper and the net would crash instead of converting.  The
warn now sits in an inner non-recording catch_warnings with an "always"
filter for its own class: filters are restored afterwards and the outer
recorder still receives it.  Probed on an unmarked file
(tests/test_cache_hash_invalidation.py, -n 2) with and without
`-W error::UserWarning`: both '21 passed, 1 skipped, 1 warning', the
warning naming the nodeid in both.

ci.yml: a workflow_dispatch on master would have run dvc-lock-check and,
if dirty, the refresh job that commits and pushes.  The manual trigger is
for re-running tests, so dvc-lock-check is now gated on a push as well
as the ref.  `schedule` never ran it anyway (it fires on the default
branch, development).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 28a166b31 ci: workflow_dispatch so data-tests can be re-run without a release (GH #941 step 1)
`data-tests` was gated on `event_name == 'push'` to master / separate_data,
so it ran once per release and never again: every master run after the
2026-09-17 failure (52 EHCVM food_acquired failures in test_ehcvm_7day.py)
was a `schedule` event at the same sha with "data-tests skipped".  Nothing
could re-exercise the job against development without cutting a release.

Adds `workflow_dispatch` to the triggers and admits that event in the
data-tests gate.  The ref is chosen in the dispatch UI, so no inputs.  The
concurrency group already keys on event_name, so a dispatch does not
cancel a push run.  Pure CI config: no cache cost.  PR #973 (the other
open edit to this file) is already merged (9827c727d), so no sequencing.

Step 2 of #941 -- the sequential cold-session diagnosis -- is NOT done
here; this only makes the job runnable on demand.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- f319c16cf tests: mark the 56 files that reached a credential-less skip at run time with requires_s3 (GH #907)
Each file in GH #907's list contained no `requires_s3` string.  Every one
of their 1,248 credential-less skips was decided AFTER the test ran and
waited out DVC -> S3 (~70-80 s per test here).  The mark decides the same
skip at collection time.

Decided per test, not per file, from a credential-free run (no in-tree
or user creds, no boto creds, empty LSMS_DATA_DIR, LSMS_SKIP_AUTH=1 --
the unit-tests job's environment) with a per-test junit record:

  - 16 files where every test skipped: module-level `pytestmark`
    (five of them already had a pytestmark; it is now a list carrying
    the existing mark plus requires_s3 -- none removed).
  - 40 files that mix data-free and data-dependent tests: the mark is
    on each wholly-skipped class (TestDelivered etc.) or function.  No
    function had parametrizations with different outcomes, so no
    finer grain was needed.  Data-free tests are untouched and still run.

Verified on the 56 files, same environment, fresh empty root each time:

  before: 660 passed, 1248 skipped, 55 warnings in 825.22s (0:13:45)   WALL 826.72 s
  after:  660 passed, 1248 skipped, 26 warnings in 69.15s (0:01:09)    WALL 70.63 s

Per-test junit diff: 1,908 identical ids, 0 outcome changes; summed
per-test time 7,094 s -> 68 s; UnmarkedS3TestWarning count after: 0.

Residual (not a skip, so not this issue): test_locality_deprecation.py::
test_locality_emits_deprecation_warning PASSES without data but still
waits ~50 s on the network before its broad `except` fires.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 62a3b453a tests: the credential-less run-time net now warns, naming the unmarked test (GH #907)
The makereport net in tests/conftest.py converts a NoCredentialsError into
a skip AFTER the test has run and waited out DVC -> S3.  That is the
expensive skip path (~75 s per test here, measured on the 56 unmarked
files: 1,248 skips in 827 s wall at -n 40).  A requires_s3 mark decides
the same skip at collection time for free.

The net still converts exactly as before; it additionally emits an
UnmarkedS3TestWarning (a UserWarning subclass) naming the item's nodeid,
so the warnings summary of a credential-less run lists every test that
still relies on the net and the unmarked list cannot regrow silently.
Verified under xdist (-n 2) on test_india_employment_activity_index.py:
'1 passed, 6 skipped, 1 warning', the warning carrying the nodeid.

Also item.fspath -> item.path (the former is deprecated).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


