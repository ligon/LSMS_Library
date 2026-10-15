# RED-TEAM review of branch `fix/grader-scale-830` @ 73b06e97b (element `grader-scale-830`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/grader-scale-830` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Uganda; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/grader-scale-830/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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
Warn-only cross-wave median-ratio check on `monetary: true` columns in `coverage_matrix.grade_feature`; it must have flagged Uganda 2015-16 Value_sold (#829, now guarded) -- reproduce that on a synthetic frame with the pre-guard values. Do not change any grade; it reports. Read-path.

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #830 -- Coverage grader is blind to scale: a monetary column 100x off in one wave grades sane -- add a cross-wave median-ratio check on monetary columns
```json
{
 "tier": "C",
 "type": "framework",
 "tractability": "auto",
 "dedup_status": "open (the instance #829 is CLOSED -- Uganda 2015-16 a5aq8 is now guarded in the wave script -- but no scale check exists in the grader)",
 "priority": 3,
 "summary": "Claim holds: grep -i 'scale|monetary|median' in lsms_library/coverage_matrix.py and diagnostics.py = 0 hits; the check list diagnostics.py:261-654 is all shape/null/dtype. data_info.yml declares `monetary: true` on 8 columns (ValuePerAnimal, HerdValue, SalesValue, PurchaseValue, Expenditure, Price, ...), so the spec's 'no per-table list' is satisfiable. One design constraint: coverage_matrix.grade_feature (:542-732) grades each wave by slicing t and calling is_this_feature_sane on the slice, so a cross-wave ratio must be computed in grade_feature where all waves are present, not inside the per-slice check. The '[1/5, 5] unless the currency re-denominated' clause needs a redenomination list (GhanaLSS 2007 cedi) -- CLAUDE.md's 'no allowlist' doctrine applies to known-bad cells, not to a currency fact, but the author should say which this is.",
 "evidence": "lsms_library/diagnostics.py:261-705; lsms_library/coverage_matrix.py:542-742; grep -c 'monetary: true' lsms_library/data_info.yml -> 8; gh issue view 829 -> CLOSED",
 "approach": "In coverage_matrix.grade_feature, after the per-wave loop, for each column with `monetary: true` in data_info.yml compute median per t (per j where a j level exists) and the ratio to the median of adjacent waves; attach a `warn: scale_ratio` note to the cell when outside [1/5, 5] and the wave is not a declared redenomination; expose via bench/feature_audit/scan.py as a `scale` finding; docs/guide/coverage.md paragraph. No tier change.",
 "verify": "tests/test_coverage_matrix.py with a synthetic two-wave frame (100x) that must flag and a 1.2x one that must not; re-run the grader on Uganda crop_production with the #829 guard disabled as the positive control. CACHE COST: none (coverage_matrix.py / diagnostics.py are read-path and reach no build fingerprint; assert with tests/test_build_transform_hash.py).",
 "contents_org": "n/a (framework); the motivating idiosyncrasy is Uganda/_/CONTENTS.org's 2015-16 a5aq8 /100 defect (#829, now guarded).",
 "blocked_by": "a redenomination list (which waves may legitimately jump)",
 "decision_for_human": "Is a currency-redenomination table (GhanaLSS 2007 and any others) an acceptable exception list for this check (yes/no)?"
}
```


## The branch: diffstat (vs development)
```
 .coder/ledger/830-grader-scale.md |  54 +++++++++
 bench/feature_audit/scan.py       |  35 ++++++
 docs/guide/coverage.md            |  43 +++++++
 lsms_library/coverage_matrix.py   | 235 +++++++++++++++++++++++++++++++++++++-
 tests/test_coverage_matrix.py     | 196 +++++++++++++++++++++++++++++++
 5 files changed, 562 insertions(+), 1 deletion(-)
```
## The branch: commit messages
--- 73b06e97b coverage grader scale check: compare currencies after NA-normalisation (GH #830)
currency_for() returns pd.NA for a country absent from data_info.yml's
`Currency:` block (Afghanistan is the one configured country without an
entry), and `pd.NA != pd.NA` is NA -- so the redenomination-boundary test
raised TypeError, which grade_feature's report-only guard swallowed into a
RuntimeWarning (check silently inert for that country) and scan.py filed as
a severity-A `scale` error.  Resolve each wave's code once, map NA -> None,
and compare those: two unknown codes are the same unknown unit and the
pair is compared.  Test added; docstring now also states that the printed
neighbour medians are the pooled levels while the ratio may be per-item.

tests/test_coverage_matrix.py + tests/test_build_transform_hash.py:
97 passed (96 -> 97).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 59a110755 coverage grader: warn-only cross-wave scale check on monetary columns (GH #830)
Every per-slice readiness check in diagnostics is a shape check; a monetary
column 100x off in ONE wave (Uganda crop_production 2015-16 Value_sold,
GH #829) graded `sane`.  grade_feature now computes, once on the whole
country frame, each wave's median (positive values) of every monetary column
and compares it to its comparable adjacent waves; a wave outside [1/5, 5]
against every comparable neighbour gets a `warn: scale_ratio[...]` note in
the cell's `detail`.  No tier changes -- it reports.

- monetary columns come from currency._monetary_columns (seed + `monetary:
  true` flags).  crop_production.Value_sold carries NO yaml flag and is
  monetary only through the seed, so a yaml scan would miss the #829 column.
- comparable pairs: adjacent waves with the same ISO code per
  currency_for() -- a redenomination boundary (GhanaLSS 2007, Tajikistan
  2000, Peru 1991) is skipped.  A currency fact already in data_info.yml,
  not a known-bad-cell allowlist; there is no list of cells.
- statistic: per-item median of ratios where a `j` level has >=3 shared
  items with support, else pooled medians; support floor 30 (Site Q's);
  tolerance 5 is a STATED tolerance.  Edge waves (one neighbour) are
  reported only when that neighbour is not itself the outlier; a two-wave
  frame reports both.  A persistent break (A,B vs C,D) is not seen -- stated.
- bench/feature_audit/scan.py emits the same records as `scale` findings
  (severity C, mirrors null_content).
- docs/guide/coverage.md paragraph; .coder/ledger/830-grader-scale.md.

Controls on the warm Uganda crop_production frame (8 waves): served frame
0 breaks (adjacent ratios 1.0-1.7); with the #829 x100 guard undone in
memory, exactly 2015-16 Value_sold is reported: median 960 (n=6,882) vs
90,000 / 120,000, ratio 0.0098 over 21 shared crops.  Cost 63 ms.

Cache cost: none -- coverage_matrix.py / scan.py are read-path and reach no
build fingerprint (tests/test_build_transform_hash.py green).

tests/test_coverage_matrix.py + tests/test_build_transform_hash.py:
96 passed (85 before + 11 new).  tests/test_currency*.py: 70 passed.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


