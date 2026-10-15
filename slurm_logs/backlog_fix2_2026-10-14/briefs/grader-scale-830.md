# Backlog fix element `grader-scale-830` -- issues #830

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
- Commit as you go; end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/grader-scale-830` (origin is the shared mirror; no GitHub push, no PR from here).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org`.
- Read `CLAUDE.md` section headings and the sections your files touch; read `.claude/skills/backlog-workflow/SKILL.md` for the rule that an open issue must describe exactly the remaining work: if your fix resolves PART of an issue, DRAFT the residual (title + body + tier) in your report.

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
