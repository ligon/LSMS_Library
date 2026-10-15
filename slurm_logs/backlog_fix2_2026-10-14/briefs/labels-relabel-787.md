# Backlog fix element `labels-relabel-787` -- issues #787

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
- Commit as you go; end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/labels-relabel-787` (origin is the shared mirror; no GitHub push, no PR from here).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org`.
- Read `CLAUDE.md` section headings and the sections your files touch; read `.claude/skills/backlog-workflow/SKILL.md` for the rule that an open issue must describe exactly the remaining work: if your fix resolves PART of an issue, DRAFT the residual (title + body + tier) in your report.

## Scope note from the scrum master
Diagnose-then-auto, p2. Do ONLY the 'no decision needed' parts the record lists for `Country._relabel_j`; the Original-path last-row-wins and the per-country Aggregate curation are decisions -- draft them. Read `.claude/skills/add-feature/food-acquired/aggregate-labels/SKILL.md` BEFORE touching `labels=` (the Contract B degrade is the API contract). Read-path only: prove 0 hashes move.

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #787 -- labels= honours its flagship request on 4 of 22 food countries, silently accepts every crosswalk column, and its Original path is last-row-wins
```json
{
 "tier": "B",
 "type": "bug",
 "tractability": "diagnose-then-auto",
 "dedup_status": "partial (finding 9 blank->'' fixed by 733a4499a GH #925; finding 6's '18 raise' shrank by six countries via PR #789/#783; findings 1, 2, 4 unchanged; finding 5 changed shape)",
 "priority": 2,
 "summary": "country.py:3071-3192 today: rdict is still a dict comprehension over all rows (:3187), so a Preferred Label mapped to >1 variant is last-row-wins (Niger 'Original': 153 conflicting keys, reproduced config-only). Unmatched j still passes through with no signal. labels='Code' still silently turns j into integers (reproduced on Uganda: j -> [100,101,102]). labels='Preferred Label' no longer raises -- but only by accident: table[['Preferred Label','Preferred Label']] yields duplicate columns, zip iterates column NAMES, rdict becomes {'Preferred Label':'Preferred Label'} and the rename is an identity (reproduced: j unchanged); a pandas change would break it. The ' Label'-suffix normalisation proposed in the thread is not implemented. Contract B (Feature degrade) is settled and untouched. _relabel_j is NOT in _build_registry._EXCLUDED_CALLABLES.",
 "evidence": "sed -n 3071,3200p lsms_library/country.py; config-only run in the private root: Niger conflicting Preferred Labels = 153; Country('Uganda')._relabel_j on a 3-row synthetic frame for 'Preferred Label'/'Bogus'/'Code'/'Aggregate'; git log -S\"str(v).strip() != ''\" -> 733a4499a; python: '_relabel_j' in _build_registry._EXCLUDED_CALLABLES -> False.",
 "approach": "In Country._relabel_j (country.py:3071-3192), no decision needed: (a) normalise the argument -- strip a trailing ' Label' before the (None,'Preferred') no-op test, so 'Preferred Label' is a deliberate no-op; (b) after the rename, count j values absent from rdict and attach attrs['labels_unmatched'] plus a warning (or raise under an env lever, mirroring the other guards); (c) document the accepted-value surface and refuse integer-typed targets unless explicitly asked. Needs a yes/no: (d) raise LabelUnavailableError (or a plain error) when a Preferred Label maps to more than one distinct variant value, since coarse->fine is inexpressible. Tests: extend tests/test_label_selection.py / tests/test_food_labels.py with the Niger duplicate-key and pass-through cases.",
 "verify": "pytest tests/test_label_selection.py tests/test_food_labels.py tests/test_ghanalss_aggregate.py; config-only sweep over the 22 food countries. CACHE COST: expected zero -- _relabel_j runs after the load in the generated method, reached from neither grab_data nor _aggregate_wave_data -- but it is not in _EXCLUDED_CALLABLES, so probe Country._table_cache_hash before/after (stash-probe recipe) and add it to the exclusion list if the probe moves.",
 "contents_org": "n/a (framework). The aggregate-labels skill (.claude/skills/add-feature/food-acquired/aggregate-labels/SKILL.md) is the contract of record and forbids converting the malformed-table / no-j cases to LabelUnavailableError; (a)-(d) above do not touch those cases.",
 "blocked_by": "none for (a)-(c); yes/no for (d)",
 "decision_for_human": "Should _relabel_j REFUSE (raise) when one Preferred Label maps to more than one distinct target value, instead of serving last-row-wins -- yes/no?"
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
