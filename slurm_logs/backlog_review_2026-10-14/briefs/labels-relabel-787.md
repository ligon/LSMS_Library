# RED-TEAM review of branch `fix/labels-relabel-787` @ 6922c077a (element `labels-relabel-787`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/labels-relabel-787` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Uganda,Malawi,Tanzania; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/labels-relabel-787/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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


## The branch: diffstat (vs development)
```
 docs/guide/data-methods.md                   |  36 ++++++
 lsms_library/countries/Serbia/_/CONTENTS.org |  15 ++-
 lsms_library/country.py                      | 167 ++++++++++++++++++++++++++-
 tests/test_food_labels.py                    | 113 ++++++++++++++++++
 4 files changed, 323 insertions(+), 8 deletions(-)
```
## The branch: commit messages
--- 6922c077a labels=: point LabelPassThroughWarning at the caller; regenerate data-methods.md
stacklevel=4 so the warning is attributed to the user's line on both the
registered-table and derived-food paths (frames: _audit_label_pass_through
-> _relabel_j -> generated accessor -> caller).  docs/guide/data-methods.md
is generated from country.py's doc_parts and pinned by
tests/test_api_discoverability.py, so the docstring edit for GH #787 (a)-(c)
is regenerated into it (`scripts/gen_data_method_docs.py`).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- dd0363fb6 labels=: normalise ' Label', report pass-through, refuse numeric targets (GH #787 a-c)
Country._relabel_j, read path only (0 of 191 probed _table_cache_hash /
build_transforms_fingerprint values moved; probe in the element's STAGE dir):

(a) A trailing ' Label' is stripped before the (None, 'Preferred') no-op
    test, so 'Preferred Label' is the same deliberate no-op as 'Preferred'
    (it used to survive only by accident: table[['Preferred Label',
    'Preferred Label']] is a duplicate-column frame whose zip iterated
    column NAMES, giving an identity rdict) and 'Aggregate Label' is
    'Aggregate'.  Verified warm: Uganda food_expenditures(labels=
    'Preferred Label') frame-equals the default read.
(b) A j absent from the rename dict passes through unrenamed; that is now
    counted BEFORE the irreversible reaggregate sum and reported on
    result.attrs['labels_unmatched'] (n_unmatched, n_values, rows,
    row_share, examples) with a LabelPassThroughWarning, fatal under the
    new LSMS_LABELS_STRICT lever ({1,true,yes}, like the other guards).
    NaN j is not counted (Site I's concern).  No allowlist, no threshold.
    Warm: Malawi labels='2004-05' -> 32 of 147 j unrenamed on 42,206 rows
    (6.2%); Uganda/Malawi 'Aggregate', Tanzania 'FTC' -> 0, silent.
(c) A NUMERIC crosswalk column (int or float: Code, FTC Code, FDC ID, the
    EthiopiaRHS/Panama year columns) is refused with LabelUnavailableError
    (degradable by Feature with one aggregated warning, like a typo of the
    variant name).  Uganda labels='Code' served j = [101, 141, 1082] as
    int64.  Mali's str-typed 'Code' column is still accepted: by dtype,
    not by name.  Serbia/_/CONTENTS.org's "labels='proizvod' yields 101"
    note is corrected; the same sentence in its categorical_mapping.org
    is a hashed build input and is left as written.

Not done here (decisions, drafted in the element report): a Preferred
Label mapping to >1 distinct target value is still served last-row-wins
(Niger 'Original' 153 keys, Senegal 134, Burkina 72; under 'Aggregate'
only Uganda 'Cassava (flour)' and Nigeria 'Unground Ogbono').

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


