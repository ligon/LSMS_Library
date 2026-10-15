# Backlog fix element `malawi-bundle` -- dispatch-plan step 9: issues #833, #839

You are a worker agent in a Slurm job with its OWN clone of `development` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) into which the warm parquets of your countries were copied; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else, and the live cache must not be touched.

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/malawi-bundle` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Country rule
BEFORE editing anything, read `lsms_library/countries/Malawi/_/CONTENTS.org` -- prose AND the LOGBOOK/WAITING/TODO entries -- and NAME, in your report, the idiosyncrasy that bears on this task. A finding that contradicts CONTENTS.org is a result to quote, not a licence to assume the file is stale. For any script-path table (`materialize: make`) also read the script's docstring first.

## Scope note
Malawi items of #833 and the Malawi comment correction of #839 only; the Mali half of #839 is gated elsewhere and out of scope.

## The task (from the triage's dispatch plan)
**What:** ONE Malawi re-warm: wire ag_i02c (sale-side S/U) into the sale merge (malawi.py:1256 'ag_i02c is unwired'); give `Boiled groundnuts` its own label (categorical_mapping.org:90); reconcile the Cassava (flour) two-code-scheme note where it applies; correct Malawi data_scheme.yml:191/:433 JOIN claim in the same commit (#839 rider).

**Verify:** harvest/sale shelled-unshelled basis agreement counts before/after (CONTENTS.org:201-217); Groundnut no longer receives vendor-snack rows; Malawi crop_production/food_acquired build clean. Cost: one Malawi cold rebuild.

**Blocked by (as triaged):** nothing

## Triage records for the issues in scope

### #833 -- tier B, diagnose-then-fix, dedup partial
- country: Malawi, Uganda, Tanzania, Niger, Ethiopia
- approach: Split per country, each a one-country config edit plus re-warm: (1) Malawi -- wire ag_i02c into the sale merge and give `Boiled groundnuts` its own label as `Sweet Potato - Boiled (Vendor)` already has; (2) Tanzania -- give harmonize_crop harvest-form labels (Coffee cherry, Cashew in-shell, ...) instead of the food label, and record the reuse in CONTENTS.org; (3) Niger -- extend harmonize_food over the drift pairs and decode 166 from the 2021-22 questionnaire (add a CONTENTS entry: none exists); (4) Uganda -- @ligon decides whether `Aggregate` buckets are for demand estimation (beta-spread, flour may stay) or price comparison (split flour out); the price study should use Preferred Labels either way; also reconcile Cassava (flour) across the two code schemes; (5) Ethiopia -- fold into #832's unit sweep.
- verify cost: one-country-rebuild
- CONTENTS.org: Malawi CONTENTS.org:183-194: 'harmonize_food maps the Preferred Label Groundnut <- Boiled groundnuts in 2016-17 and 2019-20 -- a vendor snack ... NOT fixed: splitting the label cold-rebuilds Malawi food_acquired.' and :201-217: 'Half fixed 2026-09-09 (GH #854) ... ag_i02c ... is still unread, so the shelled/unshelled mismatch between harvest and sale is still a basis error ... Wiring ag_i02c would let those attach again, correctly, and is the natural follow-up.' Tanzania and Niger CONTENTS.org carry NO entry on the harmonize_crop label reuse or the j drift pairs / code 166 (grep for harmonize_crop, Choux, 166 returns nothing) -- a genuine idiosyncrasy to add. Uganda CONTENTS.org records the 141/741 fix (commit 6451615b3 added 85 lines) but nothing on the flour-in-grain Aggregate fold.
- evidence: lsms_library/countries/Uganda/_/categorical_mapping.org:35,100,113,122-124,739,771; commit 6451615b3; lsms_library/countries/Malawi/_/malawi.py:1036-1112 (_harvest_block reads condition), :1256 ('ag_i02c is unwired'); Malawi categorical_mapping.org:90,93,140,144,148; Malawi CONTENTS.org:183-217; Tanzania categorical_mapping.org:671-684; pyarrow read of /global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/Niger/var/food_acquired.parquet (j x t counts above; 178 distinct j); Niger categorical_mapping.org:490-491,564-565,740-742,1206; .claude/skills/add-feature/food-acquired/SKILL.md:415-445; gh issue 854 CLOSED 2026-09-10, 869 CLOSED 2026-09-13, 832 OPEN; slurm_logs/price_sources/DATA_PROBLEMS.csv (P09 rows present)
- summary: Umbrella over seven per-country label folds; three items have landed, four have not, and one turns out to be a design question rather than a bug. Landed: Uganda harmonize_crop 141->Millet / 741->Matoke (6451615b3 on development, categorical_mapping.org:739,771); Malawi harvest `condition` is now a real index level read from ag_g13c (GH #854 closed, f4f9bdcb4; warm crop_production.parquet index is (t,i,plot_id,crop,u,condition)); Malawi perennial recode (#869 closed, 43d2900fb). Still open at HEAD: Malawi ag_i02c (sale-side S/U) unread, so the harvest/sale shelled-unshelled basis error persists (CONTENTS.org:208-217); Malawi `Boiled groundnuts` -> `Groundnut` fold (categorical_mapping.org:90 vs :93,:140,:144), deferred for cache cost while malawi.py was edited nine times since 09-08; Tanzania harmonize_crop reuses the FOOD labels Seeds/Nuts/Coffee & Cocoa/Tea (dry) for the harvested crop (categorical_mapping.org:671-684) with no CONTENTS entry; Niger drift pairs are in SERVED output (measured on the warm parquet: Chou 628 rows 2018-19 vs Choux 973 rows 2021-22; 'Aubergine,' 452 vs 'Aubergine, Courge/Courgette' 1156; three Sucre spellings 4883/80/5462) and the undecoded code '166' is served as a j label on 468 rows in 2021-22 (the only `166` in the mapping file is in harmonize_assets:1206). Uganda Aggregate folds Maize (flour)->Maize (:35,:122-123) and Millet Flour->Millet (:124); but the bucket contract is the CFE beta-spread test (add-feature/food-acquired/SKILL.md:415-445), not a raw/processed rule, so splitting flour out is a design decision, not a defect -- and a new instance the issue missed: code 182 `Cassava (flour)` -> Preferred `Cassava` (:100) while 1082 -> `Cassava (flour)` (:113), so Cassava is only 'clean' in one code scheme. Ethiopia's u-axis fold is unadjudicated and belongs with #832.

Read the full issue and its comments first: `gh issue view 833 --comments`.

### #839 -- tier C, auto, dedup partial
- country: Mali, Tanzania, Malawi
- approach: Correct Mali data_scheme.yml:59-65 and Malawi data_scheme.yml:191/:433 now, bundled with the next substantive edit or on their own -- @ligon's ruling in PR #918 ('rebuilding cache on this 32 core cluster is not a big deal') already covers the re-warm. Separately decide whether to canonicalise data_scheme.yml before hashing (yaml.safe_load -> stable dump): it removes the standing incentive to leave false comments in place, at the price of one corpus-wide re-warm when the payload format changes, and it does nothing for label edits that genuinely change data.
- verify cost: one-country-rebuild
- CONTENTS.org: Mali CONTENTS.org:897-908: 'The data_scheme.yml comment on 2014-15 food_acquired is STALE ... All three claims are false as of GH #380 ... Left uncorrected on purpose: a comment change in data_scheme.yml moves every Mali table's cache hash ... Correct it when that file is next edited for another reason.' Tanzania CONTENTS.org:578-586: '** DONE Missing food quantities in 2019-20 and 2020-21 (#112) ... Resolved: the current build carries the quantities ... non-null on 97.6% of 2019-20 rows and 98.5% of 2020-21 rows'. Note the Mali entry's own condition (next edit) has since been met twice without the correction landing.
- evidence: lsms_library/countries/Mali/_/data_scheme.yml:59-65; lsms_library/countries/Mali/_/CONTENTS.org:897-908 (commit 477b1a6e7); lsms_library/countries/Tanzania/_/CONTENTS.org:578-620 (commit 522458fa2); lsms_library/country.py:3558-3566 (cscheme= whole-file hash); git log --since=2026-09-08 -- Mali/_/data_scheme.yml Mali/_/Makefile -> 57d2ffafd, cd5172557; lsms_library/countries/Malawi/_/data_scheme.yml:191,433; gh issue 113 CLOSED 2026-06-15; origin/fix/prices-mali (ad5119e32) and origin/fix/prices-tanzania (38e988ee3) are the unpushed branches the issue names, both superseded by the development commits
- summary: Tanzania half is DONE on development: 522458fa2 closed the #112 WAITING entry (CONTENTS.org:578 is now '** DONE ... CLOSED [2026-09-08]', with the measured non-null counts at :590-600 and the statement that the Critical line is FALSE at :613); #113 was already closed 2026-06-15, so 'decide whether #113 is still wanted' is moot. Mali half is HALF done: the correction landed in CONTENTS.org (477b1a6e7, :897-908) but data_scheme.yml:59-65 still carries all three false claims ('incorrectly maps Quantity: s13q02', 'legacy non-canonical shape', 'mixes shapes across waves'). It was deferred because cscheme= hashes the whole file (country.py:3566) -- and that deferral has already failed: Mali's hash inputs moved on 2026-09-10 (57d2ffafd touched data_scheme.yml for plot_id) and 2026-09-14 (cd5172557, Makefile), so the country re-warmed twice with the false comment surviving. The third instance, Malawi data_scheme.yml:191 and :433 ('JOINS food_acquired.j'; 17 of 41 do not), is untouched. The framework ask (hash parsed YAML so comment edits are free) is a design choice, not a root fix.

Read the full issue and its comments first: `gh issue view 839 --comments`.

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
