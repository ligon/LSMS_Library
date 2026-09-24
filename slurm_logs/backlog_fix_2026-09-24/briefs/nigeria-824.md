# Backlog fix element `nigeria-824` -- dispatch-plan step 11: issues #824, #867

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/nigeria-824` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Country rule
BEFORE editing anything, read `lsms_library/countries/Nigeria/_/CONTENTS.org` -- prose AND the LOGBOOK/WAITING/TODO entries -- and NAME, in your report, the idiosyncrasy that bears on this task. A finding that contradicts CONTENTS.org is a result to quote, not a licence to assume the file is stale. For any script-path table (`materialize: make`) also read the script's docstring first.

## Scope note
W1/W2 only (W3-W5 are gated on #843). The #867 rider is the 'Other Inorganic Fertilizer' SPELLING only; the nominal-N chemistry is an open escalation and out of scope. Nigeria is post-planting/post-harvest: read .claude/skills/add-feature/pp-ph/SKILL.md before touching crop_production.

## The task (from the triage's dispatch plan)
**What:** Nigeria: add `unit_sold` to crop_production_for_wave frames (nigeria.py:368/451), pass sa3q11b from _/crop_production.py:118,132 for W1/W2, canonicalise through the harvest u table (value-label vocabulary). Also fold the #867 Nigeria 'Other Inorganic Fertilizer' spelling into `_NITROGEN_CONTENT` (transformations.py, no cache cost) as a rider.

**Verify:** Unit_sold agreement 96.9% / 95.3% reproduced (CONTENTS.org:976-1000); W3-W5 untouched. Cost: one Nigeria crop_production re-warm (PP/PH -- read pp-ph skill first).

**Blocked by (as triaged):** #843 for W3-W5 (OPEN; out of scope here).

## Triage records for the issues in scope

### #824 -- tier B, diagnose-then-fix, dedup partial
- country: Nigeria, Niger (open); Uganda, Malawi, GhanaLSS (done)
- approach: Nigeria: add `unit_sold` to `crop_production_for_wave` frames, pass sa3q11b from `_/crop_production.py` for W1/W2, canonicalise through the harvest u table (value-label vocabulary, per @ligon's trap warning). Niger: read the b-column in all four wave scripts and re-key the sale aggregation on (i, crop, sold unit) before any sum, then carry Unit_sold; both per the settled Uganda contract (flag, not identification). Consider splitting into two country issues and closing this one.
- verify cost: one-country-rebuild
- CONTENTS.org: Nigeria/_/CONTENTS.org:976-1000: 'The SALE has its own unit code (=sa3q11b=), and =u= is the HARVEST unit ... =_/crop_production.py= passes =unit='sa3q6b'= ... but never =sa3q11b= -- and =nigeria.crop_production_for_wave= documents an =unit_sold= parameter in its docstring that the code never reads' (96.9% / 95.3% agree). Niger/_/CONTENTS.org:785-800: 'every wave records the sold unit in its OWN column that the feature drops: =as02eq12b= (2011-12), =AS02EQ12B= (2014-15 AS2E2), =s16cq16b= (2018-19), =s16dq05b= (2021-22) ... 2011-12 98.8%, 2014-15 89.4%, 2018-19 93.1%, 2021-22 63.0% ... any per-unit use ... must first restrict to rows where the two agree'; plus :685 the Unknown-u interaction. Uganda/_/CONTENTS.org (per-wave sold-unit column evidence, 2018-19 s5aq07c_1) records the landed half.
- evidence: gh issue view 824 comments (@ligon 2026-09-08, 09-09, 09-10); commits 66c1ff6ab, 2bb32afec, 85947ff52 (git show), 42e1c19e; lsms_library/data_info.yml:784-865; Uganda/_/data_scheme.yml:182-224; tests/test_uganda_sold_unit.py; Nigeria/_/nigeria.py:368,385,451; Nigeria/_/crop_production.py:118,132; Nigeria/_/CONTENTS.org:976-1000; Niger/2014-15/_/crop_production.py:54-70; Niger/2018-19/_/crop_production.py:45; Niger/_/CONTENTS.org:785-800; warm parquet schema probe (Unit_sold only in GhanaLSS); #843 OPEN, #848 OPEN, #868 OPEN
- summary: Partially resolved with the design settled. Landed: Uganda `Unit_sold` + `Condition_sold` as reported columns (`66c1ff6a`, red-team `2bb32afe`, merge `42e1c19e`; `Uganda/_/data_scheme.yml:182-224`; 21 references in tests/test_uganda_sold_unit.py; Uganda warm parquet absent at probe time so verified from config+tests, not cache); Malawi (`85947ff5`, a DIFFERENT shape -- `_sale_block` maps ag_i02b/ag_q02b and merges sale onto harvest on (i, crop, u), so no `Unit_sold` column and the ratio is per the row's own u by construction); GhanaLSS carries `Unit_sold` (ghanalss.py:1052; warm parquet True); canonical `data_info.yml:784-865` declares `Unit_sold`/`Condition_sold` optional with @ligon's decisions (column not index level; disagreement is a data-quality FLAG not an identification -- Unit_sold is the closer denominator only 40.5%; take the unit from the survey's value-label vocabulary, never a condition column that merges cleanly). Still open, as @ligon's 2026-09-10 comment says: Nigeria -- `nigeria.crop_production_for_wave(t, frames, crop_labels)` (nigeria.py:368) reads `fr['qty_sold']` at :451 but has no `unit_sold` handling despite the docstring at :385; `_/crop_production.py:118,132` pass sa3q11a/sa3q12 and never sa3q11b (Nigeria/_/CONTENTS.org:976-1000; W1/W2 tractable now, W3-W5 has no sale price at all per open #843). Niger -- worse than 'drops the unit': `Niger/2014-15/_/crop_production.py:63-66` discards AS02EQ12B then `groupby(['i','crop']).sum(min_count=1)` SUMS Quantity_sold across sale rows in different units (2018-19/2021-22 likewise read s16cq16a/s16dq05a without the b column), so the fix is a re-key on the sold unit, not a column append. Warm parquets: 13 of 14 crop_production declarers lack Unit_sold (Malawi by design).

Read the full issue and its comments first: `gh issue view 824 --comments`.

### #867 -- tier C, human, dedup open
- country: Ethiopia, Malawi, Guinea-Bissau (plus organic labels in Tanzania, Mali, Benin, Burkina Faso, CotedIvoire, Niger, Senegal, Togo, Nigeria)
- approach: Maintainer records cited shares for `nps` and `d compound` and writes the organic policy into the `_NITROGEN_CONTENT` comment; an agent can separately add the Nigeria `other inorganic` spelling and a Guinea-Bissau `harmonize_input` (or Portuguese keys) -- no cache cost, transformations.py reaches no fingerprint.
- verify cost: warm-read
- CONTENTS.org: Ethiopia/_/CONTENTS.org:1315-1322 'W3 (2015-16): … NPS INTRODUCED … DAP nearly disappears by W5 (3 rows) as NPS displaces it' -- an NPS share decision governs most of Ethiopia's later-wave nitrogen; :1331-1333 'NO nitrogen_kg / seed_kg sums … those are transformations'. Guinea-Bissau CONTENTS.org is silent; its deliberate no-harmonize policy is in data_scheme.yml:189-196 ('has NEITHER harmonize_input NOR harmonize_seed_crop, so the raw Portuguese input labels … pass through unchanged (visible / flagged)'). Malawi CONTENTS.org silent on D Compound.
- evidence: lsms_library/transformations.py:4507-4523, :4537-4556, :4600-4632, :4660-4680; warm-parquet probe 2026-09-24 (Ethiopia/Tanzania/Guinea-Bissau/Mali/Benin/Nigeria plot_inputs; Malawi, Uganda absent); Malawi/_/categorical_mapping.org:845; Guinea-Bissau/_/data_scheme.yml:189-196; commits 01b81d4ce, 610538644; docs/releases/v0.11.1.md:49-51; .coder/ledger/863-transforms.md:344-349; slurm_logs/2026-09-16_panel_estimation/CANDIDATES.org:228-230
- summary: `_NITROGEN_CONTENT` (transformations.py:4507-4523) still has no entry for NPS, D Compound, any organic input or Guinea-Bissau's Portuguese labels; the code deliberately keeps them unmatched and loud (transformations.py:4552-4556, 4625-4632). Re-probed at HEAD against warm plot_inputs parquets with `_normalise_input_label`: Ethiopia `NPS` 5,264 / `Manure` 27,261 / `Compost` 4,295; every Guinea-Bissau label incl. `Adubos inorgânicos - ureia` 190 (real urea); Tanzania `Organic Fertilizer` 727; Mali Manure/Compost 11,148/2,673; Benin organics 271/158 -- unchanged from the issue. Malawi's warm parquet is absent, so `D Compound Fertilizer` is confirmed only as a label (Malawi/_/categorical_mapping.org:845), not at runtime. New finding not in the issue: Nigeria `Other Inorganic Fertilizer` (170 rows) is unmatched -- the strip rule yields `other inorganic`, not the key `inorganic fertilizer` (0.20) -- a spelling-shaped miss, not a content decision. No PR references #867; v0.11.1 notes (docs/releases/v0.11.1.md:49-51) and the 863 ledger (.coder/ledger/863-transforms.md:344-349) file it as the deferred content half of #863 (closed). The core ask (sourced nominal N shares for NPS ~19% and D Compound, and an organic policy) is a citation-backed human decision; GNB label harmonisation and the Nigeria spelling are mechanical sub-parts.

Read the full issue and its comments first: `gh issue view 867 --comments`.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
