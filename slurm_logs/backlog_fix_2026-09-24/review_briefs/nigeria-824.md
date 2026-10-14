# RED-TEAM review of branch `fix/nigeria-824` (element `nigeria-824`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/nigeria-824` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Nigeria built from `development`; `dvc-cache` in it is a read-only symlink to the shared blob store. `development` is available as `origin/development` in your clone (`git diff --stat origin/development..HEAD`).

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

## The task the fix agent was given
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


## The branch: diffstat (vs development)
```
 lsms_library/countries/Nigeria/_/CONTENTS.org      |  84 ++++-
 .../countries/Nigeria/_/crop_production.py         |  25 +-
 lsms_library/countries/Nigeria/_/data_scheme.yml   |  27 ++
 lsms_library/countries/Nigeria/_/nigeria.py        |  39 ++-
 lsms_library/transformations.py                    |  10 +
 tests/test_nigeria_sold_unit.py                    | 360 +++++++++++++++++++++
 6 files changed, 534 insertions(+), 11 deletions(-)
```
## The branch: commit messages
--- d568ab428 Nigeria CONTENTS.org: Unit_sold reproduced on the API frame; #867 alias measured on plot_inputs
The served frame matches the parquet counts exactly (3,451/3,374 and
3,411/3,283; Unit_sold - u = {Olodo}), so the read-path mapping passes do
not separate u from Unit_sold.  The `other inorganic fertilizer` alias
measured on Nigeria's warm plot_inputs: 170 rows unmatched -> matched,
nitrogen_kg matched rows 10,014 -> 10,184, plots 4,359 -> 4,472, total
173,672 -> 178,074 kg N.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 8cc7eed2b Nigeria: carry the sale's own unit as crop_production.Unit_sold (W1/W2, GH #824)
The GHS-Panel post-harvest A3 module asks q6 (harvest) and q11 (sale) each
with its OWN quantity and PROD. UNIT CODE.  _/crop_production.py passed the
harvest unit (u) and the sold quantity/value but never sa3q11b, and
nigeria.crop_production_for_wave documented an `unit_sold` parameter the
code never read -- so Value_sold / Quantity_sold was filed under the
HARVEST unit by assumption on every row (Nigeria/_/CONTENTS.org:976-1000).

- crop_production_for_wave reads `unit_sold` on plot-grain sale frames,
  decodes it via `dec` and canonicalises through the SAME size-agnostic
  _canon_unit as `u`; emits Unit_sold as an NA-bearing `string` column
  (pinned to string on the all-NA W3-W5 waves, the KgFactor concat rule).
- _/crop_production.py passes unit_sold='sa3q11b' for W1 and W2 only.
  W3-W5 are untouched: their sale sits in secta3ii at hh-crop grain with no
  plot linkage (GH #843), the same reason their Quantity_sold is NaN.
- data_scheme.yml declares Unit_sold {type: str, optional: true}; the
  canonical data_info.yml already declares it.

Identified by the value-label vocabulary, not the variable label: sa3q11b's
Stata value-label set is IDENTICAL to the harvest unit's in both waves (W1
36 labels, W2 48); W1's varlabel reads "(11a)...(unit)", a slip.  No
sold-condition column exists in W1/W2 A3, so no Condition_sold.

Measured (isolated cold build vs the warm parquet): 62,844 rows unchanged,
every pre-existing column byte-identical; Unit_sold non-null 2011Q1 3,486 /
2013Q1 3,430 / W3-W5 0.  Agreement on positive-sale rows recording both
units: raw codes 3,438/3,548 (96.9%) and 3,316/3,478 (95.3%) -- CONTENTS.org
reproduced exactly; served size-collapsed labels 3,466 (97.7%) / 3,349
(96.3%); on the built table 3,374/3,451 (97.8%) / 3,283/3,411 (96.2%).
Consumer rule is the canonical one: a disagreement is a FLAG, not an
identification of the price denominator.

Framework read verified on the isolated root: the stale var parquet is
evicted (GH #866), make fails on the rule-less target, try_script runs the
country script and the read serves Unit_sold.

GH #867 rider (spelling only): `other inorganic fertilizer` added to
_NITROGEN_CONTENT as an alias of the existing `inorganic fertilizer` class
share (0.20).  Nigeria's 'Other Inorganic Fertilizer' label normalised to
`other inorganic` and matched nothing (170 rows).  Not a new number; the
NPS / D Compound / organic content decisions stay open on #867.
transformations.py reaches no build fingerprint, so no cache cost there.

Cache cost: nigeria.py and data_scheme.yml are in every Nigeria table's
L2-country and L2-wave hash, so all 24 Nigeria tables re-warm cold; only
crop_production's content changes.  Not paid on the shared cache here.

Tests: tests/test_nigeria_sold_unit.py (wiring, config via ast, value-label
identity, agreement counts, #867 alias).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


## The fix agent's report (a self-report -- refute it)
**SCOPE DEVIATIONS**
- `lsms_library/countries/Nigeria/_/data_scheme.yml` — not named by the task, but the column must be declared `optional: true` there or Site B and the required-column guard have nothing to exempt. Not on the stop-list.
- `tests/test_nigeria_sold_unit.py` — new test file, not named by the task. Not on the stop-list.
- `lsms_library/countries/Nigeria/_/CONTENTS.org` — the task explicitly asks for idiosyncrasies to be recorded there.

No stop-list file was touched. Nothing is left uncommitted.

**1. Commits and branch**

Branch `fix/nigeria-824`, pushed to `origin` (the shared mirror) and confirmed at `origin/fix/nigeria-824`:
- `8cc7eed2b` the fix (nigeria.py, crop_production.py, data_scheme.yml, transformations.py, CONTENTS.org, new test)
- `d568ab428` CONTENTS.org follow-up with the API-frame and #867 measurements

I amended the first commit once after pushing, which made the branch non-fast-forward. Force-push is forbidden, so I reset my own clone back to the pushed commit and added the amendment as the second commit instead.

**2. Verification**

Package identity assertion passed before any edit, and `find lsms_library/countries -name '*.parquet'` stayed at 0 throughout.

Pytest summaries, verbatim:
- `tests/test_nigeria_sold_unit.py` alone: `15 passed in 10.25s`
- Regression selection (new file plus `test_nigeria_kg_factor`, `test_863_transforms`, `test_schema_consistency`, `test_feature_canonical_index`, `test_uganda_sold_unit`, `test_build_transform_hash`, `test_derivations`): `496 passed, 8 skipped, 43 warnings in 476.21s (0:07:56)`. The skips are Malawi tables not being warm in my private root, unrelated to this change.

Before/after numbers, all measured on the isolated root:

| claim | before | after |
|---|---|---|
| `crop_production` rows | 62,844 | 62,844 (index and all 9 pre-existing columns byte-identical) |
| `Unit_sold` non-null, 2011Q1 / 2013Q1 / W3–W5 | column absent | 3,486 / 3,430 / 0 |
| raw-code agreement 2011Q1 (CONTENTS.org figure) | 3,438 of 3,548 (96.9%) | reproduced exactly |
| raw-code agreement 2013Q1 | 3,316 of 3,478 (95.3%) | reproduced exactly |
| served size-collapsed agreement, raw files | — | 3,466 (97.7%) / 3,349 (96.3%) |
| agreement on the built table, parquet and API frame identical | — | 3,374 of 3,451 (97.8%) / 3,283 of 3,411 (96.2%) |
| #867: `Other Inorganic Fertilizer` rows unmatched in `nitrogen_kg(Nigeria plot_inputs)` | 170 | 0 |
| #867: matched rows / plots / total kg N | 10,014 / 4,359 / 173,672 | 10,184 / 4,472 / 178,074 |

The framework read (`Country('Nigeria').crop_production()`) served `Unit_sold` after the stale parquet was evicted, `make` failed on the rule-less target with three deliberate `No rule to make target` lines, and the country script ran. The sold-unit column is identified by value-label vocabulary: `sa3q11b`'s Stata value-label dict is identical to the harvest unit's in both waves (36 and 48 labels), pinned by test.

**3. Cache cost (stated, not paid on the shared cache)**

`nigeria.py` and `data_scheme.yml` are in every Nigeria table's L2-country hash and, via the `binp:` glob, every Nigeria L2-wave hash. All 24 Nigeria tables re-warm cold from L1; only `crop_production` changes content. `transformations.py` reaches no build fingerprint, so the #867 rider costs nothing. Only my private root was rebuilt.

**4. Surprises and CONTENTS.org**

Idiosyncrasies that bore on the task, from CONTENTS.org: `t` is the post-harvest quarter, not the wave directory; the sale has its own unit code while `u` is the harvest unit; W3–W5 sales sit in `secta3ii` at household-crop grain with no plot linkage (#843), so they carry no sold unit; `crop_production` is one of five rule-less Makefile targets (#866); and `_canon_unit` discards the Size axis. Nothing contradicted CONTENTS.org. The raw figures at lines 976–1000 reproduced to the row.

Surprises, both recorded in CONTENTS.org:
- The L2-country parquet stores `Quantity_sold` and `Value_sold` as `string` dtype, before and after this change. The API read coerces them to `Float64`, so nothing served is wrong, but anyone reading the cache parquet directly needs `pd.to_numeric`. Not fixed, out of scope.
- W1's variable label for `sa3q11b` reads "(11a).Quantity sold through buyer (unit)", a Stata varlabel slip. The value labels identify it.
- Served agreement is higher than the raw-code figure because the size-agnostic collapse folds big and medium sacks into one label, so those disagreements are invisible to the `Unit_sold != u` flag.
- `Olodo` appears in `Unit_sold` but never in `u` (W2 sold vocabulary only).

**5. Decisions**

- The #867 alias points `other inorganic fertilizer` at the existing `inorganic fertilizer` share of 0.20, not at `other fertilizer`'s 0.0. The label says inorganic, so the class share applies. This is a choice of which existing entry to alias, not a new number, and the comment says so. The NPS, D Compound and organic content decisions remain open on #867.
- No `Condition_sold` for Nigeria: the W1/W2 A3 files carry no sold-condition column.
- `Unit_sold` is a plain NA-bearing string column, never sentinel-filled, per the canonical note.
