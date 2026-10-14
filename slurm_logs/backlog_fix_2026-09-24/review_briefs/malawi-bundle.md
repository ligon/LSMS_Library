# RED-TEAM review of branch `fix/malawi-bundle` (element `malawi-bundle`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/malawi-bundle` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Malawi built from `development`; `dvc-cache` in it is a read-only symlink to the shared blob store. `development` is available as `origin/development` in your clone (`git diff --stat origin/development..HEAD`).

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


## The branch: diffstat (vs development)
```
 .../countries/Malawi/2010-11/_/crop_production.py  |   2 +
 .../countries/Malawi/2013-14/_/crop_production.py  |   2 +
 .../countries/Malawi/2016-17/_/crop_production.py  |   2 +
 .../countries/Malawi/2019-20/_/crop_production.py  |   2 +
 lsms_library/countries/Malawi/_/CONTENTS.org       | 179 +++++++++++++++--
 .../countries/Malawi/_/categorical_mapping.org     |   3 +-
 lsms_library/countries/Malawi/_/data_scheme.yml    |  61 ++++--
 lsms_library/countries/Malawi/_/malawi.py          | 213 ++++++++++++++-------
 tests/test_malawi_shipped_factors.py               | 134 +++++++++++++
 9 files changed, 502 insertions(+), 96 deletions(-)
```
## The branch: commit messages
--- 13da05b11 docs(Malawi): record the sale-side condition, the Groundnut split and the JOIN correction (GH #833, #839)
CONTENTS.org: new section "crop_production: the SALE has its OWN condition
too" with the before/after measurements (642 attached contradictions -> 0;
303 / 59 / 247 / 503 basis-mismatch sales suppressed and counted; Groundnut
j rows 188 / 225 -> 0 in 2016-17 / 2019-20, now Groundnut - Boiled
(Vendor)), the compatibility rule and the decision that shell_not_applicable
vs shelled/unshelled is a contradiction, and the idiosyncrasy that Module Q
carries no ag_q02c before 2016-17 (perennial sales attach on the unknown
wildcard only).  The three passages that said ag_i02c was unread / the
label split was NOT fixed now point here.  The #839 JOIN correction and the
Cassava note (Malawi's two-code-scheme cassava is the Module P recode, GH
#869; nothing changes) are recorded with the measured 24/44 and 19/51/6.

Verified on the rebuilt private-root parquets; 454 passed in the Malawi +
config-consistency test selection.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 1a040c61f fix(Malawi): read the SALE's own shelled/unshelled (ag_i02c / ag_q02c); split Boiled groundnuts; correct the JOIN claims (GH #833, #839)
The sale merge in assemble_crop_production keyed on (i, crop, u) alone, so
a sale the household reported UNSHELLED was stamped on a harvest row
reported SHELLED and vice versa.  Measured over the four waves, on attached
sales (Quantity_sold > 0) whose both sides answered S or U: 11,321 pairs,
10,679 agree, 642 CONTRADICT (Groundnut 375, Rice 97, Maize 62, Soyabean
36), plus 465 where one side said NOT APPLICABLE and the other S/U.  Those
rows carried a Value_sold / Quantity_sold on the other basis, under a
`condition` level that keys the shipped kg factors (GH #854).

_sale_block now reads the sale module's own S/U (Module I ag_i02c in every
wave; Module Q ag_q02c from 2016-17 -- 2010-11 / 2013-14 Q ask no such
question, so those perennial sales are unknown_condition, as every Module P
harvest row is) through the harvest's _crop_conditions map, and groups on
(i, _crop_code, u, condition_sold).  assemble_crop_production attaches a
sale only to a harvest row whose condition it does not contradict -- equal,
or either side unknown_condition; shell_not_applicable vs shelled/unshelled
IS a contradiction -- and only where that pairing is 1:1.  Everything else
is suppressed and COUNTED: the #854 attrs['sale_suppressed'] keeps its
shape for the AMBIGUOUS class, and a new attrs['sale_basis_mismatch'] holds
the class that used to be attached silently on the wrong basis.  Each has
its own SaleAttachmentWarning.

Measured on the rebuilt table (harvest rows unchanged, 33,420 / 12,616 /
37,170 / 51,872):

  wave     attached before -> after   contradictions attached   mismatch sales / MWK
  2010-11  5,205 -> 4,903             174 -> 0                  303 /  4,922,307
  2013-14  1,774 -> 1,715              48 -> 0                   59 /    969,200
  2016-17  4,564 -> 4,317             158 -> 0                  247 / 20,236,770
  2019-20  7,131 -> 6,628             262 -> 0                  503 / 37,981,880

The one #854 ambiguity (2010-11 303041430013, Maize, 50 kg Bag, 2,500 MWK)
now reads S on the sale side and attaches to the shelled row; the ambiguity
tally is 0 in every wave.  Checked: 0 (i, crop, u) keys mix an unsold
(all-NULL) Module I/Q line with a real sale, so the zero-valued
unknown_condition sales _sale_block mints for unsold lines never make a
real sale ambiguous.

harmonize_food: the `Groundnut` row carried `Boiled groundnuts` (a vendor
snack) in its 2016-17 / 2019-20 columns AND the Aggregate Label
`Groundnut, Vendor/Boiled` for the raw nut of 2004-05 / 2010-11 / 2013-14.
Split into `Groundnut` (Legumes; Groundnut, Whole; the three early waves)
and `Groundnut - Boiled (Vendor)` (Vendor; Groundnut, Vendor/Boiled;
2016-17 / 2019-20), on the pattern of `Groundnut - Roasted (Vendor)`.
food_acquired j == 'Groundnut' in 2016-17 / 2019-20: 188 / 225 -> 0, now
served as `Groundnut - Boiled (Vendor)`.  Both Aggregate labels already
exist in aggregate_short.yml.

data_scheme.yml (#839 rider): the crop_production and community_prices
comments claimed their labels "JOIN food_acquired.j".  Measured: 24 of 44
crop labels occur as a food label (20 do not -- Maize vs Maize Grain (Not
As Ufa), Cassava vs Cassava Tubers / Cassava Flour, Beans vs Bean, Brown /
Bean, White, crops with no food analogue, one spelling); 19 of 51 priced
items occur as a food label and 6 as a crop label.  The comments now say
so.  Corrected in this commit because the file is in every Malawi table's
cache hash and the code change costs the same re-warm.

Tests: TestSaleConditionIsRead in tests/test_malawi_shipped_factors.py
(sale-side decode; disambiguation of a shelled + unshelled plot-crop; a
contradiction is not attached and is counted in its own tally; NA vs S/U
is a contradiction; unknown on either side is compatible; two sales
compatible with one row are ambiguous).  The existing #854 tests pass
unchanged.

Cache cost: one Malawi cold re-warm (malawi.py and data_scheme.yml are in
every Malawi table's hash).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


## The fix agent's report (a self-report -- refute it)
**SCOPE DEVIATIONS**

Files touched that the task did not name by path (none match the stop-list; all committed):
- `lsms_library/countries/Malawi/{2010-11,2013-14,2016-17,2019-20}/_/crop_production.py` — one added kwarg per sale call, needed to pass the S/U column into `_sale_block`.
- `tests/test_malawi_shipped_factors.py` — a new test class appended; no existing assertion changed.
- `lsms_library/countries/Malawi/_/CONTENTS.org` — the brief's country rule asks for this.

**1. Commits and branch**

Branch `fix/malawi-bundle`, pushed to `origin`; `git ls-remote` confirms the head.

| SHA | content |
|---|---|
| `1a040c61f` | code: sale-side S/U wired; Groundnut split; data_scheme JOIN claims corrected; six new tests |
| `13da05b11` | CONTENTS.org entries |

**2. Verification**

- Package identity assert: `/local/job39198807/clone/lsms_library/__init__.py` (ok).
- Gate tests before touching prose: `8 passed, 36 deselected in 6.63s` (the two pinned #854 tests unchanged, plus the six new ones).
- Full selection with the verbatim recipe (Malawi shipped-factors, cassava recode, declared spellings, build-transform hash, null-index guard, derivations, schema consistency): `454 passed, 37 warnings in 397.04s (0:06:37)`.
- Cold rebuild of `crop_production` and `food_acquired` in the private root through the library, no PYTHONSAFEPATH exported: both built; `find lsms_library/countries -name '*.parquet'` is 0.
- Harvest/sale shelled-unshelled basis, attached sales with a positive quantity, harvest rows unchanged (33,420 / 12,616 / 37,170 / 51,872):

| wave | attached before → after | both sides answered S/U: agree / contradict (before) | contradictions attached after | basis-mismatch sales suppressed / MWK |
|---|---|---|---|---|
| 2010-11 | 5,205 → 4,903 | 2,339 / 174 | 0 | 303 / 4,922,307 |
| 2013-14 | 1,774 → 1,715 | 895 / 48 | 0 | 59 / 969,200 |
| 2016-17 | 4,564 → 4,317 | 2,941 / 158 | 0 | 247 / 20,236,770 |
| 2019-20 | 7,131 → 6,628 | 4,504 / 262 | 0 | 503 / 37,981,880 |

Total 642 contradictions before (Groundnut 375, Rice 97, Maize 62, Soyabean 36), plus 465 "not applicable vs S/U" pairs. The one #854 ambiguity (2010-11 household 303041430013, Maize, 50 kg Bag, 2,500 MWK) now attaches to its shelled row; the ambiguity tally is 0 in every wave. The rebuilt parquet reproduces the in-process numbers exactly.
- Groundnut no longer receives vendor-snack rows. `food_acquired` rows with `j == 'Groundnut'` in 2016-17 / 2019-20: 188 / 225 → 0, now served as `Groundnut - Boiled (Vendor)` (188 / 225); 2004-05 / 2010-11 / 2013-14 unchanged at 4,031 / 3,937 / 1,937.
- JOIN claims measured on the rebuilt tables: 24 of 44 crop labels occur as a food label; 19 of 51 priced items occur as a food label and 6 as a crop label. The two comments now say so.

**3. Cache cost**

One Malawi cold re-warm: `malawi.py` and `data_scheme.yml` are in every Malawi table's hash, so all eleven Malawi tables rebuild on next read, not only the two I rebuilt. I rebuilt only `crop_production` and `food_acquired` in the private root; the other nine private-root parquets are stale-on-read. The live cache was not touched.

**4. Surprises and CONTENTS.org**

- The idiosyncrasy that bears on the task, from CONTENTS.org lines 201-217: a quantity column and its unit column are a pair and the sale module has its own; `ag_i02c` was "still unread" and the #854 gate was "defensive, not corrective". That entry was a deferral, not a claim the data contradicts, and this work closes it. Nothing found contradicts CONTENTS.org.
- New idiosyncrasy recorded: Module Q carries no `ag_q02c` before 2016-17, so every 2010-11 / 2013-14 perennial sale is `unknown_condition` on the sale side and attaches on the wildcard only. Where Q does ask, the answer is overwhelmingly "not applicable", which is compatible with Module P's unknown.
- Measured and recorded: 0 household-crop-unit keys mix an unsold all-null Module I/Q line with a real sale, so the zero-valued phantom sales cannot make a real sale ambiguous.
- The 2010-11 build log shows the pre-existing #869 loud-drop warning for one row with crop code 1039; not from this change.
- Stale prose not edited: the 2010-11 and 2013-14 wave-script docstrings still describe Module I as `qty=ag_i02a, value=ag_i03` without the S/U column. A docstring-only edit would move the wave hash again for no data reason; worth folding in at merge.

**5. Decisions taken**

- Compatibility rule: a sale attaches only where its condition equals the harvest row's or either side is `unknown_condition`. `shell_not_applicable` against `shelled`/`unshelled` counts as a contradiction, not a wildcard; that is the 465-row class the task did not anticipate.
- The contradiction class lives in a new `attrs['sale_basis_mismatch']` with its own warning, rather than widening the pinned `sale_suppressed` dict.
- The Aggregate Label for the raw nut in the three early waves moved from `Groundnut, Vendor/Boiled` to `Groundnut, Whole`. The task asked only for the Preferred Label split, but leaving the raw nut in the vendor bucket would have fixed the price comparison and left `labels='Aggregate'` wrong.
- Cassava: no Malawi mapping change. The two-code-scheme instance in the triage is Uganda's; Malawi keeps `Cassava Flour` and `Cassava Tubers` apart, its crop `Cassava` is deliberately neither, and its cross-wave cassava recode was already closed by `_PERENNIAL_RECODE` (#869). Recorded in the data_scheme comment and CONTENTS.org.
- `condition='ag_q02c'` is passed for Module Q in all four waves; an absent column resolves to `unknown_condition` by design.
