# RED-TEAM review of branch `fix/niger-followup-2` @ b6feef578 (element `niger-followup-2`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/niger-followup-2` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Niger; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/niger-followup-2/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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
## The task

Apply the reviewer's four follow-ups on `fix/niger-followup` (verdict: pass-with-fixes). Read `Niger/_/CONTENTS.org` FIRST (the NPS-style extension-key design, the #679 and #833 sections) and name the idiosyncrasy you found in your report.
a. `Niger/_/CONTENTS.org:469-470` and `Niger/2021-22/_/crop_production.py:58-62`, plus `CONTENTS.org:1080-1084`: state 280 / 367 as all-raw-row counts, with 17 / 50 on the kept rows and the served 35-row / 29-group `plot_inputs` residual (exact wording per verdict §6a).
b. `tests/test_niger_2014_15_extension_key.py::TestServedKey::test_every_wave_keys_into_the_sample_universe`: add `KNOWN_STRAY = {("crop_production", "2011-12"): {"27010"}}` and assert `stray == KNOWN_STRAY.get((table, t), set())`, with the CONTENTS reference in the message -- the test must go green by NAMING hid 27010, not by weakening.
c. `Niger/_/panel_ids.py` docstring and the CONTENTS #679 section: name the five links the re-key drops (three ext-2-only pairs (89,5) (95,1) (128,11) by decision; two newly guard-skipped) beside the "3,251 -> 3,529" figure.
d. CONTENTS #679 "served rows before -> after" list and the #833 section: add household_characteristics 2014-15 3,558 -> 3,616 and the three 2021-22 derived-food-table moves (98,249 -> 98,231; 98,247 -> 98,244; 131,549 -> 131,522).
Line numbers are from the verdict and may have shifted after the merge; locate by content.
**Verify:** `tests/test_niger_*` + full suite via the recipe; the panel_ids regeneration (`make -C lsms_library/countries/Niger/_ panel-ids`) must produce NO diff to the committed JSON (the crosswalk is a committed source, GH #914); the Niger re-warm (18 tables) is the known cost -- state, do not pay on the live root.


## The branch: diffstat (vs development)
```
 .../countries/Niger/2011-12/_/crop_production.py   |  26 +-
 .../countries/Niger/2014-15/_/crop_production.py   |  57 ++-
 .../countries/Niger/2014-15/_/data_info.yml        |  16 +
 .../countries/Niger/2014-15/_/livestock.py         |  13 +-
 .../countries/Niger/2014-15/_/people_last7days.py  |  11 +-
 .../countries/Niger/2014-15/_/plot_inputs.py       |  11 +-
 .../countries/Niger/2014-15/_/plot_labor.py        |   9 +-
 .../countries/Niger/2018-19/_/crop_production.py   |   6 +-
 .../countries/Niger/2021-22/_/crop_production.py   |  47 ++-
 .../countries/Niger/2021-22/_/plot_inputs.py       |  10 +-
 lsms_library/countries/Niger/_/CONTENTS.org        | 397 +++++++++++++++++++--
 .../countries/Niger/_/categorical_mapping.org      |  51 ++-
 lsms_library/countries/Niger/_/data_scheme.yml     |  14 +
 lsms_library/countries/Niger/_/niger.py            |  80 +++--
 lsms_library/countries/Niger/_/panel_ids.json      |   2 +-
 lsms_library/countries/Niger/_/panel_ids.py        | 110 ++++--
 lsms_library/countries/Niger/_/updated_ids.json    |   2 +-
 tests/test_niger_2014_15_extension_key.py          | 238 ++++++++++++
 tests/test_niger_food_label_drift.py               | 185 ++++++++++
 tests/test_niger_region_labels.py                  | 148 ++++++++
 tests/test_niger_sold_unit.py                      | 300 ++++++++++++++++
 tests/test_niger_u_sentinel.py                     |  35 +-
 tests/test_niger_value_sentinels.py                | 184 ++++++++++
 23 files changed, 1803 insertions(+), 149 deletions(-)
```
## The branch: commit messages
--- b6feef578 Niger CONTENTS.org: scope the "every other table unchanged" claim to 2014-15; record the #865 row moves
The follow-up commit replaced one false universal sentence with another:
"every other (table, wave) is unchanged EXCEPT the 2021-22 derived food
tables" ignored food_acquired 2021-22 (131,549 -> 131,544, #833) and the
#865 sentinel drops.  Now: every other 2014-15 cell is unchanged, and the
other waves' moves are pointed to their own sections.  The #865 section
gains its served rows, re-measured 2026-10-14 on the pre-fix snapshot vs
the cold rebuild: crop_production 2011-12 11,948 -> 11,937, plot_inputs
2021-22 12,433 -> 12,432, no other wave moves.  The #833 note now says the
five summed rows come from the two 2021-22 sugar labels alone (Choux and
"Aubergine," are renames with no 2021-22 sibling).  CONTENTS.org is in no
cache hash: no re-warm.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 4998ef4d2 Niger: apply the red-team follow-ups to the #679 / #824 / #833 record (GH #679, #865, #824, #811, #833)
Four corrections from slurm_logs/backlog_fix_2026-09-24/verdicts/niger-followup.md
(pass-with-fixes), every number re-measured here on 2026-10-14:

a. CONTENTS.org (#679 section) and 2021-22/_/crop_production.py: the 280
   (livestock) / 367 (plot_inputs) figures were two-column duplicate groups
   on ALL raw rows (41,200 / 36,890), not table collisions; on the rows the
   scripts keep (owned 6,758 / applied 8,179) they are 17 / 50, both 0 with
   EXTENSION, and the served plot_inputs 2014-15 collapse still fires
   35 destroyed / 29 groups (was 88 / 82) -- the seed-slot collision.  The
   s16d "multiplicity == plots for 89.6% / 65.7%" explanation is replaced by
   the measured fact: multiplicity == number of s16c harvest lines for 100%
   of 8,691 pairs (2,633 multi, 553 sale-bearing, 0 unmatched; == distinct
   (field, parcel) plots for only 1,729 / 65.7%), which is what makes the
   exact-duplicate drop a join-artefact removal.  "2-5 rows" -> "2-8".
b. tests/test_niger_2014_15_extension_key.py: KNOWN_STRAY names 2011-12
   hid 27010 (harvest module only, the 2-row NullIndexKeyWarning on
   crop_production's v) and the universe test asserts
   stray == KNOWN_STRAY.get((table, t), set()) -- narrowed, not weakened:
   a new stray fails, and a fixed 27010 fails until the entry is removed.
   The test was never green before; 139 passed on tests/test_niger_*.py.
c. panel_ids.py docstring + CONTENTS: the re-key drops five of the old
   3,251 links (3,246 carry over identical; other waves byte-identical):
   89005 / 95001 / 128011 are ext-2-only pairs delinked by the DECISION
   (collision argument does not bite there; named as a choice), and
   101010 / 201010 are ext-0 households newly guard-skipped because their
   hids 10110 / 20110 are now (1,11,0) / (2,11,0)'s ids.
d. CONTENTS #679 list gains household_characteristics 2014-15 3,558 -> 3,616
   (the "every other table unchanged" sentence was false); #833 gains the
   2021-22 derived moves food_expenditures 98,249 -> 98,231, food_prices
   98,247 -> 98,244, food_quantities 131,549 -> 131,522 (food_acquired
   131,549 -> 131,544).  Before measured on the pre-fix warm root, after on
   a cold rebuild of all 23 entries (0 failed).

make -C Niger/_ panel-ids reproduces both committed JSONs with no diff.
Cache: panel_ids.py is a .py in Niger/_/ and therefore in every Niger
wave's input hash, so all 22 Niger table hashes move (18 build + 4
derived) -- one Niger re-warm, paid on the private root only.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- cad6c8797 Niger: re-measure three claims that were stated, not measured (GH #811, #824, #679)
- tests/test_niger_region_labels.py: RAW_SPELLINGS is now measured per wave
  on var/cluster_features.parquet grouped by t (2014-15 spells it
  'Tillabéri' and has no bare 'Niamey'; 2021-22 has no 'Agadez', only
  'gadez'), and the docstring's "16 before" is a measurement: the
  pre-#811 Region table (git show 242573697:...) applied to the 19 raw
  spellings yields 16 distinct labels; the current table 8.
- CONTENTS.org, sold-unit subsection: the old 98.8% / 93.1% are described
  as what they are -- a raw-side join predating Unit_sold -- with the
  0.1-0.8 point gap to the re-measured 98.7% / 92.3% left unadjudicated,
  instead of an invented cause.
- CONTENTS.org, #679 section: served 2014-15 rows before -> after now
  cover every table, measured through the API on all 23: plot_labor
  15,803 -> 15,935, shocks 3,771 -> 3,781, housing 3,558 -> 3,617,
  food_security 3,557 -> 3,616 added; every other (table, wave) is
  unchanged.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- fcea61515 Niger: move the u-sentinel pins the bundle legitimately moves; record hid 27010 (GH #679, #865, #833)
tests/test_niger_u_sentinel.py pinned row and 'Unknown' counts measured on
2026-09-09, before the Niger bundle; the bundle's own commits did not move
them because the previous agent's cold rebuild never finished.  Re-measured
2026-09-24 on the cold rebuild, before -> after, each with its cause:

  crop_production rows      46,341 -> 46,376  (+46 in 2014-15: the EXTENSION
                                              key un-collapses 46 groups, #679;
                                              -11 in 2011-12: lines carrying
                                              only the 999999 code are all-NA
                                              and dropped, #865)
  crop_production Unknown    4,920 ->  4,909  (2011-12 3,146 -> 3,135: those
                                              same 11 lines)
  food_acquired rows       239,882 -> 239,877 (-5 in 2021-22: the sugar fold
                                              merges five households' two
                                              forms at one (t,v,i,j,u,s), #833)
  plot_inputs rows          42,364 -> 42,426  (+63 in 2014-15, #679; -1 in
                                              2021-22: the 9999 line, #865)
  plot_inputs Unknown           75 ->     74  (2021-22 14 -> 13: that line)

Nothing else in the file changes: the config-only family and the
food_acquired / community_prices pins are untouched, and every moved
number is recorded beside the old one in the constants and the docstring.

CONTENTS.org: tests/test_niger_2014_15_extension_key.py's
test_every_wave_keys_into_the_sample_universe[crop_production] is RED on
2011-12 and was never green there -- hid 27010 (grappe 270, menage 10) has
two lines in ecvmaas2e_p2 and none in the welfare or roster file, the only
such id; served identically before the bundle, no 2011-12 crosswalk entry,
so a pre-existing source gap, not a #679 artefact.  Recorded, not fixed
(the 2011-12 script is outside this task); the test assertion is left as
it is.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 62662cfd3 Niger CONTENTS.org: Unit_sold is carried, the agreement figures re-measured, #833 recorded (GH #824, #833, #679)
04c1efbf9 said "Corrected in CONTENTS.org with the rebuild" but touched no
CONTENTS.org, so the "Prices" subsection still said the sold unit "is not
carried" and gave 2021-22's agreement as 63.0%.  Rewritten: Unit_sold is
carried since GH #824; the per-wave sold==harvest shares are re-measured
on the cold rebuild under a stated definition (98.7 / 89.4 / 92.3 / 89.0%,
n = 1,293 / 558 / 3,226 / 1,423); the 63.0% was a spelling artefact (raw
13.4%, through the u table 89.8% on the 1,818 de-duplicated sale rows);
the s16d repetition and the exact-duplicate drop are documented with the
census, including that the "multiplicity == number of plots" explanation
holds for 65.7% of multi-row pairs (not 89.6%) under a distinct
(field, parcel) plot and that the fix does not depend on it.

d36054864 said "CONTENTS.org says why" code 166 stays and "the wider split
list ... is recorded" -- neither was in the file.  New section: the three
folds and the decisions, the seven-item split list as served (2018-19
label -> 2021-22 labels), the sugar 5,537-not-5,542 merge, and the code
166 evidence (product lists and value labels end at 165; 466 of 468 rows
in Sachet, median 1 sachet, median last purchase 150 FCFA).

d0ec4b32f (#679): served 2014-15 rows before -> after per table, and the
finding that plot_inputs 2014-15 still carries 39 duplicated
(t, i, input, crop, u) groups (84 rows) after the key fix -- the raw file
is unique on the full key; the residual is the seven AS02CQ02 seed slots
harmonising to one 'Seed', a collision on the input axis, not the id.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 53e2752cb Niger: pin Unit_sold, the crop-level sale key and the s16d de-duplication (GH #824)
Recreates the test commit 04c1efbf9 relied on but never landed.  Three
families in the house pattern: config-only (data_scheme declares Unit_sold
optional str, never an index level; all four wave scripts emit it; the two
crop-level waves sum per (i, crop, Unit_sold) with single-plot AND
single-unit attribution; the 2021-22 de-duplication statically precedes
the sum and masks both frames alike; the u table maps 'sac de 100kg'),
fixture-level (_finish_crop_production folds 'Unknown' to NA, string
dtype, all-NA when the column is absent), and data-gated.

The data-gated family re-derives every number the fix put in prose, on
the cold rebuild of 2026-09-24: the s16d census (12,232 x 47; 8,691
pairs; 2,633 multi pairs / 6,174 rows; 5,786 exact-duplicate rows, 3,329
dropped; all 1,320 sale-bearing multi-pair rows are exact duplicates; the
388 distinct lines sell nothing); AS2E2 one row per (household, crop)
(5,226); Unit_sold rows per wave 1,294 / 564 / 3,226 / 1,425; no pair in
two sold units; 2021-22 single-plot Value_sold 73,206,995 (< 78,934,620
before) and Quantity_sold 32,420.5 on 1,409 pairs; and the sold==harvest
agreement per wave under a STATED definition (priced = Quantity_sold > 0
and Value_sold > 0; knows both = Unit_sold non-null and u != 'Unknown'):
98.7% (n=1,293), 89.4% (558), 92.3% (3,226), 89.0% (1,423).

The commit message's "88.7% (n=1,589)" for 2021-22 is not reproduced
under any definition tried (served: 89.0%/1,423; raw de-duplicated sale
rows: 89.8%/1,818; single-plot: 89.4%/1,409); the "13.5% raw / 89.9%
canonical" pair is (13.4% / 89.8%).  The pinned numbers are the measured
ones, with the definition in the test's constants.

Refs #824.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 99095ecee Niger: pin the food_acquired label folds and the code-166 decision (GH #833)
Recreates the test commit d36054864 relied on but never landed.  Config-only
class pins the harmonize_food rows ('89. Choux' -> 'Chou'; 134/135 Sucre ->
'Sucre (poudre ou morceaux)'; '93. Aubergine,' -> 'Aubergine' and the crop
rows with it; 'Aubergine, Courge/Courgette' NOT folded; no Preferred Label
ends in a comma; nothing decodes 166).  Data-gated class pins the served
rows per (label, wave) on the cold rebuild: Chou 628 / 973, Sucre 4,883 /
5,537, Aubergine 452 (2021-22 only), the 2018-19 bundle 1,156, code '166'
468 rows with 466 in Sachet, and that the retired labels are served on 0
rows.

2021-22 sugar is 5,537, not 5,462 + 80 = 5,542: five households bought
both forms in the same unit and the fold merges them at the
(t, v, i, j, u, s) grain -- an additive, lossless sum, stated in the test.

Refs #833.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 198f03ad1 Niger: pin cluster_features.Region at 8 served labels (GH #811)
Recreates the test commit b502bb28d cited but never landed (the previous
agent's clone was destroyed with it uncommitted).  Config-only class pins
that the Region org table maps all 19 raw spellings the four waves carry
(2018-19's eight lower-case ones included) onto exactly the eight regions
and is idempotent; the data-gated class checks the served column pooled
AND per wave, and that no raw spelling leaks.

Measured 2026-09-24 on a cold rebuild of the bundle: served Region 8
labels in every wave (16 before -- 19 raw spellings, 11 mapped, 8 unmapped).

Refs #811.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- d36054864 Niger harmonize_food: fold the cross-wave spellings of cabbage and sugar; clean 'Aubergine,'; code 166 stays (GH #833)
Served food_acquired.j carried near-duplicate items across the two EHCVM
waves (warm cache, rows): 'Chou' 628 (2018-19) vs 'Choux' 973 (2021-22);
'Aubergine, Courge/Courgette' 1,156 (2018-19) vs 'Aubergine,' 452
(2021-22); 'Sucre (poudre ou morceaux)' 4,883 (2018-19) vs 'Sucre poudre'
5,462 + 'Sucre morceaux' 80 (2021-22).

Read against the 2021-22 questionnaires (WB catalog 6276, both vagues) two
of the three are not drift but an instrument SPLIT: 2021-22 lists
'93. Aubergine,' AND '94. Courge/Courgette' where 2018-19 had one bundled
item, and '134. Sucre poudre' + '135. Sucre morceaux' where 2018-19 had
'Sucre (poudre ou morceaux)'.  DECISIONS, named:
- 'Choux' -> 'Chou': one vegetable, plural spelling.  Fold.
- Sucre 134/135 -> 'Sucre (poudre ou morceaux)': one commodity in two
  forms, 2018-19's label already says it covers both and cannot be split.
  Fold.  (Alternative left: keep the 2021-22 split and lose the cross-wave
  join.)
- 'Aubergine,' -> 'Aubergine', NOT folded with 'Aubergine,
  Courge/Courgette': eggplant and courgette are different goods, and #833's
  own direction is split-not-fold.  The crop rows ('Aubergine',
  'aubergine') follow to 'Aubergine'.
- Code 166 (468 rows, 2021-22) CANNOT be decoded: both questionnaires'
  product lists end at 165 'Bières industrielles', the .dta value labels
  end there, and the sibling EHCVM-2 files label 166 country-specifically
  (Burkina / Mali 'Riz importé 3').  466 of the 468 rows are in 'Sachet',
  median 1 sachet, median last purchase 150 FCFA.  Left as '166' rather
  than named by inference; the task's "no j label '166'" is unmet by
  construction and CONTENTS.org says why.

The wider split list (Piment, Gingembre, Café, Pastèque/Melon,
Vinaigre/moutarde, Crabes/crevettes, Huile d'arachide) is recorded, not
folded.  Cache: categorical_mapping.org -- one Niger re-warm, shared.

Refs #833.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- b502bb28d Niger cluster_features.Region: add the lower-case 2018-19 spellings (GH #811)
The Region table mapped the 2011-12 / 2014-15 / 2021-22 spellings ('gadez',
'Tillaberi', 'Communauté urbaine de Niamey') but not 2018-19's, which are
all lower-case and unaccented ('agadez', 'diffa', 'dosso', 'maradi',
'tahoua', 'tillaberi', 'zinder', 'niamey'), so the served column carried 16
labels for 8 regions.  Eight rows added; served Region 16 -> 8 labels
(measured on the rebuild, pinned by tests/test_niger_region_labels.py).

Not cache-free: categorical_mapping.org is a build input in every Niger
table's hash -- one re-warm, shared with this bundle.  Sibling instance of
the Guinea-Bissau issue; Burkina_Faso and Senegal are not touched here.

Refs #811.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 04c1efbf9 Niger crop_production: carry the SOLD unit, key the crop-level sale on it, drop s16d's repeated lines (GH #824)
`u` is the HARVEST unit; every Niger wave records the sale's unit in a
column the feature dropped (as02eq12b, AS02EQ12B, s16cq16b, s16dq05b).
Value_sold / Quantity_sold was therefore a price per harvest unit that is
only right where the two units coincide.  Per the contract settled on
Uganda (canonical data_info.yml, Unit_sold note): a per-row COLUMN, not an
index level; canonicalised through the same `u` table; NA-bearing, never
sentinel-filled (the table's `Unknown` is folded back to NA); a
disagreement is a data-quality FLAG, not an identification.

- 2011-12 / 2018-19: the sale is on the harvest row; Unit_sold is read
  straight off the b-column.
- 2014-15 / 2021-22: the sale block is crop-level.  It is now summed over
  (i, crop, Unit_sold) -- never across units -- and attributed only to a
  crop grown on ONE plot AND sold in ONE unit (a second unit would
  duplicate the harvest row).  Measured: no pair sells in two units in
  either wave, so the re-key is a guard today and the grain it names.
- The `u` table gains the one 2021-22 sold-unit spelling it lacked
  ('sac de 100kg'); the others ('tiya', 'tongolo', 'panier', 'Autres',
  'Sac de 50kg') were already there.
- data_scheme.yml declares Unit_sold optional (a row with no sale has no
  sold unit).

DECISION, found on the way and named: the 2021-22 s16d sale file REPEATS
its crop-level lines once per plot (12,232 rows for 8,691 household-crop
pairs; multiplicity == number of plots in s16c for 89.6% of the multi-row
pairs; 5,786 of the 6,174 multi-pair rows are byte-identical on all 47
columns, and EVERY one of the 1,320 that carries a sale is).  The old
script summed them and served a single sale 2-4x on 144 single-plot crops.
Exact duplicates are dropped before the sum: Value_sold on the single-plot
pairs 78,934,620 -> 73,206,995 FCFA, Quantity_sold 36,975 -> 32,420.5,
1,409 pairs either way.  A line identical on every column is the same
record, not a second sale.

Sold-unit == harvest-unit share on priced rows, canonicalised, measured
on the rebuilt wave parquets: 2011-12 98.7% (n=1,296 raw), 2014-15 89.4%
(n=559), 2018-19 92.3% (n=3,226 raw), 2021-22 88.7% (n=1,589).  CONTENTS
.org's 63.0% for 2021-22 was a spelling artefact -- the sold labels are
lower-case ('tiya', 'sac de 100kg') where the harvest labels are not, and
compared raw they agree on 13.5%; through the `u` table 89.9%.  Corrected
in CONTENTS.org with the rebuild.

Cache: niger.py, categorical_mapping.org and data_scheme.yml -- one Niger
re-warm, shared with the rest of this bundle.

Refs #824.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- f231e6a6c Niger: map the unlabelled all-nines "don't know" codes to NA, exact value only (GH #865)
The ECVMA harvest / sale modules record "ne sait pas" as an all-nines number
with no value label, so convert_categoricals cannot see it and it was served
as a harvest of 999,999 units (Site Q / #857 reported it, and by contract
never NaNs).  The code is NOT the same width across waves, nor between the
quantities and the sale value.  Raw counts (get_dataframe,
convert_categoricals=False):

  2011-12 ecvmaas2e_p2   as02eq07a 999999 x11  (all unit code 99 'manquant';
                                                four crops, not "eleven Mil")
                         as02eq12a 999999 x3
                         as02eq13  9999999 x2  (both hid 20607)
  2014-15 ECVMA2_AS2E1P2 AS02EQ07A 9999 x23    (all unit code 99)
          ECVMA2_AS2E2P2 AS02EQ12A 9999 x7, AS02EQ13 9999999 x7
  2021-22 s16b_me_ner2021 s16bq03a 9999 x1     (plot_inputs)
  2018-19: none on any of the three columns.

Each WAVE script now maps its own literal codes to NA through a new
niger._na_exact (QTY_SENTINELS / VALUE_SENTINELS at the top of the script),
the convention _coerce_wage (999999) and _COMMUNITY_PRICE_SENTINELS already
honour for wages and community prices.  The literals sit in the wave script,
not in _finish_crop_production, so a new code moves one wave's hash rather
than all 18 Niger tables (niger.py moves anyway in this bundle for #679).
Never a range: a real 9,998 or 10,000 survives (pinned).

Warm-cache counts served before this change: crop_production Quantity
999999 x11 (2011-12) and 9999 x23 (2014-15); Quantity_sold 999999 x3 / 9999
x5; Value_sold 9999999 x2 (2011-12) + x5 (2014-15); plot_inputs 2021-22
Quantity 9999 x1.  After: 0 on every column (tests/test_niger_value_sentinels
.py pins the served side and the raw counts).  A line carrying only a
sentinel becomes all-NA and is dropped by _finalize_result.

Recorded, not stripped: plot_inputs 2011-12 / 2014-15 carry 999 as the
column maximum on the input quantities (35 / 8 rows) -- a candidate, not
adjudicated.  CONTENTS.org has the table.

Refs #865, #861, #857.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- d0ec4b32f Niger 2014-15: key households on (GRAPPE, MENAGE, EXTENSION) (GH #679)
ECVMA2 identifies a household by (GRAPPE, MENAGE, EXTENSION) -- EXTENSION 0 =
found in place, 1 = moved whole, 2 = a split-off interviewed as a household
of its own -- but every 2014-15 table built `i` from (GRAPPE, MENAGE), so 59
of the 3,617 households were merged into their parents and every
within-household key collided: 210 (t, i, pid) groups in household_roster /
individual_education / people_last7days (165 of them two POPULATED people,
not the "blank line beside a real one" CONTENTS.org described), 2,006 in
assets, 367 in plot_inputs, 280 in livestock, 46 in crop_production.  All
are 0 with the full key.

- 2014-15/_/data_info.yml: EXTENSION is the third element of all 7 `i:`
  lists (sample, household_roster, individual_education, shocks,
  food_security, assets, housing).  Every 2014-15 source file carries the
  column, checked one by one.
- the five wave scripts (crop_production, livestock, people_last7days,
  plot_inputs, plot_labor) pass all three columns to niger.i, which appends
  the extension as one digit: '1001' -> '10010', (2,1,2) -> '20012'.
- panel_ids.json / updated_ids.json regenerated with `make panel-ids`.

DECISION, stated rather than taken silently: extensions 0 and 1 link to the
2011-12 parent; extension 2 (split-off) is a NEW household and is NOT
linked.  Linking it would make id_walk rename parent and split-off to one
2011-12 id and re-merge them -- the collision this fix removes, under
another name.  A (GRAPPE, MENAGE) never carries both 0 and 1 (measured:
{0} 3,421, {1} 75, {0,2} 57, {1,2} 2, {2} 3), so linking both never merges
two 2014-15 households.

Crosswalk before -> after: 3,251 -> 3,529 households linked 2014-15 ->
2011-12.  The transitive-chain guard in panel_ids.py skipped 286
households under the old key (a 2011-12 hid spelled like another 2014-15
id) and skips 5 under the longer key.  No linked value is itself a key.
One namespace collision the guard cannot reach is recorded, not fixed: the
unlinked split-off '20012' (g=2, m=1, e=2) is also 2011-12 hid 20012.

Cache: niger.py is in every Niger table's hash; all 18 tables re-warm once
(the whole bundle shares that one re-warm).

Refs #679, #637, #871.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


