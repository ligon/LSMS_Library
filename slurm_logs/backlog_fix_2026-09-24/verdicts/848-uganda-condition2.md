VERDICT: pass -- every before/after figure in the commit and report reproduces from two independent cold builds, untouched cells are byte-identical, the moved tests fail on development and pass on the branch, the diff is confined to the task's files, and the 2019-20 questionnaire instructs "list up to 2 conditions per plot/crop" (for 2018-19 the same reading rests on the data plus the next revision of the same module), so the second slot is a partition of the harvest by condition (summable), not a re-report.

Reviewer: Claude Fable 5.1, job 39199802, 2026-09-24.  Clone at cc25a626c (branch `fix/848-uganda-condition2`); merge-base with origin/development = 88f3fef96; development worktree at b5103b64f.

Environment facts (not branch findings)
- The private `LSMS_DATA_DIR` was NOT seeded with warm parquets: it held 14 `AGSEC4A/B.dta` files and nothing else.  Both "before" and "after" below are therefore genuinely cold builds -- "before" from the development worktree into `$STAGE/data_base`, "after" from the branch clone into `$LSMS_DATA_DIR` -- with `lsms_library.__file__` asserted in-process for each.
- `data_access.get_data_file` called from a non-wave-script caller (a heredoc) materialised the two questionnaire PDFs at `$LSMS_DATA_DIR/../Documentation/` and `$LSMS_DATA_DIR/../../2018-19/Documentation/` -- i.e. OUTSIDE the data root, one level above `$STAGE`.  Pre-existing path-inference behaviour on development, unrelated to this branch; I moved both files into `$STAGE/work/` and removed the stray directories.

1. Refutations (claim -> what I ran -> result)
- "rows 130,606 -> 133,331; 2018-19 B 7,041 -> 8,463; 2019-20 B 7,372 -> 8,675; every other (t, season) identical."  HELD.  `$STAGE/work/measure.py` on both trees; `$STAGE/work/compare.py` aligned the 116,193 untouched rows on the full 8-level index: index equal, 0 unequal cells in all 8 columns (NA-aware).  The two touched cells keep all 7,041 / 7,372 slot-1 keys (0 before-only), add 1,422 / 1,303 keys, and change exactly 14 / 4 common keys (the additive collisions).
- "+1,422 = 1,436 - 14; +1,303 = 1,307 - 4."  HELD, and the agent's explanation of the triage's 3-vs-4 is imprecise.  In-process (`$STAGE/work/rawprobe.py`, `crop_production_for_wave` with slot 2 alone vs slot 1 alone): 1,436 / 1,307 slot-2 rows, 14 / 4 served-key collisions.  At the RAW CODE level the count is also 14 / 4 with zero rows where codes differ but labels agree -- so no code-to-label many-to-one is involved.  The fourth 2019-20 group (`H35904-04-01`, `Other Crop`, `u='Unknown'`, `fresh`) collides because BOTH slots lack a unit code and both sentinel to `Unknown`; the triage evidently required a non-null unit code.  Not a defect, but the CONTENTS sentence "because the served key goes through the code-to-label maps" should say "because a missing unit code sentinels to `Unknown` in both slots".
- "sum(Quantity) 7,309,085.96 -> 7,434,726.31; Quantity_sold 18,134,251.40 -> 18,197,452.70; Value_sold 10,709,007,434.04 -> 10,879,808,638.04."  HELD to the cent.
- "KgFactor non-null 14,149 -> 15,074."  HELD (2018-19 B 7,026 -> 7,951; nothing else moves).
- "u='Unknown' 2018-19 B 0 -> 4, the source's; 2019-20 B 133 -> 137; unknown_condition 0/3 -> 1/4; distinct u 30/30 -> 32/32."  HELD.  Raw file: `s5bq06b_2` null on exactly 4 records that carry a measure -- 3 coffee (crop 810) sales with Quantity, Value and KgFactor, 1 beans (210) row with Quantity 0 and nothing else.
- "harvest_kg 78,532 -> 78,571 rows; 28,680,848.595 -> 29,093,322.408 kg; 2018-19 5,904,803 -> 6,152,896; 2019-20 3,668,441 -> 3,832,822."  HELD (per-wave sums for the five untouched waves identical to the last digit).
- "kg_factor_sources reported 14,050 -> 14,972; survey_median 57 -> 555; metric 86,978 -> 88,190; none 29,521 -> 29,614; reported_implausible 99 -> 102."  HELD.  The test comment's arithmetic ("934 positive factors minus the 9 collisions") gives 925, not the measured 922; the difference is the 3 slot-2 factors classed `reported_implausible` (99 -> 102).  Consistent, just not stated.
- "GrainConflictWarning groups 11 -> 12, one 2018-19 B Condition_sold conflict."  HELD -- counted from the wave-script stderr in both build logs (11 groups in 4 warnings; 12 in 5), and reproduced in-process: old colmap 0 groups for 2018-19, new colmap 1 (`ab7c3a9b...-2-1`, Beans, `Condition_sold` blanked).  2019-20: 0 both ways.
- "KgFactor and harvest_month on the 18 collisions are slot 1's (first())."  HELD: 0 collisions have a missing slot-1 KgFactor with a populated slot-2 one, and 0 have a missing slot-1 month with a slot-2 month, so `first()` composes nothing here.  (The `first()` in that block predates the branch; GH #871's most-complete-row rule has not been applied to it.  Not this PR's scope.)
- "sale rows knowing both units 40,083 -> 41,162; agreement 0.9606 -> 0.9614; largest disagreement cell 162 -> 165; 2009-10 A coverage 0.968."  HELD using the test's own definitions (0.96061 -> 0.96145; 0.96757 unchanged; 162 -> 165).
- "Slot 2 is a CONDITION slot, not a season slot: harvest-end 2018 on 1,423 of 1,435, June 660 / July 504; AGSEC5A ends 2017 on 5,583."  HELD from `s5bq06f_1_2` / `s5bq06f_2` / `s5aq06f_1_1`: {2018: 1,423, 2017: 6, 2019: 6}; months {6: 660, 7: 504}; 5A {2017: 5,583, 2018: 1,555}, month 12 on 3,466.  The 2018-19 form's 5B header reads "planted in the second season of 2017 (first cropping season of 2018)", which is why the slot-2 variable label says "second season of 2017": it is the planting-season phrasing of the same 5B crop.
- "Vocabulary: s5bq06b_2 100% in harvest_units, 69.4% unit-only; s5bq06c_2 100% in harvest_conditions, 91.1% condition-only, modal 45; 2019-20 64.2% / 84.8%; sold side 574/577 and 506/511 non-null."  HELD exactly (46 unit codes / 35 unit-only; 20 condition codes / 9 condition-only).
- "s5bq06d_2 935 non-null, 934 positive, median 20; 2019-20 s5bq06d_1 7,366 / s5bq06d_2 921."  HELD.
- "AGSEC5A 2018-19 has no _2 harvest slot."  HELD: its only `_2` column is `s5aq05_2`.
- "Verified: 224 passed / 8 skipped and 555 passed / 15 skipped."  Reproduced as one run: 582 passed, 8 skipped (below).
- "Cache cost: full Uganda re-warm; data_info.yml moves no hash."  HELD by probe: `_table_cache_hash` differs between the trees for all 5 Uganda tables probed (crop_production, household_roster, sample, food_acquired, plot_inputs) and is byte-identical for Malawi crop_production / household_roster, Niger food_acquired, Nigeria crop_production.
- Site Q (quantity screen), which the report does not mention: 2019-20 offenders 4 -> 5.  NOT a slot-2 quantity.  The added slot-2 `Kg` rows move the `(t, u='Kg')` p90 reference from 360 to 350, and the pre-existing slot-1 Tea row `H42707-04-01` (36,000 kg) crosses 100x at the new reference (102.9x; it was exactly 100.0x before, and the rule is strictly greater).  The three sugarcane offenders are unchanged; 2018-19 stays at 4 of 15,616.  Reference-shift artefact, no action.
- Report's "NullIndexKeyWarning on v (926 rows) is an after-only count; I did not capture it before."  Now captured: 922 before (2018-19: 121), 926 after (2018-19: 125).  The 4 extra NaN-`v` rows are slot-2 rows of households that already lacked a `sample` match; no new household is affected.

2. Claimed vs measured
| quantity | claimed | measured before | measured after |
|---|---|---|---|
| rows | 130,606 -> 133,331 | 130,606 | 133,331 |
| 2018-19 B / 2019-20 B rows | 7,041 -> 8,463 / 7,372 -> 8,675 | 7,041 / 7,372 | 8,463 / 8,675 |
| collisions 2018-19 / 2019-20 | 14 / 4 | -- | 14 / 4 (served key and raw code) |
| sum Quantity | 7,309,085.96 -> 7,434,726.31 | 7,309,085.96 | 7,434,726.31 |
| sum Quantity_sold | 18,134,251.40 -> 18,197,452.70 | 18,134,251.40 | 18,197,452.70 |
| sum Value_sold | 10,709,007,434.04 -> 10,879,808,638.04 | 10,709,007,434.04 | 10,879,808,638.04 |
| KgFactor non-null | 14,149 -> 15,074 | 14,149 | 15,074 |
| u='Unknown' 2018-19 B / 2019-20 B | 0 -> 4 / 133 -> 137 | 0 / 133 | 4 / 137 |
| unknown_condition 2018-19 B / 2019-20 B | 0/3 -> 1/4 | 0 / 3 | 1 / 4 |
| harvest_kg rows / kg | 78,532 / 28,680,848.60 -> 78,571 / 29,093,322.41 | 78,532 / 28,680,848.595 | 78,571 / 29,093,322.408 |
| harvest_kg 2018-19 / 2019-20 kg | 5,904,803 / 3,668,441 -> 6,152,896 / 3,832,822 | 5,904,803.45 / 3,668,441.15 | 6,152,896.01 / 3,832,822.40 |
| kg sources reported/median/metric/none/implausible | 14,050/57/86,978/29,521/99 -> 14,972/555/88,190/29,614/102 | 14,050/57/86,978/29,521/99 | 14,972/555/88,190/29,614/102 |
| GrainConflict groups | 11 -> 12 | 11 | 12 |
| sale rows both / agreement / largest cell | 40,083 / 0.9606 / 162 -> 41,162 / 0.9614 / 165 | 40,083 / 0.96061 / 162 | 41,162 / 0.96145 / 165 |
| 2009-10 A coverage | 0.968 unchanged | 0.96757 | 0.96757 |
| NaN-v rows (NullIndexKeyWarning) | 926 after, before not captured | 922 | 926 |
| Site Q offenders 2018-19 / 2019-20 | not reported | 4 / 4 | 4 / 5 (reference shift on a slot-1 row, see 1.) |
| index unique / grain_reports / null_read_reports | -- | True / 0 / 1 (harvest_month, pre-existing) | True / 0 / 1 (same) |

3. Regression
- Branch, `-n 4 --dist=loadfile`, BLAS pinned, `LSMS_BUILD_WORKERS=1`, nine Uganda files + `test_schema_consistency.py test_feature_canonical_index.py test_build_transform_hash.py` (`/local/job39199802/pytest_branch.log`):
  `582 passed, 8 skipped, 45 warnings in 288.13s (0:04:48)`
- The branch's four touched test files copied onto the development worktree against the before-root (`pytest_base_movedtests.log`):
  `3 failed, 13 passed, 9 warnings in 11.44s` -- FAILED `test_crop_kg_factor.py::test_uganda_harvest_kg_baseline`, `test_gh842_uganda_harvest_unit.py::test_2018_19_season_B_serves_a_real_unit`, `test_uganda_sold_unit.py::test_shipped_counts_reproduce`.  `test_uganda_crop_condition.py` (comment-only change) passes on both, as expected.  No test in the branch is new; the moved pins and the rewritten gh842 assertion all discriminate.
- `bench/feature_audit/scan.py --features crop_production --countries Uganda`: `records: 171, by status: {'warn': 171}` -- all pre-existing classes (quantity screen, `harvest_month` null in 2009-11, NaN `v`, extra `v` level vs scheme).  Nothing attributable to #848.
- `find lsms_library/countries -name '*.parquet'`: 0 in both trees.  No parquet or other artefact written outside the two private roots by the builds.

4. Scope findings
- Diff vs merge-base is exactly the 11 files in the task brief; no blessed/lockfile/dependency/coverage-snapshot edit; no xfail/skip added; the only `approx` change is the moved Harvest_kg pin with before/after in the commit message, matching measurement.
- `#647` has no comments and no verdict, so leaving the `_11`/`_21` part-harvest slots unwired with the existing one-sentence note is the correct branch of the task's conditional.
- `git status --short` is empty in the clone and was empty in the worktree after restoring the copied tests.
- The gh842 rewrite derives the allowed sentinel count from `s5bq06b_2.isna()` only; a slot-2 unit code OUTSIDE `harvest_units` would also serve `Unknown` and break the equality.  It passes because slot 2 is 100% in-vocabulary today; the premise is measured, not guarded.  Acceptable.

5. Residual risks and what a human must decide
- Nothing blocks the merge.  The one substantive question -- does summing slot 1 and slot 2 into `harvest_kg` double-count a harvest re-weighed dry? -- has questionnaire backing for 2019-20 and data-plus-inference backing for 2018-19, and a human should know which is which.  The 2019-20 form says, on 5A p.13, "*list up to 2 conditions per plot/crop and ask the following questions for each of the conditions" (the 5B page's extracted text does not repeat the footnote, but its block has the identical one-pair-per-slot layout and the file ships the same `_2` family), i.e. the slots partition the harvest by state, each with its own quantity, unit, kg factor and sale.  The 2018-19 form (pp.13-14, 24-25) carries NO such instruction in its text; for that wave the partition reading rests on the data below, on the file shipping the identical `_2` family, and on 2019-20 being the next CAPI revision of the same module.  The data agree: on the 934 2018-19 rows carrying both kg factors, slot-2 kg / slot-1 kg has median 1.83 with only 4.7% within +/-10% and 46% above 2x (a dried re-report of the same mass would sit below 1), and the dominant pairs are (fresh in pods -> dried grain) 423, (fresh in cob -> dried grain) 222 -- a small fresh portion beside the bulk dried harvest.
- The 18 summed collisions are records that report the SAME condition in both slots, which the instrument does not intend; summing follows the existing de-dup policy and is the same open question as the pre-existing 433 groups (CONTENTS "OPEN GRAIN QUESTION").  Not new.
- Cost, as stated: full Uganda re-warm on merge (every Uganda L2 hash moves).  `.coder/coverage/latest.csv` was not regraded on this branch; the Uganda `crop_production` cells will move at the next `make matrix`.

6. Recommended follow-up commits (optional; none required for merge)
- `lsms_library/countries/Uganda/_/CONTENTS.org` (#848 DONE entry): replace "because the served key goes through the code-to-label maps" with "because both slots lack a unit code on that record and sentinel to `Unknown`"; add the NaN-`v` count 922 -> 926 (2018-19 121 -> 125) beside the other before/after rows.
- `tests/test_crop_kg_factor.py` comment: "934 positive factors minus 9 collisions, minus the 3 that grade reported_implausible (99 -> 102) = +922".
