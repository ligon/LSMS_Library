**CHANGES.** Candidate `b3365f8d494e39eb764c1fca9f45c9f52e8a4ae5` against base `f138a3d61c203f892148592675a7ef72e160a322`.

The screen code holds up under every test I ran. Nothing in it blocks the PR. What blocks is the rung memo, together with its echo in the Mali `CONTENTS.org` entry: it leaves out the fact that most changes the user's decision. One wording fix and one scope point for the PR body also remain.

## Findings

**F1 — should-fix; blocks the memo going to the user, not the screen PR. Mali's harvest files already carry the instrument's own kilograms per unit, unwired.**
- **Evidence** (jobs 41278221 and 41277988, `out/D-s7f-41278221.json`):
  - Mali 2017-18 `eaci17_s7fp2.s7fq13d` is "Equivalent en Kg", the fourth sub-column of Q7F13 (`Mali/_/CONTENTS.org` lines 3729 and 3863-3864, DDI V876).
  - It is positive on 23,203 of the 23,350 harvest rows with a positive quantity.
  - It is a per-unit factor: 1 on 100% of `Kilogramme` rows; median 100 for `Sac` (15,052 rows), 250 for `Charretée`, 25 for `Gerbe`.
- **Why it matters:** it fits the canonical `KgFactor` slot exactly, the `reported` layer, which outranks any `price_implied` layer. So the memo's "main use" (Mali 2017-18: +14.8M kg on 9,981 rows) is mostly survey evidence waiting to be wired, not a gap a price rung should fill.
- **As an answer key** for the rung's `Sac` masses:

  | crop | rung | `s7fq13d` | rung / `s7fq13d` |
  |---|---|---|---|
  | Mil | 120 | 100 | 1.20 |
  | Maïs | 105 | 100 | 1.05 |
  | Arachide | 60 | 100 | **0.60** |
  | Coton | 100 | 100 | 1.00 |
  | Sésame | 94 | 100 | 0.94 |

  Also: `Gerbe` of millet 12.5 against 25 (0.50), `Charretée` of cotton 214 against 250 (0.86). Median kg per household-crop is 1,550 / 1,200 / 500 under `s7fq13d`, against the rung's 1,560 / 1,050 / 360 (millet / maize / groundnut).
- **The keys disagree in sign for millet:** 1.20x against `s7fq13d`, but 0.81-0.83x against the held-out 100 kg sack.
- **Limits:**
  - Raw-side only, not joined to served rows.
  - `Sac` = 100 on 95% of millet rows, so it looks like a nominal conversion, not a weighing.
  - 2014-15's `s3aq08c` (9,502 of 9,571 rows) mixes factors and totals: it equals the quantity on 10% of `Kg` rows, and on those 511 rows quantity × factor sums to 45.5M kg. It is not usable as is.
- **No ruling found** leaving this column unwired. The "No … kg factor … follows" lines at `CONTENTS.org` 3750 and 3898 concern the state bridge only. The worker read lines 1424-1523, which mention the harvest "survey factor 1,000 kg" on `Autre`.
- **Fix:** add it to memo §6, §7 and §10 and to the Mali 2026-11-13 entry, both as an unwired instrument answer and as the best answer key. Re-pose Q1, Q2 and Q8, and list wiring it as `KgFactor` as a separate ruling to take first.

**F2 — should-fix (memo); clean on the screen. The gates count sale rows, not households.** (job 41278567)
- **Screen:** 4 of 17,346 judged rows sit on a cell with fewer than 5 households on either side, and 0 of the 157 named rows do. A 5-household gate names the same 157.
- **Rung:** Niger 2018-19 roselle's "exactly 5 `Kg` sales" come from 2 households. A 5-household gate cuts that wave's servable rows from 161 to 23. Mali's 5/7/5 `Kg` sales are 5/7/5 distinct households.
- **Exact duplicate sale records** (same household, crop, unit, quantity and value) count separately: Niger 107 (2011-12) and 126 (2018-19); Mali 58 (2014-15) and 19 (2018-19).

**F3 — must-state, for the user. Site Q channels and the shared strict lever.**
- **Scope:** the 2026-11-12 "QUESTION BACK" offered "report-only 0.1x-10x band" and "band + Site Q channels" as separate options. The ANSWERED ruling says only "a report-only screen". The brief then asked the worker to choose the lever.
- **Measured change** (job 41277988): under strict, only Niger one-wave reads of 2011-12, 2014-15 and 2021-22 newly raise.
  - Niger 2018-19, Niger all waves and every Mali read already raised at base.
  - On the Nigeria overlay nothing changes: 2011Q1, 2013Q1 and 2016Q1 one-wave reads and the all-waves read already raise at base.
- **CI and tests:** no CI workflow sets `LSMS_QUANTITY_STRICT`. Every test that sets it uses synthetic frames or a stub.

**F4 — should-fix, one line.** The warning in `_format_survey_kg_price_report` says "A sack or a total keyed against a small unit's label has been the answer in this corpus". The candidate's own Niger `CONTENTS.org` entry says that "is not established". Soften it to "may be", or drop it.

**F5 — should-fix at integration.** The module docstring and the CONTENTS entries say "only Niger and Mali serve `Quantity_kg`", which goes stale with the Nigeria stream. Measured on a Nigeria `3d34550d5` overlay (jobs 41278029, 41278382):
- 2016Q1: 8,507 rows carry `Quantity_kg`, all in units stating no mass; 5,520 judged; 0 named; median survey / implied 1.25; 81% within 2x.
- Frame equal, no new warning, strict unchanged, 0 hashes moved.
- 364 tests passed, 3 skipped.

**F6 — note.** The screen's own `try` works: with an injected failure the 2018-19 report still files, a "could not run" `SurveyKgPriceWarning` appears, and the frame is equal. The reverse does not hold: when the distributional audit fails, `check_quantities` returns early, the price screen never runs, and only the generic warning appears. Give the price screen its own guard regardless.

**F7 — note on 3E.** It is within the ruling's words, but the measurement the ruling was made on excluded 3E both ways. Including it names 157 rows against 154 without.
- The "79 rainy rows lose" is net: 103 lost and 24 gained.
- All 103 are one cell, Oseille / Tiya 2011-12. A single 3E `Kg` sale moves that cell's baseline from 5 to 6 sales and its max/min spread from 4.0 to 12.0, past the gate.
- So it is a one-donor veto under the max/min statistic below 10 sales, not a broad difference in 3E prices.
- Excluding donors by a registered `*-sale-window` `Derivation` key would be a generic rule, not a Niger one.

**F8 — note.** `bench/feature_audit/scan.py` has no taxonomy entry for these warnings, as for the existing Site Q ones. Each becomes a severity-C finding: +6 per Niger `crop_production` cell. The coverage grader ignores warnings, so no tier moves.

**F9 — notes on the memo.**
- **"Ninefold" is coverage, not level.** In Mali 2017-18, the share of household-crops with positive kg goes from about 5% to 65-85%. Per-household medians land in the 2018-19 and 2021-22 range.
- **`KgFactor_sold` key:** medians reproduce (1.00 / 0.95; groundnut 1.50, millet 0.83), but my counts are 39 / 261 sales against the memo's 48 / 264.
- **"Farm-level rows are never recipients"** is true only of the #1457 sale rows. Mali's Farm-level tree rows carry a quantity (244, 159 and 221 by wave) and would be rung recipients. Niger's 3E Farm-level rows are recipients too (33 judged).

## What I re-ran, cold, in private roots

- **Report-only contract:**
  - Niger 49,042 and Mali 50,752 rows pass `assert_frame_equal` (exact) base against candidate, with attrs and digests equal.
  - Calling `check_quantities` directly on a deep copy leaves values, attrs, dtypes and index codes unchanged.
  - New warnings: exactly 6 on Niger, none on Mali.
- **Firing:** low/high 6/6, 26/89 and 2/28 by wave; judged 1,932 / 6,394 / 9,020.
- **Independent estimator** (plain pandas): 0 disagreements on implied mass, both counts, rung or flag, over Niger, Mali and Nigeria.
  - Stated-mass and unit-usability readings agree on every label.
  - Case-folding merges no label on any served frame, so it is a guard only.
- **Variants:**
  - State-blind: 159 named.
  - Spread gate on the baseline only (Q-S4, which the worker left unmeasured): 163 named, none lost.
  - 3E out as donor: 154 named.
- **Cost:** 52 ms of a 751 ms warm Niger read (693 ms with the screen stubbed out), 47 ms of 703 ms on Mali, 44 ms on the 66k-row Nigeria frame.
- **Hash census:** 0 of 42 build fingerprints, 0 of 38 framework-import fingerprints, 0 of 555 tables, 0 of 2,321 waves, with no errors on either side.
  - Removing any single exclusion, or `check_quantities`'s, moves 0: the exclusions are defensive.
  - Edits to the factor function, the stated-mass helper and the spread constant move 0.
  - The positive control (a statement in `food_kg_factors`) moves 3 / 75 / 503.
  - Removing either new exclusion fails two tests, so they are pinned.
- **Tests:** 10 files, 340 passed, 1 skipped (job 41278016).
- **Memo numbers reproduced:**
  - Mali 2017-18: +14,789,672 kg on 1,656,241 served, 9,981 rows.
  - Top 1% of rows carry 27.4%; the 857k-kg row is one Site Q already names.
  - Held-out sacks: 0.80 / 1.25 (Niger 100 / 50 kg), 0.80 / 1.00 (Mali).

**Not re-run:**
- the region variant, selection and seasonality;
- the make backend;
- `Feature` under strict on base (inferred from the per-wave reads);
- the worker's 84-file test set;
- the 12 `issues.log` lines from my test job, which I did not attribute.

**Hygiene:**
- In-tree parquets: 0. No `dvc` CLI was used.
- Three trivial `python3` calls ran on n0035 by mistake: an edit to my own script, `print()`, and `-V`. None imported `lsms_library` or touched data.
- All my jobs have ended.

## The worker's questions

- **Q-S1 (3E):** see F7. Tell the user that the measurement behind the ruling excluded 3E.
- **Q-S2 (state rung):** keeping it is supported; it removes 2 low names in 2021-22.
- **Q-S3 (national only):** not re-measured.
- **Q-S4 (spread gate on both sides):** now measured. A baseline-only gate names 6 more rows and judges 64 more.
- **Q-S5 (`KgFactor`):** a widening of the ruling. Mali's `s7fq13d` becomes relevant once wired.
- **Q-S6 (shared lever):** see F3.
- **Rung questions:** Q1, Q2 and Q8 need re-posing per F1 and F2. Q9 is confirmed (the 857k-kg row). Q10 is confirmed: groundnut reads 0.60x against `s7fq13d`.

## Must-state for the PR body

1. The scope point in F3, with what strict actually changes.
2. Nigeria at integration: 5,520 judged, 0 named, and the stale docstring.
3. The +6 severity-C findings in `scan.py`.
4. The exclusions are defensive and pinned by tests.
5. The memo needs the F1 and F2 corrections before the user rules.

Scripts are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg-redteam/scripts/` and aggregates in `.../reports/cropkg-redteam/out/` (`A-*`, `B-*`, `C-*`, `D-*`, `E-*`, `F-*`). Jobs A-C, E and F used `rtlib_v1.py`. Job F used `rtlib.py`, which adds the household gate.
