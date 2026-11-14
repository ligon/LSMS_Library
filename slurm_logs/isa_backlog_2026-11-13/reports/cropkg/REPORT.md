# REPORT.md

# cropkg: GH #1559, a report-only screen of survey kg against price-implied kg per unit, and a design memo for the rung (revised after red team)

## Changes since b3365f8d4

Two commits:
- `5a7ee1a7ff75936b19c8b835a100591090bce0b7` (code: F4, F5, F6)
- `529f0bd57c667e69d11a71f859f6244acc0c5d5d` (`CONTENTS.org`: F1, F2, F7, F9)

The memo `out/RUNG_DESIGN.md` is revised as well.

**Screen code**
- **F6, its own guard.** A failure in the distributional audit used to make `check_quantities` return before the price screen ran. Now that failure still warns and still drops its own partial findings, as before, but the price screen runs.
  - The price screen already had its own guard the other way round.
  - New test `test_each_screen_has_its_own_guard` covers both directions.
- **F4, warning text.** "A sack or a total keyed against a small unit's label has been the answer in this corpus" now reads "…may be the answer; that is not established".
- **F5, stale docstring.** The module docstring no longer says only Niger and Mali serve `Quantity_kg`. It cites the Nigeria stream at integration: 2016Q1 serves 8,507 such rows, 5,520 judged, none named, median 1.25 (red team jobs 41278029 and 41278382).
- **Effect on data.** Frames are still `assert_frame_equal` to base: Niger 49,042 rows and Mali 50,752, attrs equal (job 41279383).

**F1: Mali 2017-18's `s7fq13d` is unwired survey evidence.** I joined it to the served rows (`scripts/analysis3.py`, job 41279383).
- **The column.** `eaci17_s7fp2.s7fq13d`, "Equivalent en Kg", is the fourth sub-column of Q7F13.
- **The join.** I rebuilt the keys as `2017-18/_/crop_production.py` builds them. 2017-18's `updated_ids` map is empty, so no id walk is needed.
  - 24,449 of 24,449 served seasonal rows join.
  - `Quantity` equals `s7fq13a` on all 23,348 positive rows.
  - `s7fq13d` is positive on 23,205 of them, and is exactly 1 on all 1,688 `Kg` rows.
  - So it is a factor per unit, which is the canonical `KgFactor` slot, the `reported` layer.
- **Wired through today's ladder (simulated):**

  | | today | price rung | `s7fq13d` wired |
  |---|---|---|---|
  | Mali 2017-18 kg | 1.66M | 16.4M | **32.8M** |
  | household-crops with kg | 7.6% | 65-85% | 94.6% |

  - The rows would resolve as: `reported` 21,039, `survey_median` 2,345, `none` 1,535.
  - The reported-factor screen would refuse 2,167 rows. I did not split the reasons.
  - No other wave changes.
- **As an answer key** for the rung's 9,981 rows: rung / `s7fq13d` is 1.05 at the median (p10-p90 0.60-1.20, 94% within 2x).
  - `Mil` / `Sac` 1.20
  - `Maïs` / `Sac` 1.05
  - `Arachide` / `Sac` 0.60
  - `Coton` / `Sac` 1.00
  - `Mil` / `Gerbe` 0.50
- **The keys disagree in sign for millet:** 1.20 against `s7fq13d`, but 0.81-0.83 against the held-out sack and `KgFactor_sold`.
- **2014-15's `s3aq08c`** mixes factors and totals, so it is not usable as is (red team).
- **In the memo:** sections 2, 6 and 7(d), and section 10 Q0. Q1, Q2 and Q8 are re-posed around it.
- **In Mali `CONTENTS.org`:** a new paragraph, and the 2026-11-13 entry is revised.

**F2: the gates count sale rows, not households** (red team job 41278567).
- **Screen:** a 5-household gate names the same 157 rows.
- **Rung, Niger 2018-19:** roselle's 5 `Kg` sales come from 2 households. A 5-household gate cuts the wave's servable rows from 161 to 23.
- **Rung, Mali:** the 5 / 7 / 5 sales behind its main cells are 5 / 7 / 5 households, so the gate leaves Mali unchanged.
- **Exact duplicate sale records** count separately today: Niger 107 (2011-12) and 126 (2018-19); Mali 58 (2014-15) and 19 (2018-19).
- **In the memo:** section 3, the summary, and a re-posed Q8 (a floor counted in households). Also added to Niger `CONTENTS.org`.

**F7: Niger 3E.**
- The net 79 rainy rows lost is 103 lost and 24 gained.
- All 103 are one cell, `Oseille` / `Tiya` 2011-12. A single 3E `Kg` sale moves that cell's spread from 4.0 to 12.0, past the gate: a one-donor veto under max/min below 10 sales.
- The measurement the ruling was made on excluded 3E both ways.
- Updated in the memo, in Niger `CONTENTS.org`, and in Q-S1.

**F9: corrections in the memo and `CONTENTS.org`.**
- **"Ninefold"** is a change of coverage, not of level.
- **`KgFactor_sold` counts:** 48 / 264 sale rows with a factor and a positive sold quantity; 39 / 261 if the sale must also be priced. The medians are the same.
- **Farm-level rows that carry a quantity are rung recipients.** For Mali the red team counts 244, 159 and 221 by wave; I did not check the per-wave split. Niger's 3E rows are recipients too.

**Must-states, now in the PR body:**
- the scope of the Site Q channels and what strict changes (F3);
- Nigeria at integration (F5);
- 6 new severity-C findings in `scan.py` per Niger `crop_production` cell (F8);
- the exclusions are defensive and pinned by tests.

## The ruling

@ligon 2026-11-12, "GH1559 price-implied kg per unit for crops"; the user selected "Also build a rung":

> (1) a report-only screen flagging survey kg outside 0.1x-10x of the price-implied kg per unit (baseline: kg sales plus sacks at stated mass), and (2) design a price-implied INFERRED rung for crop units in harvest_kg_factors, as its own design, never merged with the food ladder.

- Part (1) is code on the branch.
- Part (2) is a memo only, in `out/RUNG_DESIGN.md`.

## 1. Commits

Branch `feature/1559-crop-kg-screen-20261113`, base `f138a3d61c203f892148592675a7ef72e160a322`. Every commit printed `SUCODER: published`.

| SHA | what |
|---|---|
| `4aefeec251547b06c31dceb38de0f3867be08ad9` | The screen, its exclusions and tests. |
| `cd1eebc6a1525a1831599f79467281d63aa365ba` | Read the flag with `notna()`: pandas 3 infers `str`. |
| `7d27171ab433df54d45196f9395c04ea619a796c` | Rewritten on integer codes. The first cut cost 314 ms of 946 ms. |
| `acd6fccba70e07867a152044f95be1f2f0656ed2` | Pins. |
| `49aea7436bf1e6a34d47851d88836363da103818` | `CONTENTS.org` and docstring. |
| `b4a3b498edeca844cde27e84b9e95d5afd2c54ad` | `CONTENTS.org` by-crop checks. |
| `b3365f8d494e39eb764c1fca9f45c9f52e8a4ae5` | Mali `CONTENTS.org` floor. |
| `5a7ee1a7ff75936b19c8b835a100591090bce0b7` | Red team F4, F5, F6. |
| **`529f0bd57c667e69d11a71f859f6244acc0c5d5d`** | **Head.** Red team F1, F2, F7, F9 in `CONTENTS.org`. |

`git diff --stat f138a3d61..HEAD`:

```
 lsms_library/_build_registry.py             |   5 +
 lsms_library/countries/Mali/_/CONTENTS.org  |  55 +++
 lsms_library/countries/Niger/_/CONTENTS.org |  60 +++
 lsms_library/quantity_audit.py              | 578 +++++++++++++++++++++++++++-
 tests/test_gh1559_survey_kg_price_screen.py | 496 ++++++++++++++++++++++++
```

`transformations.py` and `country.py` are untouched.

## 2. What the screen is

- **Where it runs.** `quantity_audit.audit_survey_kg_prices`, called from `check_quantities` (Site Q, read path, `Country._finalize_result`). It has its own guard both ways (F6). Its estimator is `survey_kg_price_factors`.
- **What it judges.** `Quantity_kg / Quantity` against the price-implied kg per unit:
  - the numerator is the median `Value_sold / Quantity_sold` of the cell's sales in that unit, using the sale's own `Unit_sold`;
  - the denominator is the median price per kg of the same wave's and crop's sales in `Kg`, or in a unit stating its mass (`KNOWN_METRIC` plus `_parse_explicit_metric`, volumes excluded), read at that mass.
- **Rungs.** `(t, j, state, u)` where both sides record a state, then `(t, j, u)`. It never pools across waves or crops.
- **Gates.** At least 5 sales each side, and a spread of at most 10x on both sides (p90/p10 at n >= 10, max/min below).
- **Which rows.** Only units stating no mass are judged, the exact complement of `kg_survey_off_label`.
- **What it ignores.** `Quantity_sold_kg` is not read, because it is the survey's own arithmetic.
- **Report-only.** It returns its input unchanged. Each offender is named with its implied factor, both medians, both cell sizes and the rung.
- **Channels: Site Q's.** `quantity_reports()` with `site='survey-kg-price'` and `column='kg_survey'`; the `LSMS_QUANTITY_STRICT` lever; and `SurveyKgPriceWarning`, a subclass of `QuantityImplausibleWarning`.
  - **Scope point (F3).** The 2026-11-12 QUESTION BACK offered "report-only 0.1x-10x band" and "band + Site Q channels" as separate options. The ANSWERED ruling says only "a report-only screen", and the brief asked me to choose the lever. I chose Site Q's, because #1559's body names the missing channels as the gap.
  - **What strict changes (measured, red team job 41277988).** Only Niger one-wave reads of 2011-12, 2014-15 and 2021-22 newly raise. These were already raising at base:
    - Niger 2018-19;
    - Niger all-waves;
    - every Mali read;
    - every Nigeria-overlay read.

    No CI workflow sets the lever.

## 3. Idiosyncrasies named, per country

I read the crop sections of both `CONTENTS.org` files by section, not in full (Niger 3,985 lines, Mali 4,085), and grepped both for TODO, WAITING and LOGBOOK entries; none bore on this change.
- **Niger:** lines 1129-1213, 2366-2505, 2506-2566, 2627-2771, 2915-3033, 3051-3086, 3681-3816.
- **Mali:** lines 879-983, 1424-1523, 1711-1751, 2142-2173, 3224-3270, 3956-4038.
- **Missed, as the red team found:** the 2026-11-09 and -10 Mali entries at lines 3718-3906, which name `s7fq13d`.

**Niger**
- **Units.** `u` is the harvest unit; `Unit_sold` is the sale's own unit (GH #824). Labels are compared case-folded, because s16d's labels are lower-case.
- **Farm-level sale rows** (GH #1457) are donors, never recipients.
- **s16d repeats** are dropped upstream.
- **3E dry season** (GH #1462): the sale covers the first cycle and the value window is unstated. 3E is included in the screen and vetoes one cell (F7).
- **`Quantity_kg`** is a total (GH #1436). 2021-22's is an estimate.
- **State** is recorded only in the EHCVM waves.
- **2018-19** asks no kilograms. It has 21 `Kg` sales, from few households (F2).

**Mali**
- **Units.** EAC-I harvest units are unsized (`Sac`). Sold units are compared by label: code 10 is `Autre` on the harvest list and `Tonne` on the sold list.
- **Tree sales** are a monthly quantity beside a total value (`tree-sale-window`).
- **2021-22** has 70 mass-less survey-kg rows.
- **`KgFactor_sold`** is on the field sales.
- **New:** `s7fq13d` is unwired (F1).

**Contradictions with `CONTENTS.org`:** none. On the ruling's "only 2021-22 records [state]": that is exact for the waves with survey kg, but the EHCVM 2018-19 waves record a state as well.

## 4. Measurements

**Jobs**, all savio4_htc, private node-local roots, cold:

| job | commit | what |
|---|---|---|
| 41275992 | `cd1eebc6a` | build, compare, analysis, prior Part 2 reproduction, tests |
| 41276357 | `acd6fccba` | rerun after the rewrite; every section identical |
| 41276485 | `49aea7436` | build, compare, tests |
| 41276727 | `49aea7436` | `analysis2` |
| 41279383 | `5a7ee1a7f` | quick tests, census, build, compare, `analysis3`, tests |
| 41275939, 41276345 | | census and probes |
| 41276953, 41277355, 41279604 | | census |
| 41277110 | | `issues.log` attribution |

- Scripts are in `reports/cropkg/scripts/` and aggregates in `reports/cropkg/out/`.
- Red team jobs 41277988-41278567 are cited where used.

**The screen changes nothing.**
- Frames: `assert_frame_equal` exact, attrs equal (jobs 41275992, 41276357, 41276485, 41279383).
- Warnings: Niger gains exactly 6 `SurveyKgPriceWarning`s; Mali gains none.

**Firing, Niger:**

| wave | survey-kg rows | stating no mass | judged | low | high | firing / judged |
|---|---|---|---|---|---|---|
| 2011-12 | 9,258 | 7,070 | 1,932 | 6 | 6 | 0.62% |
| 2014-15 | 8,324 | 7,246 | 6,394 | 26 | 89 | 1.80% |
| 2018-19 | 0 | | | | | |
| 2021-22 | 11,942 | 9,798 | 9,020 | 2 | 28 | 0.33% |

- Total: 157 of 17,346 judged.
- Mali: 1 row judged, none named.
- Nigeria with the Nigeria stream (red team): 2016Q1 has 5,520 judged and 0 named.

**Tails** (survey / implied):

| wave | p1 | p50 | p99 |
|---|---|---|---|
| 2011-12 | 0.25 | 1.14 | 4.60 |
| 2014-15 | 0.14 | 1.13 | 11.39 |
| 2021-22 | 0.30 | 1.25 | 5.17 |

- The largest folds are 31.5x / 100x / 29.6x high and 750x / 58x / 78x low.
- The named rows cluster in cells: 2014-15 Niébé / Tiya and Sorgho / Botte (30 each), and 2021-22 Mil / Botte (16, at 200 kg).

**Reproduction of the prior Part 2:** 9 / 116 / 27 named, 1,978 / 6,394 / 9,020 judged, exactly.

**Alternatives** (the screen names 157):

| alternative | names |
|---|---|
| state-blind | 159 |
| region then national | 155 |
| 3E excluded | 154 |
| spread gate on the baseline only (red team) | 163, judging 64 more |
| 5-household gate (red team) | 157 |

**Overlap with `kg_survey_off_label`:** 0 rows (75 against 157).

**Cost:** 47 ms of a 659 ms warm Niger read (red team: 52 of 751), and 41 of 658 ms on Mali.

**Hygiene**
- In-tree parquets: 0. No `dvc` CLI was used.
- `issues.log`: 120 lines in each test job. 108 come from `test_failure_is_not_absence.py`, identically on base. The other 12 are unattributed (cache-gated tests).
- **Disclosed:** one trivial `python3 -c "import sys"` on n0035, on piped JSON. No library import, no data.

## 5. Hash census

| from | to | job | `btf` | `fwimp` | tables | waves |
|---|---|---|---|---|---|---|
| `f138a3d61` | `529f0bd57` (head) | 41279604 | 0 of 42 | 0 of 38 | 0 of 555 | 0 of 2,321 |
| `f138a3d61` | `5a7ee1a7f` | 41279383 | 0 | 0 | 0 | 0 |

- Every earlier commit also moved 0.
- The exclusions are defensive: removing any one of them moves 0 (red team), and tests pin them.
- **Rung probes:** P1-P3 moved 0. The positive control P4 (a statement in `food_kg_factors`) moved 3 / 75 / 503.

## 6. Tests

`pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -q -rs -p no:cacheprovider`, BLAS pinned, 84 files. The list is in `out/pytest-41279383.files`; it is the earlier set plus the new test.

| commit | job | passed | skipped |
|---|---|---|---|
| `5a7ee1a7f` | 41279383 | **3,468** | 1 |
| `49aea7436` | 41276485 | 3,467 | 1 |

- The one skip is the warm Tanzania parquet, so the Niger and Mali real-data classes ran.
- The head differs from `5a7ee1a7f` only in `CONTENTS.org`.
- The new file has 25 synthetic and 9 cache-gated tests.
- **Not run:** `Feature()`, the make backend, and strict on real data (the red team measured strict).

## 7. Issues

**#1559: partial.** Part (1) is done for mass-less units with price coverage. Part (2) awaits a ruling on the memo.

Drafted residuals:
- **Tier B, "Wire Mali 2017-18 `s7fq13d` as `KgFactor` (GH #1559 follow-up)".** It joins every served row and is 1 on every `Kg` row. Simulated, the wave goes from 1.66M to 32.8M kg, with 2,167 rows refused by the reported-factor screen. 2014-15's `s3aq08c` needs its own rule. Expect one Mali re-warm (not measured).
- **Tier B, "Build the price-implied crop rung (GH #1559 part 2)".** After the memo's Q0-Q10 are ruled.
- **Tier C, "Mass-less survey kg with no price cell stay unscreened".** Niger 2011-12 judges 27% of its eligible rows.
- **Tier C, `scan.py`.** Add a taxonomy entry for Site Q / `SurveyKgPriceWarning`. Today each warning is a severity-C finding: 6 more per Niger `crop_production` cell (red team F8).

## 8. Questions for the user

**Screen**
- **Q-S1.** 3E is included. The measurement behind the ruling excluded it. Including it names 157 rows against 154, and the rainy-module loss is a one-donor veto in one cell. Exclude donors carrying a registered `*-sale-window` key?
- **Q-S2.** Keep the state rung?
- **Q-S3.** National only?
- **Q-S4.** Gate both sides (157 names) or the baseline only (163)?
- **Q-S5.** Widen the screen to reported `KgFactor`?
- **Q-S6.** Keep the shared strict lever (see §2)?

**Rung** (memo §10):
- **Q0:** wire `s7fq13d`, first.
- **Q1:** default on, re-posed around Q0.
- **Q2:** reported factors as a `survey_median` source.
- **Q3:** window-mismatched donors.
- **Q4:** cross-wave borrowing.
- **Q5:** the 250 kg ceiling.
- **Q6:** declared `u_kg` in the baseline.
- **Q7:** region.
- **Q8:** a floor counted in households.
- **Q9:** rows Site Q names.
- **Q10:** groundnut.

**Integration:** the Nigeria stream's `Quantity_kg` is judged (0 named on its overlay). `CLAUDE.md`'s Site Q section could gain a row; I did not edit it.

## 9. Memo summary (`out/RUNG_DESIGN.md`)

- **Read first.** Mali 2017-18's main use of the rung is unwired survey evidence (`s7fq13d`). Wiring it is ruling Q0.
- **Coverage, without Q0.**
  - Niger 2018-19: 161 of 13,240 rows. Its baselines are 5 `Kg` sales from 2 households, and roselle's `Gros sac` reads lighter than its `Sac Moyen`.
  - Mali 2017-18: +14.8M kg on 1.66M, a change of coverage, not level. Its main cells rest on 5-7 `Kg` sales and households. The top 1% of rows carry 27%. One row Site Q already names would get 857k kg.
- **Answer keys:**
  - **`s7fq13d`, the best:** rung / `s7fq13d` is 1.05 at the median (millet 1.20, maize 1.05, groundnut 0.60).
  - **Held-out sacks:** 0.80 for 100 kg, and 1.25 (Niger) / 1.00 (Mali) for 50 kg. The tonne key is cotton and is not evidence.
  - **Survey kg:** 1.13-1.25.
  - **`KgFactor_sold`:** 0.95 without cotton.
  - **Agreement:** cereals within about ±20%. Groundnut and rice are not.
- **Design.**
  - **Ladder:** its own `price_implied` layer, after `inferred` (dormant on crops) and before `none`.
  - **Keys:** `(t, j, state, u)` then `(t, j, u)`, with an optional caller-resolved region.
  - **Gates:** at least the screen's, probably counted in households.
  - **Disclosure:** per row and in attrs.
  - **Never** inside `food_kg_factors`' closure.
- **Caveats, measured:**
  - **State:** groundnut 2.35x per Tiya; EAC-I records no state.
  - **Selection:** 21-24% of survey-kg rows come from sellers.
  - **Seasonality:** about 1.3x in Mali (prior job).
  - **Survey offset:** 1.1-1.4x, with a sign the lot-size lean cannot explain.
- **Cache cost:** 0 (P1-P3, with the P4 positive control). Q0 would cost an estimated one Mali re-warm.

---

