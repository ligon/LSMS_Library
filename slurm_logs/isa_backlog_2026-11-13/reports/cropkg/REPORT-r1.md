
# cropkg: GH #1559, a report-only screen of survey kg against price-implied kg per unit, and a design memo for the rung

## The ruling

@ligon 2026-11-12, "GH1559 price-implied kg per unit for crops", the user selected "Also build a rung":

> (1) a report-only screen flagging survey kg outside 0.1x-10x of the price-implied kg per unit (baseline: kg sales plus sacks at stated mass), and (2) design a price-implied INFERRED rung for crop units in harvest_kg_factors, as its own design, never merged with the food ladder.

- Part (1) is code on the branch.
- Part (2) is a memo only, in `out/RUNG_DESIGN.md`.

## 1. Commits

Branch `feature/1559-crop-kg-screen-20261113`, base `f138a3d61c203f892148592675a7ef72e160a322`. Every commit printed `SUCODER: published`.

| SHA | What |
|---|---|
| `4aefeec251547b06c31dceb38de0f3867be08ad9` | The screen, its exclusions and its tests. |
| `cd1eebc6a1525a1831599f79467281d63aa365ba` | Reads the flag with `notna()`. pandas 3 infers `str` for the flag column, so a missing flag came back as NaN, not None. The quick tests caught it (job 41275938, 7 failed). |
| `7d27171ab433df54d45196f9395c04ea619a796c` | Rewritten on integer codes. The first cut cost 314 ms of a 946 ms warm Niger read. |
| `acd6fccba70e07867a152044f95be1f2f0656ed2` | Pins the Niger and Mali firing in the tests. |
| `49aea7436bf1e6a34d47851d88836363da103818` | Niger and Mali `CONTENTS.org` entries, and the module docstring with the measured numbers. |
| `b4a3b498edeca844cde27e84b9e95d5afd2c54ad` | `CONTENTS.org`: the follow-up checks by crop. |
| **`b3365f8d494e39eb764c1fca9f45c9f52e8a4ae5`** | **Head.** Mali `CONTENTS.org`: the rung's baselines sit at the floor. |

`git diff --stat f138a3d61..HEAD`:

```
 lsms_library/_build_registry.py             |   5 +
 lsms_library/countries/Mali/_/CONTENTS.org  |  31 ++
 lsms_library/countries/Niger/_/CONTENTS.org |  55 +++
 lsms_library/quantity_audit.py              | 568 +++++++++++++++++++++++++++-
 tests/test_gh1559_survey_kg_price_screen.py | 466 +++++++++++++++++++++++
```

`transformations.py` and `country.py` are not touched.

## 2. What the screen is

- **Where it lives.** `quantity_audit.audit_survey_kg_prices`. It is called from `check_quantities`, which is Site Q's hook in `Country._finalize_result`, so it runs on the read path.
  - It has its own `try`, so a failure in it cannot silence the other Site Q findings.
  - Its estimator is `survey_kg_price_factors`.
- **What it judges.** `Quantity_kg / Quantity`, against the price-implied kg per unit:
  - The numerator is the median of `Value_sold / Quantity_sold` over the (t, crop, state, unit) cell's sales in that unit.
  - The unit is the sale's own unit (`Unit_sold`).
  - The denominator is the median price per kg of the same wave's and crop's sales in `Kg`, or in a unit that states its mass, read at that mass.
  - A stated mass is read, never estimated: `KNOWN_METRIC` plus `_parse_explicit_metric`, with volumes excluded.
- **Rungs.** `(t, j, state, u)` is used where both the sale and the harvest record a state. Otherwise it falls back to `(t, j, u)`.
  - It never pools across waves or across crops.
- **Gates.** These are the food ladder's numbers, kept as the screen's own constants:
  - at least 5 sales in the unit;
  - at least 5 sales stating a mass;
  - a spread of at most 10x on both sides (p90/p10 at n >= 10, max/min below).
- **Only units stating no mass are judged.** That makes it the exact complement of `kg_survey_off_label`.
- **It does not read `Quantity_sold_kg`.** That column is the survey's own arithmetic, so reading it would let the survey judge itself.
- **It is report-only.** It returns its input frame unchanged and names every offender with:
  - the implied factor;
  - both medians;
  - both cell sizes;
  - the rung used.
- **Channels: Site Q's.** The ledger, `quantity_reports()` with `site='survey-kg-price'` and `column='kg_survey'`, and the `LSMS_QUANTITY_STRICT` lever.
  - It has its own `SurveyKgPriceWarning`, a subclass of `QuantityImplausibleWarning`.
  - Why this choice:
    - It is Site Q's concern: a value that is present, non-null and implausible.
    - #1559's body names the missing Site Q channels as the gap.
    - Site Q's lower tail and its `Sale_price` screen are already flags rather than verdicts on the same lever.
  - **Cost of sharing the lever:** Niger's only other Site Q report is `Quantity` 2018-19. So under strict, a one-wave read of Niger 2011-12, 2014-15 or 2021-22 now raises where it used to pass. That is question Q-S6.

## 3. Idiosyncrasies named, per country

I read the crop sections of both `CONTENTS.org` files by section, not in full (Niger 3,985 lines, Mali 4,085). I also grepped both for TODO, WAITING and LOGBOOK entries and found none bearing on this change.
- **Niger sections read:** sold unit vs harvest unit (lines 1129-1213); condition (2366-2505); survey kg as totals (2506-2566); dry season (2627-2771); survey kg served (2915-3033); multi-plot sales (3051-3086); sold kg served (3681-3816).
- **Mali sections read:** sized and unsized units, kg factors against labels, `crop_production` u (879-983); `Unit_sold` and tree rows (1424-1523); revised tree sales (1711-1751); field sold factors (2142-2173); survey kg 2021-22 (3224-3270); sold kg served (3956-4038).

**Niger**
- **`u` is the harvest unit; `Unit_sold` is the sale's own unit (GH #824).** The screen prices each sale in its sold unit and judges the harvest row in its `u`.
  - Labels are compared case-folded, because the s16d sold labels are lower-case.
- **The crop-level sales of 2014-15 and 2021-22 sit on Farm-level rows with `u='Unknown'` (GH #1457).** They are donors, never recipients.
- **The s16d repeats are dropped upstream (GH #824).** So no sale is counted twice as a donor.
- **The 2011-12 dry season (3E, GH #1462) is a set of Farm-level rows.**
  - Its sale covers the first cycle; its value window is unstated.
  - `Quantity` and `Quantity_kg` are both multiplied by the number of cycles, so kg/q is cycle-free.
  - **3E is included in the screen.** Framework code cannot hold a Niger rule. The effect is measured in section 4.
- **`Quantity_kg` is a total (GH #1436).** 2021-22's is an estimate (2.5 kg per Tiya on 65% of rows). `Botte` is the main carrier.
- **State is recorded only in the EHCVM waves.**
  - The ECVMA waves are `unknown_condition`.
  - `Condition_sold` comes from GH #1462.
- **2018-19 asks no kilograms.** Its `Gros sac` and `Sac Moyen` state no size, and it has 21 `Kg` sales.
- **New, added to `CONTENTS.org`:**
  - The screen's firing.
  - 3E sales perturb the price cells.
  - A roselle size inversion on a 5-sale baseline.

**Mali**
- **EAC-I harvest units are unsized (`Sac`).** Sold units carry their own labels; in 2017-18, code 10 means `Autre` on the harvest list and `Tonne` on the sold list, so units are compared by label.
- **Tree sales are a monthly quantity beside a total value** (`tree-sale-window`, 331 rows).
- **2021-22 has `Quantity_kg` on 306 rows (an estimate), 70 of them in units stating no mass.**
- **The EAC-I field sales carry `KgFactor_sold`.** I used it as an answer key.
- **New, added to `CONTENTS.org`:**
  - `KgFactor_sold` agrees with prices: 0.95-1.00 overall, but groundnut reads 1.50.
  - The 2017-18 baselines for the rung sit at the floor.
  - Carts exceed 250 kg.
  - The tonne answer key is cotton and so is not evidence.

**Contradictions.** None with `CONTENTS.org`. One point of precision against the ruling's text: "only 2021-22 records [state]" is exact for the waves that have survey kg. The EHCVM 2018-19 waves also record state, so the rung's state rung would reach them.

## 4. Measurements

**Jobs** (all on savio4_htc, private node-local roots, cold):

| Job | Commit | Steps |
|---|---|---|
| 41275938 | `4aefeec25` | Cancelled after the quick tests caught the pandas 3 bug. |
| 41275939 | `4aefeec25` | Census, probes P1-P3. |
| 41275992 | `cd1eebc6a` | Quick tests, build base and cand, compare, analysis, prior Part 2 reproduction, tests. |
| 41276345 | `7d27171ab` | Census, probes P1-P4. |
| 41276357 | `acd6fccba` | Quick tests, build, compare, analysis, tests. |
| 41276485 | `49aea7436` | Census, build, compare, tests. |
| 41276727 | `49aea7436` | Build, `analysis2`. |
| 41277110 | `b4a3b498e` and base | `issues.log` attribution. |
| 41276953 | `b4a3b498e` | Census. |
| 41277355 | `b3365f8d4` | Census. |

Scripts are in `reports/cropkg/scripts/` (`job*.sh`, `build.py`, `compare.py`, `analysis.py`, `analysis2.py`, `probe_edit.py`). Aggregates are in `reports/cropkg/out/`.

**The screen changes nothing.**
- Niger 49,042 rows and Mali 50,752 rows: `assert_frame_equal` to base passes with `check_exact`, and attrs are equal (jobs 41275992, 41276357, 41276485).
- The warning diff for Niger is exactly 6 new `SurveyKgPriceWarning`s, one per wave and direction. For Mali it is none: 42 warnings on each side.
- No "could not run" warning.

**Firing, Niger.**

| wave | survey-kg rows | in a unit stating no mass | judged | low (<0.1x) | high (>10x) | firing / judged | named kg |
|---|---|---|---|---|---|---|---|
| 2011-12 | 9,258 | 7,070 | 1,932 | 6 | 6 | 0.62% | 2,748 |
| 2014-15 | 8,324 | 7,246 | 6,394 | 26 | 89 | 1.80% | 44,853 |
| 2018-19 | 0 | 0 | 0 | | | | |
| 2021-22 | 11,942 | 9,798 | 9,020 | 2 | 28 | 0.33% | 58,216 |

- Total: 157 of 17,346 judged rows (0.91%), or 157 of 29,524 survey-kg rows.
- **Mali:** 306 survey-kg rows, 70 in units stating no mass, 1 judged (survey/implied 0.30), 0 named.

**Tails** (quantiles of survey/implied):

| wave | p1 | p5 | p50 | p95 | p99 |
|---|---|---|---|---|---|
| 2011-12 | 0.25 | 0.45 | 1.14 | 1.91 | 4.60 |
| 2014-15 | 0.14 | 0.49 | 1.13 | 2.25 | 11.39 |
| 2021-22 | 0.30 | 0.60 | 1.25 | 2.40 | 5.17 |

The largest folds:

| wave | high | low |
|---|---|---|
| 2011-12 | 31.5x | 750x |
| 2014-15 | 100x | 58x |
| 2021-22 | 29.6x | 78x |

**The named rows cluster in a few cells:**

| wave | crop / unit | named | survey kg per unit | implied |
|---|---|---|---|---|
| 2014-15 | Niébé / Tiya | 30 high, 5 low | 29 high, 0.15 low | 2.5 |
| 2014-15 | Sorgho / Botte | 30 high | 110 | 8.1 |
| 2014-15 | Mil / Botte | 12 high, 12 low | 151.5 high, 0.76 low | 10.2 |
| 2021-22 | Mil / Botte | 16 high | 200 | 10 |

**Agreement where judged.** Survey/implied at the median: 1.14, 1.13 and 1.25 by wave, with 88-92% within 2x. This reproduces the ruling's 1.1-1.4x.

**Reproduction of the prior Part 2.** Rerunning the prior `part2.py` on these frames, and also my "part2-like" alternative (region then national, no state, 3E excluded), gives exactly the prior numbers:
- judged rows 1,978 / 6,394 / 9,020;
- medians 1.186 / 1.138 / 1.25;
- named, low plus high, 9 / 116 / 27.

**Alternatives the ruling does not settle** (Niger names; the screen names 157):

| alternative | names | only in the alternative | only in the screen | where |
|---|---|---|---|---|
| state-blind | 159 | 2 | 0 | 2021-22 low: 4 against 2 |
| region then national | 155 | 1 | 3 | |
| 3E excluded | 154 | 1 | 4 | |

- With 3E in, 79 rainy-module rows lose their implied mass: rainy rows judged go from 1,978 to 1,899, plus 33 3E rows judged.
- Gating only the baseline, as the food ladder does, was not measured.

**Overlap with the label screen.** `kg_survey_off_label` names 75 rows and this screen 157. None is named by both, by construction.

**Cost, measured with a spy inside a warm read** (best of 5):

| country | screen | warm read | share |
|---|---|---|---|
| Niger | 47 ms | 659 ms | 7.1% |
| Mali | 41 ms | 658 ms | 6.3% |

The first cut handled every row as text and cost 314 ms of 946 ms. I rewrote it on integer codes, and every measured section was identical.

**Hygiene.**
- In-tree parquets: 0 in every job. No `dvc` CLI was used.
- **`issues.log`:** 0 lines in every job that did not run tests. 120 lines in each of the three test jobs.
  - 108 of the 120 come from `tests/test_failure_is_not_absence.py`, identically on base `f138a3d61` and the candidate (job 41277110).
  - The other 12 are unattributed. They came from the cache-gated tests, which I did not rerun on base.
- **n0035 disclosure:** one `python3 -c "import sys"` on piped JSON. No library import, no data, no cache.

## 5. Hash census (`tools/hash_census.py` plus `hashdiff.py`, 38 countries)

| from | to | job | `btf` | `fwimp` | tables | waves |
|---|---|---|---|---|---|---|
| `f138a3d61` | `b3365f8d4` (head) | 41277355 | 0 of 42 | 0 of 38 | 0 of 555 | 0 of 2,321 |

Earlier commits gave the same zeros: `4aefeec25` (job 41275939), `7d27171ab` (41276345), `49aea7436` (41276485), `b4a3b498e` (41276953).

- No country moved, and no `build_transforms_fingerprint` moved.
- New entry points are in `_EXCLUDED_CALLABLES`. A test pins this with a probe that fails when the exclusion is removed.

**Rung cost probes** (no-op edits in a job-local clone of `7d27171ab`):

| probe | edit | moved |
|---|---|---|
| P1 | a statement in `harvest_kg_factors` | 0 |
| P2 | a new `KG_FACTOR_LAYERS` entry | 0 |
| P3 | a new helper in `transformations.py` | 0 |
| P4 (positive control) | a statement in `food_kg_factors` | 3 `fwimp`, 75 tables (Ethiopia 25, GhanaLSS 22, Uganda 28), 503 waves |

P4 is exactly the cost `CLAUDE.md` records for a kg-factor edit since #1509, which shows the probe would have seen a move.

## 6. Tests

**Command:** `pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -q -rs -p no:cacheprovider` with BLAS pinned and `LSMS_BUILD_WORKERS=1`, on a warm private root after a cold build. The 84 files are listed in `out/pytest-41276357.files`:
- every Niger and Mali test;
- the new `tests/test_gh1559_survey_kg_price_screen.py`;
- `test_quantity_screen.py`, `test_build_transform_hash.py`, `test_crop_survey_kg.py`;
- `test_gh1569_sold_diagnostics.py`, `test_gh1482_1485_quantity_diagnostics.py`, `test_gh1493_near_zero_screen.py`;
- `test_crop_kg_factor.py`, `test_schema_consistency.py`;
- `test_null_read_guard.py`, `test_null_index_guard.py`, `test_failure_is_not_absence.py`;
- `test_derivations.py`, `test_quantity_kg.py`, `test_median_price_valuation.py`;
- `test_gh1528_sold_condition.py`, `test_ehcvm_crop_production_condition.py`;
- `test_gh1475_country_crop_transforms.py`, `test_gh1460_crop_level.py`, `test_feature_attrs_carriers.py`.

**Results:**

| commit | job | passed | skipped |
|---|---|---|---|
| `49aea7436` | 41276485 | 3,467 | 1 |
| `acd6fccba` | 41276357 | 3,467 | 1 |
| `cd1eebc6a` | 41275992 | 3,464 | 3 (two of them the pin placeholders, since pinned) |

- The one skip is Tanzania's warm parquet in `test_quantity_screen.py`. So the Niger and Mali real-data classes ran, pins included.
- The head differs from `49aea7436` only in `CONTENTS.org`, and the census at the head is 0.

**The new test file** has 24 synthetic tests and 9 cache-gated ones. It covers:
- both tails, and the input returned unchanged;
- the warning subclass and the strict lever;
- the band pinned equal to `SURVEY_KG_LABEL_BAND`;
- stated-mass units never judged;
- sacks read at their stated mass, and volumes excluded;
- thin and dispersed cells staying silent;
- no pooling across waves or crops;
- the sale's own unit doing the pricing;
- sentinel and currency units excluded;
- `Quantity_sold_kg` not read;
- the state rung tried first, then falling back;
- a one-wave slice naming the same rows;
- a pooled `Feature` frame keeping countries apart;
- the exclusion pin;
- on real data: the Niger and Mali pins, no overlap with `kg_survey_off_label`, and a one-wave read naming the same rows.

**Not run:**
- `Feature('crop_production')` across countries;
- the make backend;
- `LSMS_QUANTITY_STRICT` on real data (synthetic only);
- the warm Tanzania test.

## 7. Issues

**#1559: partial.**
- Part (1) closes the stated gap for mass-less units that have price coverage.
- Rows without price coverage stay unscreened: 5,138 of Niger 2011-12's 7,070 mass-less rows, 852 of 2014-15's and 778 of 2021-22's.
- Part (2) awaits a ruling on the memo.

Drafted residuals:
- **Tier B.** "Build the price-implied crop rung in `harvest_kg_factors` (GH #1559 part 2)." Body: build per `RUNG_DESIGN.md` once Q1-Q10 are ruled. Its own layer `price_implied` after `inferred`; estimator in `transformations.py`, shared with the screen; never in `food_kg_factors`' closure (probe P4). The Mali 2017-18 baselines are 5-7 `Kg` sales.
- **Tier C.** "Survey kg in mass-less units with no price cell stay unscreened (GH #1559)." Body: Niger 2011-12 judges 1,932 of 7,070 (27%). A wider cell, or a reference that is not a price, is a separate design.

## 8. Questions for the user

**Screen**
- **Q-S1.** Niger 3E is included as donor and recipient. Should a donor whose `Derivation` carries a registered window key be excluded? Measured: 3 names more with 3E in, and 79 rainy rows lose their judgement.
- **Q-S2.** Keep the state rung (it removes 2 lows), or go state-blind?
- **Q-S3.** National only, because the read path cannot resolve a region. Region then national gives 155 names against 157.
- **Q-S4.** The spread gate is on both sides. The food ladder's baseline-only gate was not measured.
- **Q-S5.** Scope is `Quantity_kg` only. Should reported `KgFactor` (Uganda and others) also be screened against prices? That would widen the ruling.
- **Q-S6.** The shared `LSMS_QUANTITY_STRICT` lever makes Niger's one-wave reads of 2011-12, 2014-15 and 2021-22 raise under strict. Keep the shared lever, or give the screen its own?

**Rung** (memo section 10):
- Q1 default on or opt-in;
- Q2 a rung from `KgFactor_sold`;
- Q3 window-mismatched donors;
- Q4 cross-wave borrowing;
- Q5 the 250 kg ceiling (carts);
- Q6 declared `u_kg` in the baseline;
- Q7 a caller-resolved region;
- Q8 a stricter floor for the rung;
- Q9 rows Site Q names;
- Q10 groundnut in state-blind waves.

**Integration notes for Sue**
- The screen judges any country serving `Quantity_kg`. The Nigeria stream adds that column, so Nigeria's firing is unmeasured and the pins cover only Niger and Mali.
- `CLAUDE.md`'s Site Q section could gain a row for this screen. I did not edit it.

## 9. The rung memo (`out/RUNG_DESIGN.md`), summary

- **Estimator.** The screen's own. Evidence is sales in their own unit. Gates are at least the screen's.
- **Ladder position.** A layer `price_implied` after `inferred` and before `none`. `inferred`'s price-ratio branch never fires on crops (0 rows), because it needs `Expenditure`.
- **Disclosure.** Through `KgFactorSource`, per-row columns and an attrs record.
- **Coverage.**
  - Niger: 161 of 13,240 `none` rows in 2018-19 (no baseline).
  - Mali 2017-18: 9,981 rows and +14.8M kg, against 1.66M kg served today.
    - 10.9M kg of that is groundnut, millet and maize `Sac` cells resting on 5, 5 and 7 `Kg` sales.
    - The top 1% of served rows carry 27.3%.
    - One row Site Q names would get 857k kg.
- **Answer keys.**
  - Held-out stated sacks:
    - 100 kg sacks: 0.80 (Niger and Mali).
    - 50 kg sacks: Niger 1.25, Mali 1.00.
    - The tonne key is cotton and is not evidence.
  - Survey kg: 1.13-1.25.
  - Mali `KgFactor_sold`, without cotton: 0.95 (millet 0.83, maize 0.95, sesame 1.00, groundnut 1.50).
  - So cereals, sesame and cowpea are within about ±20%. Groundnut and rice are not.
- **Caveats, measured.**
  - **State:** groundnut 2.35x per Tiya; Mali 2017-18 is wholly state-blind.
  - **Selection:** 21-24% of survey-kg rows come from sellers.
  - **Seasonality:** about 1.3x, cited from the prior job.
  - **Survey offset:** 1.1-1.4x, with a sign the lot-size lean cannot explain.
  - **Floor:** roselle's `Gros sac` reads 21 kg, below its `Sac Moyen` at 28 kg, on a 5-sale baseline.
- **Cache cost: 0.** Probes P1-P3, with the P4 positive control.

---

