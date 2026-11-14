
## GH #1559: survey kg against price-implied kg per unit (report-only screen)

**Ruling** (@ligon 2026-11-12, "GH1559 price-implied kg per unit for crops", "Also build a rung"):

> (1) a report-only screen flagging survey kg outside 0.1x-10x of the price-implied kg per unit (baseline: kg sales plus sacks at stated mass), and (2) design a price-implied INFERRED rung for crop units in harvest_kg_factors, as its own design, never merged with the food ladder.

This PR is part (1). Part (2) is a design memo for the user to rule on (`reports/cropkg/out/RUNG_DESIGN.md`). Nothing in it is wired.

### What changes

- **New in `lsms_library/quantity_audit.py`:**
  - `audit_survey_kg_prices` / `survey_kg_price_factors`.
  - Called from `check_quantities` (Site Q, read path), in its own `try`.
- **What it names.** Any row whose `Quantity_kg / Quantity` is outside 0.1x-10x of the price-implied kg per unit:
  - median price per unit of the same wave's and crop's sales in that unit (the sale's own `Unit_sold`);
  - over the median price per kg of sales in `Kg` or in a unit stating its mass, read at that mass;
  - volumes excluded.
- **Rungs and gates.**
  - Rungs: `(t, j, state, u)` then `(t, j, u)`.
  - Gates: at least 5 sales each side, and a spread of at most 10x on both sides.
- **Which rows.** Only units stating no mass are judged, so it never names a row that `kg_survey_off_label` names.
- **Report-only.** It returns its input unchanged and names each offender with its implied factor, both medians, both cell sizes and its rung.
- **Channels: Site Q's.**
  - `quantity_reports()` with `site='survey-kg-price'`, `column='kg_survey'`.
  - `LSMS_QUANTITY_STRICT`.
  - `SurveyKgPriceWarning`, a subclass of `QuantityImplausibleWarning`.
  - Under strict, Niger's one-wave reads of 2011-12, 2014-15 and 2021-22 now raise; before, only 2018-19 did.
- **Other files.**
  - `_build_registry._EXCLUDED_CALLABLES` gains the two entry points.
  - New `tests/test_gh1559_survey_kg_price_screen.py`.
  - Niger and Mali `CONTENTS.org` entries.
- `transformations.py` and `country.py` are untouched.

### Measured (cold, private roots; burst jobs 41275992, 41276357, 41276485, 41276727)

- **Frames equal to base.** Niger 49,042 rows and Mali 50,752 rows are `assert_frame_equal` to development `f138a3d61`. The only new warnings are 6 `SurveyKgPriceWarning`s on Niger.
- **Niger firing:**

| wave | judged | named (low / high) |
|---|---|---|
| 2011-12 | 1,932 | 12 (6 / 6) |
| 2014-15 | 6,394 | 115 (26 / 89) |
| 2021-22 | 9,020 | 30 (2 / 28) |

  - Total 157 of 17,346 judged (0.91%). 2018-19 asks no kilograms.
  - Median survey/implied 1.14 / 1.13 / 1.25 by wave.
  - The names are a few cells: 2014-15 cowpea and sorghum per Tiya/Botte, and 2021-22 millet per Botte at about 200 kg.
- **Mali:** 1 row judged, none named.
- **Reproduces the prior #1559 measurement exactly** under its own choices (region, no state, 3E excluded): judged 1,978 / 6,394 / 9,020, named 9 / 116 / 27.
- **Cost:** 47 ms of a 659 ms warm Niger read, and 41 of 658 ms on Mali.
- **Hash census,** `f138a3d61` against `b3365f8d4`: 0 of 42 `build_transforms_fingerprint`, 0 of 38 `framework_imports_fingerprint`, 0 of 555 table hashes, 0 of 2,321 wave hashes.
- **Rung probes** (no-op edits): `harvest_kg_factors`, `KG_FACTOR_LAYERS` and a new helper each move 0. The positive control in `food_kg_factors` moves the known #1509 set (75 tables, 503 waves, 3 `fwimp`).

### Tests

Job 41276485 at `49aea7436`: **3,467 passed, 1 skipped**.
- The skip is the warm Tanzania test; the head differs only in `CONTENTS.org`.
- The run covers 84 files: every Niger and Mali test, Site Q, the build-transform hash test, survey kg, sold diagnostics, schema and the null guards.
- The new file has 24 synthetic and 9 cache-gated tests (real-data pins).

### For maintainer review

- **Screen choices, each with its measured alternative:**
  - Niger 3E included: 157 names against 154 without it.
  - State rung: 157 against 159 state-blind.
  - National only: 157 against 155 with region then national.
  - The spread gate on both sides: the baseline-only gate was not measured.
- **Shared strict lever versus a lever of its own.**
- **Rung:** questions Q1-Q10 in the memo. Chiefly: Mali 2017-18 would gain 14.8M kg on a wave serving 1.66M, and 10.9M of that comes from cells resting on 5-7 `Kg` sales.
- **Not run:** `Feature()`, the make backend, and strict on real data. Nigeria's firing is unmeasured, because the Nigeria stream adds `Quantity_kg` there.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
