
## GH #1559: survey kg against price-implied kg per unit (report-only screen)

**Ruling** (@ligon 2026-11-12, "GH1559 price-implied kg per unit for crops", "Also build a rung"):

> (1) a report-only screen flagging survey kg outside 0.1x-10x of the price-implied kg per unit (baseline: kg sales plus sacks at stated mass), and (2) design a price-implied INFERRED rung for crop units in harvest_kg_factors, as its own design, never merged with the food ladder.

This PR is part (1). Part (2) is a design memo (`reports/cropkg/out/RUNG_DESIGN.md`), and nothing in it is wired. The memo carries the red team's F1 and F2 corrections. The user should rule on it, starting with Q0: wiring Mali 2017-18's `s7fq13d` as `KgFactor`.

### What changes

- **New in `lsms_library/quantity_audit.py`:** `audit_survey_kg_prices` and `survey_kg_price_factors`.
  - Called from `check_quantities`, Site Q's read-path hook.
  - It has its own guard both ways: a failure in either audit cannot skip or silence the other.
- **What it names.** A row whose `Quantity_kg / Quantity` is outside 0.1x-10x of the price-implied kg per unit:
  - the median price per unit of the wave's and crop's sales in that unit (the sale's own `Unit_sold`);
  - over the median price per kg of sales in `Kg` or in a unit stating its mass, read at that mass, volumes excluded.
- **Rungs and gates.** `(t, j, state, u)` then `(t, j, u)`; at least 5 sales each side; a spread of at most 10x on both sides.
- **Which rows.** Only units stating no mass are judged, so it never names a row that `kg_survey_off_label` names.
- **Report-only.** It returns its input unchanged and names each offender with its implied factor, both medians, both cell sizes and the rung.
- **Other files.** `_build_registry._EXCLUDED_CALLABLES` gains two entries (defensive: removing either moves 0 hashes, and tests pin them). New `tests/test_gh1559_survey_kg_price_screen.py`. Niger and Mali `CONTENTS.org` entries.
- `transformations.py` and `country.py` are untouched.

### Must-state

1. **Channels and the strict lever.**
   - The 2026-11-12 QUESTION BACK offered "band" and "band + Site Q channels" as separate options. The ruling says "a report-only screen", and the brief asked the worker to choose. It rides Site Q's channels: `quantity_reports()` with `site='survey-kg-price'`, `LSMS_QUANTITY_STRICT`, and `SurveyKgPriceWarning`, a subclass of `QuantityImplausibleWarning`.
   - Under strict, only Niger one-wave reads of 2011-12, 2014-15 and 2021-22 newly raise. Every other Niger, Mali and Nigeria-overlay read already raised at base.
   - No CI workflow sets the lever.
2. **Nigeria at integration.** With the Nigeria stream, 2016Q1 has 8,507 `Quantity_kg` rows: 5,520 judged, 0 named, median survey/implied 1.25. Frames are equal and no hash moves (red team).
3. **`bench/feature_audit/scan.py`.** It has no taxonomy entry for these warnings, so each becomes a severity-C finding: 6 more per Niger `crop_production` cell. The coverage grader ignores warnings, so no tier moves.
4. **The exclusions are defensive and pinned.**
5. **The memo carries F1 and F2** and needs the user's ruling before any rung is built.

### Measured (cold, private roots)

- **Frames equal to base.** Niger 49,042 and Mali 50,752 rows are `assert_frame_equal` (exact) to `f138a3d61`, attrs equal. The only new warnings are 6 on Niger.
- **Niger firing:**

  | wave | judged | named (low / high) |
  |---|---|---|
  | 2011-12 | 1,932 | 12 (6 / 6) |
  | 2014-15 | 6,394 | 115 (26 / 89) |
  | 2021-22 | 9,020 | 30 (2 / 28) |

  - Total: 157 of 17,346 judged (0.91%).
  - Median survey/implied: 1.14 / 1.13 / 1.25.
- **Mali:** 1 row judged, 0 named.
- **Reproduces the prior #1559 measurement exactly** under its choices.
- **Alternatives** (the screen names 157):

  | alternative | names |
  |---|---|
  | state-blind | 159 |
  | region then national | 155 |
  | 3E excluded | 154 |
  | spread gate on the baseline only | 163 |
  | 5-household gate | 157 |

- **Cost:** 47 ms of a 659 ms warm Niger read.
- **Hash census, `f138a3d61` against `529f0bd57`:** 0 of 42 `build_transforms_fingerprint`, 0 of 38 `framework_imports_fingerprint`, 0 of 555 tables, 0 of 2,321 waves.

### Tests

Job 41279383 at `5a7ee1a7f`: **3,468 passed, 1 skipped** (warm Tanzania). The head differs only in `CONTENTS.org`.
- 84 files: every Niger and Mali test, Site Q, the build-transform hash test, survey kg, sold diagnostics, schema and the null guards.
- New: 25 synthetic tests (guards both ways, report-only, gates, units, state, slices) and 9 cache-gated pins.

### For maintainer review

- The screen's questions, Q-S1 to Q-S6, are in the report. Q-S1: 3E is included, though the ruling's own measurement excluded it.
- The rung's questions, Q0 to Q10, are in the memo. Q0, wiring `s7fq13d`, comes first: it would move Mali 2017-18 from 1.66M to 32.8M kg.
- Not run: `Feature()`, the make backend.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
