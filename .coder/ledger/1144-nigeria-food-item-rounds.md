# Prior-Art Ledger - #1144 Nigeria food item quarters

**Search tier:** ripgrep + git floor. Base: 6fda46bc5437758bd650cf4016f0782a124281cf.

## §1 Task

Correct food-only j decoding per quarter after repairing source-contradicted W2 quarter cells. Preserve the recorded categories Groundnuts, Pepper and Maize (shelled) instead of adding unrecorded shell/fresh/cob qualifiers. Keep all Preferred/Aggregate curation, no-argument crop/community decoding, raw measurements and features unchanged. Root's approved design is copied beside the implementation evidence before code changes.

## §2 Existing machinery

| Symbol / input | Source | Existing verification | Decision |
|---|---|---|---|
| `harmonized_food_labels()` | Nigeria/_/nigeria.py:190 | food source/sentinel tests; complete mapping control required | Extend with food-only quarter branch, preserve noarg body |
| `food_acquired_for_wave` | Nigeria/_/nigeria.py:2505 | test_nigeria_food_acquired_sources.py, test_gh1047_nigeria_unit_codes.py | Reuse canonical source reshape; pass t to j decoder |
| `food_unit_labels(t)` | Nigeria/_/nigeria.py:2478 | test_nigeria_food_unit_decode.py | Reuse decode patterns; unit rules unchanged |
| `inputs_food_acquired_w1_card_weight` | Nigeria/2010-11/_/mapping.py:123 | test_gh1035_nigeria_w1_stated_weights.py | Pass W1_PH_ROUND in independent raw-index reconstruction |
| `harmonize_food` | Nigeria/_/categorical_mapping.org:38 | held DTA dictionaries and per-code retained source counts in private diagnosis JSON | Correct W2 native quarter cells only |
| `_crop_labels`, community code resolvers | Nigeria/_/nigeria.py:216 and community wave scripts | complete noarg mapping + private built-frame controls | Reuse unchanged noarg behavior |
| `Country._relabel_j` | country.py:3615 | label-selection tests, Feature scan | Reuse ordinary unmatched-label disclosure, no new Aggregate bucket |
| Food kg/value transforms | STANDING.md §2, transformations.py | paired kg-only/rung/price accounting | Reuse; no inference or factor changes |

## §3 Definitions

Inherit STANDING.md §§3-4, CLAUDE.md data-access/derived-values rules and lsms_library/data_info.yml. Nigeria CONTENTS acquisition-source section records questionnaire renumbering and W4 last-purchase versus consumption windows; the live nonfood RecallWindow WAITING item remains separate. A quarter column is a codebook for that instrument, not proof that its existing curation is right. Held source dictionaries contradict current W2 PP/PH cells; the approved design specifies corrections. W1 card-weight inputs must have the same j index as the served row.

## §4 Invariants

- No Preferred/Aggregate recuration or fabricated Code row. No new feature, source acquisition, API requirement, dependency, conversion factor or framework change.
- Exact normalized existing Preferred match precedes historical aliases. Only source-supported unambiguous aliases are accepted; the recorded-native Groundnuts/Pepper/Maize (shelled) cases override broad-to-specific old aliases.
- Unknown raw codes keep the raw-code contract. Missing known quarter curation is reported; missing/W5 quarter cannot silently fall back to the global code meaning. Raw NaN item behavior stays unchanged.
- The W2 quarter-table correction must prevent the 2,135 PH regressions the naive candidate would introduce; fill eight missing positive PH entries (6,288 rows).
- The source-backed existing-Preferred changes affect 5,789 rows; native preservation affects 14,710 additional rows. No newly merged distinct observed raw categories; raw row/Q/E/missingness preservation must be proved at source keys.
- Preserve every #1035 card-weight decision, Quantity_kg and derivation input row while j keys change.
- Existing later-wave specificity questions are precise residuals, not silently adjudicated. W3/W4 unrelated curated behavior remains unchanged.
- Every build uses image Python, exact package/config identity, copied private L1 and physical private L2, offline socket guard, numeric threads=1 and build workers=1. No shared cache promotion or deletion.

## §5 Reuse decisions

Extend the existing country label resolver and food-only consumers; correct only quarter cells. Reuse canonical aggregation, raw-code preservation, derivation registry/input APIs, food transformations and Feature audit. No new estimator or reducer. The country module/Org inputs can move all Nigeria hashes; measure that scope and compare an unchanged country. No Nigeria nutrition feature is declared, so do not claim it was verified.

## §6 Open questions

None within the approved recorded-native policy. Preserve later-wave source-specificity uncertainties as explicit residual text, including W4 PH maize cob/shell and tinned-milk qualifiers; do not broaden this correction into recuration.

## Phase 3 verification

Implementation: f452ceef933bd9c3f6ab381ff2119881d5cbe2c3. Safe evidence: `slurm_logs/isa_backlog_2026-10-21/1144/REPORT.org`, `validation.json` and `source-grounding.json`.

- **OK (anchored on sections 2-5):** a second call-site search finds exactly the food builder and the separate W1 raw-input consumer passing quarters; crop uses the unchanged no-argument branch. The new helper extends the existing Org/format_id/decode machinery; no competing quarter-food resolver or replacement estimator was added.
- **OK (sections 3-4):** the complete 258-code no-argument mappings and all Code/Preferred/Aggregate cells match immutable 6f exactly. Only the nineteen enumerated W2 quarter cells differ. Opposite PP/PH source meanings, exact-name priority, reviewed aliases and the three recorded-native choices match the held metadata.
- **OK (section 4):** private offline paired cold cohorts conserve every one of 595,725 raw rows and all four value columns/missingness after 20,499 documented item-key changes, with no new observed multicode merge. Four later-quarter frames are exact without replacement. Every household/round expenditure total and all 1,856 card-weight raw-input rows remain exact; the derived subset still has 4,740.73 stated kg.
- **OK (sections 2-4):** actual normal/Preferred/Preferred Label Country calls and a new-Country warm replay preserve Groundnuts 2,839, Pepper 10,446 and Maize (shelled) 1,425 rows, with no warm parquet writes. The ordinary Aggregate API reports three unmatched names/14,710 rows and preserves their incoming detail. No bucket was added.
- **OK (sections 2, 4-5):** complete cold crop/community frames are exact; the full Uganda food/sample comparators are exact. All 24 Nigeria hashes move and none of Uganda's 28 does; final stored food/crop/community stamps match current inputs. Native-name fidelity reduces existing exact food-row crop/community key coverage by 277/1,977, disclosed rather than hidden.
- **OK (sections 2-5):** existing pooled inference changes 1,724 quantities and 1,239 kgvalue prices, including later waves. The kg-only total falls 102.4865777663 kg; metric/survey_kg/none rung totals stay unchanged. Unitvalue prices and expenditures are exact. No nutrition consumer exists to verify.
- **OK (sections 2-4):** all 36 new regressions pass; at immutable 6f, 31 fail and five unchanged controls pass. The full focused suite has 115 pass, zero skips and one inherited #1034 test-assumption failure reproduced at 6f. Root owns the separate test-only amendment; its exact private amended case passes here (patch SHA256 `8145796ca742bb7fd0cf7230df3bd74a11aa4f1841cca620b3b6f48727149f1f`). No existing test, fixture or golden was changed in #1144.
- **OK (sections 4-5):** Feature phases 1/2 on the private Nigeria/Uganda roots have no failure/error/skip. Baseline 441 records (438 warn/3 pass) becomes 445 (442 warn/3 pass). Full multiset differences are accounted for: 42 quantity-warning records differ only in the verified item names; ten repeated metric diagnostics change cell count 77 to 75; two existing Aggregate cardinality findings change; four new expected label-passthrough warnings appear. Full scanner records and raw examples remain private.

No runtime CONTRADICTION or REINVENTION against this ledger was found. The inherited GH1035 test's all-quarter uniqueness assertion contradicts #1034's approved shared native vocabulary, not the W1 card rule; it is explicitly separate. The W4-PH cob/shell and tinned-milk/sugar specificity questions remain unadjudicated with exact residual issue text in the report. Independent review and root's separately reviewed #1034 test amendment remain the integration gates.

Sue
