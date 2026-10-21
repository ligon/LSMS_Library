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

Pending counterfactual tests, paired cold builds, derivation-input and crop/community controls, derived/Feature accounting.

Sue
