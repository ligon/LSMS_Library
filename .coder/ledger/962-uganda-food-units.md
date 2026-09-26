# Prior-Art Ledger - Uganda food units (GH #962)

**Search tier used:** ripgrep + git; no GitNexus index or memory server available.
**Base:** GitHub development `4c2223aa1`, including merged fraction-parser PR #963.

## §1 Task, restated

Finish Uganda's food-unit corrections without inventing a physical unit for a
missing report or erasing recorded physical labels. Rebuild `food_acquired`,
its runtime food derivations and `nutrition`; attribute changes while preserving
expenditures, acquisition sources and household coverage. The parked
`fix/962-uganda-unit-sentinel` and `fix/962-value-count-draft` branches contain
rejected approaches and are evidence, not changes to cherry-pick.

## §2 Existing machinery

| symbol | path:line at base | purpose and tests | decision |
|--------|-------------------|-------------------|----------|
| `harmonized_unit_labels` | `lsms_library/countries/Uganda/_/uganda.py:275` | Reads country Org `u` table; currently restores null cells to `---`; lacks focused regression | extend |
| `food_acquired` | `lsms_library/countries/Uganda/_/uganda.py:363` | Reads raw data, decodes labels, warns and removes unlisted codes | extend missing-unit handling only |
| `food_acquired_to_canonical` | `lsms_library/countries/Uganda/_/uganda.py:436` | Folds home/away purchases and separates acquisition sources | extend classification after source split |
| `_finalize_canonical_food_acquired` | `lsms_library/build_transforms.py:143` | Preserves positive quantity or expenditure, aggregates duplicate canonical keys | reuse |
| `U_UNKNOWN`, `_unit_sentinel_mask` | `lsms_library/transformations.py:3371` | Recognized absence of unit; covered by food kg inference/sentinel tests | reuse |
| `food_kg_factors` | `lsms_library/transformations.py:1784` | Factor ladder and provenance; `tests/test_food_kg_inference.py` | reuse unchanged |
| `food_quantities_from_acquired` | `lsms_library/transformations.py:2751` | Converts known units and carries native unresolved quantities; `tests/test_food_prices_units_kwarg.py` | reuse unchanged |
| `nutrition.py` | `lsms_library/countries/Uganda/_/nutrition.py:19` | Selects kg rows before FCT multiplication; historical invariance test records unresolved drift | verify independently |

Also inherit `.coder/ledger/STANDING.md` §§2-5 (IO, API derivation and cache hashes).

## §3 Definitions & conventions in force

- `.claude/skills/add-feature/food-acquired/SKILL.md`, "LCU-only goods":
  `u='Value'` with `Quantity = Expenditure`; currency is not kilograms.
- `transformations.py`, `food_quantities_from_acquired` docstring: unresolved
  quantities retain their native unit; consumers wanting kilograms select `kg`.
- `.claude/skills/add-feature/food-acquired/units/SKILL.md`, "Decode toolkit"
  and "Accepted residuals": never fabricate labels; document undecodable codes.
- Uganda `_/CONTENTS.org:794`: Org table is the unit-label source of truth;
  `conversion_to_kgs.json` is dropped at canonicalization, not an API factor input.
- Uganda `_/CONTENTS.org:842`: unlisted unit codes are currently dropped with a
  warning. Changing that separate behavior requires its own mass comparison.
- `STANDING.md` §3 and `lsms_library/data_info.yml`: canonical axes and API schema.

## §4 Invariants & assumptions

- Missing unit with positive reported quantity means `Unknown`, never the real
  `Number of Units (General)` label. Missing unit with expenditure only means
  `Value`, and its quantity equals that expenditure so the runtime carry rule works.
- Classify each acquisition source separately, before duplicate aggregation.
  One wide raw row can produce different unit meanings across its sources.
- Blank Preferred Labels are not all missing units: 134 codes have physical
  labels in wave columns. Check their agreement and promote documented labels;
  do not convert them all to `Unknown` or widen global sentinel spellings.
- Preserve reported physical quantities, every expenditure and source label.
  Any change outside the recorded-unit corrections must be explained separately.
- Nutrition uses only kg rows. Test its matrix product from rebuilt quantity/FCT
  inputs, but do not regenerate `tests/fixtures/uganda_baseline.json` or published
  artifacts. Agreement with an old total is not evidence of correct units.
- Use private L2 caches and shared read-only L1 blobs. Measure hash scope, rebuild
  cold and retain before/after evidence; inherit `STANDING.md` §4.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| native unit labels | extend curated country table/decoder | existing table contains source provenance |
| missing-unit / value-only distinction | extend country canonicalizer | source-specific quantities and expenditures become available here |
| unit factors, quantities, prices, expenditures | reuse runtime transformations | existing sentinels already implement the required behavior |
| nutrition | reuse kg selection and FCT matrix product | change the incorrect inputs, then independently verify output |
| regression verification | new focused synthetic tests and cold comparison | old draft's structural scan cannot prove row semantics |

## §6 Open questions for the human

- No new decision blocks the agreed missing-unit rules. The source-label audit
  must resolve which blank labels can be promoted without interpretation; any
  conflicting physical labels will be reported before implementing their mapping.
- Whether to revise a published paper's numerical baseline remains separate
  from correcting unit semantics. No baseline rewrite is part of this task.

### Phase 3 - verification

Pending implementation, targeted tests, independent review and cold comparison.

-- Sue
