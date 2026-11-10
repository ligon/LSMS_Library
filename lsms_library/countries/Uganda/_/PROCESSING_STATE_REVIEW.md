# Uganda processing-state decisions still needed (GH #833)

Source-only memo, 2026-11-09. Source revision:
`4381d701ffb06ab447a37c66362119d1c1e23009`.
The [narrowed issue](https://github.com/ligon/LSMS_Library/issues/833)
credits merged [PR #1534](https://github.com/ligon/LSMS_Library/pull/1534)
for Maize, Millet, Cassava, Sweet Potatoes, Ground Nuts and Sim Sim.
Those six families are already implemented; this memo proposes no further
fold, split, relabel or change to the food/crop join.

## Exact residual food entries

These are the existing `harmonize_food` cells in
[categorical_mapping.org](categorical_mapping.org), not observations of
served foods or wave coverage. All 12 rows retain a Nutrition Label equal
to their Preferred Label.

| Current Aggregate Label | Food Code → Preferred Label |
|---|---|
| Beans | 140 → Beans (fresh); 141 → Beans (dry) |
| Peas | 145 → Peas; 162 → Peas (dry) |
| Rice | 110 → Rice; 1101 → Rice (white); 1102 → Rice (brown); 1103 → Rice flour |
| Ginger | 1352 → Ginger (fresh); 1353 → Ginger (powder) |
| Soybean | 1761 → Soybeans (fresh); 1762 → Soybeans (dry) |

Beans, Ginger and Soybean explicitly name different states. Peas 145 does
**not** name a state: the contrast with 162 does not establish that 145 is
fresh. Rice includes both variety words and an explicit flour entry; its
unqualified and white/brown names do not alone establish their preparation
basis. A generic fresh/dry or grain/flour rule would therefore introduce
meanings not recorded in all of these cells. The issue leaves these five
families unruled; the existing six-family rule is not authority to extend it.

## Crop identity is a separate namespace

The same file's `harmonize_crop` has 210/211 → Beans, 120 → Rice,
840 → Ginger and 320 → Soybean. These four label strings match the current
food Aggregate strings. That is a label join, not evidence of matching
physical state, edible basis or processing yield. Crop rows carry their own
`condition` coordinate.

For peas the distinction is stronger: crop codes 221/222/223/224 retain
Field Peas/Cow Peas/Pigeon Peas/Chick Peas, and none maps to bare `Peas`.
The existing comment explicitly declines that lossy species fold. A new food
state decision must not silently erase it. Numeric food and crop codes are
also different namespaces: food 141 is Beans (dry), while crop 141 is Millet.

## Finite decision and verification plan

1. For each of the 12 food codes, identify its own wave-specific producer
   codelist/questionnaire and released-field definition. Keep printed labels,
   export labels and inferred interpretations separate. In particular,
   establish what unqualified Peas 145 and Rice 110 mean before naming states.
   This memo has not read new survey sources, headers or rows.
2. ROOT/maintainer chooses each family's treatment independently, including
   which existing bare crop label, if any, is intended to meet a food bucket.
   Evidence of a shared spelling or price cannot make that choice.
3. Freeze an exact reviewed Preferred→Aggregate diff. Preserve food codes,
   Preferred and Nutrition labels, acquisition source, units and all payloads.
   Do not bundle a crop species fold, cassava recode or millet reinterpretation.
4. Only after separate execution authority, use focused metadata/invented-frame
   controls: each Preferred Label has one Aggregate; an explicit fixture
   containing every affected Preferred Label and two acquisition sources
   conserves all monetary values under the reviewed grouping; unknown labels
   retain the documented pass-through signal. Verify rename-only behavior for
   food_acquired separately from derived-food summation. No such tests ran here.
5. Any native/API before/after needs its own finite approved operand plan.
   Record affected wave/item/source membership and monetary totals, and, if
   kilograms or demand fits are proposed, their factor support and estimation
   scope separately. No scientific or data acceptance follows from metadata.

The first six-family implementation's existing tests are
`tests/test_gh833_uganda_aggregate_states.py`. They deliberately scope their
state rule to the six ruled families; extending that list would encode a new
decision. No test was weakened, added or run for this documentary memo.

## Separate outstanding questions

Millet 115 remains as ruled, with its cross-wave form/value-label disagreement
recorded in CONTENTS. [#1525](https://github.com/ligon/LSMS_Library/issues/1525)
still owns the 2019-20 cassava export bridge; 1081/1082 are not swapped.
Neither [#647](https://github.com/ligon/LSMS_Library/issues/647)'s 5A1 routing
nor [#1000](https://github.com/ligon/LSMS_Library/issues/1000)'s FCT preparation
basis is decided by this memo. Historical quantities and price comparisons
in CONTENTS were not rerun.
