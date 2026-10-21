# Prior-Art Ledger - #1034 Nigeria W3 food unit labels

**Search tier used:** ripgrep + git floor. Base: f8d236694.

## Section 1 Task, restated

Decode the four unit codes labelled by Nigeria W3's planting Stata source but missing or residual-labelled in the 2015-16 column of the country `u` table. Keep the existing food builder, all quantities/expenditures, other wave mappings and kilogram machinery. This does not address item labels (#1144) or W4 size vocabulary (#1033).

## Section 2 Existing machinery

| Symbol | Source | Existing tests | Decision |
|---|---|---|---|
| `round_wave` | Nigeria/_/nigeria.py:2467 | test_nigeria_food_unit_decode.py | Reuse quarter-to-wave selection |
| `food_unit_labels` | Nigeria/_/nigeria.py:2478 | test_nigeria_food_unit_decode.py, test_gh1047_nigeria_unit_codes.py | Reuse numeric spelling variants and wave column |
| `food_acquired_for_wave` | Nigeria/_/nigeria.py:2505 | test_gh1041_nigeria_missing_unit.py, test_gh1047_nigeria_unit_codes.py | Reuse source reshape and native quantities |
| W3 wave builder | Nigeria/2015-16/_/food_acquired.py | test_nigeria_food_acquired_sources.py | Reuse PP/PH CSV sources and distinct t |
| `u` Org table | Nigeria/_/categorical_mapping.org:330 | test_nigeria_food_unit_decode.py | Extend four W3 labels only |
| `_finalize_canonical_food_acquired` | transformations.py | source-split and sentinel tests | Reuse canonical row aggregation; no new reducer |
| Country/Feature food derivation | STANDING.md Section 2 | food/schema/Feature audit | Reuse; no kilogram factors introduced |

## Section 3 Definitions and conventions

Inherit STANDING.md Sections 3-4 and CLAUDE.md Data Access / food schema; canonical declarations remain in lsms_library/data_info.yml. Nigeria CONTENTS.org:211-217 explicitly corrects its earlier PH-only negative: planting labels 131 SACK/BAG MEDIUM, 151 BASIN MEDIUM, 152 BASIN LARGE, 161 BUNDLE MEDIUM. Match established title-case vocabulary; add no stated mass. CONTENTS.org:134-163 explains why the two rounds share a wave column despite round-specific source coverage. Unit labels and kg conversion are different tasks (food-acquired/units skill).

## Section 4 Invariants and assumptions

- Preserve the existing no-argument food/crop item decoder; #1144 remains separate.
- Preserve all existing u table cells except the three 2015-16 placeholder cells; add code161 with no mapping in other wave columns. Keep the existing Preferred Label values for131/151/152 so global Code lookups do not change.
- W3 PP has twelve served affected rows in the issue census (131 three,151 eight,152 one); code161 is labelled but has no reported positive served row. PH has no such served code rows; confirm current raw data and cold builds rather than assuming a per-round decoder has changed.
- The `u` table is shared by both W3 rounds. Its new mappings therefore exist for either round; PH data are unchanged because PH does not use these codes. Do not claim a PP-only resolver.
- Content hashes cover country categorical Org input and can invalidate every Nigeria table; measure the exact blast radius before landing. Do not claim food-only hash impact.
- All builds use asserted worktree identity, image Python, thread counts1, LSMS_BUILD_WORKERS=1 and physical private L2 roots. Read cached raw sources through sanctioned IO. No raw acquisition, DVC CLI or shared L2 writes.

## Section 5 Reuse decision

| Quantity | Decision | Reason |
|---|---|---|
| Unit decode | Extend existing table | Correct four known source labels; no new decoder |
| Source rows and measurements | Reuse existing builder | Relabel u only; verify exact row and value preservation |
| Kilograms and prices | Reuse transforms | No new unit factors; report any inference change caused by relabelling |
| Verification | Extend independent-label/synthetic tests, cold compare, existing Feature scan | Table self-comparison alone cannot catch semantic mistakes |

## Section 6 Open questions for the human

None within root's approved four-label scope. Source labels, code161's absence from positive rows and all-round preservation will be checked from held data. #1144 item mislabelling and #1033 W4 size semantics remain out of scope.

## Phase 3 verification

Pending implementation and private cold comparison.

Sue
