# Prior-art ledger: Mali harvest median strata (GH 1754)

Search tier: ripgrep + git, pinned development 47df62d49. Inherits STANDING.md.

## §1 Task
Measure changing only the donor/recipient partition on the Malian 2017-18 survey-median rung. Keep production and all earlier/later rungs unchanged.

## §2 Existing machinery
- `transformations._survey_median_factors`: accepted factors by country, wave, unit and condition with unit-only fallback; reused through `harvest_kg_factors`.
- `Mali/2017-18/_/crop_production.py`: indigenous 7F versus 11F provenance and explicit perennial flag; no need to infer from crop name or Farm-level.
- Tests: `tests/test_gh1749_mali_kgfactor.py` and the harvest factors test suite.

## §3 Definitions
See STANDING.md and Mali CONTENTS.org, GH 1749. 7F supplies a reported per-unit factor. 11F harvest quantity is annualised from a per-harvest-month quantity. The unread tree equivalent field's mass basis is not established.

## §4 Invariants
No production change or new native factor. Use existing screening, minimum-reports and both median fallback groupings. Preserve order, every row, unchanged non-affected outputs and the reported-factor provenance.

## §5 Reuse decision
Call the current ladder for the baseline. For the counterfactual, partition only the existing survey median code's operands, then restore positional order before the same downstream choice. Isolate full input and print only aggregate evidence.

## §6 Ruling
The maintainer selected separate tree and seasonal medians for Mali on 2026-11-25. Keep the report-only gap and suspect cart patterns separate.

## Verification
On independently cold 18-provider roots, native crop and unaffected wave/seasonal factor fingerprints match the base exactly. The 2017-18 grouped harvest moves from 23,359 to 23,256 rows and from 32,791,070.26 to 32,417,919.26 kg. On underlying rows, 104 tree records lose their only factor and 71 keep the same kg via metric. The metadata/sample thresholds and reported-factor ceiling are unchanged.
