# Malawi reported organic inputs - GH #1109

Source-backed amendment, 2026-10-21: Malawi now serves organic fertilizer quantity and native unit from Module D ag_d37a/ag_d37b, with the raw affirmative ag_d36 answer carried as nullable Applied=True.
All 11,075 organic records are retained: 11,068 positive amounts and seven missing amounts.
Observed zero remains zero in the regression controls; the actual held sources contain no affirmative-use zero amount.

The earlier country note and initial #1109 patch said "Module D records no organic quantity".
Independent review of all seven held headers disproved that claim; the six sources loaded by current wave scripts contain the amounts and units.
These fields were unmaterialized by the library, not unasked by the survey.
The initial c08 result (all 11,075 organic amounts NA) is superseded.
SOURCE-AMENDMENT.md and the living ledger document the corrected evidence and approved design.

The organic field has its own five-code dictionary: 2 Kilogram, 10 Bucket, 11 Wheelbarrow, 12 Ox cart, 13 Other (Specify).
The implementation uses existing canonical Kilogramme/Ox-Cart spellings and adds both fields to the 2016 Cross_Sectional column projection.
It reads quantities numerically without imputation or rescaling and keeps the existing organic-only keyed sum(min_count=1) overlay.
Other input reducers and the framework all-null-row policy remain unchanged.

A second boundary check rejected literal numeric fallback: Country's unrelated food-unit aliases converted raw organic code 1 to Kilogramme and code 3 to 90 kg Bag.
The approved source-qualified fallback preserves these six unlabelled source answers as Organic unit code 1 (five rows) and Organic unit code 3 (one row).
They are distinct from eight genuinely missing units, which use existing U_UNKNOWN under #1041.
No food/framework mapping, organic N share, container weight or free-text Other interpretation was added.

## Final verification

- **72 targeted tests passed**, including 28 actual-helper/assembly/finalizer cases, the real Country categorical-map collision regression, existing nitrogen tests, the private warm Malawi N check and plot declaration/harmonization checks.
  No test was skipped; the Ethiopia warm test was excluded because the private seed covers Malawi/Uganda.
- Before production edits, c08 gives **15 failures / 12 passes** on the then-27-case regression.
  The added actual-Country fallback regression first fails on bare numeric units and then passes with source-qualified units.
- A fresh offline private Malawi build uses 28 physically copied existing L1 blobs, exact canonical import/config identity, the mounted image Python and single-thread builds.
  It retains **180,184 rows**, including **169,109 nonorganic records** with exact original index/order/values/dtypes, and all organic index parts/other measures except the intended unit rekeying and quantity restoration.
  The comparator is the independent review's frozen-c08 cold artifact; source hashes identify the tested code snapshot.
- Independent raw-source accounting checks all seven headers and every one of the 11,075 loaded organic answers at both wave and finalized public keys.
  Final native units: **5,518 Kilogramme; 2,155 Ox-Cart; 1,451 Bucket; 1,204 Other (Specify); 733 Wheelbarrow; eight Unknown; five Organic unit code 1; one Organic unit code 3**.
  The six source-qualified codes receive **no inferred kg factor**.
- Default nitrogen output and its input-match/refusal tally are **exactly unchanged**: 33,466 plots, **840,235.7795800001 kg N**.
  All 11,075 organic inputs remain explicitly refused a nominal share; qualified unit names introduce no conversion or nutrient contribution.
- Cold/warm Malawi output is exact, and pooled Malawi/Uganda retains **264,400 rows** (180,184 + 84,216).
  All Uganda and nonorganic Malawi rows are exact; other organic fields/index parts are exact modulo Quantity/u.
- The final relevant scanner has **15 warnings, zero failures and zero errors**, with the same check identities/statuses as c08.
  Only the two null-index warning details change, reflecting native missing-unit rows falling from 28,724 to 17,649.
  Existing null crop, nonorganic null unit, column-content and index-order warnings remain explicit and outside this change.
- Hash comparison moves all **22 measured Malawi source-table hashes** and **zero of 24 Uganda hashes**: one Malawi country rewarm.
  No shared cache was edited/promoted/cleared, no raw data acquired, and no DVC CLI, dependency/lock/fixture/baseline change occurred.

Current safe aggregate artifacts are named source-amended-*.json; raw logs and row-level frames remain private under control/isa-backlog-20261021/Malawi/source-amendment.
The older baseline-cold.json, fixed-final-cold.json, comparison.json, warm-feature.json and one-line-failure.json describe historical f8/c08 or rejected reducer behavior, with their recorded SHAs/hashes; they do not describe the final source-backed result.
The rejected global min_count patch's loss of 437 nonorganic records remains valid independent evidence for the separate residual.

## Remaining scope

RESIDUAL.md retains the precise 437-record diagnosis and dedup reconciliation with #905/#1118/#637; all those records keep their existing behavior here.
No issue or comment was created.
For #1118 reporting only, **17,649 native null-unit rows remain, all nonorganic and with the existing nonmissing Quantity**.
Organic rows now have observed units, source-qualified unlabelled identity or Unknown; only seven organic quantities are missing, replacing the obsolete 11,075-missing-amount denominator.
#1118 is partial; disputed inorganic units (#1111), product composition (#1106), free-text Other and non-crop markers remain separate.

Ledger verification is OK against sections 2-4 after explicitly correcting the false source premise and the measured post-read alias collision.
Independent review is next.

Sue
