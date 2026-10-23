# Nigeria #1144: approved country-scoped correction

Sue, 2026-10-21. Diagnosis evidence: private `east/DESIGN-1144.md` and
`east/source-label-counts-1144.json`, measured at frozen Nigeria #1034 head
`6fda46bc5437758bd650cf4016f0782a124281cf`.

## Decision

Correct food item decoding per quarter, after repairing the enumerated quarter
cells contradicted by held source dictionaries. Do not apply the unchanged
table blindly: that would introduce 2,135 W2-PH errors. The documented safe
existing-Preferred subset corrects 5,789 rows. Fill the eight positively used
missing W2-PH cells from their recorded source meanings.

Preserve the three observed source categories that lack a recorded qualifier:
`Groundnuts` (W1 PP/PH and W2 PP), `Pepper` (those same rounds), and
`Maize (shelled)` (W2 PP/PH). The old alias is insufficient evidence for adding
shell status, freshness or cob status. This follows the repository's interface
and source-detail contract; it does not choose an unrecorded food attribute.
The diagnosis counts 2,839, 10,446 and 1,425 retained rows respectively.
Use these native labels in the food-only resolver. Keep Preferred/Aggregate
curation unchanged; do not append fabricated Code rows. Report their ordinary
label-coverage diagnostics and any effect on joins or derived outputs.

Resolve exact normalized existing Preferred matches before aliases. Permit
only unambiguous aliases whose meaning is supported by this diagnosis; the
three native cases above override historical broad-to-specific aliases.
Preserve existing behavior for unrelated later-wave curated labels and record
unadjudicated later source-specificity questions as precise residuals.
This is a bounded correction, not a claim that every inherited label is proven.
Unknown raw codes retain the existing raw-code contract; an absent quarter
(including W5) is explicit and cannot silently use a different round's codebook.
Known used codes with missing curation must be reported, never silently given
another round's global meaning.

## Scope and grounding

Keep the complete no-argument `harmonized_food_labels()` mapping unchanged for
crop/community consumers. Food acquisition passes its actual quarter. Update
the separate food-only W1 card-weight derivation-input reconstruction to pass
`W1_PH_ROUND`, and correct its old wave-blind documentation. No framework,
new feature, USDA/API, conversion-factor, Aggregate bucket or dependency work.
Read Nigeria CONTENTS including live caveats, quote contradictions, and commit
an issue ledger/design before code. Country lane: `isa-Nigeria`, branch
`isa/Nigeria-20261021`; frozen #1034 PR branch must not advance.

## Required evidence

Fail targeted regressions against 6f first: opposite W2 PP/PH 44/77/78 meanings,
W1 collisions, the three native labels, exact-match priority, aliases, missing
curation, numeric code forms, unknown/NaN raw codes and absent/W5 quarter.
Assert complete no-argument map equality; retain source rows, quantities and
expenditures with an explicit old-to-new item-key accounting and no new merge
of distinct observed raw categories. Verify every #1035 derivation-input row.
Compare cold private baseline/candidate once for the food cohort, report kg-only
and conversion-rung totals and price changes (including pooling propagation),
unchanged crop/community frames, scoped hashes, warm replay and the Feature
finding multiset against an unchanged comparator. Nigeria has no nutrition
feature: make no nutrition verification claim.

Freeze focused commits for independent adversarial review before PR creation.
Draft precise residual issue text for inherited source-specificity questions.
