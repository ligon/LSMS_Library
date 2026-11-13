# Mali #822: restore the three native community-price readings

Sue, 2026-10-21. Read-only source census at f8d236694; private evidence in
`control/isa-backlog-20261021/Mali/822/`. The owner has not edited runtime.

## Approved boundary

Read all three 2014-15 EACIS04 Unit/Poids/Prix triplets in both passages.
Declare native `(t,v,j,u,variety,visit,obs)`, using source reading slots 1, 2 and 3.
Retain variety CODES and passage visits; update the Country scheme, wave
builder and existing finalizer/index assertions. The old Country frame has
no obs; Feature alignment supplied neutral 1. The original reading 3 subset
must match exactly after adding obs=3, including every old kept/dropped key.
No aggregation, selection, invented observation counters or source rekeying.

The source files omit Stata value labels from passage 2 s04q03 and s04q06.
Blind reuse of the label-only path would discard 66,434 positive readings.
Decode exact known numeric codes through the same community questionnaire's
common unit dictionary, evidenced identically on all passage 1 slots and
passage 2 s04q09, before reusing existing native-label normalization. Do not
borrow food/inorganic codes, truncate fractional values, invent labels,
change existing Preferred curation or choose arbitrary aliases.
Record the metadata omission and its dictionary evidence in ledger/docs.

Reuse the existing CONTENTS definition that Quantity is the lot weight in
KG and Price is the per-lot quote; state that the already-recorded PDF
evidence was not newly re-read because its blob is not held. Stata metadata
corroborates the repeated Unit/Poids/Prix reading identities. No acquisition
is required for this bounded restoration.

## Preserve eligibility; report the separate unknown-unit problem

Keep the existing item/unit/positive-price/quantity-sentinel policies exactly.
The source has 129 positive-priced unplaced-unit entries: 49 code 99 Manquant,
19 code 0, 58 other integer codes and 3 fractional codes. Price>0 alone does not
adjudicate their validity or unit meaning. Do not map these to known units,
reclassify product availability, infer corrected fields or alter the old
reading 3 drop set. Draft a separate diagnosis-first residual with exact masks,
counts, known metadata gaps and appropriate source/instrument questions.
It must not claim all 129 are verified valid price observations. Also record
any distinct unhandled quantity-sentinel evidence without silently cleaning.

The approved result under current eligibility is 155,931 rows:
obs1=66,699; obs2=49,014; obs=3=40,218; +115,713 over baseline.
All full keys are unique/non-null, base cells23,646->39,245 and every original
cell survives. Preserve every numeric measure for the old reading 3 subset.
The unwired 2018-19 feature remains#821.

## Implementation and verification

Commit the per-task ledger/design before code; reuse the existing finalizer,
codebook/label utilities and i==v compatibility shim. Add targeted actual-script
and public Country/Feature controls that fail baseline. Cover all readings,
both passages, missing value-label metadata, exact numeric spelling matches,
unlisted/fractional units, weights/sentinels, variety label quote drift,
source eligibility, native obs and deliberate duplicate-key errors.
Update shared community-price declaration tests only where native Mali obs
changes expectations; coordinate its other regions with frozen Niger#1088.
No fixture/golden refresh, assertion weakening or general framework edits.

Use separate private baseline/candidate L2 and relevant held L1; exact image,
checkout/config identity and offline guards. Compare current source-derived
expected rows with cold/warm served frames, original reading 3 subset and every
cell. Run scoped tests and the Feature harness with an unchanged, verified
warm comparator. Report all normalized findings and all-country hash scope
(the country module/scheme should move 24 Mali hashes only). Freeze focused
commits, stop processes and request independent review before PR creation.
