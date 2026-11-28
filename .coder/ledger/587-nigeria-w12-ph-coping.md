# GH587 - Nigeria W1/W2 post-harvest food coping

## Scope and finding

Use each visit's own nine released numeric day counts for 2011Q1 and 2013Q1.
The old drivers' PP-only claim is contradicted by the official World Bank
release dictionaries (study 1002 file F51 and study 1952 file F90) and the
exact own harvest Stata exports. The exact W1/W2 PH household forms
(1002/48230 PDF p.66 and 1952/48241 PDF p.71) both print Section 12 Q1(a-i). W1's own source is
`2010-11/Data/Post Harvest Wave 1/Household/sect12_harvestw1.dta`;
W2's is `2012-13/Data/Post Harvest Wave 2/Household/sect12_harvestw2.dta`.
No changes to the other GH587 coverage cells or to W3-W5.

## Prior art and invariants

See STANDING.md, `587-nigeria-w3-ph-coping.md`, the country CONTENTS
note on W3 PH, and the existing W3 own-visit driver. Use
`food_coping_for_wave` and `PH_QUARTER`. Explicitly bind s12q1a-i in their
released order to the same nine existing strategies. Do not copy PP answers,
rewrite identifiers or weights, apply new filters, add a range coercion or
fill missing days. No new table, index, units, FIES binding or compiler policy.
Retain PP calculations and all other visits exactly.

## Verification

Exercise both actual drivers with invented sources for explicit own-file and
quarter selection, independent value and strategy oracle, exact PP output,
preserved zeros, omitted true missing answers, and failure before write when
a required native field is absent. Check exact staged raw-source digests and
read only necessary fields for the independent native oracle. Compare
private Country and Feature cold/warm results with the pre-change tree and
the oracle. Exercise the Make route and manually verify the hash footprint
before publishing. Keep non-coping #587 residuals open.
