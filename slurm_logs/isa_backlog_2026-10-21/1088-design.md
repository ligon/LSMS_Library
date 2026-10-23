# Niger #1088: exact quantity missing-code recognition

Sue, 2026-10-21. Read-only current-source replay at baseline f8d236694,
reported by the Niger country owner before coding.

Three held CS07 grids contain 23,409 / 34,380 / 21,960 source rows. The
current reshape and actual Country finalizer serve 46,043 rows (32,352 /
13,691 by wave). Exactly 76 served 2014-15 quantities are 9999.900390625:
43/32/1 by source slot, on 21 clusters and five varieties. Quantity-only
normalization preserves every key, Price and other column; content-dedup
outcomes and the 15+15 obs>3 residual rows remain unchanged.

## Evidence and boundary

Correct the existing issue wording: float32(9999.0) is exactly9999.0;
9999.900390625 equals promoted float32(9999.9), a distinct nominal value.
The held 2014 file declares these columns Stata double, so a prior precision
history is possible but not proven. Numeric labels do not document the code,
and questionnaire PDFs are not held. The nominal9999.9 missing-code
interpretation is the existing issue/CONTENTS ruling, not new instrument
confirmation. Record these qualifications in the ledger and country docs.

At the existing quantity-read mask, recognize exact nominal9999.9 and its
float32-promoted representation, retaining existing zero/nonpositive and
9999/99999 handling. Use explicit equality only. No tolerance, rounding,
range clipping or blanket dtype conversion. Leave Price sentinel semantics,
units, product-absence policy, grain, variety/visit/obs keys and duplicate-entry
policy unchanged. Do not decide the existing nonrepeat second-record tail.
No acquisition, framework edit, new feature or other-country edit.

## Required checks

Commit ledger/design before runtime changes. Fail the actual-helper/finalizer
regression on baseline, including observed positive Price with both exact
missing-code representations; cover legitimate nearby quantities,9999/99999,
zero/missing, all three slots and a second-record case. Existing variety/grain
controls must remain unchanged. Cold paired private builds and warm replay must
retain every row/key/Price/other measure and change only the exact76quantities
to NA. Recheck duplicate-entry counts. Run a scoped Feature community_prices
audit with a healthy unchanged comparator, copying only its relevant verified
warm parquets; no shared-cache mutation or broad rebuild. Measure country hash
impact and freeze focused commits for independent review before PR creation.
