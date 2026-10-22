# Partial GH1119 scope after Burkina Faso author validation

The Burkina Faso change aligns exactly two seed-crop Preferred Label cells with
actual native crop labels. It preserves all 22,877 current input observations
and the reviewed GH1123 identities. Independent acceptance is pending.

Keep GH1119 open for Senegal spelling work, Guinea-Bissau extraction/source
work, and any shared marker-policy adjudication. This fix changes no
Farm-level / not-crop-specific / missing marker or fallback semantics.

Keep GH1187 (unlabelled 2018 code 21 source meaning and unverified Seed class)
and GH1188 (gift/purchase field and unit routing) separate. Do not borrow 2021
code21 meaning or certify current purchase-named columns. Do not add absent
2014/2021 input/crop builders or allocate household seed quantities to parcels.

At actual household/crop existence grain,23 maize and59 cowpea seed records
still have no matching crop record. Quantities and all seed_kg values remain
unchanged; this is honest coverage, not a request to fabricate/allocate data.

Retain the first Feature-currency expected-frame stop and the wrong wave
accessor stop. Both are private harness failures, with original artifacts
preserved and no production/API-output replay or attribute normalization.
Separate relevant-wave hashes were measured AFTER builds; only the 549 API /
39 framework census was measured before builds. No broader timing claim.

The dispatcher's separately SHA-verified 02:48 receipt confirms the actual
parent PR1189 remains open/unmerged at cfd and GH1119 remains open/unchanged.
Final parent/issue-state reconciliation belongs to the dispatcher before
stacked PR publication on actual PR1189's branch, not development. No writer
GitHub mutation, merge or closure is authorized. Sue.
