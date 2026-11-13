Title: Mali EAC-I community price entries with unresolved unit codes need source diagnosis
Tier: B, diagnosis-first; no automatic eligibility change.

The held 2014-15 EACIS04 p1/p2 source files contain 129 entries with Price>0
and a unit outside the common community-unit dictionary after restoring the
missing p2 reading1/2 value labels. This is 49 documented missing-code99
(Manquant),19code0,58otherintegercodes and3fractional codes. Their Quantity
values partition into79positive non-999/9999,22zero,15sentinel999 and13missing.
No Price999/9999 occurs in these entries and no source price field has a
value-label dictionary. Price>0 alone does not verify these as valid answers.

GH822 preserves today's resolvable-item/unit, positive-price filter and weight
sentinel rules. Nineteen of these entries were already excluded in reading3;
the remaining110 are in newly wired readings1/2. The exact old keep/drop sets
remain invariants. The missing-label issue is distinct: p2 q03/q06 lack Stata
unit labels, but the same common1-14/99dictionary is present on all p1 slots
and p2 q09. Correctly restoring it does not adjudicate out-of-dictionary codes.

Read the instrument and producer metadata, classify missing/nonanswer versus
misplaced fields, compare adjacent readings and any released corrections,
and determine whether an original unit can be recovered with evidence. Do not
round fractional codes, infer units from weight or price, convert unknowns to
an existing native label, or claim all129are genuine price observations.
Record any additional sentinel evidence separately. Acquisition of an unheld
questionnaire or a new source release requires its own authorization.

Evidence: private source-safe aggregate census at
control/isa-backlog-20261021/Mali/822/unmapped-patterns.json; no raw rows or
household identifiers should be published. The standing Poids(KG) questionnaire
definition in Mali/_/CONTENTS.org is reused; the PDF was not newly read.
Separate missing2018-19community-module wiring remains GH821.

Sue
