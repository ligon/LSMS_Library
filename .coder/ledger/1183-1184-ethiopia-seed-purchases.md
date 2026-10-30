# Ethiopia #1183 / #1184 — seed-purchase source semantics

## §1 Task and reconciliation
Read live issues and comments: both remain open, zero comments. Public development
is d65e8b13683409ab0005f1a5590208cd7d94ae62. Merged #1118 normalized final units
only; its ledger and current CONTENTS explicitly preserve both purchase defects.
Diagnose questionnaire routing before changing missing purchase flags or row
eligibility. Keep the all-missing holder/crop sum a separate measurement.

## §2 Prior art
Search floor: Git/ripgrep, current country notes and prior #1118 ledger.
Reuse `plot_inputs_for_wave`, unchanged native drivers, `_yesno_bool`, the existing
unique-plot purchase join, and normal `get_dataframe` / `to_parquet` plus existing
hash invalidation. Relevant skills: prior-art-ledger and pp-ph. The skill audit
passes; its Poetry wrapper created an empty local environment, which was removed.
All substantive validation uses the existing approved mounted Python.
Inherit STANDING.md §§2–4; do not create another reader, decoder or build framework.

## §3 Definitions and country idiosyncrasies
CONTENTS: §3 is field grain, §4 is plot/crop grain, §5 is purchase grain. W1 §5
has field/crop keys; W2–W5 §5 has holder/crop keys and attaches only where that
pair maps to exactly one planted plot. Never fan out a holder amount across plots.
Applied-only organic/pesticide observations and the #1041/#1118 unit rule stay.
Panel eras and PP/PH source/reference periods stay. The existing notes say both
purchase boundaries need source-routing diagnosis: that caveat is evidence, not
permission to call absent answers No or delete purchase-only rows.

## §4 Invariants and measurements
Preserve reported Yes, No and zero, source/crop labels, keys, row multiplicities,
unique-plot attachment, all unrelated numeric payloads/dtypes/null masks and attrs.
Use explicit exact nonmissing NumPy or ExtensionArray predicates (GH1206), not
pandas frame equality alone. Private cached inputs, one CPU, 8 GiB, thread one,
exact package/config roots, no acquisition/providers/shared-cache mutation.
First finite scope: two cached section5 sources and their cached post-planting
questionnaires; no payload observations claimed at this ledger checkpoint.

## §5 Reuse/extension decision
Source interpretation: reuse recorded questionnaire instructions and source flags;
do not infer routing from missing-value counts. If an asked quantity is unreported,
extend only the existing holder/crop sum with missing-preserving aggregation,
retaining explicit zeros and known flags. Do not bundle a decoder/retention change
merely because the two issues interact.

## §6 Remaining question
Does a missing section5 purchase answer encode a defined skip/No, or an unknown
answer? For records whose purchase fields are the sole payload, what source rule
settles retention? Independently establish whether a missing amount on an explicit
Yes answer is genuinely unreported before implementing the narrow #1184 sum fix.

### Source diagnosis and chosen implementation boundary
The cached W2 post-planting form (MD5 5ce83f65b3ec26bee1e03e4708816ebd),
section 5 Q3, routes explicit No to Q13. Q5 asks total purchased/credit quantity;
the zero-if-nothing instruction is on Q10 up-front payment, not Q5. This was
inspected as literal text-drawing strings in local PDF streams 56 and 58, not a
full PDF/CMap/page reconstruction. Official WB dictionary entries independently
confirm Q3 and Q5 meanings:
https://microdata.worldbank.org/catalog/2247/variable/F113/V5189?name=pp_s5q03
https://microdata.worldbank.org/catalog/2247/variable/F113/V5192?name=pp_s5q05_a

Two normal cached section5 reads find 287 explicit W2 Yes rows with both amount
components missing, versus 49 explicit Yes rows reporting both zero. These are
different source states; a grouping-only missing-preserving sum is justified for
#1184. Keep the decoder/caller and row-retention predicates exact. The measured
265/187 post-caller grouping/output counts remain to be remeasured.

#1183 remains separate: all 4,433 / 90 missing flags have missing purchase
quantity components, but one W2 missing flag carries a positive purchase value
and financing answer; 20 W1 missing flags carry a financing answer. Missing Q3
cannot universally mean a documented No. Source evidence does not settle what to
do with the 371 purchase-only W1 records once their fabricated False/zero payload
is removed. No decoder, flag, eligibility or row deletion is implemented.
