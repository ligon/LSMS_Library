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
