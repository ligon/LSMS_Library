# #1230 mapping composition reproduction

## §1 Task
Reproduce item2 using invented source values and the existing df_data_grabber tuple contract. No survey reads or production changes. Numeric Org key parsing (item1) remains separate.

## §2 Existing machinery
Search tier: git/ripgrep. country.py Wave.get_column_mapping nested map_formatting_function emits (columns,(formatter,mapping_dict)) for list-form mappings with a registered formatter (around1511). local_tools.py df_data_grabber nested grabber handles tuples but returns within the first loop iteration (around2180). tests/test_categorical_mapping_resolution.py covers table resolution; tests/test_uganda_comm_frame_ea.py uses the existing string-function workaround. Reuse actual grabber with only its source-reader leaf replaced by an invented DataFrame.

## §3 Definitions
Cite STANDING.md §§3–4, CLAUDE.md and data_info.yml. idxvars are identifier strings, finalized using format_id; mapping dictionaries pass unmatched values through. This fixture is not an actual Uganda survey or namespace decision.

## §4 Invariants
No raw/API/cache/network operations; exact import identity and isolated checkout. Dictionary stage must receive formatted values when explicitly sequenced, without changing row order. Preserve current observed failure as evidence. No leading-zero/numeric-parser policy inferred.

## §5 Reuse
Reuse df_data_grabber, format_id and actual pandas; invented getter only. Extend no production symbol during reproduction. A future fix needs parser-through-grabber tests and inventory of existing tuple consumers.

## §6 Open questions
Confirm observed mapping is silently skipped versus raising in particular formatter shapes. Numeric-key semantics and mixed nested sequences remain outside initial reproduction; no human permission needed for this hermetic diagnosis.

## Current evidence
Actual parser-through-grabber reproduction silently drops the dictionary after the formatter. Six regression cases give five failures/one pass on parent ffbe9121, then the candidate plus existing resolution/value-column tests gives62passed/1skipped. No survey input was read. The candidate reuses grabber for every stage, passing a one-column temporary frame between stages so only the first stage sees composite raw columns.

Inventory covers all100tracked wave data_info.yml files:177list-table mappings, no nested explicit sequences. Actual imported formatter registries for the76waves containing those declarations have no target-name collisions and no import errors, under source-payload/network refusal. This bounds current declared automatic formatter+dict paths; arbitrary Python callers are not an exhaustive runtime census. The build-transform fingerprint still changes globally; this is not a corpus rewarm acceptance claim. Item1 Org numeric-key policy remains open. Independent source review pending.
