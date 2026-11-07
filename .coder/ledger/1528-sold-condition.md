# Prior-Art Ledger — GH1528 own-wave sold condition

Base: c964c6c4f25945dc14636fd094893b16b12180b7, freshly fetched from https://github.com/ligon/LSMS_Library.git development. Search tier: git/ripgrep/AST. Filesystem origin/development111b is stale and is not an integration base. Earlier mistaken checkout remains untouched under its original ref; current branch is fix/1528-sold-condition-current-20261107.

## §1 Task

Carry the existing EHCVM sale answer as optional Condition_sold and condition_sold_native for Burkina_Faso2018-19 and Mali2018-19/2021-22. Fresh GH1528 has no comments and directs own-wave code-list verification and the existing Niger fold/NA pattern. Canonical data_info.yml already records the Nov5 native sold-answer ruling. No new wave, harvest-state guess, unit conversion, factor, value or default policy is authorized.

## §2 Existing machinery

| Machinery | Decision / test anchor |
|---|---|
| Burkina2018 crop_production.py _crop_conditions/_crop_condition_labels/_finish_crop_production | Extend only sale field reads/tail; reuse current fold and literal-label normalization. tests/test_ehcvm_crop_production_condition.py and test_gh824_burkina_unit_sold.py |
| Mali/_/mali.py ehcvm_crop_conditions/ehcvm_crop_condition_labels/finish_ehcvm_crop_production | Extend only sold-column transport using current own fold and dtype tail; existing condition and EHCVM crop tests |
| Mali derive_hh_crop_sale_attachment(harvest,sales,columns=...) | Reuse explicit column list to transport2021 sold state onto the same single-row/Farm-level decision; do not modify default _SALE_COLUMNS or EAC-I callers. Existing GH824/1457 source tests |
| Niger _crop_sold_conditions and2018 script | Existing NA-bearing fold/native-answer pattern; code precedent only, no Niger operands or metadata admitted |
| Current harmonize_crop_condition_ehcvm1 tables | Reuse only after each own sold-variable code list is verified. No cross-wave numeric-code assumption. |
| Country finalizer, Feature alignment, get_dataframe/get_data_file/to_parquet and cache hashes | Reuse unchanged. Standing ledger§§2–4 and CLAUDE.md govern. |

## §3 Definitions and country grounding

Inherit STANDING.md§§3–4, CLAUDE.md Data Access/Grain Collapse and canonical data_info.yml crop Condition_sold/condition_sold_native. Both are columns, not index levels; no sold state uses unknown_condition. Native labels retain survey wording, without vocabulary respelling.

Full current Burkina_Faso/Mali CONTENTS (including TODO/WAITING/LOGBOOK) reviewed via complete prior source text plus current deltas and remaining current sections, before source admission. Burkina2018 native i uses ehcvm_i, not the newer country formatter; its current crop builder covers2018 only. The released five-state dictionary includes Non concerné although the printed form lists four: the country record explicitly says “The printed form is not what was fielded.” Do not force the printed four-code list or wire2021/2014.

Mali EAC-I2014 rural holdings versus urban households and2017 Lourd/Leger differ from the two EHCVM instruments. Only EHCVM is in this task. The record says “The s16c variable numbers flip between the waves”:2018 s16cq16c is SOLD state,2021 s16cq16c is harvest kg TOTAL,2021 sold state is s16dq05d. Harvests derived from dispositions may mix states and retain unknown_condition; sold-state wiring cannot relabel them. Historical “Recorded and not served” harvest-state prose is superseded by the Nov4 condition entry; the sold side remains explicitly unread. Older tree sale-window contradiction, no-sale NA ruling, source namespaces, identity maps and planted/harvest periods remain untouched.

## §4 Invariants

Preserve every existing native key, index level, crop/harvest condition, Quantity, Quantity_kg, Quantity_sold, Value_sold, Unit_sold, period and Derivation. Transport the2021 sale answer to exactly its own existing sale destination. Do not change sale eligibility/attachment or sum/choose states. EAC-I receives typed NA optional fields only; it has no newly read state. Added-payload row retention is measured explicitly, never hidden by an extra filter or asserted unchanged in advance. A literal answer beside no positive amount is not silently discarded; resolve any real contradiction against the no-sale contract with source evidence before that part proceeds.

Private roots/shared existing L1 only; sanctioned readers, exact package/config/cwd, mounted main interpreter, BLAS/build1, pytest2 loadfile. No DVC CLI, LSMS_NO_CACHE, shared L2/rewarm, dependency/fixture weakening, GitHub writes, source IDs in public reports or changes in held1509/48/default policy.

## §5 Reuse/verification plan

1. Bind own-source sidecars/bytes and variable labels/code lists for three sold fields; numeric+labelled source frames stay private. Record all observed blank/off-scheme/no-sale contradictions and existing drop-set membership.
2. Add minimal fields/tail/declarations with existing helpers; pass explicit2021 sale columns to the existing attachment helper rather than broadening shared defaults. Source/oracle tests include harvest/sold disagreement, swapped code-list negative controls, blank/off-scheme labels, one/Farm-level attachment, old quantity/value/unit/key/NA equality and payload-only retention.
3. Freeze exact source before source/API actors. Compare independent source answers and exact old payload/native drop sets. Run appropriate targeted controls, typed normal/Make/Feature/fresh-process warm checks, measured hash footprint and bounded credential-free CI. Separate actual passes, prior evidence, failed attempts and unexecuted gates.

## §6 Open questions/authority

Root explicitly authorizes a minimal implementation if own-source bindings are unambiguous. Source verification is pending;2021's code list is not inferred from2018. An actual ambiguous code meaning/no-sale contradiction stops only that part with an evidence memo. Current1510 collector and1475/1517 framework lanes are separately owned; this branch changes neither. This ledger is surfaced before any production edit. Root owns review/integration/GitHub, and may sequence newer development after the frozen comparison.
