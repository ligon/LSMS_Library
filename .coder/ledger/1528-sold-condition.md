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

Root explicitly authorizes a minimal implementation if own-source bindings are unambiguous. Source verification is closed: each own sold dictionary independently has codes1–5 Epis/Grain/Décortiqué/Non-décortiqué/Non concerné. There are5356/2111/1140 recorded answers and zero state-without-positive-sale contradictions across Burkina2018/Mali2018/Mali2021. See source-summary.json and owner-only closed numeric/labelled frames.2021's code list was not inferred from2018. An actual ambiguous code meaning/no-sale contradiction stops only that part with an evidence memo. Current1510 collector and1475/1517 framework lanes are separately owned; this branch changes neither. This ledger is surfaced before any production edit. Root owns review/integration/GitHub, and may sequence newer development after the frozen comparison.

## §7 Implementation and focused controls

The minimal patch adds the two canonical optional string fields, reuses each own-wave fold/literal-label helper, and transports2021 fields via an explicit attachment-column list. Default _SALE_COLUMNS and EAC-I callers are unchanged. The existing attachment registry names newly transported columns and its own s16dq05d operand; its inputs callable exposes that native answer without changing row selection.

Focused credential-free controls:129 passed,6 skipped. New source/API tests are separately credential-gated. New synthetic fixtures initially omitted required intercropped and typed empty merge keys; those fixture construction errors were corrected without product changes, with the original failure log retained. The harvest-source AST oracle now selects the required column among legitimate multiple helper calls, with absent/wrong-column and duplicate-binding refusals tested; no harvest-binding assertion was removed. Those gates are now closed as qualified in Phase3 below; the exact original head and failed attempts remain preserved.

### Phase 3 — verification

Original reviewed/runtime freeze: afe6f4389ac69cfe97a102a07ccc7b1644dd1fcf against c964c6c4f25945dc14636fd094893b16b12180b7. Ordinary integration tested at0586f02af8597d8ab8deac6a61a5fb4a208278b3 against current GitHub development3b28d293f71b962c3d878c2886be9ee794407915 (1510 +1475/1517 landed). All13 original patch files were byte-identical before this final documentation update; incoming paths were disjoint. The original author tree remains atafe6. No rebase/force or filesystem development advancement.

- _crop_sold_conditions / ehcvm_crop_sold_conditions — OK (§§2–4): reuse each verified own-wave fold; NA for absent/off-scheme sold state, no harvest guess, no sentinel column. Actual dictionaries independently verified by the reviewer too.
- Own script reads and finish tails — OK (§§2–5): correct2018 s16cq16c /2021 s16dq05d binding, literal native labels, optional nullable string columns, no index changes. Three source oracles passed; crop alignment reuses the unchanged harmonize_food table and is not an independent crop-label audit.
-2021 explicit attachment transport / registry input exposure — OK (§§2–4): existing one-row/Farm-level placement, positive-sale gate, default _SALE_COLUMNS and EAC-I scripts unchanged. All1070 single-row and70 Farm-level answers retained. Registry attrs change only its transported columns list.
- Source-binding oracle — OK (§5): fixed dictionary independent of production state helpers; explicit own crop-table header selection, with strict old harvest binding and duplicate/wrong-column refusals retained. No missingness assertion weakened.

Own source/API evidence:5356 Burkina2018 +2111 Mali2018 +1140 Mali2021 =8607 answers, all accompanied by a positive sale. Every old value/key/order/dtype/missing value equals coldc964, with zero added/removed rows. Burkina stays15587 API rows from15593 raw records, including the same six pre-existing drops; Mali stays50752, including10341 +24921 EAC-I rows with new fields entirelyNA. All sampling/population/recall/unit attrs exact; only the declared Mali attachment columns expand. Sold versus known harvest fold differs on270 Burkina2018,167 Mali2018 and4 Mali2021 rows;1089 Mali2021 sold states accompany unknown harvest state, which stays unknown.

Cold default, independent cold Make, requested-wave subsets, two-country Feature66339 rows, and fresh-process warm passed onafe6; Make/warm frame+attrs exact. Full credential-free isolated CI onafe6:11462 passed,5652 skipped,4 xfailed in1313.32s (job41006173 COMPLETED0:0). Independent review exactafe6:139 passed,6 skipped, including10 external actual-script/registry/serialization controls, plus direct own3 metadata bindings; no P1/P2.

Bounded current-base integration job41007730 COMPLETED0:0: fresh actual source/API/Feature/subsets/warm on0586 are ALL-field/index/order/dtype/missingness/attrs exact to closedafe6;233 relevant1528/1510/1475 controls passed,6 skipped. No repeated c964 baseline or full suite. Full metadata-only census onafe6 covered38 registered countries/450 country/1895 wave/35 build/186 framework fingerprints: only Burkina21 +Mali20 country hashes and Burkina21 +Mali80 wave hashes changed; global build/framework fingerprints and other countries unchanged. Current3b28 two-country/global comparison repeats that41/101 footprint with zero global changes and zero source/provider/build calls.

Durable public receipts: reports/1528-sold-condition-20261107/{SOURCE-BINDING.md,source-summary.json,api-comparison.json,attrs-comparison.json,answer-coverage.json,hash-comparison.json,FINAL-RECEIPT.json,integration-3b28/}; independent verdict: reports/review-1528-sold-condition-20261107/VERDICT.md, under the root's isa-sm-20261103 burst evidence directory. Native frames/source IDs and reader logs remain owner-only. The failed first source-summary serializer, initial synthetic-fixture errors and first API oracle header error are preserved, not counted green.

Final ledger/CONTENTS changes follow closed actors. Runtime/config/tests remain byte-identical to0586 and receive a separate byte/hash equivalence receipt. Full integrated-head GitHub CI/Windows smoke/publication remain coordinator gates; the full suite was not replayed on0586. No new source ambiguity or scientific policy decision; held1509 eligibility and48 weights/whole-wave semantics remain outside scope.
