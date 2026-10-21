# Malawi #1111: instrument-covered applied totals, partial fix

Addresses #1111 for 2010-11 and 2019-20 only. The drivers already selected enumerator-total kg quantities; they now label those quantities `Kilogramme` before the existing additive aggregation. They no longer decode timing as a unit. No amount is rescaled, no application axis is added, and no reducer, eligibility, ID, source, schema, factor, share or shared-helper behavior changes.

Baseline: `f8d236694c26fe9e709c5548f02c70f74ea72c33`. Ledger-first published commit: `63c2c4315`. Root approved this exact boundary after the frozen read-only diagnosis. The independent organic fix at `de9c2734b8a6bcf1326d0d4285ff9c1db1221767` / PR #1175 is not stacked here. Composition is root-owned and pending after this lane freezes.

## Primary evidence and country detail

Live #1111 says: "It needs C4, the questionnaire's Module D Q39 unit code list, before any code change." The held instruments settle the chosen amounts' unit for these two waves:

| Instrument / held sidecar MD5 | Page | Native pair | Existing selected total | Timing, not unit |
|---|---:|---|---|---|
| IHS3 agriculture, `d6e0d71486fd6ae4d7dda5b8e1854278` | printed/PDF 13 | first b/c; second g/h | `ag_d39d/i`: TOTAL KGs | `ag_d39e/j`: MONTH |
| IHS5/IHPS agriculture, `86cc1f419c1320eb969d11a6d24a63e0` | printed/PDF 10 | first b/c; second h/i | `ag_d39d/j`: TOTAL KGs | `ag_d39e`: first-application weeks |

The 2019 Panel and Cross_Sectional extracts independently match these roles; f is the second-application gate and g its type. A native unit belongs to its native quantity, not the enumerator total. This preserves the existing amount choice; selecting native pairs would be a separate enhancement. Quantity medians, EPAR, and nutrient totals do not authorize a 1,000x conversion.

The country CONTENTS was read in full, including both live TODO #116 entries. Relevant idiosyncrasies include the redundant 2010 Panel source (driver uses Full_Sample), distinct modern XS/Panel universes, the 2016 XS `cs-17-` prefix versus bare 2019 XS IDs and subsequent ID walking, and the IHS4 XS restricted reader for an unused bad-byte string. All remain unchanged.

CONTENTS' earlier "Module D records no organic quantity" is contradicted by the reviewed #1109 evidence for `ag_d37a/b`; this branch preserves f8 organic behavior and records the independent correction explicitly. Its earlier `ag_d39c` "acquired-amount" / e-j applied-unit interpretation, and the shared helper's common-unit-dictionary comment, conflict with the separate native, total and timing fields above. The new CONTENTS note qualifies those claims; the shared helper stays byte-identical. Q39's mixture-under-main-type instruction and nutrient composition remain #1106.

## Independent source conservation and actual API behavior

Sanctioned held-source reads independently reconstructed selected slots, without using the production fertilizer decoder or assembler for expected quantities. Held PDFs and sources were physically copied and MD5 verified; source IDs and group members stay private.

| Source | Slot 1: rows / selected kg sum | Slot 2: rows / selected kg sum |
|---|---|---|
| 2010 Full_Sample ag_mod_d | 11,364 / 662,587.5 | 5,285 / 306,767 |
| 2019 Cross_Sectional ag_mod_d | 8,602 / 409,435.15 | 3,962 / 199,327.55 |
| 2019 Panel ag_mod_d_19 | 2,763 / 128,137.75 | 1,217 / 59,509 |

These 33,193 source application slots contribute exactly 1,765,763.95 kg. Every baseline and candidate corrected-group sum equals the independently grouped source contributions. Existing coincident applications already aggregate: baseline has 33,152 groups in the corrected subset, candidate 32,917. Newly equal kg keys coalesce 235 additional pairs: 51 in 2010 (4,670 kg across 102 members) and 184 in 2019 (17,322 kg across 368 members). Each is one same-source plot/input's first and second application, affirmative use, with two positive observed kg answers. There are no duplicate source-slot identities. Their complete private member payloads and public group amounts/null masks are conserved.

Labeling before aggregation matters. Labeling an already aggregated frame could leave duplicate kg keys and expose an application to a later first-selector loss. The new tests reject that mutant. The current additive full-key reducer is reused unchanged, including its existing all-missing-to-zero behavior.

| Measured API quantity | f8 baseline | Candidate |
|---|---:|---:|
| All Country plot_inputs rows | 180,184 | 179,949 |
| Nonorganic subset rows | 169,109 | 168,874 |
| Inorganic rows | 54,339 | 54,104 |
| Inorganic numeric Quantity sum, all four waves | 2,899,339.95 | 2,899,339.95 |
| Inorganic Gram rows | 13,302 | 3,121 |
| Inorganic null-unit rows | 17,044 | 5,462 |
| All input null-unit rows | 28,724 | 17,142 |
| Uganda comparator rows | 84,216 | 84,216 |
| Feature(Malawi, Uganda) rows | 264,400 | 264,165 |

The earlier 169,109/168,874 figures refer to the nonorganic subset, not the full Country result. The full numeric Quantity sum is unchanged at 6,387,505.990000077; that is an arithmetic conservation check across mixed units, not a physical kg total.

The whole candidate Country and Feature frames equal the baseline after only the evidenced unit projection and existing additive aggregation, with exact values, dtypes and attrs. All unaffected Country rows retain order and every field/key/null mask. The complete 2013 and 2016 public and wave-cache frames remain exact. All 11,075 f8 organic rows retain zero Quantity and null unit. All nonfertilizer branches, including seed purchase measures, remain exact. Inorganic purchase/Improved payloads in the corrected subset are all absent before and after.

Existing retention is explicit: selected 2010 slots include 12 missing quantities, 20 source-use No and 13 missing-use records; selected 2019 slots include 18 zero quantities. The source predicates and reducer do not change. This is not a #1174 retention repair.

`nitrogen_kg` uses unchanged factors/shares. Its diagnostic sum changes from 840,235.7795800001 to 1,028,195.63206 kg. The measured binary-float delta is 187,959.85247999988 kg, differing from decimal 187,959.85248 by -1.1641532182693481e-10. The private analysis checks absolute error below 1e-8; no library/test tolerance or factor was changed. This is an aggregate diagnostic of corrected identities, not a uniform conversion rule or a claim that unresolved waves are correct.

## Cache and public-build proof

Both sides ran normal target-cold Country builds, followed by fresh-process warm Country/Feature/nitrogen reads. No assumed-fresh path, read-gate bypass, manual stamp, or shared-cache clear/promotion was used. The healthy Uganda comparator has a verified current f8 hash and all-wave manifest; its entire frame/attrs and both copied cache files remain byte-exact. Warm reads compare complete frames/dtypes/attrs and check every private L2 path, byte hash, size and mtime: zero writes on both sides.

Actual inventory is **549 API hash entries / 39 framework fingerprints**. The complete f8 keyset and values match the independent Burkina #1123 inventory (SHA256 `beebfd5841612ed29da663b82f4b4a1843188df4bb6dc0b66b5ad3153a4e2128`). All **26 Malawi API hashes** change: 22 nonderived plus four runtime-derived (`food_expenditures`, `food_prices`, `food_quantities`, `household_characteristics`). All **523 other-country entries** and **39 framework fingerprints** remain exact. The actual `Wave._input_hash` loop hashes every Python input in the wave and country `_` directories for every table; keeping the country helper unchanged does not narrow that footprint. This disproves the read-only diagnosis's narrower-footprint hypothesis. Earlier frozen receipts are not rewritten.

Normal candidate serving rebuilt sample from all five held waves. Separately, physically fresh f8 and candidate roots with held L1 only each built the full sample: 57,118 rows, all five wave frames and country cache exact in order/values/dtypes/attrs, matching the main roots too. Fresh-process warm sample reads made zero writes. The 18 complete categorical warning records match exactly between the two fresh cold builds; their ten source-label bindings and the main rebuild's six captured events are accounted separately in VALIDATION.md.

## Validation and residual

Actual-driver tests: candidate 12 pass; baseline 10 fail / 2 pass; an in-memory late-label mutant 12 fail. The bounded actual-driver, plot-ID, transforms and nitrogen selection passes 101 tests. No existing test, fixture, golden, share or tolerance is weakened. The existing warm test helpers that request assumed-fresh serving were not used; normal held-data Country/Feature warm verification supplies that evidence.

The scoped scanner actually ran phases 1 and 2 for plot_inputs in Malawi and Uganda, producing 15 findings on each side. Every record is reconciled in VALIDATION.md; no new error/finding appears. Phase 2 has no plot_inputs kwarg variants in the current harness.

2016 still lacks the agriculture questionnaire sidecar; 2013 questionnaire MD5 `7240c127062552554f2e03ece6a05f0d` is unheld. The literal C4 gate remains for both. Unknown normalization, unlisted seed codes, seed-purchase mixed units, #1106 composition, #1174 retention and organic #1109 integration remain separate. See RESIDUALS.md. This is a partial fix, not closure of #1111.

Private evidence: `/local/job40255479/control/isa-backlog-20261021/Malawi/1111-fix`. Raw frames, group identities, full logs/warnings and scanner JSONL stay there. Safe validation receipts and commands accompany this report. The immutable f8 and reviewed de9 lanes are checked against the frozen full tracked-file/mode manifests.

Sue (GPT-6), 2026-10-21.

## Test-only review descendant

Independent review of frozen `9b883974a81d6ee664bbca425b7d484f0f14eb7a` returned NEEDS_FIX because the new test asserted f8's known organic zero/null/no-Applied defect, outside this unit-fix boundary. The private reviewer receipt is `/local/job40255479/control/isa-backlog-20261021/review-1111-Malawi/verdict.json`; no independent runtime/pytest worker had started, and the snapshot was released unchanged. Root authorized removal of exactly those five selection/assertion lines. Every fertilizer, missing/zero, namespace, seed and pesticide assertion remains; production and fixture payloads are untouched. This permits the independently reviewed organic behavior without claiming that composition has been tested here.

The original `validation.json`, runtime/source/quantity proofs and `/Malawi/1111-fix` private evidence remain bound to 9b and immutable. The safe `test-descendant-validation.json` records this descendant's reruns, new test hash and exact all-other-blob/mode equivalence. Fresh private test evidence is `/local/job40255479/control/isa-backlog-20261021/Malawi/1111-tests-descendant`; there is no unnecessary source rebuild or attempted de9 composition. Independent review and root-owned composition remain required. Sue (GPT-6).
