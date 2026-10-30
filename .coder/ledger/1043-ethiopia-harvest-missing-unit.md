# Ethiopia1043 — missing harvest units use the closed sentinel

## §1 Task and current evidence
Live1043 remains open,0comments (read2026-10-30). It identifies a missing harvest
unit minted as Other(Specify), outside the sentinel mask, and asks for perwave
counts and crop kg-ladder accounting. Basepublicd65 still fills that spelling
at crop_production_for_wave's final index construction. The related later-wave
sale-unit fillKg is ALREADY fixed by mergedGH1253 (64649c770); do not re-fix it.

## §2 Existing machinery / search floor
Read prior-art-ledger, STANDING, countryCONTENTS/live TODO/LOGBOOK/WAITING,
food-acquired unit discipline, existingGH962 sentinel inventory and GH852shipped
factor ledger. Reuse crop_production_for_wave, _clean_unit_label, the unchanged
five native scripts/columnmaps, normal get_dataframe, U_UNKNOWN/_unit_sentinel_mask,
crop_conversion_factors and harvest_kg_factors. No new decoder/provider/formatter,
unit inference, table build infrastructure or scientific classification.

## §3 Definitions in force
CONTENTS requires reported Q3/Q5/Q6 food quantities with their own units, two
separate panel eras, and crop sale quantities attached only when their recorded
unit matches harvest. The2026-10-23 ruling retains every row carrying a sale
value, including0; the old prose tags those unitless rows Other(Specify).
This source-policy contradiction is retained as history, then explicitly corrected
for missing harvest units under the closed U_UNKNOWN vocabulary (#1041/#1234).
Reported Other codes/labels remain genuine reported units and must not be remapped.
W1 implicitKg is instrument-defined and staysKg; blank decoded sale units remainNA.
The controlling contract is the units skill's closed-vocabulary rule
(`.claude/skills/add-feature/food-acquired/units/SKILL.md`, lines150–153),
`transformations.U_UNKNOWN` / `_U_SENTINELS`, and the existing #1041 missing-unit
handling recorded in CONTENTS. These settle an actually absent unit as Unknown;
the issue's older TierC wording does not authorize relabelling a reported Other.

## §4 Invariants and finite source scope
Preserve rowpopulation/order, all quantities/sale values/flags/dtypes and recorded
units; change only genuinely absent cleaned harvestu toU_UNKNOWN. Preserve empty
row filtering, including value-only rows. The private sect12 folded-key unit fill
must agree with final keys; measure any attachment effect rather than assume it.
23logical sources/22unique already-cached regular L1objects,91,874,033B:19source
endpoints declared by five native scripts(27calls), plus4logical CropCFfiles
(W4/W5sameblob). Use normal readers with native category flags and preservedfull
inputframes; compare old/new helpers on exactly the same inputs, then kg ladders
with and without shippedfactors. No source acquisition, FDCnutrition, shared cache,
curation/pins or arbitrary assertions relaxed. OneCPU/8GiB/thread1/private data.

## §5 Reuse decision and validation
First establish source-authoritative absent units with newsynthetic controls;
paired actual native helpers require exact payload/cell comparisons and counted
key/collision effects. Only afterward compose realCountry/Feature/cache checks as
necessary; preserve failures and avoid replaying historical compiler cohorts.
Existing tests that deliberately assert the incorrect missing-u label need a
narrow expected-sentinel update; all numeric, row-retention and sale-grain
assertions stay. Remove the obsolete GH962exception for mintingOther after the
fill is gone, and document the already-merged sale-unit correction accurately.

## §6 Residual decisions
No new ruling is needed for an unrecorded unit: reuse the existing closed sentinel
contract. Other/unknown positive recorded codes, questionnaire residual units,
shippedfactor duplicate rule and crop/sale reference windows remain unchanged.
If paired source validation finds row/payload or linkage changes, report them
before treating this as an index-label-only fix. No actual source observation is
claimed at this source-preparation commit.

## §7 Native source results (separate from served API grain)
The retained one-shot paired normal-reader diagnosis completed35calls across
23logical sources. Both helpers returned87,045 native rows. Missing-unit labels
changed Other(Specify)→Unknown on0/379/79/168/278 rows in chronological wave order
(904total); reported Other stayed0/4,349/111/666/381. This is native-builder grain,
not a Country/Feature count. Native kg-layer accounting and totals were unchanged
with both default and shipped factors. The original in-memory pandas checks are
not promoted to exact numeric proof: a separate supplement read only the ten
owned saved operands, passed explicit ExtensionArray and nonmissing NumPy value
equality with exact dtypes/masks, and detected both tiny/nextafter negative controls.
No raw-reader or constructor replay was used for that supplement. Receipts live
under `/local/job40561868/ethiopia1043-harvest-unit-v1/`; API grain remains a
separate next validation scope. Synthetic118pass/16deselected is also retained;
the unit-column-absent branch test was renamed because its synthetic time is
2013-14, whereas the actual native W1 observation separately verifies W1Kg.

## §8 Canonical API result and retained measurement failure
Normal public-base and candidate builds each return 86,403 Country rows. Served
Unknown counts become 0 / 379 / 79 / 168 / 277 in chronological wave order
(903 total), versus 904 at native-builder grain. Genuine reported Other remains
0 / 4,321 / 111 / 666 / 381; all other served unit counts stay unchanged. The
missing-unit class retains 11 positive-Quantity rows and 884 sale-value-only rows.
Every non-u key and payload, dtype and missing mask matches as a full row multiset
using explicit ExtensionArray and nonmissing NumPy equality on owned saved
operands. This does not assert order across renamed unit index keys.

Normal declared unit masses give unchanged API factor layers: default metric
40,525 / none 45,878; shipped 62,705 / metric 7,821 / none 15,877, with declared
13,977 in either mode. Harvest_kg has 36,648 rows and sum 8,799,669.50962 by default,
versus 66,645 rows and sum 12,696,657.323532056 with shipped factors. Before/after
accountings match exactly. This fixes missingness classification, not conversion
coverage; it never invents a factor for an unrecorded unit.

The first baseline actor failed its measurement oracle after completed builds:
Feature adds documented constant condition / season / production_obs axes. Its
failure remains in api-baseline-v1. Additive readback copied the completed private
cache without producer replay, checked the three configured sentinel values, and
projected them by name before comparing Country/Feature payloads. Candidate cold
passed in 59.83 s; fresh-process warm passed in 10.83 s with identical private
cache bytes/size/mode/mtime and no parent Make or id_walk. Both comparisons use
strict EA/NumPy payload checks. No shared cache mutation, acquisition, nutrition
lookup, new scientific rule, full-suite/CI result or historical cohort replay is
claimed.
