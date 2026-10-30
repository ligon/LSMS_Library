# Nigeria #1292: selected PH education source dates

## §1 Task
Retain the five currently selected PH education files, questions and mappings,
but date their answers at each logical PH visit only. Prepare actual-Wave
synthetic evidence on public d65; hold real data and Country/Feature acceptance.

## §2 Existing machinery
Reuse Wave.column_mapping's per-file t override, actual Country.waves/getitem,
Wave.resources/data_scheme/getter/grab_data, df_data_grabber and format_id,
the public harmonize_education Org tables, and the Wave quarter filter.
The frozen StageA packet at nigeria-1292-stage-a-v1 supplies the independent
five-file manifest and baseline blocks; do not execute or mutate it. Existing
shared successful-empty PR1309 is separate aggregate-path work. Search tier:
git/rg/AST plus official World Bank metadata; no GitNexus.

## §3 Definitions and evidence
Inherit STANDING.md, CLAUDE.md and canonical data_info.yml. One physical Nigeria
folder holds PP and PH visits; t is a logical visit label, not every respondent's
calendar quarter. Public-d65 data_scheme says the selected PH section2 rows appear
under BOTH quarters; this correction changes that documented behavior.
Official variable dictionaries bind W1/W2/W3/W4 s2aq9 and W5 s2q9 to completed
education level in the selected PH file. W5 s2q3 is respondent identity, not
attainment. Earlier PP instruments separately contain W1 s2q7/W2 s2q8 attainment;
PH-only describes this selected source series, not all Nigeria education.
Sources: catalog1002/F30/V700;1952/F69/V1970;2734/F41/V1124;
3557/F45/V1498;6410/F57/V2187 at microdata.worldbank.org. Companion recovery
instrument-authority.md records links and qualifications.

## §4 Invariants
No source file/question/map/identifier/eligibility change; no legacy s2q3
substitution, PP wiring, private1120science,1311compiler or shared framework
adoption. Preserve original PH keys/order/values/nulls/dtypes/attrs and original
synthetic operands; candidate PP must be schema-preserving empty. Actual
Country resources and Wave operations remain real; replace only source-reader
leaves with invented frames. LSMS_NO_CACHE/private roots prevent parquet writes.
No source payload/cache/provider/Make/sample/finalizer/Feature path is admitted.

## §5 Reuse decision
Date overrides: reuse the existing five-YAML correction, byte-exact to its frozen
source blocks. Test carrier: extend existing actual-Wave StageA comparison with
portable tmp_path metadata and invented observations; no data decoder or second
extractor. New guard glue is needed to admit the real education Wave getter
while retaining all neighboring build/provider/crosswalk traps.

## §6 Verification and remaining acceptance
P3 source1d117831/tree26b4 completed35 disjoint synthetic cases,105 passing
setup/call/teardown stages and120 actual Wave/extractor/invented-reader calls
with120 closed exact metadata epochs across seven complete guards, zero denials
and outer exit0. The focused carrier preserved PH keys/order/values/nulls/dtypes/
attrs and operands and returned a schema-preserving empty PP frame. Independent
ordinary-receipt review authenticated all149 exported members,35 sealed operator
members,1836 source members and exact paired journals; it ran no tests or actors.
Root result: ROOT-REVIEW SHAfe0ed09c18ec640fa92b79d9c79d974c248c179aa2dc19872cafd09182097938.
Review: slurm_logs/isa_recovery_2026-10-29/Nigeria/independent-Ethiopia-P3-result-review.md.

P1 remains timeout124 with unknown exact partial case/callback progress and no
final acceptance. P2 remains source-refused on F1 and unexecuted. The later
normal-QoS remote review failed under SuCoder79 and supplies no accepted review;
those histories are not added to P3 counts or relabelled successful.

This integration changes explanatory comments only and performs no new project
imports, tests, actors, survey/cache reads or public writes. The five measured
YAML blocks and224-line carrier stay byte-exact; schema non-comment content
stays byte-exact. Raw held-source/StageA/S/C, sample spine, Country aggregation/
finalizer, Feature, Make, cold/warm/hash/cache and scientific acceptance remain
separate held gates. Successful-empty aggregate dependency1309 remains open
and unadopted. Exact early W1/W2 questionnaire skip routing versus the old
left-school-only explanation is still unestablished; the official W2 file
metadata describes Education–New Household Members, not a proven skip path.
Sparse metadata counts do not settle eligibility, and earlier PP attainment
sources remain separate and unwired here. Logical Q1 labels denote visits,
not a promise that every actual interview occurred within that calendar quarter.
