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
calendar quarter. Public data_scheme says the selected PH section2 rows appear
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

## §6 Remaining acceptance
This preparation performs no project imports or tests. Root and independent
review precede bounded runtime admission. Raw held-source, sample spine and
Country/Feature/cold/warm/cache acceptance remain separate. Exact early W1/W2
questionnaire skip routing versus the old left-school-only explanation remains
unestablished; sparse metadata counts do not settle it. No new human decision
is required to prepare these source-only artifacts.
