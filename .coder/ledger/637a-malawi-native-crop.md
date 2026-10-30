# Prior-art ledger: Malawi native crop codes (#637 defect A)

Search tier: Git/ripgrep/AST. Public base26803b81050df527add7ed5908db6c827a65eed6.
Read Malawi CONTENTS including TODO/WAITING/LOGBOOK; it is unchanged from reviewed
104655. G/P native codes overlap;2016 XS uses cs-17,2019 bare IDs. Cowpea/FCT/unit
and other country caveats remain. Inherit STANDING.md, CLAUDE.md and data_info.yml.

## Scope and authority
Implement settled maintainer #637 comment6366561080 and #1198/#1263 precedent:
production_obs identifies the native crop code before perennial offset/recode.
Historical crop_variety “splits no key”/“awaits a grain ruling” statements are
retained as history and reconciled additively. This is neither physical plot
identity nor source-line position. Same-code/unit/condition repeats keep their
reported sum and first nonmissing month. No seed/sale/factor/eligibility policy
or missing-household/plot inference changes; #1357 excludes anonymous households.

## Reuse and implementation
Reuse _crop_codes, unit maps, _sum_reported, unchanged _attach_sales/reference
ladder and variety-keyed factors. Extend _harvest_block with reported:G/P:<code>,
include it in _consolidate_harvest and refuse repeated full native grain instead
of the final lossy preferred-label collapse. Four native guards/country registry
append the existing canonical axis. Three function ASTs change;99 other named
definitions, merged labor and G/H work remain exact. No global schema/core edit.

## Validation and qualifications
Nine production/test files are extracted byte-exact from reviewed/tested f4e40f.
That source's bounded synthetic selection passed58,35 data-gated deselected;
no cohort is replayed for publication. Current base retains all Malawi source
and target tests from104655, and core/schema/runtime-lock equivalence is checked.

Accepted staged actual evidence used24 G/P/I/Q captures once,8 native children,
2 real compilers and4 independent native-code/own-payload/sale oracles. Candidate
native counts33549/12671/37177/51973 total135370; unrecognized-crop exclusions
1/24/0/66 remain. Perennial alias-pair coexistence was0 in each wave; all12 old/new
reference-pool checks passed. This is measured evidence, not inference from
unchanged sales AST. Existing reducers and sale science were not re-estimated.

Saved mandatory compiler CALL and normal Country135370-row comparisons passed
unchanged full typed comparators. First genuine Feature and fresh-process warm
Country/Feature passed exactly; five cache files/maps and seven adopted inputs
closed unchanged. Sample and cache prerequisites are explicitly composed; no
fresh normal sample/cache/weights claim. Original compiler-check120s timeout
and subsequent strict-read failure remain failed, not relabelled successful.

The strict read correctly rejects41 missing plot_id rows in2016-17, already41
in baseline/candidate/pre-sales, all known households. No null i/v/production_obs
in the accepted Country. This differs from historical40, and from anonymous
household policy. No deletion/sentinel inference is added to obtain strict green.
Eight native RETURN/storage full typed comparisons stay false: qualified saved
attribution found attrs JSON representation differences, strict axes/dtypes and
separately equal payload/masks; it is not a full typed-frame pass. Whole #637,
#1120 missing-plot representation and other country residuals remain separate.
