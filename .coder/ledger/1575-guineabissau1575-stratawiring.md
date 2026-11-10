# Prior-Art Ledger — GH1575 Guinea-Bissau 2018-19 strata wiring

Search tier: git + ripgrep + AST floor; no GitNexus tool is available.
Base: `9f89151ddd783a07fa57b9277da3677f7024ce7c`.

## §1 Task, restated
Implement the user-selected documented region/milieu combination only for
Guinea-Bissau/2018-19 sample, with unchanged shared812 helpers and existing
sources, joins, IDs, weights, Rural and all non-strata payload. Add own registry
key, composite spelling aliases, dated notes and meaningful invented-frame tests.
The 2026-11-10 approved-four-country CHARTER supersedes historical NOT SELECTED.

## §2 Existing machinery
- `lsms_library/ehcvm.py:820–889`: `derive_region_milieu_stratum`,
  `strata_label`, `stamp_region_milieu_stratum`, `inputs_region_milieu_stratum`.
  Reuse unchanged. Tested in `tests/test_gh812_ehcvm_strata.py` Label/Stamp/Wiring.
- Own `2018-19/_/data_info.yml:4–37`: cover/cluster-keyed weight merge and
  `(i,t)` final index. Own `mapping.py` has no sample/inputs_strata collision.
- Own `_/categorical_mapping.org:34–97`: bare strata aliases and full Region
  alias pairs. Extend strata only by Region pairs × verified Urbano/Rural.
- `lsms_library/derivations.py`: typed registry/nullable Derivation contracts;
  own existing food/community registry callables establish hyphenated module path.
- `tests/test_guinea_bissau_cluster_features.py:128`: existing nine-region
  vocabulary test; only reserved function adapts to region part + native suffix.

## §3 Definitions & conventions in force
Inherit `.coder/ledger/STANDING.md` §3, `CLAUDE.md` sample/cluster identity,
canonical `lsms_library/data_info.yml` sample optional Derivation contract.
Own CONTENTS (full 833 lines read first, including all headings): “The
Guinea-Bissau EHCVM 2018/19 sample covers all regions with urban and rural areas
surveyed in all regions apart from the autonomous sector of Bissau (SAB) where
all respondents are from urban areas.” Its later planned-design appendix quotes
“La strate est la combinaison région/milieu de résidence.” Seventeen nonzero
allocations is table arithmetic, not a formally quoted count. Earlier region-only
advice/spelling-equality is retained historically, superseded by selected scope.
Merged1637 notes and1640 managed BIDs supply documentation, not native cards.

## §4 Invariants & assumptions
Inherit STANDING §4, especially attrs/id_converted and sanctioned IO. No native
or normal Country/Feature/sample data calls in this worker. Invented providers
only; private empty runtime/data, network namespace, BLAS/build1, n2/loadfile.
Missing/blank either operand → NA; unknown nonstring coding raises existing
StratumError. No coercion, fill, pruning, inferred exclusions, count goldens,
weight changes, shared helper/global schema/country module/2021 edits.
Preserve complete CONTENTS prefix and bare aliases/Region table. Registry scope
only2018-19; registry scope alone does not police explicit out-of-scope inputs.

## §5 Reuse decision
Reuse all shared812 construction/stamp/input functions without edits. Extend own
wave YAML/hooks and own registry. Extend only own strata aliases using full own
Region crossproduct with native Portuguese suffixes, contingent on verified card.
New own pure tests reuse actual df_data_grabber/column_mapping and invented source
providers, rather than a new pipeline/framework. Existing shared812 test edits
belong to deputy, including REGION_ONLY method and Togo Feature negative oracle.

## §6 Open questions
Native full label card pointer requested through dispatch QUESTIONS channel; tiny
zero-record metadata plan supplied to manager/deputy. This holds only alias/native
acceptance choices; source/pure construction remains authorized. Actual comparisons,
native-dependent reserved test, final footprint/full gates remain later stages.

### Phase 3 — verification
Pending frozen source and focused pure run. No actual data acceptance claimed.
