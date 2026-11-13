# Prior-Art Ledger DRAFT - GH1091 Niger community builder retirement

Sue, 2026-10-21. Read-only diagnosis at d602f8d63520e521a730f85551b366c4d45d9a35.
No tracked change or implementation is authorized. Search tier: ripgrep + git.

## Section 1: Task
Diagnose retirement of the concatenate-only Niger country community_prices builder, preserving the accepted variety/visit/obs grain, source eligibility, duplicate-entry reductions and GH1088 exact quantity missing codes. Measure ordinary default and Make paths, including supported and absent wave subsets, before proposing deletions.

## Section 2: Existing machinery
- Niger/_/community_prices.py:26-45 loops four candidate paths, concatenates existing wave frames, asserts unique grain and writes country output. No source transformation or id_walk.
- Niger/_/Makefile:105-118 declares COMMUNITY_PRICE_WAVES, an active country target invoking two wave scripts and concatenator, and an independent wave pattern. COUNTRY, LSMS_DATA_ROOT and VAR_DIR serve other targets.
- Country._has_country_builder (country.py:4893) recognizes scripts first, otherwise fresh empty-root make -n; tested in test_gh1039_make_backend_no_country_rule.py.
- Country._aggregate_wave_data/load_from_waves (country.py:4977/5249) provides authoritative wave aggregation; make routing at5800-5850; GH1039/1099 tests.
- Country._assert_built_required_columns (country.py:4817) enforces required Price/Quantity on script paths after finalization. materialize: make remains and keeps this gate.
- Niger/_/niger.py:_community_price_triples/_finish_community_prices, two wave scripts: accepted native source reshape, filters and duplicate-entry policy, covered by test_community_prices_variety.py and test_niger_u_sentinel.py.
- GH1088 ledger: exact quantity codes, 76 missing quantities; no Price/key/grain change.
- local_tools.get_dataframe/to_parquet, normal hash/coverage read gates; STANDING sections2-4.
- CLI _load_table/_materialize (cli.py:287/309) calls ordinary Country API then serializes separately; output artifacts are not raw L2 cache frames.
- Feature assembly and bench/feature_audit/scan.py phases1/2 provide argument-matched diagnostics.

## Section 3: Definitions in force
Inherit STANDING.md sections3-4, CLAUDE.md and canonical data_info.yml. GH788 maintainer comments6244122374/6259387154 settle framework authority. GH840/1086 accepts (t,v,j,u,variety,visit,obs); no grain decision remains. Niger units dictionary uses Original Label, not Code. ECVMA community v is native grappe, not household i; EHCVM grappe/vague is a cross-section. 2014 household EXTENSION is not relevant to the community index.

## Section 4: Invariants and contradictions
Retain all46043 price readings if remeasurement confirms them; Price, Quantity, Description, axes/order/dtypes/nulls and recursively typed full attrs require strict exact checks. Preserve 15+15 nonrepeat second-record slots and all1088 masks. No source/science/eligibility/reducer/unit or framework change. CONTENTS:1185 says old no2018 cluster-file claims are false; current Make105-109 still says only ECVMA has an EA-level module. Current docstring acknowledges2018 is present but unwired, yet its except comment still says EHCVM waves have no EA-level module. CONTENTS:1299-1303 still calls nonrepeat representation a maintainer decision, while accepted1086 context explicitly settles the three-level design; quote both rather than reopen.

## Section 5: Reuse decision
Reuse authoritative framework wave aggregation, source scripts, IO/read gates, required-column enforcement and existing absence contract. Proposed implementation, subject to measured diagnosis and root approval: remove country concatenator and its active country target plus sole-use COMMUNITY_PRICE_WAVES, preserve wave pattern and all shared variables. Do not edit data_scheme or source transforms. Predicted hash impact follows existing whole-Makefile/country-script hash inputs and must be measured only on an actual later candidate; no candidate result is claimed here.

## Section 6: Open scope
Measure exact wrong-wave behavior under Make, cold versus populated absent requests, raw/native versus final public attrs, CLI serialization and both Feature currency defaults. Establish comparator only from already-held hash/coverage/provenance-gated cache; otherwise report precise gap. Tanzania remains1091 residual; Mali is independently ongoing at its reviewed source pin. No new human grain/science decision is required for this Niger retirement.
