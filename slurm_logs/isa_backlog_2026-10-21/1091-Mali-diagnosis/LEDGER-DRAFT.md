# Prior-Art Ledger - GH1091 Mali community builder retirement

DIAGNOSIS ONLY at aba2badfe2f701a8b3793f1243fd8ed4fce18a53.
This private draft is not a tracked implementation or approval.
Search tier: ripgrep/git floor and direct Python/Make route inspection.
Inherits `.coder/ledger/STANDING.md`, `CLAUDE.md`, canonical `data_info.yml`
and `.coder/ledger/822-mali-community-price-readings.md`.

## §1 Task, restated

Retire only Mali's concatenate-only country community_prices builder in
favor of existing framework wave aggregation, under settled #788 authority.
Base on reviewed #822/PR1180, preserving155931 rows and native obs1/2/3;
do not use f8's known incomplete40218-row result as the preservation oracle.
Niger and Tanzania remain separate #1091 residual scopes.

## §2 Existing machinery

| Machinery | Source anchor | Existing controls | Decision |
|---|---|---|---|
| Country concatenator | Mali/_/community_prices.py:21-39 | actual Make cold/warm diagnosis | retire; no scientific operation exists |
| Wave rule | Mali/_/Makefile:102-103 | actual default/Make source builds | retain byte-for-byte |
| Country var rule | Mali/_/Makefile:105-106 | fresh-empty-root make -n | retire only this rule/recipe |
| Native slot assembly | Mali/2014-15/_/community_prices.py:44-91 | test_mali_community_price_readings.py and source-backed822 proof | reuse unchanged |
| Finalizer/i==v shim | Mali/_/mali.py:825-879 | test_community_prices_variety.py and822 controls | reuse unchanged |
| _has_country_builder | country.py:4893 | test_gh1039_make_backend_no_country_rule.py | reuse actual predicate; no framework edit |
| load_from_waves/absent_subset_frame | country.py:5008/:5249 |1039/1099 behavior, actual subset probes | reuse authoritative aggregation |
| Required-column gate | country.py:5848 and _is_script_path:4867 | test_gh808_script_path_predicate.py | preserve materialize:make and wave script |
| CLI materialize | cli.py:_load_table/_materialize/materialize | existing CLI tests | calls Country; no second concatenator |
| IO/cache policy | STANDING.md §2/§4 | source/copy/current metadata gates | sanctioned get_dataframe/get_data_file/to_parquet only |

Precedents: #1021/merge5b64a7796 removes dead derived scripts/rules;
#1026/bad9b5dc7 removes Tanzania food_acquired.py/country rule while retaining
its wave pattern; #808/d68f4b193 demonstrates dependency cleanup and retained
script-path schema guard. These establish a method, not another country's
measured source semantics.

## §3 Definitions in force

- #788 comments6244122374/6259387154: framework authoritative, one builder per
  table, one country re-warm per retirement. This is settled, not a decision
  request. #840/merged1086 accepted variety/visit/obs; do not reopen the grain.
- Mali CONTENTS:1248-1268 and822 ledger: native obs1/2/3, variety CODE,
  passage visit,155931 eligible rows, old obs3 exactly40218.
- CONTENTS:801: Quantity is Poids(KG), Price is the lot quote. No new PDF read.
- Scheme:329-383 declares seven public levels, optional Description and
  materialize:make; native wave shim i==v is deliberately dropped by framework.

## §4 Invariants and baseline caveats

- STANDING.md §4 sanctioned IO, pre-finalize cache and id_converted rules.
- Preserve all source rows, quantities/prices, units, descriptions, variety,
  passage, obs; no reducer/index/mapping/source-eligibility change.
- All-wave and supported2014-15 public values must match the complete reviewed
  822 oracle, in actual order with exact axes/dtypes/null masks and logical attrs.
- Existing Make public output lacks only id_converted=True relative to default.
  This is a measured pre-existing attrs difference, not normalized equivalence.
  Framework-authoritative expected attrs are the DEFAULT full attrs; a later
  Make route change must report the explicit id_converted addition.
- Default COUNTRY raw has seven levels/id_converted=True/current hash/coverage;
  Make's COUNTRY script raw has eight shim levels/attrs{}/no hash or coverage.
  Raw-layer equality is scoped honestly; do not declare these complete frames
  equal after dropping metadata or silently stripping the shim.
- Unsupported strict subsets must return declared empty frames; baseline Make
  may return unrelated2014-15 data. Never require preserving that defect.
- Current Make backend can write script-produced COUNTRY caches. After country
  retirement its framework wave route should write NO COUNTRY cache, while
  legitimately reading/writing wave caches. It is not a cache bypass.

## §5 Reuse decision

Delete duplicated aggregation and reuse the existing framework. No new helper,
generic var recipe, manual stamp, independent CLI concatenator or framework edit.
Retain materialize:make because the wave script remains the source builder.
No data_scheme edit is required; comments should be corrected only in the
Makefile already being edited and in CONTENTS, not unrelated stale prose.

## §6 Boundary approval needed

Root must read diagnosis before implementation. The scientific authority and
grain are settled. The proposed implementation contract explicitly distinguishes
unchanged payloads from corrected Make subset handling and authoritative attrs.
There is no working candidate, measured postchange hash delta or completed fix.

### Phase 3 diagnosis verification

OK (sections2-5):12 normal empty-target cold requests and6 fresh-process warm requests preserve complete native frames;18 native/18 raw country comparisons are exact against their separately identified822 layer oracles.
OK (sections3-4): supported public payload/full DEFAULTattrs are exact822; baseline Make differs solely by absent id_converted=True, explicitly retained as a finding.
CONTRADICTION (section4): existing Make serves155931 unrelated2014-15 rows for every unsupported strict request and the combined subset; no product equivalence is accepted.
OK (sections2/5): actual fresh-empty Make rule probe is positive, all has no community dependency, and both deletion/removal are necessary to select existing wave aggregation.
OK (sections3-4):18 source-backed native frames retain obs1=66699/obs2=49014/obs3=40218 and passage counts66159/89772.
OK (section4): both actual Feature argument variants preserve all payload/common recursively typed attrs, with sole id_converted delta visible; actual CLI forwarding reproduces supported/default and unsupported/Make behavior.
Postchange tests,549/39inventory and independent review remain FUTURE gates; protection/quiescence receipts accompany the final read-only release.
Any future tracked ledger must be committed FIRST on a separately approved
focused writer lane based on reviewed822, before deleting the builder/rule.
