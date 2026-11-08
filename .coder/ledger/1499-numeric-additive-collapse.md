# Prior-Art Ledger — GH1499 numeric operands before additive collapse

Search tier: git + ripgrep + stdlib AST floor. Local author base: actual GitHub development5a06d85cd02237cc515b0cda57012a9cab3d5e11 (PR1632). Critical reducer/selector/audit/parser ASTs remain exact to609b; this is not full-tree identity. Recovered remote7d74438 is ledger-only/BLOCKED before any reducer/product/test execution; its files and publication failure remain immutable. Current source admission and finite plan live in the task's external1499-numeric-additive-local report packet.

## §1 Task, restated

USER explicitly chose **convert valid numeric strings before aggregation** on2026-11-08. Repair both actual module-level `_normalize_dataframe_index` and Feature `_collapse_duplicate_index`, before declared additive sums/necessary existing Price division. Reject malformed nonmissing tokens, including empty/whitespace strings that a parser may turn into NA; genuine NA remains NA. No scientific reducer, factor, weight, grain, unit, row/ID or eligibility change. Root owns publication and release; this author uses invented Alpha/Beta frames only.

## §2 Existing machinery

| Symbol / source | Existing tests | Reuse decision |
|---|---|---|
| feature._ADDITIVE_MEASURE_COLUMNS (Quantity,Expenditure forfood;Value forassets) | tests/test_assets_additive.py | reuse unchanged; no new additive field/table |
| country._normalize_dataframe_index; feature._collapse_duplicate_index | assets/gh323/gh871 tests | extend only checked consumed-operand view |
| country.most_complete_row; _audit_index_collapse/_record_grain_report | tests/test_gh871_most_complete_row.py; gh323 contract/explicit reducers | preserve original inline selection/audit/order |
| cython groupby.sum(min_count=1) | assets/gh871 | preserve algorithm/order/all-NA behavior |
| Price=Expenditure/Quantity.where(Quantity!=0) | same tests | preserve; no Price average/weighting exists here |
| derivations.union_keys_by_group | gh871 | preserve additive union/nonadditive selected provenance |
| local_tools._to_numeric | static source | unsuitable strict contract: catches ValueError/TypeError and returns malformed input |
| schema/canonical dtype finalizers | schema tests | do not move/call/change: permissive and too late for this defect |

Read STANDING.md/CLAUDE/canonical data_info, current grain policy and older323 ledger with chronology: current declared additive exceptions supersede the historical blanket no-aggregation wording. No strict pre-sum/no-new-NA helper was found. Corrected independent peer26 matrix is PREP, not executed evidence; original24 malformed→NA assumption was explicitly superseded by ROOT-COERCION-CORRECTION.md. Source/API examples in skills give no source grant here.

## §3 Definitions & conventions in force

Inherit STANDING.md §§3–4 and CLAUDE Grain Collapse/Pandas3.0/Derived Values; data_info.yml is canonical schema authority. Only existing declared additive arithmetic operands may be prepared. An overwritten or selected Price, IDs/index/unit/categorical codes/descriptive columns/unique paths/nonadditive tables have no conversion authority. Numeric representation repair does not authorize a new parser grammar, positivity/finite filter, NA token, calendar, grouping key or estimator.

## §4 Invariants

- Preserve numeric values/dtypes/reduction order and existing exceptions on healthy numeric operands. Object-carried numeric-only values must not be unnecessarily float-cast; validate via pandas if needed but keep original numeric representation.
- Preserve min_count1/genuine missing masks; no newly missing cell from a nonmissing input. Empty/whitespace/new-NA text must fail with column/context, without including survey values in public error text.
- Reuse pandas conversion, not custom locale/unit/thousands grammar. Check parser-produced NA against original missingness; errors=raise alone is insufficient.
- Keep original audit/observed-row selection/provenance/sum/Price order. A checked numeric view can be inserted immediately before consumed sums, after existing audit/selection, retaining warning/strict ordering.
- Preserve full keys/rows/index/order/labels/units/attrs/input immutability on success/refusal. No null-key dropna, complete-row ranking, warning/severity or provenance policy change.
- One narrowly shared private helper in feature.py is proposed; country already lazily imports its additive map, so extend that existing dependency rather than create a cycle. No full-table/global coercion.
- country normalizer is an unscoped @build_transform: broad build/cache invalidation must be measured by ROOT, not avoided through exclusions. No shared rewarm.
- Source-free finite scope: exact MAIN checkout/config/import identity, fresh private empty nonsymlink L1/data/config before import, falsecreds/unshare/thread1/pytest2loadfile, original13 refusal file byte-exact. No real Country/Feature/Wave constructor/API/source/getter/reader/writer/Make/warm/DVC/client/credentials/network access.
- Runtime/temp/pytest outputs remain outside published evidence. Regular copied proofs only. Old native/import/publication failures stay unchanged; no remote restart.

## §5 Reuse decision and finite plan

Reuse all policy/map/audit/selector/provenance/numeric reducer/Price machinery. New narrow helper is justified because existing parser swallows malformed data and finalizers blank it after the damaging sum. Validate nonnumeric operands with pandas; refuse newly missing values and parsing failures with generic actionable column context. Preserve numeric passthrough; convert only consumed textual representations (including typed all-missing text where necessary for arithmetic). No hand parser.

First ONE unchanged-current baseline demonstration through both ACTUAL reducers (with/withoutPrice ×string/numeric), retaining actual concatenation/TypeError and numeric-control behavior; remote attempted0reducers, so this is not replay. Then freeze minimal implementation/new hermetic test selection before ONE affected+new candidate cohort, aligned to peer26 contract plus typed numeric passthrough and error/input controls, with justified existing source-free companions and one staticAPI audit. Baseline/candidate runtime remain UNRUN until the concrete local operator/bootstrap/selection is frozen and ROOT-reviewed. No source, metadata census or fullsuite in this finite author stage.

## §6 Open runtime gate for ROOT

The original strict pre-import containment denied python-magic's /dev/null before qualified ldconfig. ROOT conditionally approved that known metadata discovery only after the native worker was already terminal; it was NOT applied there. This is a new local context. Freeze exact qualified max2magic ldconfig startup plus unchanged source/13guards before any local baseline. Original strict socket boundary also needs explicit treatment of the already-diagnosed installed urllib3 loopback capability probe if that import is required; do not silently allow a bind or replay known failures. Concrete frozen source bindings/guard delta and finite argv follow externally for ROOT review. No product/test execution or scientific decision is implied by this ledger commit.

### Phase3 — pending

Grounding/critical-AST admission complete; no local reducer/baseline/candidate/runtime execution or product edits yet. Full current integration/independent review/hashscope/fullCI/GitHub remain separate ROOT-owned gates. Previous remote native0/taskBLOCKED with supervisor74 publication timeout is preserved, not called a product success.

### Phase2 review correction —2026-11-08

Frozen5bfc0932 remains preserved. Independent source-free72 controls passed, then four separately authorized precision edges produced2PASS/2FAIL: BOTH actual reducers parsed a native Decimal together with a text cell, returning float64 instead of the native numeric counterpart's object/Decimal carrier. The failed assertions stopped at dtype; no further scalar-difference measurement is inferred. Reviewer packet reports/review-1499-numeric-additive-20261108 records one shared-helper P2, not two separate bugs. ROOT authorized this preservation correction within §4, without a new scientific/parser/Decimal policy.

The helper now parses only textual cells when an object carrier also contains nonmissing native objects, and reassembles their unchanged original values with the converted text using ordinary pandas Series inference. Text-only/nullable-text parsing, strict malformed/new-NA refusal, original grouping/min_count1/selection/audit/Derivation/Price order remain. Fourteen new author cases cover both actual reducers, Quantity/Expenditure Decimal and large-native-integer counterpart equality, real NA, and malformed/empty text alongside a native Decimal. All original108 author cases/eight companions and failed independent controls stay unchanged. ROOT authorized ONE corrected130-case selection (116 never executed plus14 new); no baseline8 replay.

Actual immutable23c2/nonledger5a baseline8 is CLOSED/PASS and source-free; no candidate test has executed yet. Startup failures at import-time lazy DVC initialization, class-definition suites and xdist OS/platform metadata are preserved externally, with exact pinned import-only/startup qualifications and finite positive/negative controls. Original13 runtime source refusals stay byte-exact; no source/getter/API/cache/Make/warm campaign is authorized. The current frozen preflight/operator is external; source-free author and independent corrected-head verification, strictAPI, ROOT metadata/fullCI/current integration remain pending. The original remote task/publication failure remains separate.
