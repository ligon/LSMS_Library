# Prior-Art Ledger — GH #1482 and Site Q portion of #1485

Inherits [STANDING.md](STANDING.md), [857](857-crop-quantity-screen.md) and
[1259](1259-site-q-positive-reference.md). Search tier: ripgrep + git + AST;
GitNexus/cq tools unavailable. Base c780562b299bb100cdd006037ba424c8ad3264e9.

## §1 Task, restated

Extend read-time Site Q warnings/counts to plot_inputs.Quantity and the head
counts declared by livestock schemes. Never clip, mask, rescale, aggregate or
change returned data. #1485 TLU/fertilizer_rate are reserved to another worker.
Frozen task-context.json contains both open issue bodies and zero comments;
no merged Site Q work after #1493/#1436 already implements these extensions.

## §2 Existing machinery

| Symbol / source | Role / tests | Decision |
|---|---|---|
| quantity_audit._SCREENED_COLUMNS / _NEAR_ZERO_COLUMNS | Table/column dispatch, tests/test_quantity_screen.py and tests/test_gh1493_near_zero_screen.py | extend upper registry only |
| quantity_audit._axis / _AXES | Index or column aliases, categorical/nullable codes; tests/test_quantity_screen.py | extend livestock alias and new boundary axes |
| quantity_audit._cell_references / _group_quantiles | Positive-only p90, 30-row floor, K=100, one-native-unit reference floor; same tests plus #1259 | reuse arithmetic; supply constrained rungs only for new tables |
| quantity_audit.audit_quantities / check_quantities / quantity_reports | Report/warning/strict ledger; check returns same object | reuse |
| Country._finalize_result (country.py Site Q call) | Runs cold/warm after read; already excluded from fingerprints | unchanged |
| _build_registry._EXCLUDED_CALLABLES / _EXCLUDED_CONSTANTS | Audit entry points and ledger excluded; tests/test_quantity_screen.py::test_*fingerprint* | unchanged; measure base/tip metadata hashes |
| bench/feature_audit/README.md; scan.py | Flag not adjudicate; real Country builds outside this stage | read, no real audit execution |

All 16 livestock schemes enumerated before axes/columns selected: Benin,
Burkina_Faso, CotedIvoire, Ethiopia, EthiopiaRHS, GhanaLSS, GhanaSPS,
Guinea-Bissau, Malawi, Mali, Niger, Nigeria, Senegal, Tanzania, Togo, Uganda.
15 declare (t,i,animal); EthiopiaRHS (t,i) declares HerdValue/TLU only.
The head-count union is HeadCount, HeadAcquired, HeadSold,
HeadCount12MonthsAgo, HeadBornOrReceived, HeadEaten, HeadLostOrDied.
Monetary values, TLU and RaisedOrOwned are not head counts. 14 plot_inputs
schemes declare input, j/crop and u (u may be a payload column).

## §3 Definitions and conventions in force

STANDING §3/§4 and AGENTS.md Site Q govern. Canonical livestock column notes:
lsms_library/data_info.yml livestock; each stock/flow is its own column,
never pooled or substituted. Species are animal, not the crop axis.
Burkina record read in FULL including TODO/WAITING entries. Concrete anchors:
CONTENTS.org:1654 household × input, no parcel; :1678 unlabelled code21
history later ruled Arachide; :1808 purchase/gift units separate;
:2701 NPK has no grade, Tonne concentration unadjudicated. Scheme :293
records livestock is 2018-19-only, EHCVM formatter, s17 owned-species gate.
No source evidence newly obtained. All country-row counts in issue bodies or
historical docs are attributed, not re-measured.

## §4 Invariants and assumptions

- Same input object, full axes/domains, payload/missingness/dtypes/attrs remain.
- Existing screened tables retain their exact ladder and report structure.
- New livestock comparisons never drop animal to pool species, even when thin;
  absent animal means no species count judgement (EthiopiaRHS is herd totals).
- New screens preserve supplied country,t,period,season,visit and u boundaries;
  missing labels are groups, not invented labels. Plot inputs may relax crop,
  then input, only within those fixed boundaries; no unit conversion.
- No new lower-tail rule, constants, allowlist, sentinel or fertilizer correction.
- Pure read-time diagnostics: no cache-schema bump; verify metadata hashes
  source-free. No Country/Feature data getters, cache reads or raw reads authorized.
- Run hermetic tests under unshare -rn, false credential gate, private runtime
  roots, exact import assertions, BLAS/OMP/MKL=1, LSMS_BUILD_WORKERS=1,
  pytest -n2 --dist=loadfile; logs retained. No fixtures/baselines weakened.

## §5 Reuse decisions

Reuse Site Q's p90 arithmetic, ledger, warnings and strict lever. Extend alias
resolution and upper-column registry. Extend _cell_references with optional
rungs, keeping its default identical: livestock has a single same-species rung;
plot_inputs uses fixed boundaries + input + crop, then input, then boundaries.
This is necessary because the default coarse wave rung mixes species and units.
No new estimator; no transformation/data/config policy change.

## §6 Open questions / gates

Two-file source-only validation plan sent through the existing absolute
QUESTIONS_FROM_AGENT channel. No source grant yet. Census schema/label reach
cannot establish positive-cell support, real firing rates, reported Tonne
interpretation or whether Malawi head counts contain money. Corpus validation
and country corrections remain separate stages/decisions. Thin cells may be
unjudged; silence does not certify their values.

### Phase 3 verification

Pending implementation and finite hermetic/config/hash checks.

Coordinator steering supersedes native runtime execution: no further unshare
children in this worker. The initial source-free API audit already exited0
(after the preserved Poetry-absent make failure); hermetic runtime checks and
metadata census are prepared in the private evidence operator
`verify_source_free.sh` for a separate ROOT deterministic stage. No unit test
or data-stage execution is claimed at freeze.
