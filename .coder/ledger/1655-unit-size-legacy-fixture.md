# GH1655 CI — explicit legacy availability in unit-size error controls

## §1 Scope and original evidence
Test-only correction from a2345c214e3298dc06f6d254af056ce01f104fb2.
Original run42604104679 is FAILED: two positive fallback cases for food_prices
and food_quantities;13174PASS/5649SKIP/4XFAIL,676.69s. Preserve that result.
GH218 correctly removed dead Togo Make recipes, invalidating this fixture's
assumption that real Togo still supplies those legacy routes. No production,
config, Makefile, API, hash, scientific or source-data change is authorized.

## §2 Prior art / reuse
Search tier: git,rg,source/AST floor. Inherit STANDING.md and the full read of
218-retired-make-residual.md plus218-retired-route-remote-task.md. No dedicated
1532 ledger exists in this pinned tree; its executable contract is explicit in
Country generated dispatch and tests/test_failure_is_not_absence.py §1532/1516,
plus test_1081_derived_categorical_errors.py generic fallback controls.
Reuse Country._legacy_can_build(method_name,waves), _warn_legacy_fallback and
existing aggregation stub. Keep UnitSizeLabelError outside generic catches.
No new helper or dispatch policy is needed.

## §3 Grounding and definitions
Personally read all902 Togo CONTENTS lines including TODO/WAITING/LOGBOOK text;
SHA256b6c455b34ecdc230248779d3af553211b51cdad5bf94aabae4c809a995b08db0.
Relevant quirk: native2018 sources live in Data1, while Data contains undocumented
_forEthan extracts. The note calls the retired food script "dead by the same
test" and its Make prerequisites absent. This contradicts the fixture's claim
"a bare ValueError still tries the legacy build, which Togo has" after cleanup;
report the contradiction without declaring country documentation stale.
The unresolved source quote "except Lome Commune where all respondents are from
rural areas" versus GrandLome98urban/0rural remains untouched. No geographic or
unit semantics are adjudicated. STANDING §3 derived-table definitions apply.

## §4 Invariants
Keep all named-error passthrough assertions and positive LegacyFallbackWarning,
original ValueError match, fallback RuntimeError note and call-order assertions.
Stub legacy availability only for the positive table. Pair with explicit absent
legacy route: original error identity, no legacy warning and only source attempt.
No weakening/removing old assertions/fixtures/skips/tolerances. All other tests
and every product/config/Make byte remain exacta234. Private config/data/L1/DVC,
false credentials, MAIN/PYTHONPATH/countries identity/unshare/thread1/2loadfile.
No actual sources/API/producer/build/cache or census replay.

## §5 Verification plan
ONE whole tests/test_ehcvm_unit_size.py plus existing relevant1532 dispatch and
legacy fallback companions in tests/test_failure_is_not_absence.py,
tests/test_1081_derived_categorical_errors.py and the bare-call legacy positive
in tests/test_food_prices_units_kwarg.py. Discover exact selectors before running;
no old full suite/census. Save actual log/JUnit/return/source identity and preserve
any failure. Final ledger commit separate; ROOT/1452 independent review follows.

## §6 Status
Ledger-first checkpoint only; correction and verification UNRUN. ROOT already
authorized the concrete correction and finite controls; no further scope choice.

## Phase3 — bounded correction verified
Ledger-firste461252e4 preceded runtime d4b3931bde44e10e6adf1bd1511ffbea19009f66.
Only the positive fixture's availability stub and obsolete docstring changed;
all original positive and named-error assertions remain. Three new absent-route
cases require original bare ValueError identity, source-only call order, no
legacy warning/build and no fallback note. No library/config/Make byte changed.

ONE private MAIN/unshare/false-credential/empty-L1/thread1/two-loadfile actor
ran the complete affected module plus the exact selected existing1532 dispatch,
legacy success/failure,1081generic fallback and bare-price companions:
217PASS,0FAIL/ERROR/SKIP,46.74s pytest/53.970s outer,exit0. Runtime head and clean
worktree identical before/after. Private data files0; privateconfig/config.yml
was intentionally created and is recorded rather than claimed absent. Two
upstream google.api_core Python-version FutureWarnings remain. No baseline,
fullsuite, metadata census, actual data/API/producer or cache rewarm replay.
Original2FAIL/13174PASS/5649SKIP/4XFAIL run remains FAILED and immutable.

Independent source review passed exactd4b3931b: all old assertions/othermethod
ASTs and all other objects conserved. No independent focused-cohort replay.
Evidence: reports/1655-unit-size-legacy-fixture-repair-20261109 in the private
isa-sm-20261103 burst; focused.xml/log,PRELAUNCH,IDENTITY,RECEIPT and final
object proof/manifest. Independent review: review-1655-unit-size-legacy-fixture-
20261109. This final ledger-only child preserves every runtime/test/config byte
of the measured d4 head. ROOT owns publication/composition and fresh exact-head
configured CI; this focused run does not relabel the failed full run green.
