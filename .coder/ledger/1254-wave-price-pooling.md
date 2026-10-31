# #1254: explicit cross-wave median-price pooling

## §1 Task and ruling

Implement the settled public read-path API rule, not a new estimator. Maintainer
comment [6367366848](https://github.com/ligon/LSMS_Library/issues/1254#issuecomment-6367366848)
requires t in every grouping whenever carried, with explicit opt-in to pooling,
and measured fingerprint cost. Source base f952a32e646bcebe94b4c400ea30edef02b172bb.

## §2 Existing machinery and precedents

Search: Git/rg/stdlib AST (no data access). Reuse median_price_valuation and its
weighted/geographic/national ladder, sold-unit factor resolver and attrs exactly.
The internal _median_price_rung already constructs a RangeIndex and passes
('_t','_j','_u'); tests/test_own_production_valuation.py::test_waves_do_not_pool
pins that internal path. tests/test_median_price_valuation.py covers single-wave
and custom/weighted medians; tests/test_gh1093_declared_unit_consumers.py pins
sold-quantity conversion. Existing _series resolves index before columns; keep
that priority for ambiguous t, and existing duplicate-level errors, without
normalizing input. tests/test_build_transform_hash.py and framework_imports_fingerprint
are fingerprint-cost authorities, not a promise that read-path means zero cost.

## §3 Definitions

Inherit STANDING.md/CLAUDE.md and 585-own-production-valuation.md §4: t must
stratify every geography rung AND unconditional national fallback to avoid
pooling incompatible-year local currency. Same-rule explicit pooling is optional,
not a wave/currency conversion. Ethiopia CONTENTS retrospective window, Panel I/II,
SalesValue, source-grain, missing-answer and #1357 exclusions remain unchanged.

## §4 Invariants

Keep returned row/index order, sold-unit rules, threshold count, weighting,
positive-price screen, zero/missing policy, quantity multiplication and conversion
attrs. No source/decoder/cache/scientific-table changes. A frame with no carried
t and all existing single-wave results retain behavior. Explicit caller t in
item_keys remains an explicit grouping even with pooling enabled; the new flag
only disables automatic t inclusion. No duplication of an existing t key.

## §5 Decision

Extend keyword-only API with pool_waves=False. If t is carried as a named index
level or column and automatic grouping is enabled, prepend t unless already in
item_keys. pool_waves=True explicitly disables that automatic step without
removing any requested key. Keep private internal _t/_j/_u machinery untouched.
Test invented independently priced two-wave cells and below-threshold national
fallback, t columns/index/ambiguity/duplicates, custom keys, explicit opt-in,
no-t/single-wave and weighted paths; preserve exact source frames. Bound private
roots/import identity/thread limits and measure source/import fingerprint impact.

## §6 Acceptance

Root accepted this implementation and finite synthetic/metadata validation scope.
Independent red-team, configured CI and fresh conflict/exact-head integration
remain. No raw-data reconstruction or extra policy approval is needed. No test
results or zero-hash-cost claim is made before actual verification.

### Verified finite result

Six default-path cases fail on unchanged source (505 versus10/1000), then the
implementation passes202 targeted synthetic cases with eight data cases
explicitly deselected. Final additions strengthen custom-key perturbation and
default duplicate-t coverage:21 new regressions plus the existing strengthened
build-closure guard pass in a separate narrow run. All runs assert imported
checkout and use approved3.11.6/private roots/one numeric thread/one CPU; no
source/payload/network operations were permitted. The initial import-only guard
refusal for libmagic's ldconfig lookup is preserved separately, before tests.

Fresh baseline/candidate metadata processes show identical four registered build
fingerprint scopes, all16 entry points and the framework-import closure over747
tracked country Python sources; median_price_valuation is unreachable from the
registered build closure. These are measured fingerprint components, not549
actual L2 table/wave hash calls. No Country/getter/build/re-warm ran. See the
scoped public report and external bound logs/JSON for precise runtime provenance.
