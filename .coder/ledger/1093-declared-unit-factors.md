# Prior-Art Ledger -- GH #1093: declared masses for crop transforms

**Search tier used:** ripgrep+git floor, function/call-site reads and tests.
No GitNexus query/impact MCP tool is callable in this session; the impact map below is the fallback.
Baseline: development `976fc840d12ba40f1602b7dff58b81548632b2a9`.
Inherits [STANDING.md](STANDING.md), especially sections 2-5; also reads the [#931 ledger](931-crop-u-kg.md).

## Section 1 / §1 Task, restated

Complete #931's declared-unit-mass propagation for `median_price_valuation`'s sold-quantity denominator, `seed_kg` and `nitrogen_kg`; attach the existing country-keyed `unit_kg` record to `plot_inputs`, as already done for `crop_production`.
Pin `yield_kg`'s existing explicit/default forwarding to `harvest_kg`.
This changes read-time transforms, not reported quantities, product identities, valuation-source selection or stored table definitions.
GH #1093 has no comments or additional rulings as read on 2026-10-20; the user approved this issue explicitly through the parent.

## Section 2 / §2 Existing machinery

| Symbol | Anchor on baseline | Existing tests | Decision |
|--------|--------------------|----------------|----------|
| `Country._unit_kg_factors` | country.py:2524 | test_gh919_country_qualified_units.py | Reuse composition of merged raw/canonical unit names and declared masses; no transform-side Country import |
| `_declared_kg_factors` | transformations.py:4121 | test_crop_kg_factor.py:646 onward | Reuse per-row flat/nested maps, explicit-map/attrs precedence, country matching, invalid-factor handling |
| `_kg_factor_series` | transformations.py:3538 | test_crop_kg_factor.py and agriculture transform tests | Reuse existing metric/parser/inference fallback and index/column u handling |
| `_unit_sentinel_mask` | transformations.py, called by harvest factors at :4465 | test_crop_kg_factor.py:746 | Reuse refusal of missing-unit sentinel even if a map names it |
| `harvest_kg_factors` | transformations.py:4192, overlay :4475 | test_crop_kg_factor.py:646-790 | Preserve reported/shipped/survey-median/metric/inferred ordering and explicit-empty override |
| `yield_kg` | transformations.py:4797, forward :4870 | test_crop_kg_factor.py plus yield tests | Existing implementation already forwards unit_kg; add discriminating tests |
| `nitrogen_kg` / `seed_kg` | transformations.py:5333 / :5453 | test_867_nitrogen_content.py, agriculture transforms | Extend signature with unit_kg=None and use declared overlay over existing fallback |
| `median_price_valuation` | transformations.py:7069, sold kg :7271 | test_median_price_valuation.py | Extend unit_kg=None for internally constructed sold kg; explicit kg_qty stays authoritative |
| `Country._finalize_result` | country.py:4028 | test_crop_kg_factor.py and country/Feature tests | Extend metadata stamp from crop_production to plot_inputs |
| `feature._ATTRS_CARRIERS['unit_kg']` / `merge_unit_kg_attrs` | feature.py:137 / transformations.py:4102 | test_feature_attrs_carriers.py | Reuse existing carrier; prove plot_inputs records survive pooling |
| Build fingerprint machinery | _build_registry.py:527/:547; country.py:4288 | test_build_transform_hash.py | Measure actual all-country table hashes before/after; do not assert zero from read-time intent |

Impact map: `_kg_factor_series` currently serves harvest, nitrogen, seed and sold-kg valuation only.
Do not make it implicitly consume attrs globally: harvest already applies the declaration itself and must retain its independent audit columns.
A small composition helper used by the three missing consumers may combine existing fallback, `_declared_kg_factors` and sentinel refusal without changing harvest's richer ladder.

## Section 3 / §3 Definitions and conventions

- Canonical table/index definitions: `lsms_library/data_info.yml`, per STANDING section 3; do not hardcode new schema.
- Declared factor precedence and origin: `_declared_kg_factors` docstring, transformations.py:4121; `None` reads attrs, `{}` explicitly declares nothing, nested mappings select by country level/column/attrs country/single-entry fallback.
- Declaration outranks `KNOWN_METRIC` and explicit metric label parsing, but harvest reported/shipped layers retain their established order: transformations.py:4467 and test_crop_kg_factor.py:707/:757.
- Ethiopia idiosyncrasy, read before country source: CONTENTS.org:1674 explains the now-declared 100 kg Quintal and independent external answer key; the shipped table is explicit analyst input, never automatically loaded.
- Ethiopia plot_inputs quantities are already kg, while organic/pesticide rows carry `Applied` without quantity (CONTENTS.org:1354); missing amount is not measured zero.
- Ethiopia `fertilizer_rate` requires `on='plot'`; default parcel mismatch is documented at CONTENTS.org:1411 and is separate #1110, not authorized for repair here.
- Ethiopia documentation review includes outline and live TODO/caveat sections: FCT remains TODO; shipped local-container factors have no independent numerical check; W1 has no shipped factor file and no Quintal rows.

## Section 4 / §4 Invariants and assumptions

- Inherit sanctioned IO, path routing, metadata preservation and pandas rules from STANDING section 4.
- Explicit `{}` must suppress attrs; invalid factors remain unavailable; never guess between two countries' declarations.
- `Unknown` or genuinely missing u must not acquire a unit mass from declarations.
- No new Country/config loader is introduced inside transformations; Country's existing composition stamps metadata, and loss of metadata leaves fallback behavior as today.
- Explicit `kg_qty` in valuation remains the sold kg denominator; unit_kg affects only the native-quantity conversion route.
- No declaration must reproduce existing transform values, indexes and dtypes exactly.
- No change to crop factor layer order, nitrogen shares/product matching, missing-quantity aggregation policy or parcel/plot semantics.
- Existing aggregation grain is preserved; repairing standalone transforms' cross-country grouping behavior, if discovered, is a separate residual to report.
- Changing transformations.py can move GhanaLSS nutrition import fingerprints; measure country/table hashes and identify actual affected builds.
- Runtime evidence uses private LSMS_DATA_DIR roots for base and patch, sharing only L1; output artifacts contain aggregate diagnostics, not household microdata.

## Section 5 / §5 Reuse decisions and verification plan

| Quantity | Decision | Reason |
|----------|----------|--------|
| Declared per-row mass | Reuse `_declared_kg_factors` | Tested flat/per-country/attrs resolution already exists |
| Fallback per-row mass | Reuse `_kg_factor_series` | Preserve current metric/parser/inferred behavior |
| Combining declaration and fallback | Extend through a small shared composition helper for three missing consumers | Harvest's own overlay also creates audit layers and is not a replacement for the simpler consumers |
| Country declaration metadata | Extend existing finalize stamp; reuse Feature carrier | No second loader or carrier |
| Yield forwarding | Reuse implementation, add regression | Explicit forwarding already exists |

Tests: exact one-Quintal seed 100 kg and Urea 46 kg N; u index/column; flat/nested attrs with country axis and explicit override including {}; metric override and sentinel refusal; no-declaration equality; nonmetric sold-quantity price pool/inferred price; explicit kg_qty; Country/Feature attrs; yield argument/default forwarding.
Run focused existing factor/valuation/attrs/nitrogen tests, then relevant broader tests within CPU24-27.
Build Ethiopia crop_production and plot_inputs cold from untouched base and patch in independent roots; report price-pool coverage/valuation and seed/N totals, retaining documented limitations.
Measure all actual country-table hashes at base/patch in separate processes; report rather than assume invalidation scope.

## Section 6 / §6 Open questions surfaced to the parent

No human semantic decision is required within approved scope.
Interpretation to confirm before implementation: "loader fallback" means reuse Country._unit_kg_factors metadata composition plus existing _kg_factor_series fallback, not implicitly importing/loading a Country when attrs is absent; #931 ledger explicitly names that as a hazard.
The proposed shared composition helper will keep harvest audit/layer behavior unchanged; no product-identity or valuation-source choice is made.
Await parent authorization after it surfaces this ledger.

### Phase 3 -- verification

Pending implementation and tests; no source change made yet.
