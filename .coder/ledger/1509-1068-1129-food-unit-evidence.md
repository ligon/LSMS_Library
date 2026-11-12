# Prior-Art Ledger — GH #1509 + #1068 + #1129 (food unit evidence)

> Per-task ledger. Inherits `STANDING.md` §2–§4 (`_FOOD_DERIVED`, derived
> tables, `units=`), `CLAUDE.md` "Derived Tables" / GH #850 / #1487 / #1450 /
> "None of this invalidates a cache", and `lsms_library/data_info.yml`
> (`food_acquired: (t, v, i, j, u, s)`).

**Search tier used:** ripgrep + git floor (GitNexus MCP failed to connect), plus
an import-closure probe over every country/wave `.py`
(`_build_registry._closure_parts`, the same walk `framework_imports_fingerprint`
does).

## §1 Task, restated

Two rulings by @ligon, 2026-11-12, both read-time code in
`lsms_library/transformations.py`.

- **#1509, "all purchase rows".** `food_kg_factors`' price-ratio inference (the
  `item_unit` / `item_unit_tight` / `unit` rungs, both `conversion_to_kgs` arms,
  the #1450 size pool and the #1452 tableless estimate) reads only rows whose `s`
  is exactly `'purchased'`.  Self-valued `produced` / `inkind` rows no longer
  enter the estimate; every row is still served.  Cambodia's purchased quantity
  is an allocation (`total x spent/(spent+obtained)`) and is admitted anyway; that
  is recorded as the `allocated-quantity` caveat.  Not selected: independently
  observed purchase prices only; keeping the pooling.  No whole-wave or source-less
  fallback (the 2026-11-07 comparison's "unapproved fallback" arm stays unapproved).
- **#1068 + #1129, "drop and count".** `food_prices(units='unitvalue')` drops rows
  on a currency-denominated unit (`_CURRENCY_DENOMINATED_UNITS`, `u='Value'`) and
  on a missing unit (`U_UNKNOWN`), tallied in `attrs['price_rows_dropped']`
  (`currency`, `unknown`).  `food_quantities(units='units')` drops the missing-unit
  rows and tallies them in `attrs['quantity_rows_dropped']`.

## §2 Existing machinery (this task's area)

| symbol | path (development `e5fdd2fa8`) | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `food_kg_factors` | `transformations.py:2427` | per-row kg ladder, `KgFactorSource` partition | `tests/test_food_kg_inference.py`, `test_food_kg_size_pool.py`, `test_gh1452_older_rows.py`, `test_gh1487_kg_subset_is_a_slice.py` | extend (one filter) |
| `conversion_to_kgs` / `_kg_inference_frame` | `:1178` / `:1120` | price-ratio estimator, both arms | `test_food_kg_inference.py` | reuse unchanged (also reached by the crop ladder via `_get_kg_factors`) |
| `_pooled_kg_rungs`, `_inferred_kg_rungs`, `_size_pooled_frame`, `_rows_older_than_their_table` | `:2362`, `:2239`, `:2173`, `:2389` | rungs, size pool, #1452 tableless estimate | as above | reuse unchanged; fed the filtered frame |
| `_drop_unpriceable` | `:265` | price drop + `price_rows_dropped` tally, `currency` + `money_only` buckets | `test_unpriceable_price_drop.py`, `test_currency_denominated_units.py` | extend (mode set, `unknown` bucket, label-forced drop) |
| `_unit_sentinel_mask`, `U_UNKNOWN`, `_U_SENTINELS` | `:4177`, `:4148` | "no unit recorded": NaN `u`, `Unknown`, legacy `Manquant` | `test_962_unit_sentinel.py` | reuse unchanged — the kg ladder's own test |
| `_is_currency_denominated`, `_CURRENCY_DENOMINATED_UNITS` | `:961–:986` | `{'value'}`, case-insensitive | `test_currency_denominated_units.py` | reuse unchanged |
| `S_VALUES`, `validate_acquisition_source` | `:53`, `:438` | canonical `s`; NA refused on every Country read | `test_*` acquisition-source tests | reuse (the definition of "purchase") |
| `food_expenditures_from_acquired(basis='purchased')` | `:3308` | the existing `s == 'purchased'` test | yes | reuse the same test, spelled once (`_KG_EVIDENCE_SOURCE`) |
| archived fix `cc962ee15` | `refs/archive/2026-11-11/isa/framework-1509-purchases-20261105` | blanked price inside `conversion_to_kgs`; `kg_factor_evidence` attr | none landed | reuse the blank-not-drop idea and the ledger attr; MOVE the filter to `food_kg_factors` (off the crop ladder) |

## §3 Definitions & conventions in force

- **Purchase**: `s == 'purchased'` exactly, the `S_VALUES` spelling
  (`transformations.py:53`) and the test `food_expenditures_from_acquired`
  already uses for `basis='purchased'`.  `produced`, `inkind`, `other`, NA and any
  non-canonical spelling are not purchases.  A frame with no `s` cannot be
  filtered and stays all evidence, disclosed as `source_level: False`
  (only direct calls: `data_info.yml` declares `s` on `food_acquired`, and every
  measured served frame carries it).
- **Missing unit**: `_unit_sentinel_mask` (NaN `u`, `Unknown`, `Manquant`), the
  test the kg ladder uses (`food_kg_factors`, `harvest_kg_factors`).
- **Currency unit**: `_is_currency_denominated` over `_CURRENCY_DENOMINATED_UNITS`.
- **`price_rows_dropped`** counts INPUT rows (pre-groupby); #1129's table counts
  SERVED rows (post `(t,i,j,u,s)` median).  The two are not the same number.

## §4 Invariants & assumptions

- `transformations.py` is in no `build_transforms_fingerprint`
  (`tests/test_build_transform_hash.py::test_no_transformations_symbol_reaches_any_fingerprint`).
- The only country import that reaches the edited callables is
  `GhanaLSS/_/nutrition.py -> food_quantities_from_acquired` (closure probe:
  1 file; `Ethiopia/_/ethiopia.py:1634`'s `harvest_kg` import is in a docstring).
  So only GhanaLSS's L2 hashes may move.  Do not edit `conversion_to_kgs`,
  `_unit_sentinel_mask`, `_is_currency_denominated` or `_level_or_column`: the
  crop ladder and EHCVM's `food_acquired_to_canonical` closures stay as they are.
- Blank, never drop, in the estimator: rung arrays are positional over the frame.
- `_drop_unpriceable` buckets are disjoint: `currency`, then `money_only`, then
  `unknown`; `unknown` holds only rows whose computed Price was servable, so the
  GH #591 `inf` (zero quantity) and item non-response keep their own cause.
- `serve_waves` (GH #1487): the evidence ledger counts every row the estimators read.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| purchase test | reuse `S_VALUES` spelling | one definition, already used by `basis='purchased'` |
| eligibility filter | new `_purchase_evidence` (one `mask`) called once in `food_kg_factors` | keeps it off `_get_kg_factors`' crop / plot-input callers |
| evidence disclosure | new `attrs['kg_factor_evidence']` (from `cc962ee15`) | beside the other `kg_factor_*` diagnostics; not a layer |
| unitvalue drop | extend `_drop_unpriceable` | reuse the `currency` bucket; add `unknown` beside it |
| units drop | extend `food_quantities_from_acquired` | new `attrs['quantity_rows_dropped']` (no price tally exists there) |
| missing-unit test | reuse `_unit_sentinel_mask` | same rule the kg ladder applies |

## §6 Open questions for the human (not decided here)

- `food_quantities(units='kgs')` still carries `Unknown` rows natively and sums
  them under `u='Unknown'` — the same pseudo-unit sum #1068 removes from `'units'`.
  Not ruled.
- `food_prices(units='unitprice')` keeps reported prices on `u='Unknown'` (GhanaLSS
  2016-17 produced rows).  Not ruled.
- `units='units'` keeps `u='Value'` rows (recorded currency amount; no ratio).
- Cambodia's allocated quantity stays evidence by the ruling.
