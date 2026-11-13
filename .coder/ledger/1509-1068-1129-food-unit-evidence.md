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

---
### Phase 3 — verification

Measured cold in private roots, base `e5fdd2fa8` vs candidate (jobs 41236013 / 41236014,
census 41236015, data tests 41236531 / 41236522); report at
`burst/isa-sm-20261112/reports/framework-1509-1068/REPORT.md`.

- `_purchase_evidence` / `_purchase_rows` / `_KG_EVIDENCE_SOURCE` — OK (§3 purchase): the
  `S_VALUES` spelling, matched exactly; `food_expenditures_from_acquired` spells the same test
  inline (`astype(str) == 'purchased'`), so no tested helper existed to reuse — not REINVENTION.
  Non-purchase x1000 moves 0 factor rows in all nine measured countries (base: up to 704,055).
- `food_kg_factors` wiring — OK (§4 blank-not-drop; §5 off `conversion_to_kgs`): partitions still
  sum to the frame; `serve_waves` ledger over all rows read (synthetic test).
- `_kg_evidence_ledger` — OK (§5): a diagnostic beside the partition, not a layer.
- `_drop_unpriceable` — OK (§2 reuse `currency`; §4 disjoint buckets): Burkina 405,127
  `money_only` unchanged, 178 `unknown`; `inf` on an `Unknown` row stays loss (synthetic test).
- `food_quantities_from_acquired(units='units')` — OK (§3 missing unit = `_unit_sentinel_mask`).
- Closure invariant (§4) — OK: only GhanaLSS moves (22/555 table, 154/2,321 wave, 1/38 fwimp,
  0/42 btf).
- §6 open questions stand: kgs carries `Unknown` natively; `unitprice` keeps `Unknown`; `units`
  keeps `Value` (Q == E on every GhanaLSS / Uganda `Value` row); Cambodia allocated quantity.
- Movement, not correction: GhanaLSS kg -13.0% (237,029 produced rows to `none`), Uganda -1.37%;
  no external unit weight scored.

---
### Amendment (2026-11-12) — the disclosed non-purchase fallback rung

Ruling (`docs/isa-sweep-20261103:.sucoder/isa-decisions.org`, "GH1509 GhanaLSS
consequence"): "Fallback rung, disclosed".  Purchases only, EXCEPT a `(j, u)` cell
with no purchase evidence in any wave may fall back to non-purchase (self-valued)
evidence on a NEW, separately labelled rung of the kg-factor ladder.

| quantity | decision | reason |
|---|---|---|
| rung | `item_unit_nonpurchase`, between `unit` and `none` in `FOOD_KG_FACTOR_LAYERS` | below every purchase rung (`unit` included), above `none`; the ruling is per `(j, u)` and the gates live on the item arm, so it is item-keyed (its u-pooled twin, `unit_nonpurchase`, came with the second amendment below) |
| estimator | reuse `_pooled_kg_rungs` / `_inferred_kg_rungs` (item arm only, `unit_arm=False`; the switch was removed by the second amendment, which reads the u arm too) on `_nonpurchase_evidence(df)` | same floors, tight exception and dispersion gate, same size pool and #1452 overlay; self-valued on BOTH sides of the ratio (a purchase baseline under a self-valued numerator is the mixing #1509 removed) |
| eligibility | new `cell_priced` array in `_KG_RUNG_ARRAYS`, computed on the purchase frame | "the row's cell holds one priced purchase in any wave", on the item arm's join key, before `serve_waves` slicing, and through the size pool for an unsized label; independent of every floor, so a gated-out purchase cell never reaches the rung |
| disclosure | `kg_factor_sources` (the partition), per-row `kg_item_unit_nonpurchase` / `kg_cell_has_purchase`, and `kg_factor_evidence['fallback']` (incl. `none_by_reason`) | separate label = visibly weaker evidence |
| cost guard | no second estimate without `s`, without a priced non-purchase row, or when every cell has purchases | purchases-only countries pay nothing and cannot move |

Not changed: `conversion_to_kgs`, `_kg_inference_frame`, `_get_kg_factors`,
`_unit_sentinel_mask`, `_is_currency_denominated` (shared with the crop ladder).
Pre-existing, not changed: the dispersion gate's counterfactual
(`baseline_refused_spread`) is estimated only when the gated estimate leaves some
item cell, so a frame whose only cell the gate refuses reports 0 refused rows.

#### Amendment — Phase 3 verification

Measured cold in private roots, three arms: base `e5fdd2fa8`, purchases-only head
`6c14097e9`, rung `322530074` (jobs 41240317 / 41240318 / 41241044; comparison 41242351);
report at `burst/isa-sm-20261112/reports/framework-1509-1068-final/REPORT.md`.

- Eligibility (§ amendment) — OK: 0 rows on the rung whose cell has purchase evidence, in
  every measured country; GhanaLSS `none_by_reason.cell_has_purchase` = 1.
- Ladder position — OK: head -> cand source changes are `none -> item_unit_nonpurchase` only
  (208,090 GhanaLSS rows), and no factor moves off the new rung.
- Recovery — the item-keyed ceiling exactly: 208,090 = the 166,548 + 41,542 rows purchases
  alone had moved from `item_unit` / `item_unit_tight` to `none`; every one at its `e5fdd2fa8`
  factor.  28,939 rows (u-pooled on base) stay `none` (`no_estimate`).  kg 9.57M -> 9.91M
  (23.4% of the purchases-only kg loss).
- Purchases-only countries — Cambodia, Serbia, Uganda, Burkina Faso, Malawi, Niger frame-equal
  to head (0 rows on the rung).
- Gates — the rung's own dispersion refusals: 23,156 GhanaLSS rows; tight baselines 41,542.
- Closure (§4) — OK: footprint identical to head's (GhanaLSS only: 22/555, 154/2,321, 1/38, 0/42).
- Movement, not correction: the recovered factors are the pre-#1509 self-valued ones; no
  external unit weight scored.

---
### Second amendment (2026-11-12) — the unit-level non-purchase rung

Ruling (`docs/isa-sweep-20261103:.sucoder/isa-decisions.org`, "GH1509 second
(unit-level) non-purchase rung"): "Add a unit-level rung".  Below the per-item
non-purchase rung, a separately labelled unit-level non-purchase rung (pooled across
items per unit), same no-purchase-evidence condition, to recover GhanaLSS's remaining
28,939 rows.

| quantity | decision | reason |
|---|---|---|
| rung | `unit_nonpurchase` (`FOOD_KG_UNIT_NONPURCHASE_LAYER`), between `item_unit_nonpurchase` and `none` | named by the rule of the first: the `unit` estimator on `nonpurchase` evidence; `item_unit` : `unit` :: `item_unit_nonpurchase` : `unit_nonpurchase` |
| estimator | the same one non-purchase `_pooled_kg_rungs` call, now with its u arm (`conversion_to_kgs(item_col=None)`); `unit_arm` removed | the size pool and #1452 overlay come with it, as on the purchase side; one estimate, two candidates |
| gates | the purchase `unit` rung's: currency labels out, sentinel / `no_kg` refuse, survey_kg / metric / every rung above outrank, survey-kg rows anchor the baseline only, finite positive factors | `unit` has none of the item floors (`conversion_to_kgs`: "gating a fallback would leave rows with nothing"), so neither does its twin; a row the item fallback refuses lands here, as a refused purchase row lands on `unit` |
| evidence pool | every non-purchase row of the label, any item | the twin of `unit`, which pools every purchase of the label; only the SERVED row's own `(j, u)` cell must have no purchase |
| eligibility | `~cell_priced`, unchanged | "same no-purchase-evidence condition" |
| disclosure | partition, `KgFactorSource`, per-row `kg_unit_nonpurchase`, `kg_factor_evidence['fallback']['unit_rung']`; `rows_refused_spread == refused_spread_to_unit_nonpurchase + refused_spread_to_none`; `kg_factor_size_pool.served_by_unit_rung` | as the first rung; the gate ledger mirrors `kg_factor_baseline_gate` |
| not reported | the non-purchase estimate's stated-vs-inferred contradictions | the warning stays the purchase rungs', like `kg_factor_wave_spread` |

#### Second amendment — Phase 3 verification

Measured cold in private roots, three arms: base `e5fdd2fa8`, item fallback head
`5b2496a12`, unit rung `ee9d6cd23` (jobs 41244889 / 41244890 / 41245126; comparison
41247107); report at `burst/isa-sm-20261112/reports/framework-1509-unit-rung/REPORT.md`.

- Eligibility — OK: 0 priced purchases in the 89 GhanaLSS and 10 Uganda cells the rung
  serves (independent recount over all waves); 0 rows with `kg_cell_has_purchase`.
- Ladder position — OK: head -> cand source changes are `none -> unit_nonpurchase` only;
  no factor moves off the new rung.
- Recovery (GhanaLSS) — all 28,939 rows, each at its `e5fdd2fa8` factor (to 1e-9);
  +10,063 kg (9.909M -> 9.919M, -9.89% against base); `food_prices()` 560,023 and
  `kgprice` 187,096 (1991-92 28,754), both back at base.  23,156 of the rows are the item
  fallback's dispersion refusals.  41 rows stay `no_estimate`; the `none` count equals base's, 3,571,848.
- Uganda — moves: 15 of 18 rows (5 labels), +65 kg, factors within 2x of base's (median
  ratio 0.77).  3 stay `none` (2 purchase cells, 1 no estimate).
- Unchanged — Cambodia, Serbia, Burkina Faso, Malawi, Niger, EthiopiaRHS frame-equal to head.
- Closure (§4) — OK: GhanaLSS only (22/555, 154/2,321, 1/38, 0/42), as before.
- Movement, not correction: no external unit weight scored.

---
### Third amendment (2026-11-12) — ladder order and build-time Country-API dependencies

Rulings (`docs/isa-sweep-20261103:.sucoder/isa-decisions.org`, "GH1509 ladder order and
build-time Country-API dependencies"), made after the red team
(`burst/isa-sm-20261112/reports/framework-1509-redteam/VERDICT.md`, F2 and F3):

- "Item self-valued first": for a `(j, u)` cell with NO purchase evidence in any wave,
  `item_unit_nonpurchase` ranks ABOVE the u-pooled purchase rung `unit`; then
  `unit_nonpurchase`; then `none`.  Cells with any purchase evidence are unaffected.
- "Make the dependency visible": a country script that calls the Country food API at build
  time imports the transform, so `framework_imports_fingerprint` folds the kg-factor code
  into its hash.

The order recorded in the two sections above (`unit` above both fallbacks) was this branch's
choice, not a ruling's; the maintainer set the order on 2026-11-12.

| quantity | decision | reason |
|---|---|---|
| ladder | `FOOD_KG_FACTOR_LAYERS = ('survey_kg', 'metric', 'item_unit', 'item_unit_tight', 'item_unit_nonpurchase', 'unit', 'unit_nonpurchase', 'none')` | one total order expresses the ruling because `kg_item_unit` needs purchases in the cell and `kg_item_unit_nonpurchase` needs none (never both set: 0 rows in 11 countries); the `np.where` swap is in both the `resolved` and the `source` chain |
| gate ledger | `rows_refused_spread == refused_spread_to_unit + refused_spread_to_unit_nonpurchase + refused_spread_to_none` | `unit` is now below the item fallback, so a row its dispersion gate refuses and `unit` serves was costed by the gate |
| build-time callers | `Uganda/_/nutrition.py`, `Ethiopia/_/nutrition.py` (`uga/eth.food_quantities()`) import `food_quantities_from_acquired`; GhanaLSS already did | `rg` over `countries/*/_/*.py` and `countries/*/*/_/*.py` for `.food_quantities(`, `.food_prices(`, `.food_expenditures(` and `Country(`: only these two read a derived food table through the API (Panama's `nutrition.py` is unreachable and reads a retired parquet; Uganda `income.py`'s `getattr` reads `earnings` / `enterprise_income`) |
| functional use | each script asserts `lsms_library.country.Country._FOOD_DERIVED['food_quantities'] == food_quantities_from_acquired.__name__` | the import must describe the real route; `lsms_library.country.Country`, not `ll.Country`, because the scripts' exec tests replace `ll.Country` with a fake |
| enforcement | `tests/test_gh1509_build_time_food_api_dependency.py`: static AST sweep + no-op probe (exactly Ethiopia, GhanaLSS, Uganda move; 0 `btf`) | a twelfth script that reads the API without the import goes red |

#### Third amendment — verification

Measured cold in private scratch roots, three arms in one job on one node (41251759,
n4257): base `e5fdd2fa8`, unit-first head `0966d62ef`, this order `30523f946`; report at
`burst/isa-sm-20261112/reports/framework-1509-final2/REPORT.md`.

- Ladder (GhanaLSS) — head -> cand changes exactly 79,773 produced rows, all `unit ->
  item_unit_nonpurchase`, 147 cells (1991-92 18,943, 1998-99 13,313, 2005-06 14,990,
  2012-13 18,577, 2016-17 13,950), each at its `e5fdd2fa8` factor (to 1e-9); 46,889 ->
  337,368 kg; kg 9,918,751 -> 10,209,230 (-7.25% against base).  0 rows in a purchase cell
  change.  Counts in every price/quantity mode unchanged.
- Every other country (GhanaSPS, Uganda, Ethiopia, Cambodia, Serbia, Burkina Faso, Malawi,
  Niger, Panama, EthiopiaRHS) frame-equal head -> cand.
- Eligibility recount — 0 fallback rows in a cell with any purchased row, in all 11.
- #1487 — `kgs` / `kgvalue` subsets are slices in every wave of GhanaLSS, Uganda, GhanaSPS,
  all three arms.
- Closure (§4) — base -> cand: 75/555 `_table_cache_hash`, 503/2,321 `_input_hash`,
  3/38 `fwimp`, 0/42 `btf` (Ethiopia, GhanaLSS, Uganda, every table and wave); the no-op
  probe in `food_kg_factors` moves exactly the same set against cand.
- Warm root — base-built Uganda `var/nutrition.parquet`: head serves it stale (0.9 s,
  base's Energy); cand rebuilds it (70 s) and serves its cold value.
- `nutrition` — GhanaLSS Energy -15.08% against base (-18.67% under the unit-first
  order); Uganda -1.07% (either order); Ethiopia frame-equal.
- Movement, not correction: no external unit weight scored.
