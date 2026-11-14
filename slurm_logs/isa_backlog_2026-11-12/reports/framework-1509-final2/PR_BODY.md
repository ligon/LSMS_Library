**food kg evidence is purchases, with two disclosed self-valued fallback rungs (item-level ranked above the pooled `unit`); `unitvalue` / `units` drop `Value` and `Unknown`; nutrition scripts version the kg ladder (#1509, #1068, #1129)**

This PR implements six rulings by @ligon, all of 2026-11-12. The kilogram code is read-time code in
`lsms_library/transformations.py`; two country scripts gain an import.

**#1509, "all purchase rows", as amended ("Fallback rung, disclosed"; "Add a unit-level rung"; "Item
self-valued first").**
- `food_kg_factors`' price-ratio inference reads only rows whose `s` is exactly `'purchased'`. Both
  `conversion_to_kgs` arms, the #1450 size pool and the #1452 tableless estimate read purchases only.
  Every row is still served.
- **The one exception is two rungs of their own**, for a `(j, u)` cell with no priced purchase in any
  wave read (judged on the item arm's join key; for an unsized label, on its size-pooled family):
  - `item_unit_nonpurchase`: the item estimator on the non-purchase rows alone, with the same
    `min_reports`, `min_baseline`, tight exception and dispersion gate;
  - `unit_nonpurchase`: the u-pooled `unit` estimator on the same rows, with `unit`'s gates only.
  - **Order, set by the maintainer** ("Item self-valued first", after the red team showed the first
    order was this branch's choice): `FOOD_KG_FACTOR_LAYERS = ('survey_kg', 'metric', 'item_unit',
    'item_unit_tight', 'item_unit_nonpurchase', 'unit', 'unit_nonpurchase', 'none')`. A no-purchase
    cell sees `item_unit_nonpurchase`, `unit`, `unit_nonpurchase`, `none`; a purchase cell sees
    `item_unit`, `item_unit_tight`, `unit`, `none` as before. One tuple works because the purchase item
    rungs need purchases in the cell and the fallbacks need none (`kg_item_unit` and
    `kg_item_unit_nonpurchase` are never both set: 0 rows in 11 countries).
  - **A cell with any purchase evidence never reads either rung, even when that evidence fails a gate.**
- Both ratios are self-valued on both sides. Sentinel units, currency units and `no_kg` declarations
  refuse both.
- **Disclosed** in the partition (`attrs['kg_factor_sources']`, still sums to the frame), per row
  (`KgFactorSource`, `kg_item_unit_nonpurchase`, `kg_unit_nonpurchase`, `kg_cell_has_purchase`) and in
  `attrs['kg_factor_evidence']['fallback']` (rows, cells, `s`, tight and single-wave counts, a
  `unit_rung` block, `none_by_reason`, and the item gate's refusals: `rows_refused_spread ==
  refused_spread_to_unit + refused_spread_to_unit_nonpurchase + refused_spread_to_none`, the `unit` term
  new with the reorder).
- Cambodia's allocated purchased quantity stays evidence, under the `allocated-quantity` caveat.
- The crop and plot-input ladders are untouched.

**"Make the dependency visible".** `Uganda/_/nutrition.py` and `Ethiopia/_/nutrition.py` read
`food_quantities()` through the Country API at build time, which `framework_imports_fingerprint` cannot
see, so a kg edit moved Uganda's `nutrition` and no Uganda hash. Both now import
`food_quantities_from_acquired` (GhanaLSS's `nutrition.py` already did) and assert that
`Country._FOOD_DERIVED` still routes `food_quantities` through it. `rg` over every country and wave
`_/*.py` found no other build-time caller (Panama's `nutrition.py` is unreachable; Uganda `income.py`
reads `earnings` / `enterprise_income`). `tests/test_gh1509_build_time_food_api_dependency.py` refuses a
new script that calls the API without the import.

**#1068 / #1129, "drop and count".** `food_prices(units='unitvalue')` drops rows on a
currency-denominated unit (`u='Value'`) and on a missing unit, counted in `price_rows_dropped` as
`currency` and `unknown`. `food_quantities(units='units')` drops missing-unit rows and counts them in
`attrs['quantity_rows_dropped']`. `unitprice` and the kg modes are unchanged.

### Measured

Cold, private scratch roots, three arms on one node (job 41251759): base `e5fdd2fa8`, unit-first head
`0966d62ef`, this order `30523f946` (the code at the PR head, `30f461b04`, differs only in docstrings).

| country | kg base | kg head | kg this PR | vs base | `food_prices()` base / PR | `kgprice` | `unitvalue` | `units` |
|---|---|---|---|---|---|---|---|---|
| GhanaLSS | 11,007,392 | 9,918,751 | **10,209,230** | -7.25% | 560,023 = | 187,096 = | 1,676,514 -> 568,130 | 1,695,046 -> 1,694,111 |
| GhanaSPS | 1,384,290 | 1,438,672 | 1,438,672 | **+3.93%** | 387,440 = | n/a | 387,440 = | 393,296 = |
| Uganda | 1,241,553 | 1,224,670 | 1,224,670 | -1.36% | 348,181 -> 348,178 | 325,051 -> 325,048 | 355,510 -> 348,181 | 355,544 -> 354,639 |
| Serbia | 260,550 | 255,023 | 255,023 | -2.12% | 196,274 = | 196,274 = | 196,279 -> 196,274 | 196,279 = |
| Cambodia | 57,198 | 56,579 | 56,579 | -1.08% | 35,187 = | n/a | 35,187 = | 35,190 = |
| Ethiopia | 1,747,435 | = | = | 0 | 263,627 = | n/a | 263,717 -> 263,627 | 326,082 -> 325,838 |
| Malawi | 2,567,830 | = | = | 0 | 680,372 = | n/a | 681,614 -> 680,373 | 971,062 -> 969,198 |
| Panama | 1,226,520 | = | = | 0 | 483,661 = | 483,661 = | 490,846 -> 483,661 | 560,290 -> 559,847 |
| Burkina Faso | 657,254 | = | = | 0 | 234,524 = | n/a | 234,702 -> 234,524 | 318,297 -> 318,027 |
| EthiopiaRHS | 216,820 | = | = | 0 | 35,625 = | n/a | 37,860 -> 37,660 | 54,783 -> 54,671 |
| Niger | 411,667 | = | = | 0 | 165,029 = | n/a | 165,029 = | 239,878 -> 239,870 |

- **The reorder ("Item self-valued first") moves GhanaLSS only**: exactly 79,773 produced rows in 147
  cells go `unit -> item_unit_nonpurchase` (1991-92 18,943, 1998-99 13,313, 2005-06 14,990, 2012-13
  18,577, 2016-17 13,950), each now at its base factor (to 1e-9; a median 6.6x the pooled factor it had),
  46,889 -> 337,368 kg, +290,479 kg. 0 rows in a purchase cell change. Every other country is
  frame-equal to `0966d62ef`, prices included. The item gate's `rows_refused_spread` goes 23,156 ->
  48,989: the 25,833 added rows were refused under the old order too, and are counted now because
  `unit` sits below the item fallback (`refused_spread_to_unit`).
- **GhanaLSS against base:** `item_unit_nonpurchase` 287,863 rows and `unit_nonpurchase` 28,939, all at
  their base factors; still short are 238,616 rows in cells WITH purchase evidence that the item rungs
  refuse and `unit` serves (-756,133 kg), plus 42,104 kg of purchase factors changed in place. The
  rulings keep those cells off the self-valued rungs.
- **GhanaSPS moves** (red team F1): 250,258 of 407,929 rows change factor, 17,961 by more than 2x;
  18,741 change rung (9,403 `item_unit -> unit`, 9,338 `unit -> item_unit`); `food_prices()` keeps
  387,440 rows and its Price moves. No GhanaSPS row reaches a fallback, and no GhanaSPS hash moves.
- **Uganda** (F4): -1.36% kg is 141,884 rows changing factor (4,850 by more than 2x) and 4,042 changing
  rung, 15 of them onto `unit_nonpurchase`.
- **`nutrition`** (F3): GhanaLSS Energy -15.08% against base (-18.67% under the unit-first order),
  Protein -5.80%, Carbohydrate -17.58%, Fiber -11.63%, on 34,798 households; Uganda Energy -1.07%,
  Protein +0.40%, on 23,777 (the same under either order); Ethiopia frame-equal.
- **#1487:** every `waves=[w]` in `kgs` and `kgvalue` is frame-equal to the all-waves slice in GhanaLSS
  (7 waves), Uganda (8) and GhanaSPS (3), in all three arms.
- **The gate and wave-spread figures CLAUDE.md quotes for #850 are pre-#1509** (F7b): Uganda
  `kg_factor_baseline_gate.rows_refused` 13,571 -> 2,834; GhanaLSS 428,168 -> 446,696; GhanaLSS's
  538,583 item-rung rows are all single-wave. Now stated there.
- Non-purchase `Expenditure` x1000 moves no factor (measured on the earlier arms; the reorder changes
  no estimate).
- **These are movements, not corrections.** No external unit weight was scored.

**Cache** (F3). Base -> this PR: 75 of 555 table hashes (Ethiopia 25, GhanaLSS 22, Uganda 28: every
table of the three), 503 of 2,321 wave hashes (125 / 154 / 224), 3 of 38 framework-import fingerprints,
0 of 42 build-transform fingerprints. A no-op statement added to `food_kg_factors` moves exactly the same
set, so every later kg-factor edit re-warms these three countries cold. On a warm copy of the base root,
`0966d62ef` served Uganda's base-built `nutrition` as fresh (0.9 s, base's Energy); this PR grades it
stale, rebuilds it (70 s) and serves its cold value. Ethiopia re-warms although its `nutrition` does not
move. CLAUDE.md's "None of this invalidates a cache" is replaced.

**Cost.** Same node, arms in parallel: GhanaLSS `food_kg_factors` 18.8 s (base) -> 39.0 s (the
non-purchase estimate; 38.9 s at `0966d62ef`), `food_quantities()` 43 -> 63 s, `food_prices()` 37 -> 58 s;
Uganda `food_kg_factors` 4.6 s. Purchases-only countries pay nothing. The reorder costs nothing.

**Tests.**
- New: `test_gh1509_build_time_food_api_dependency.py` (4, data-free), `test_gh1509_data_pins.py` (4,
  requires_s3: GhanaSPS and GhanaLSS kg and rungs, GhanaLSS and Uganda nutrition Energy), and a test of
  the item gate's refusal falling to `unit`. Moved pins, each with its reason: the layer tuple; the
  unit-rung neighbour pin; `test_a_purchase_unit_rung_outranks_the_fallback` became
  `test_the_fallback_outranks_a_purchase_unit_rung`; both helpers assert the three-term identity.
  `test_ghanalss_unitvalue_and_units_modes_are_unaffected` is renamed
  `test_ghanalss_native_unit_modes_read_no_kg_factor` (F8).
- Full non-S3 suite: this PR 15,182 passed, 0 failed (at `30523f946` and again at the head `30f461b04`); base 15,109 passed, 0 failed.
- Data-gated (warm private roots): this PR 845 passed, **1 failed** (23 files); base 773 passed, 0
  failed (19 files: base lacks the four #1509 files). At `8abd5fa3b` (cand root, 24 files with the data pins): 849 passed, the same 1 failed. Census `30523f946` -> `8abd5fa3b`: 0 hashes move. At the PR head `30f461b04` (job 41253845): census against `30523f946` 0 of 555 / 0 of 2,321 / 0 of 38 / 0 of 42; full non-S3 suite 15,182 passed, 0 failed; data-gated (25 files) 854 passed, the same 1 failed.

### For maintainer review

1. **Uganda's `nutrition` invariance fixture.** `tests/test_uganda_invariance.py::
   test_parquet_matches_baseline[var/nutrition.parquet]` passes on base and fails here (and at
   `0966d62ef`, whose Uganda `nutrition` is frame-equal to this PR's): #1509 moves Uganda Energy -1.07%.
   Per the #967 precedent (`Uganda/_/CONTENTS.org`) the entry moves only on your explicit approval, so
   it is not regenerated. Proposed two-field change: `shape` stays `[23777, 15]`; `content_hash` ->
   `e9fb4c67b2463694a2ec7b46ee5e6df1ef424db19753a74c2b82ad6a48a6a600` (measured on n4257; another BLAS
   kernel may give a second exact fingerprint, not measured).
2. **Unit-rung eligibility is per `(j, u)` cell, not per label** (F5). Uganda `Sack (unspecified)`: the
   label has one priced purchase; that purchase's cell (2 rows) stays `none`, while 8 rows in other
   items' Sack cells are served the non-purchase factor 2.749 kg (base 3.556). This is the ruling's
   literal condition; is label-level eligibility wanted instead? (One-line change, not made.)
3. **Attrs naming** (F6): the #1068 ruling named `attrs['price_rows_dropped']`; `units` mode writes
   `attrs['quantity_rows_dropped']`, because it is not a price table.
4. **`currency` counts every expected currency row, `unknown` only priceable ones** (F7). In
   `unitvalue`, Serbia `currency = 3,772` but only 5 served groups were removed (the rest were already
   unpriceable, so `lost_fraction` goes 1.88% -> 0); GhanaLSS 3,551,979 input rows against 1,107,464
   served groups. A `currency_removed` count was not added.

Closes #1509, #1068, #1129.

---

### Close draft: #1509

> Implemented in `<PR>` as ruled on 2026-11-12 ("all purchase rows") and as amended the same day
> ("Fallback rung, disclosed"; "Add a unit-level rung"; "Item self-valued first").
>
> - `food_kg_factors`' price ratio reads only `s == 'purchased'` rows, and every row is still served.
> - A `(j, u)` cell with no priced purchase in any wave reads, in order:
>   - `item_unit_nonpurchase`, the item estimator on the non-purchase rows alone, with the same floors
>     and gates;
>   - then the u-pooled purchase rung `unit`;
>   - then `unit_nonpurchase`, the u-pooled estimator on the non-purchase rows;
>   - then `none`.
> - A cell with any purchase never reads either self-valued rung, even when that purchase evidence
>   fails a gate, and keeps its order.
> - Both rungs are counted in the partition and in `attrs['kg_factor_evidence']['fallback']`.
> - Country scripts that read the food API at build time (Uganda and Ethiopia `nutrition.py`) import
>   the transform, so a kg-factor edit re-warms them, as GhanaLSS already did.
>
> Measured cold, `food_quantities()` kg against base:
> - GhanaLSS -7.25% (11.01M -> 10.21M). 287,863 rows on the item rung and 28,939 on the unit rung, each
>   at its old factor. The rest is cells with purchase evidence now served by purchase factors.
> - GhanaSPS +3.93% (every cell has purchases; no row on a fallback rung).
> - Uganda -1.36%, Serbia -2.12%, Cambodia -1.08%.
> - Ethiopia, Malawi, Niger, Panama, Burkina Faso and EthiopiaRHS unchanged.
> - GhanaLSS `food_prices()` 560,023 rows and `kgprice` 187,096, as before #1509.
> - `nutrition`: GhanaLSS Energy -15.08%, Uganda -1.07%, Ethiopia unchanged.
>
> Movements, not corrections: no external unit weight was scored. Cache: every hash of Ethiopia,
> GhanaLSS and Uganda moves once (75 tables), nothing else. Closing.

### Close draft: #1068

> Option (a), as ruled ("drop and count"), is implemented in `<PR>`.
>
> - `food_prices(units='unitvalue')` drops `u='Unknown'` rows, legacy `Manquant` and a NaN `u`
>   (`_unit_sentinel_mask`), and counts them in `price_rows_dropped['unknown']`.
> - `food_quantities(units='units')` drops them and counts them in
>   `attrs['quantity_rows_dropped']['unknown']`.
> - An `Unknown` row with a zero quantity stays the loud GH #591 `inf`.
> - Served `Unknown` rows in `units` are now 0: Malawi 1,864, Uganda 905, GhanaLSS 935, Panama 443,
>   Burkina 270, Ethiopia 244, EthiopiaRHS 112, Niger 8 (measured cold); Nigeria 590 and Senegal 21
>   (the red team's transform-level figures, not measured end-to-end).
> - `unitvalue` drops (currency plus unknown): GhanaLSS 1,108,384, Uganda 7,329, Panama 7,185, Malawi
>   1,241, EthiopiaRHS 200, Burkina 178, Ethiopia 90, Serbia 5; Nigeria 422 (red team, transform level).
> - GhanaSPS's counts do not move: its 13,481 (`unitvalue`) and 13,728 (`units`) rows on a NaN `u` were
>   already deleted, silently, by the old groupby; they are now counted under `unknown`.
>
> Not ruled, and left as they were; open a follow-up if wanted:
> - `units='kgs'` still carries `Unknown` rows natively.
> - `unitprice` keeps reported prices on `Unknown`.
>
> Closing.

### Close draft: #1129

> Option (a), as ruled, is implemented in `<PR>`. `food_prices(units='unitvalue')` drops
> `_CURRENCY_DENOMINATED_UNITS` rows and counts them in `price_rows_dropped['currency']`, the bucket the
> kg modes already use.
>
> - **GhanaLSS:** 1,676,514 -> 568,130 rows. All 1,107,464 rows at exactly 1.0 are gone, and 1987-88,
>   1988-89 and 1991-92 serve none.
> - **Panama:** 490,846 -> 483,661.
> - **Uganda:** 6,429 rows at 1.0 gone.
>
> `units='units'` keeps `Value` rows. It forms no ratio, and there `Quantity == Expenditure` on every
> GhanaLSS and Uganda `Value` row.
>
> CLAUDE.md's "Kwacha per Kwacha" sentence, the generated `food_prices` docstring and
> `docs/guide/coverage.md` are corrected. Closing.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
