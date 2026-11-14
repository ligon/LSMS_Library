**food kg evidence is purchases, with a disclosed non-purchase fallback rung; `unitvalue` / `units` drop `Value` and `Unknown` (#1509, #1068, #1129)**

This PR implements three rulings by @ligon, all of 2026-11-12. All the code is read-time code in
`lsms_library/transformations.py`.

**#1509, "all purchase rows", as amended ("Fallback rung, disclosed").**
- `food_kg_factors`' price-ratio inference reads only rows whose `s` is exactly `'purchased'`.
  Both `conversion_to_kgs` arms, the #1450 size pool and the #1452 tableless estimate read
  purchases only. Every row is still served.
- **The one exception sits on its own rung**, `item_unit_nonpurchase`, between `unit` and `none`:
  - `FOOD_KG_FACTOR_LAYERS = ('survey_kg', 'metric', 'item_unit', 'item_unit_tight', 'unit',
    'item_unit_nonpurchase', 'none')`.
  - It serves a row only if the row's own `(j, u)` cell holds no priced purchase in any wave
    read, and no purchase rung serves the row.
  - The cell is judged on the item arm's join key; for an unsized label, on its size-pooled
    family.
  - **A cell with any purchase evidence never reads it, even when that evidence fails a gate.**
- The rung runs the item estimator on the non-purchase rows alone, so its ratio is self-valued on
  both sides. It keeps the same `min_reports`, `min_baseline`, tight exception, dispersion gate,
  size pool and #1452 overlay. Sentinel units, currency units and `no_kg` declarations refuse it.
- **Disclosed** in four places:
  - the partition, `attrs['kg_factor_sources']`, which still sums to the frame;
  - the per-row `KgFactorSource`;
  - two new per-row columns, `kg_item_unit_nonpurchase` (the candidate) and
    `kg_cell_has_purchase` (the eligibility test);
  - `attrs['kg_factor_evidence']['fallback']`: rows, cells, `s`, tight and single-wave counts,
    the rows its own dispersion gate refused, and a `none_by_reason` partition of the `none` rows.
- `produced`, `inkind`, `other` and an NA `s` are not purchases.
- Cambodia's allocated purchased quantity stays evidence, under the `allocated-quantity` caveat
  (docstring and `Cambodia/_/CONTENTS.org`).
- The filters live in `food_kg_factors`, not in `conversion_to_kgs`, so the crop and plot-input
  ladders are untouched.

**#1068 / #1129, "drop and count".**
- `food_prices(units='unitvalue')` drops, by label, rows on a currency-denominated unit
  (`u='Value'`) and rows on a missing unit. They are counted in `price_rows_dropped` as
  `currency` and as a new `unknown` bucket.
- `food_quantities(units='units')` drops missing-unit rows and counts them in
  `attrs['quantity_rows_dropped']`.
- `unitprice` and the kg modes are unchanged.

**Measured cold in private roots, three arms:**
- base `e5fdd2fa8`;
- purchases only, `6c14097e9`;
- this PR, `322530074`. `transformations.py` is byte-identical at the PR head, `5b2496a12`.

| `food_quantities()` kg | base | purchases only | this PR |
|---|---|---|---|
| GhanaLSS | 11.01M | 9.57M (-13.0%) | **9.91M (-10.0%)** |
| Uganda | 1,241,553 | 1,224,605 (-1.37%) | 1,224,605 |
| Serbia | 260,550 | 255,023 (-2.12%) | 255,023 |
| Cambodia | 57,198 | 56,579 (-1.08%) | 56,579 |
| Burkina Faso, Malawi, Niger | — | unchanged | unchanged |

- **GhanaLSS:**
  - 208,090 of the 237,029 produced rows that lost their factor get it back on the new rung, all
    at their base value. That is exactly the item-keyed ceiling: the rows purchases alone had
    moved from `item_unit` / `item_unit_tight` to `none`.
  - The rung serves 87 cells, in 1991-92 to 2012-13.
  - Only 23.4% of the lost kilograms return. Measured row by row against base, the 1.10M kg
    still missing is:
    - 1.05M kg on 318,389 rows that moved from their own item cell to a purchase `unit`
      factor, which the ruling keeps above the fallback;
    - 42,104 kg from purchase factors that changed in place;
    - 10,063 kg on 28,939 rows whose base factor was u-pooled and which stay `none`.
  - `food_prices()` rows: 560,023 / 538,099 / **557,502**. `kgprice`: 187,096 / 151,446 /
    **182,675**, of which 1991-92 is 28,754 / 15,028 / **26,854**.
- **Cambodia, Serbia, Uganda, EthiopiaRHS, Burkina Faso, Malawi and Niger:** frame-equal to purchases only,
  with 0 rows on the rung.
- **Non-purchase `Expenditure` x1000** moves no factor by more than 2e-16. On base it moved
  700,073 GhanaLSS rows.
- **`unitvalue`:** GhanaLSS 1,676,514 -> 568,130 rows. Its 1,107,464 rows at exactly 1.0 are gone,
  and GLSS1-3 is empty in that mode.
- These are movements, not corrections. No external unit weight was scored.

**Cache.** Only GhanaLSS moves, through `nutrition.py`:
- 22 of 555 table hashes;
- 154 of 2,321 wave hashes;
- 1 of 38 framework-import fingerprints;
- 0 of 42 build-transform fingerprints.

**Cost.** The second, item-only estimate adds time to each GhanaLSS `food_kg_factors` call,
28 -> 44 s, and to Uganda's, 3.9 -> 5.7 s. Purchases-only countries pay nothing.

**Tests.**
- New `tests/test_gh1509_nonpurchase_fallback.py` (24 tests) and
  `tests/test_gh1509_1068_1129_food_unit_evidence.py` (24 tests).
- The pins these rulings change are moved, each with its reason.
- Data-gated run on 54 food files, warm private roots: this PR 2,593 passed, 0 failed;
  purchases-only head 2,569 passed, 0 failed. At the PR head, the pinned and #1509 files: 212
  passed, 0 failed.
- Full data-free suite: the failure set is identical to the purchases-only head (112 each, none
  a food or kg test).
- Data-free synthetic subset: 957 passed. The 4 that fail are `test_build_transform_hash.py`
  subprocess tests, which fail under that harness on every arm and pass in the data run.

Closes #1509, #1068, #1129.

---

### Close draft: #1509

> Implemented as ruled on 2026-11-12 ("all purchase rows") and as amended the same day ("Fallback
> rung, disclosed") in `<PR>`.
>
> - `food_kg_factors`' price ratio reads only `s == 'purchased'` rows, and every row is still
>   served.
> - A `(j, u)` cell with no priced purchase in any wave, which no purchase rung serves, reads a
>   separately labelled rung, `item_unit_nonpurchase`. It is the item estimator on the
>   non-purchase rows alone, with the same floors and gates.
> - It is counted in the partition and in `attrs['kg_factor_evidence']['fallback']`.
> - A cell with any purchase never reads it, even when that purchase evidence fails a gate.
>
> Measured cold, `food_quantities()` kg:
> - Cambodia -1.08% and Serbia -2.12%, unchanged by the rung.
> - Uganda -1.37%; its 18 u-pooled rows stay `none`.
> - GhanaLSS -10.0% against base (-13.0% under purchases alone). 208,090 of the 237,029 rows that
>   lost their factor get it back on the new rung, each at its old value. Of the 1.10M kg still
>   below base, 1.05M is rows now served by purchase-based `unit` factors, which the ruling keeps
>   above the fallback; 10,063 kg is 28,939 rows whose old factor was u-pooled.
> - `food_prices()` 557,502 rows; `kgprice` 182,675 (1991-92 26,854).
> - Burkina, Malawi and Niger are unchanged.
>
> Non-purchase values x1000 move no factor. Only GhanaLSS's cache hashes move. Closing.
>
> Not built, for a follow-up if wanted: a u-pooled non-purchase rung, which would return the
> 28,939 GhanaLSS rows.

### Close draft: #1068

> Option (a), as ruled ("drop and count"), is implemented in `<PR>`.
>
> - `food_prices(units='unitvalue')` drops `u='Unknown'` rows, legacy `Manquant` and a NaN `u`
>   (`_unit_sentinel_mask`), and counts them in `price_rows_dropped['unknown']`.
> - `food_quantities(units='units')` drops them and counts them in
>   `attrs['quantity_rows_dropped']['unknown']`.
> - An `Unknown` row with a zero quantity stays the loud GH #591 `inf`.
> - EthiopiaRHS's 46 served `Unknown` unitvalue rows are now 0.
> - Served `Unknown` rows in `units` are now 0: Malawi 1,864, Uganda 905, GhanaLSS 935,
>   Panama 443, Burkina 270, EthiopiaRHS 112, Niger 8.
>
> Not ruled, and left as they were; open a follow-up if wanted:
> - `units='kgs'` still carries `Unknown` rows natively.
> - `unitprice` keeps reported prices on `Unknown`.
>
> Closing.

### Close draft: #1129

> Option (a), as ruled, is implemented in `<PR>`. `food_prices(units='unitvalue')` drops
> `_CURRENCY_DENOMINATED_UNITS` rows and counts them in `price_rows_dropped['currency']`, the
> bucket the kg modes already use.
>
> - **GhanaLSS:** 1,676,514 -> 568,130 rows. All 1,107,464 rows at exactly 1.0 are gone, and
>   1987-88, 1988-89 and 1991-92 serve none.
> - **Panama:** 490,846 -> 483,661.
> - **Uganda:** 6,429 rows at 1.0 gone.
>
> `units='units'` keeps `Value` rows. It forms no ratio, and there `Quantity == Expenditure` on
> every GhanaLSS and Uganda `Value` row.
>
> CLAUDE.md's "Kwacha per Kwacha" sentence, the generated `food_prices` docstring and
> `docs/guide/coverage.md` are corrected. Closing.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
