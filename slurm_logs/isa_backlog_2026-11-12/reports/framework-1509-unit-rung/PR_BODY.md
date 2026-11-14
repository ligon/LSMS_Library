**food kg evidence is purchases, with two disclosed non-purchase fallback rungs; `unitvalue` / `units` drop `Value` and `Unknown` (#1509, #1068, #1129)**

This PR implements four rulings by @ligon, all of 2026-11-12. All the code is read-time code in
`lsms_library/transformations.py`.

**#1509, "all purchase rows", as amended twice ("Fallback rung, disclosed"; "Add a unit-level
rung").**
- `food_kg_factors`' price-ratio inference reads only rows whose `s` is exactly `'purchased'`.
  Both `conversion_to_kgs` arms, the #1450 size pool and the #1452 tableless estimate read
  purchases only. Every row is still served.
- **The one exception sits on two rungs of its own,** between `unit` and `none`:
  - `FOOD_KG_FACTOR_LAYERS = ('survey_kg', 'metric', 'item_unit', 'item_unit_tight', 'unit',
    'item_unit_nonpurchase', 'unit_nonpurchase', 'none')`.
  - `item_unit_nonpurchase` is the item estimator on the non-purchase rows alone. It keeps the
    same `min_reports`, `min_baseline`, tight exception and dispersion gate.
  - `unit_nonpurchase` is the u-pooled `unit` estimator on the same rows: one factor per label,
    pooled over items. It has `unit`'s gates and no others, so a row the item fallback refuses
    lands there, as a refused purchase row lands on `unit`.
  - Both serve a row only if the row's own `(j, u)` cell holds no priced purchase in any wave
    read, and no rung above serves it. The cell is judged on the item arm's join key; for an
    unsized label, on its size-pooled family.
  - **A cell with any purchase evidence never reads either rung, even when that evidence fails a
    gate.**
- Both ratios are self-valued on both sides. The size pool and the #1452 overlay apply to both.
  Sentinel units, currency units and `no_kg` declarations refuse both.
- **Disclosed** in four places:
  - the partition, `attrs['kg_factor_sources']`, which still sums to the frame;
  - the per-row `KgFactorSource`;
  - per-row columns: the candidates `kg_item_unit_nonpurchase` and `kg_unit_nonpurchase`, and
    `kg_cell_has_purchase` (the eligibility test);
  - `attrs['kg_factor_evidence']['fallback']`: rows, cells, `s`, tight and single-wave counts;
    the item gate's refusals and where they landed; a `unit_rung` block; and a `none_by_reason`
    partition of the `none` rows.
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

**Measured cold in private roots.** Each later arm was measured against the earlier ones:
- base `e5fdd2fa8`;
- purchases only, `6c14097e9`;
- with the item fallback, `5b2496a12`;
- this PR, `ee9d6cd23`. `transformations.py` is byte-identical at the PR head, `0966d62ef`.

| `food_quantities()` kg | base | purchases only | + item fallback | this PR |
|---|---|---|---|---|
| GhanaLSS | 11.01M | 9.57M (-13.0%) | 9.909M | **9.919M (-9.89%)** |
| Uganda | 1,241,553 | 1,224,605 (-1.37%) | 1,224,605 | **1,224,670 (-1.36%)** |
| Serbia | 260,550 | 255,023 (-2.12%) | 255,023 | 255,023 |
| Cambodia | 57,198 | 56,579 (-1.08%) | 56,579 | 56,579 |
| Burkina Faso, Malawi, Niger, EthiopiaRHS | — | unchanged | unchanged | unchanged |

- **GhanaLSS:**
  - Purchases alone cost 237,029 produced rows their factor. All of them come back, each at its
    base value:
    - 208,090 on the item rung (87 cells), the rows purchases alone had moved off their own item
      cell;
    - 28,939 on the unit rung (89 cells, 7 labels), the rows whose base factor was u-pooled.
  - Both rungs serve 1991-92 to 2012-13 only. An independent recount finds 0 purchases in any
    served cell.
  - `food_prices()` is 560,023 rows and `kgprice` 187,096 (1991-92 28,754). Both are back at
    base, wave by wave.
  - The 1.09M kg still below base is all on rows a purchase rung serves at another factor, which
    the rulings keep above both fallbacks:
    - 1.05M kg on rows moved from their own item cell to a purchase `unit` factor;
    - 42,104 kg from purchase factors that changed in place.
- **Uganda:** 15 of the 18 rows purchases alone left without a factor land on the unit rung, at
  factors within 2x of base's (median ratio 0.77): +65 kg, +15 `food_prices()` rows. 8 of them
  sit on a label (`Sack (unspecified)`) that has purchases of other items but no purchase `unit`
  factor; the condition is the row's `(j, u)` cell, as ruled. The other 3 stay `none`.
- **Cambodia, Serbia, EthiopiaRHS, Burkina Faso, Malawi and Niger:** frame-equal with and
  without the rungs.
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

**Cost.** The one non-purchase estimate, which gives both rungs, adds time to each GhanaLSS
`food_kg_factors` call: 26 -> 52 s against base, measured on different nodes. Uganda goes
3.2 -> 6.3 s. Purchases-only countries pay nothing.

**Tests.**
- New files: `tests/test_gh1509_unit_nonpurchase_rung.py` (19 tests),
  `tests/test_gh1509_nonpurchase_fallback.py` (24) and
  `tests/test_gh1509_1068_1129_food_unit_evidence.py` (24).
- The pins these rulings change are moved, each with its reason.
- Data-gated run on 54 food files, warm private roots: this PR 2,612 passed, 0 failed; the
  item-fallback head 2,593 passed, 0 failed; base (51 files) 2,544 passed, 0 failed.
- At the PR head, the named food-kg, #1509, currency and #1487 files plus
  `test_build_transform_hash.py`: 381 passed, 0 failed.
- Full data-free suite: the failure set is identical to the item-fallback head's (113 each, none
  a food or kg test). The one beyond the earlier 112 is a CI-wrapper JUnit-mapping test that
  fails on both arms in the full run and passes alone on both.
- Data-free synthetic subset: 1,017 passed. The 4 that fail are `test_build_transform_hash.py`
  subprocess tests, which fail under that harness on every arm and pass in the data run.

Closes #1509, #1068, #1129.

---

### Close draft: #1509

> Implemented in `<PR>` as ruled on 2026-11-12 ("all purchase rows") and as amended the same day
> ("Fallback rung, disclosed"; "Add a unit-level rung").
>
> - `food_kg_factors`' price ratio reads only `s == 'purchased'` rows, and every row is still
>   served.
> - A `(j, u)` cell with no priced purchase in any wave, which no purchase rung serves, reads one
>   of two separately labelled rungs:
>   - `item_unit_nonpurchase`, the item estimator on the non-purchase rows alone, with the same
>     floors and gates;
>   - below it, `unit_nonpurchase`, the u-pooled `unit` estimator on the same rows, with `unit`'s
>     gates.
> - Both are counted in the partition and in `attrs['kg_factor_evidence']['fallback']`.
> - A cell with any purchase never reads either, even when that purchase evidence fails a gate.
>
> Measured cold, `food_quantities()` kg:
> - Cambodia -1.08% and Serbia -2.12%, unchanged by the rungs.
> - Uganda -1.36%. 15 of the 18 rows that lost their factor are back on the unit rung, within 2x
>   of their old factor; 3 stay `none`.
> - GhanaLSS -9.89% against base (-13.0% under purchases alone). All 237,029 rows that lost
>   their factor get it back, each at its old value: 208,090 on the item rung and 28,939 on the
>   unit rung. The 1.09M kg still below base is rows now served by purchase-based factors,
>   which the ruling keeps above both fallbacks.
> - GhanaLSS `food_prices()` is 560,023 rows and `kgprice` 187,096 (1991-92 28,754), both as
>   before #1509.
> - Burkina, Malawi, Niger and EthiopiaRHS are unchanged.
>
> Non-purchase values x1000 move no factor. Only GhanaLSS's cache hashes move. Closing.

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
