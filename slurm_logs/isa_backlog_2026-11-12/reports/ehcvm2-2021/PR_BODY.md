## EHCVM-2 2021-22: wire Togo, Benin and Cote d'Ivoire (#552)

Ruling @ligon 2026-11-12, "Go: restore and wire". This restores the three archived wirings (`refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104`, 2026-11-04) onto current development and brings them under its rulings. Guinea-Bissau is not included.

### What it serves
Every table each country's earlier EHCVM wave serves, for 2021-22, with these exceptions as archived: Cote d'Ivoire has no `plot_labor` (asked, not distributed), and Benin has no `community_prices` (no `s05_co`). Ids are `E2_`-prefixed, so they cannot meet the earlier wave's: no cross-wave household link is served, which is a decision for the maintainer. `v` is the bare `grappe`, since the clusters are the same (`_/sampling.yml`: `stable`). Weights are each wave's own `poids` (`weight` = `panel_weight`; no longitudinal weight is shipped).

### What changed from the archive, because development moved
- **Shocks, on the #1556/#1561 ruling.** In all three waves, `AffectedConsumption` = `s14bq04f` (purchases) and `AffectedFoodStock` = `s14bq04e` (stock), checked against each file's own labels.
- **`ehcvm.py`.** The Benin and CIV refs each added `harvest_totals` / `derive_hh_crop_harvest_total`, and git kept both silently; Benin's identical copy is kept.
- **Generated records.** `food_recall.csv` (#851) and the population source are merged as sources and regenerated; the recall count is now "58 of 70".
- **Tests.** Three development strata tests pinned "2021-22 has no `sample`" and are narrowed to the entry's `waves`.

### Unchanged, measured (cold, private roots, base `e5fdd2fa8`)
- Every older wave of every non-derived table is frame-equal to base, with the same household ids.
- New optional columns are NA on the older waves.
- No weight and no panel id of an existing wave changes.

### Moves, disclosed (GH #1487 pooling)
The older waves' inferred kilograms move. Kilograms on rows served `u = kg` go Togo 2018 +5.3%, Benin 2018-19 +11.1% and CIV 2018-19 +0.2%. `food_prices` medians among moved rows are 0.87, 0.82 and 0.99. Native `Quantity` / `Expenditure` do not move.

### Cache
- `Country._table_cache_hash`: Togo 22/22, Benin 22/22, CotedIvoire 21/21, and GhanaLSS 21/21. GhanaLSS moves because of `Très grand` in `_UNIT_SIZE_WORDS`, through `nutrition.py`.
- `build_transforms_fingerprint`: 0/40.
- Niger, Mali, Senegal, Burkina and Guinea-Bissau: 0.
- No shared cache was touched.

### Questions for the maintainer
1. **Strata.** 2021-22 keeps region-only strata: GH #1575 was ruled for the earlier wave alone. The waves now share no strata label (Togo 11 vs 6, Benin 23 vs 12, CIV 66 vs 33). Should 2021-22 be aligned, and for Benin, through the documented construction or the weights file's own `strat`?
2. **Benin panel link.** Benin's `PanelHH` sits at real-link level (head sex 94.8%). Should Benin become a panel country? Togo and CIV do not qualify.

### Tests
On 125 data-gated test files (`tests/*.py` naming these countries or EHCVM, plus sample, sampling, recall, hashes, units, kinship, weights, provenance), base passes 5,921, and this branch 6,121 passed, 0 failed (jobs 41236417 / 41236416). All of `tests/` with `-m "not requires_s3"`: 15,168 passed, 0 failed (base 15,109; jobs 41236819 / 41236820). `scan.py` over the three countries finds no new fail.

Residuals: #1598 (own-country NSU), #1557, #1566 (checked: Togo 3.7%, CIV 4.1% of `s12q09` off the 25 grid), Guinea-Bissau 2021-22, and an out-of-scope `derivation_inputs` wave that is not refused.

Report: `burst/isa-sm-20261112/reports/ehcvm2-2021/REPORT.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
