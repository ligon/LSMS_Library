## EHCVM-2 2021-22: wire Togo, Benin and Cote d'Ivoire (addresses #1697)

Addresses #1697 (split from #552). Rulings by @ligon on 2026-11-12: "Go: restore and wire", "Same rule as #1575" (strata), "Extend Mali's derivation" (harvest) and "Make Benin a panel country" (a follow-up, #1705). This PR restores the three archived wirings (`refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104`, 2026-11-04) onto current development, brings them under development's rulings and applies the day's rulings. Guinea-Bissau 2021-22 is not included.

### What it serves
For 2021-22, every table each country's earlier EHCVM wave serves. Two exceptions, as archived:
- Cote d'Ivoire has no `plot_labor`: the days and wages were asked but are not distributed.
- Benin has no `community_prices`: there is no `s05_co`.

Household ids are `E2_`-prefixed, so they cannot meet the earlier wave's, and no cross-wave household link is served. `v` is the bare `grappe`, because the clusters are the same (`_/sampling.yml`: `stable`). Weights are each wave's own `poids`: `weight` = `panel_weight`, since no longitudinal weight is shipped.

### Strata, on the #1575 rule
2021-22 `sample.strata` is region (Benin: department) x milieu, built the same way as the earlier wave:
- the shared `ehcvm.strata_label` and `stamp_region_milieu_stratum`;
- `'2021-22'` added to each `<C>::sample::region-milieu-stratum` entry's `waves`;
- each wave read from its own cover;
- a missing or blank label gives NA, and a code is refused.

Measured on a cold `sample()`, in private roots:

| | 2018 / 2018-19 labels | 2021-22 labels | shared |
|---|---|---|---|
| Togo | 11 | 11 | 10 |
| Benin | 23 | 23 | 23 |
| Cote d'Ivoire | 66 | 66 | 66 |

- **Togo** serves region 6 under each wave's own label, unreconciled: `Lomé commune Urbain` in 2018 (1,034 households) and `Grand Lomé Urbain` in 2021-22 (1,169). It is the one stratum that a cross-wave group still splits.
- **Cote d'Ivoire** upper-cases the region before calling the shared helper, as the wave's `Region` formatter already does. The file labels 23 of its 33 regions in lower case on 2018-19's codes, so without the fold 46 of the 66 strata would split by case. The registry `rule` says so. This is a choice; please confirm it.
- **Benin**'s weights-file `strat` is recorded in `CONTENTS.org` as a comparison and not served. It is **department x vague** (`strat` = 10 x department + vague on 8,032 of 8,032 households, constant within grappe): not region x milieu, as the restored YAML comment said, and not an alternative to the DDI's 23 strata. Each constructed stratum spans its department's two `strat` codes.
- In all three countries:
  - no stratum is NA;
  - the key is on every row with a stratum, and on no other;
  - the region part equals `cluster_features.Region` and the milieu part equals `Rural`;
  - the earlier waves are frame-equal to base;
  - the 2021-22 `v`, `Rural`, `weight` and `panel_weight` are frame-equal to the pre-change head `df1df60ed`.

### Other rulings and pointers
- **Harvest from dispositions** (Benin and Cote d'Ivoire 2021-22) is kept as wired. The registry entries now say it extends Mali's ruled derivation. #1437 stays open.
- **Benin panel link** is ruled, but it is a follow-up PR (#1705) and is not built here. `sampling.yml` and `CONTENTS.org` point to it.
- **Benin leavers.** `household_roster` leaves out the 6,138 people `s01` lists as gone, as `ehcvm_individu` does. #1565, which is still undecided, says that decision should precede this step. Flagged; behaviour unchanged.

### What changed from the archive, because development moved
- **Shocks follow the #1556 / #1561 ruling.** In all three waves `AffectedConsumption` = `s14bq04f` (purchases) and `AffectedFoodStock` = `s14bq04e` (stock), checked against each file's own labels.
- **`ehcvm.py`.** Git kept two silent copies of `harvest_totals` / `derive_hh_crop_harvest_total`. One identical copy is kept.
- **Generated records.** `food_recall.csv` (#851) and the population source are merged as sources and regenerated.

### Unchanged, measured (cold, private roots, base `e5fdd2fa8`)
- Every older wave of every non-derived table is frame-equal to base, with the same household ids. The new optional columns are NA there.
- No weight and no panel id of an existing wave changes.

### Moves, disclosed (GH #1487 pooling)
The older waves' inferred kilograms move. Kilograms on rows served `u = kg`:
- Togo 2018: +5.3%;
- Benin 2018-19: +11.1%;
- Cote d'Ivoire 2018-19: +0.2%.

`food_prices` medians among moved rows are 0.87, 0.82 and 0.99. Native `Quantity` and `Expenditure` do not move.

### GhanaLSS
**No served value changes.** GhanaLSS was built cold for base and candidate, each in its own private root:

| table | rows | frame | `attrs` |
|---|---|---|---|
| `food_acquired` | 5,328,568 | exactly equal (same order, same dtypes) | equal |
| `food_quantities()` | 1,623,676 | exactly equal | equal |
| `food_prices()` | 560,023 | exactly equal | equal |
| `nutrition` | 34,798 | exactly equal | equal |

The archived `Très grand` is the only word added to `_UNIT_SIZE_WORDS`, and no GhanaLSS `u` label carries any size suffix the vocabulary recognises (0 rows), so the size pool has nothing to pool. Its 21 moved hashes are a re-warm to the same values.

### Cache
- `Country._table_cache_hash` moves for Togo (22/22), Benin (22/22), Cote d'Ivoire (21/21) and GhanaLSS (21/21). GhanaLSS moves through the `Très grand` entry in `_UNIT_SIZE_WORDS`, via `nutrition.py`.
- `build_transforms_fingerprint`: 0/40.
- Every other country: 0. The strata change adds no country to this scope.
- No shared cache was touched.

### Tests
Targeted data-gated selection: 38 files, namely every test file naming these countries or EHCVM, the GH1575 / gh812 strata files, sample, sampling, recall, the derivation registries, `test_build_transform_hash.py`, population, panel ids, size pool and shocks. Run with `-n 8 --dist=loadfile` in a fresh private root.

| side | targeted | schema (`-k` the three countries) |
|---|---|---|
| this head `6538bc8b8` | 2,845 passed, 0 failed | 26 passed |
| base `e5fdd2fa8` | 2,627 passed, 0 failed (without the three 2021-22 wave files) | 26 passed |

No strata or wave test was skipped. In-tree parquets after every job: none.

### Residuals
- #1598: own-country NSU weights for all three countries.
- #1557 and #1566.
- Guinea-Bissau 2021-22.
- The survey-kg `Quantity_kg` of 2021-22 `crop_production` for Benin and Cote d'Ivoire.
- Togo's link, which is not ruled. Togo's `CONTENTS.org` measures `panel` = 1 at real-link level, where #1705's text says Togo's flagged keys sit at the baseline.
- `ehcvm.py`'s comment that "Benin, Togo, Guinea-Bissau and Cote d'Ivoire serve the region alone" is now stale for every wave. It was left alone to keep the hash scope.

Report: `burst/isa-sm-20261112/reports/ehcvm2-2021-final/REPORT.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
