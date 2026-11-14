## EHCVM-2 2021-22: wire Togo, Benin and Cote d'Ivoire, with the producer's undocumented constructions removed (addresses #1697)

Addresses #1697 (split from #552). This PR restores the three archived wirings (`refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104`) onto development and applies the rulings @ligon gave on 2026-11-12:
- "Go: restore and wire";
- "Same rule as #1575" (strata);
- "Extend Mali's derivation" (harvest);
- "Make Benin a panel country" (a follow-up, #1705, not built here);
- "Invert, registered" (Togo `s07bq03a`);
- "H to NA, flag L" (Cote d'Ivoire `s07bq03a`);
- "Accept the case fold" (Cote d'Ivoire strata, own key);
- "Decode 2018 with 2021 labels" (Togo 2018 units, #1557).

The governing principle (@ligon, 2026-11-12): *"Without a very good reason we don't want to serve up someone else's undocumented imputations."* Guinea-Bissau 2021-22 is not included.

### What it serves
For 2021-22, every table each country's earlier EHCVM wave serves, with two exceptions, as archived:
- Cote d'Ivoire has no `plot_labor`: the days and wages were asked but are not distributed (verdict filed).
- Benin has no `community_prices`: there is no `s05_co`.

Household ids are `E2_`-prefixed, so no cross-wave household link is served. `v` is the bare `grappe`. Weights are each wave's own `poids`, and `weight` = `panel_weight`. Strata are region (Benin: department) x milieu, by the #1575 construction.

### Togo 2021-22 `s07bq03a`: the producer's regional factor is divided out
The file's `s07bq03a`, and no other column, is the household's answer times 1.115 x {Centrale 1.5, Savanes 1.2, Plateaux 1.1, Kara 0.9, Maritime 0.9, Grand Lomé 0.82}, by the cover's region. Grand Lomé rows with own production or a gift carry 1.2 instead. No document explains the factor. The doc search is `burst/isa-sm-20261112/reports/ehcvm2-s07bq03a-docsearch/REPORT.md` §2, and its mechanics were re-measured on the file before wiring.

The factor is now divided out before the purchased residual `s07bq03a - s07bq04 - s07bq05` is formed. Each source row takes one of three rules, each under its own registered key, which sits beside the 7-day key on the purchased row:

| source rows (110,686) | rule | key `Togo::food_acquired::…` |
|---|---|---|
| 93,986: every unit but `unité`, plus 497 Grand Lomé `unité` rows with own production or a gift | `s07bq03a / f` | `s07bq03a-regional-factor-divided-out` |
| 12,665 `unité` rows outside Grand Lomé | the one integer k with floor(k x f) = `s07bq03a`; floor is one-to-one for f >= 1 | `s07bq03a-floored-count-recovered` |
| 4,035 `unité` rows that cannot be recovered | Quantity and Expenditure NA, never guessed | `s07bq03a-floored-count-withheld` |

The withheld rows break down as:
- 3,848 Grand Lomé purchase-only `unité` rows. floor(k x 0.9143) sends 1 to 0, and 607 of these are filed as 0 though the household reported consuming the item.
- 182 Grand Lomé `unité` rows with own production or a gift whose ×1.2 does not reproduce a quarter-grid count.
- 5 `unité` rows elsewhere that no integer reproduces.

A withheld row keeps its produced and in-kind rows, which are served as filed. It gets one NA purchased row where a purchase is implied: 3,895 rows.

**A reading to confirm.** The ruling withholds Lomé's rounded counts. Outside Grand Lomé, f ≥ 1.0035, so floor(k x f) is one-to-one on integers, and counts are recovered under their own key:
- 12,665 source rows, 10,039 served, 4,508 with changed counts;
- exact only if the household's count was an integer. In 2018, 99.5% are integers, so about 1 in 200 may have been a fraction.

Withholding these too is a one-line change, if preferred.

Measured cold, previous head `6538bc8b8` against this head:
- **Phantom purchases on `Jamais` rows**: 11,952 of 12,963 → **305** (2018: 166). The smallest remaining residual is 0.03.
- **Purchased rows**: 107,841 → 83,264. Produced (17,794) and in-kind (10,579) rows are unchanged.
- **`Expenditure` NA among purchased rows**: 35.3% → **21.0%**. Of the 17,525 NA rows, 3,895 are the withheld counts; excluding those, the share is 17.2% (2018: 19.0%).
- **Purchased `Expenditure`**: 34,464,072 → 29,435,020 FCFA. By region:

  | region | before | after |
  |---|---|---|
  | Centrale | 4,999,457 | 3,002,467 |
  | Savanes | 5,633,891 | 4,212,402 |
  | Plateaux | 7,166,796 | 5,922,533 |
  | Kara | 4,274,783 | 4,260,494 |
  | Maritime | 5,836,244 | 5,818,926 |
  | Grand Lomé | 6,552,901 | 6,218,198 |

- **`food_quantities(units='kgs')` 2021-22**: 70,309 → 58,744.
- **Derived tables**: the withheld rows are absent, never zeros.

**Must read before comparing waves.**
- The doc search's best reading is that the producer applied an under-reporting correction. Its regional multipliers follow, in rank order, each region's drop since 2018 in the number of foods households report (Spearman 0.93 over six regions).
- The doc search's approximate estimate is that without the factor, poverty in Centrale would rise about 20 points from 2018, against the official published +2.9. That figure is the doc search's estimate; it was not measured here.
- So a 2018 against 2021-22 comparison through the library will show a much steeper consumption fall than official statistics. That is what households reported, before the producer's adjustment.
- **The producer's filed values stay recoverable.** `Country('Togo').derivation_inputs('Togo::food_acquired::s07bq03a-regional-factor-divided-out')`, and the same call for the two count keys, returns each keyed row's `s07bq03a` as filed, with `s00q01`, `s07bq04` and `s07bq05`. `s07bq03a / recover_s07bq03a(...)` is the factor.
- The served quantity no longer reproduces UEMOA's `depan`, which carries the factor. This is by design.

### Cote d'Ivoire 2021-22 `s07bq03a`: H withheld, L flagged
**H rows** are positive `s07bq03a` values off the quarter grid with 4+ decimals, at relative tolerance 1e-6: 20,507 of 303,274. They read as the producer's outlier replacement. The rule is documented: UEMOA/World Bank, EHCVM Document de travail no 4, §2.2, "la valeur aberrante sera remplacée par la médiane dans la région et le milieu de résidence". Its application to this file is not documented anywhere.
- On H rows, `s07bq04` and `s07bq05` are rescaled too: 2,721 of 2,722 and 2,047 of 2,047 rows are off the grid.
- So every row an H source row serves (purchased, produced, in-kind) carries **Quantity NA and Expenditure NA** under `CotedIvoire::food_acquired::s07bq03a-outlier-replaced-withheld`.
- A served key that an H row shares with another row is withheld whole. Otherwise the core collapse would sum the other row alone and serve it as if whole (this affected 12 rows on the first candidate).
- **Counts:** 20,536 served rows (15,769 purchased, 2,722 produced, 2,045 in-kind). They held 3,035,627 FCFA, **2.5%** of purchased `Expenditure` (121,393,491 → 118,357,864).
- **Purchased `Expenditure` NA:** 17.5% → 23.1%.
- **Downstream:** `food_expenditures`, `food_quantities` and `food_prices` drop an NA before summing, so H rows are absent there, never zeros.

**L rows** are off the grid with ≤ 3 decimals: 25,784 source rows.
- They are served as filed under `CotedIvoire::food_acquired::s07bq03a-partial-triplet-conversion`, a flag with low-to-moderate confidence.
- **Counts:** 25,852 served rows, 1.9% of `Expenditure`.
- **2018-19 control:** 0 H rows at every tolerance, and 2,868 L-shaped rows, so the L flag over-covers by about that baseline.

### Togo 2018 units (#1557): the one change to an older wave
Bare codes 659, 660 and 662 now serve as `Main`, `Quartier` and `Tête`, the labels Togo's 2021-22 `s07b` gives them.
- **Evidence:** the 2021 unit-weight file `ehcvm_nsu_tgo2021.dta` uses the same codes for the same goods: 659 on Banane douce/Plantain, 660 on six meats, 662 on Igname/Manioc/Taro. The 2018 rows carry those goods.
- **Rows changed:** 2,059 2018 `food_acquired` rows change `u`, and nothing else:
  - `Tête` 1,512 and `Tête (98)` 4;
  - `Main` 518;
  - `Quartier` 25.

  By item, they are Igname 1,342, Banane douce 441, Manioc 153, Plantain 98 and meats 25.
- **No `Expenditure` moves.** The decode is on the `u` level only.
- `Togo/_/CONTENTS.org:466` is corrected. Code 114 (one row) stays a code.

### Cote d'Ivoire strata (red team F1)
2021-22 has its own key, `CotedIvoire::sample::region-milieu-stratum-upper-region`. Its function upper-cases the region and calls the shared construction. `rec.resolve()(derivation_inputs(...))` now re-derives all 12,965 served strata, with 0 mismatches. The old key is 2018-19's alone, and its assumption text is fixed. The served value is unchanged.

### Older waves (cold, private roots)
**Frame-equal to base `e5fdd2fa8`:**
- `sample`, `food_acquired`, `food_expenditures` and `food_quantities(units='units')` of every older wave, in all three countries.
- Togo 2018's `food_acquired` and `units` frames are equal once base's three codes are decoded.
- Benin is frame-equal to the previous head in both waves, every table.

**Pooled kg factors do move** (GH #1487 pools across a country's waves), on top of the movement already disclosed for wiring 2021-22. A row's unit value is unchanged by the division. But the 2021-22 evidence set changes. The cause is inferred from row counts; no decode-only build isolated it:
- valued phantom purchases leave;
- withheld rows leave;
- Togo's decoded units now pool with 2021-22's.

Measured against the previous head:

| | inferred kg, prev → this head | rows that move | median ratio (p10, p90) | `food_prices` rows that move (median ratio) |
|---|---|---|---|---|
| Togo 2018 | 66,944 → 68,062 (+1.7%; base 63,577) | 27,478 of 110,742 | 1.036 (0.992, 1.167) | 14,266 of 67,434 (0.966) |
| Cote d'Ivoire 2018-19 | 359,122 → 361,326 (+0.6%; base 358,449) | 66,494 of 296,942 | 1.032 (0.933, 1.25) | 40,452 of 182,890 (0.969) |
| Benin 2018-19 | 0 moved | | | |

### `derivation_inputs` joins, every new key
Joins are on `(t, i, j)` (strata on `(t, i)`).
- **One-to-one:** the Togo withheld key (3,895 = 3,895) and the Cote d'Ivoire strata key (12,965).
- **Exceptions, by `j` folds:** the Togo divided-out key joins 67,992 of its 69,330 served rows one-to-one, and the count key 10,035 of 10,039. The rest come from the `harmonize_food` folds (two file items under one served `j`):
  - 1,172 of the divided-out key's rows are pairs told apart by `u`;
  - **166 are each the sum of two source rows in one unit** (332 input rows), which the core collapse adds;
  - the count key's 4 are two pairs.

  So the brief's "one-to-one" holds except on those folds.
- **Cote d'Ivoire H/L:** one input row serves up to three rows, one per `s`. 7 H served rows share `(t, i, j)`.
- **Re-derivation:** from the inputs, the registered function reproduces the served quantity exactly on every one-to-one row (max |diff| 0.0).

### Must-state items (red team, 2026-11-12)
- **F1:** fixed, above.
- **F2a, F2b:** ruled and applied, above.
- **F3, cache cost.** Against base, `Country._table_cache_hash` and `Wave._input_hash` move:

  | country | table hashes | wave hashes |
  |---|---|---|
  | Togo | 22 of 22 | 44 of 44 |
  | Benin | 22 of 22 | 44 of 44 |
  | Cote d'Ivoire | 21 of 21 | 126 of 126 |
  | GhanaLSS | 21 of 21 | 147 of 147 |

  - **GhanaLSS is a full cold re-warm at both tiers**, through the `Très grand` entry in `_UNIT_SIZE_WORDS` via `nutrition.py`. No GhanaLSS served value changes. The standing hold says no shared-cache rebuild, so the merge stales GhanaLSS everywhere.
  - `build_transforms_fingerprint`: 0 of 40.
  - Every other country: 0.
  - **This head against the previous head:** only Togo (22/22, 44/44) and Cote d'Ivoire (21/21, 21/126) move.
  - The new `derived-from-data` basis moves nothing.
- **F4:** #1598 (own-country NSU weights) is **deferred, not unruled**. The GO ruling names it inside the food step, and #1598 says to sequence it after the wave wiring. It stays open. The three `CONTENTS.org` files are corrected.
- **F5:** Benin's 6,138 leavers (`s01q00aa` = non) are dropped, matching `ehcvm_individu`. This is a de facto choice pending #1565, and it is reversible.
- **F6:** Togo crop-state codes 3-5 (`Tubercule`, `Gousse`, `Régime`; 1,032 rows) map to `other_condition` **pending a ruling**. The answer is kept in `condition_native`.
- **F7:** the `assets` index `(t, i, j)` is not unique in old and new waves alike: in 2021-22, Togo 958, Benin 1,871 and Cote d'Ivoire 2,088. The `harmonize_assets` relabel of `j` runs in `_finalize_result` after the index collapse. It is pre-existing and this PR extends it. An issue is drafted in the report, not filed.
- **F8:** `ehcvm.harvest_totals` / `derive_hh_crop_harvest_total` duplicate Mali's rule as code, per the ruling "Extend Mali's derivation", and keep Mali out of the fingerprint. An edit to one does not reach the other.
- **Shocks:** 2021-22 serves only shocks with follow-ups (Togo 2,666 of 7,229; Benin 4,756 of 10,982; Cote d'Ivoire 7,528), so a cross-wave incidence reads as a false fall. Cote d'Ivoire's damaged keys raise `GrainCollapseWarning` on every read of `shocks` and `plot_inputs`.
- **Unlabelled codes:** Cote d'Ivoire crop codes 56-60 are served as numbers. 2021-22 `crop_production` has no `Quantity_kg`.

### Framework change
`derivations.BASES` gains `derived-from-data`: a regularity measured in the distributed file that no document states. Like `sampling.py`'s use of the term, it is not documentation. `docs/guide/derivations.md` and `tests/test_derivations.py` are updated. The change moves 0 hashes.

### Tests
The targeted data-gated selection has 38 files:
- every test file naming these countries or EHCVM;
- the GH1575 / gh812 strata files;
- sample, sampling, recall and the derivation registries;
- `test_build_transform_hash.py`, population, panel ids, size pool and shocks.

Each run used `-n 8 --dist=loadfile` in a fresh private root, with BLAS threads pinned and `LSMS_BUILD_WORKERS=1`.

| side | targeted | schema (`-k` the three countries) |
|---|---|---|
| this head `9b5585cf2` | **2,849 passed, 0 failed**, 83 skipped, 4 xfailed | 26 passed |
| base `e5fdd2fa8` | 2,627 passed, 0 failed, 86 skipped, 4 xfailed (without the three 2021-22 wave files) | 26 passed |

- No Togo, Cote d'Ivoire or EHCVM wave test was skipped for want of data.
- Re-pinned on purpose:
  - `test_ehcvm_7day`: the `AFTER` totals for Togo and CIV 2021-22, the allowed keys, and the in-kind NA for CIV H;
  - both wave oracles (now independent re-implementations of the inversion and the H/L split);
  - the CIV strata key scope in `test_gh1575_cotedivoire…` and `test_gh812_ehcvm_strata`;
  - `test_derivations`, which now also pins `derived-from-data`.
- In-tree parquets after every job: none.

### Residuals
- #1598 (NSU weights, three countries);
- #1565 (leavers);
- #1566 (`s12q09`);
- #1705 (Benin link);
- the F6 ruling;
- the F7 issue;
- Guinea-Bissau 2021-22;
- Togo's link (not ruled).

Report: `burst/isa-sm-20261112/reports/ehcvm2-2021-final2/REPORT.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
