## EHCVM-2 2021-22: wire Togo, Benin and Cote d'Ivoire, with the producer's undocumented constructions removed (addresses #1697)

Addresses #1697 (split from #552). This PR restores the three archived wirings (`refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104`) onto development and applies the rulings @ligon gave on 2026-11-12:
- "Go: restore and wire";
- "Same rule as #1575" (strata);
- "Extend Mali's derivation" (harvest);
- "Make Benin a panel country" (a follow-up, #1705, not built here);
- "Invert, registered" (Togo `s07bq03a`);
- "Recover; a 0 becomes NA" (Togo floored counts, after the second red team);
- "H to NA, flag L" (Cote d'Ivoire `s07bq03a`);
- "Withhold all three" (Cote d'Ivoire H scope, after the second red team);
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
| 12,644 `unité` rows outside Grand Lomé | the one positive integer k with floor(k x f) = `s07bq03a`; floor is one-to-one for f >= 1 | `s07bq03a-floored-count-recovered` |
| 4,056 `unité` rows that cannot be recovered | Quantity and Expenditure NA, never guessed | `s07bq03a-floored-count-withheld` |

The withheld rows break down as:
- 3,848 Grand Lomé purchase-only `unité` rows. floor(k x 0.9143) sends 1 to 0, and 607 of these are filed as 0 though the household reported consuming the item.
- 182 Grand Lomé `unité` rows with own production or a gift whose ×1.2 does not reproduce a quarter-grid count.
- 5 `unité` rows elsewhere that no integer reproduces.
- **21 `unité` rows elsewhere filed as 0, which recover to 0** (new in this round; below).

A withheld row keeps its produced and in-kind rows, which are served as filed. It gets one NA purchased row where a purchase is implied (no own production and no gift, a positive filed residual, or a count that recovered to 0): **3,916 rows** (3,895 before the recovered-0 ruling).

**Counts that recover to 0 (ruling "Recover; a 0 becomes NA (Recommended)").** The ruling keeps the exact integer recovery outside Grand Lomé, serves any count that recovers to 0 NA under the withheld key so that no reported item disappears, and asks for the probable fractional counts to be disclosed. It settles the second red team's N3 and N3b as one decision: the integer reading stands outside Grand Lomé, and Grand Lomé's floored counts stay withheld.
- **What changed.** A count outside Grand Lomé filed as 0 is the floor of a count below 1/f, so a fraction. It now takes the withheld branch. Measured: **21 rows**, all with `s07bq02` = `Oui`, in Kara 9, Centrale 4, Plateaux 4, Savanes 3 and Maritime 1.
- **The 11 with no own production or gift** were served no row at all before, so the item vanished. Each now gets an NA purchased row.
- **The 10 with own production or a gift**, each below 1/f (0.5 ×6, 0.25 ×3, 0.16 ×1), kept their produced and in-kind rows. They also get an NA purchased row, because a filed 0 leaves the filed-residual test silent. **This is a literal reading, for the maintainer to confirm:** `s07bq06` puts none of the ten's last purchase within 30 days (`Jamais` 4, `Plus de 30 jours` 6), so 4 NA rows sit on `Jamais` rows. Serving those ten no purchased row instead, as every other withheld row with own production or a gift and no positive filed residual is served, is one clause in `_purchase_implied`. Either way the derived tables drop the NA, so no total moves.
- **Probable fractional counts**, from the red team's simulation (Q1b and N3, not re-run here):
  - 2018, the same instrument without the factor, has 79 fractional counts among 16,410 positive `Unité` rows (0.48%). 65 of the 79 are half Maggi cubes, so the rate is concentrated in one item, and that item's 2021-22 rate is not measured.
  - Simulated on 2018's counts, an integer comes back exactly. A fraction comes back as an integer, typically 0.5 below it and never beyond 0.75 either way, or is withheld.
  - That puts **about 54 of the 12,670 `unité` rows outside Grand Lomé (0.43%)** at a probable fractional count, about one in five filed as 0. The 21 filed as 0 are measured and are now withheld. The two numbers are not reconciled.

**The purchases the division removes are a registered deletion** (red team N4): `Togo::food_acquired::s07bq03a-purchase-removed-by-division`, `rows: 0`.
- **Rule.** A source row whose filed residual is above 0 (the reshape's own test) and whose recovered residual is 0 or below is served no purchased row. Its produced and in-kind rows are served as filed.
- **Inputs.** `inputs_purchase_removed_by_division` returns exactly those source rows, with the filed `s07bq03a`. No served row carries the key, and `attrs['derivations']` lists it at 0.
- **Measured: 25,234 rows** (24,489 divided, 745 count-recovered), equal to the red team's count.
  - By `s07bq06`: `Jamais` 11,645, `Plus de 30 jours` 11,871, within 30 days 1,718 (`7 dernier jours` 1,108, `30 derniers jours` 462, `Hier` 148).
  - Every one reports own production or a gift.
  - The filed residual is above 1e-6 on all of them. The recovered residual is exactly 0 on 25,224 and below 0 on 10.
- **Partition.** The 108,057 source rows with a positive filed residual split into 79,535 served from the recovered value, 25,234 deleted and 3,288 withheld and served NA.
- **Grain.** 155 of the deleted rows share `(t, i, j)` with a served purchased row (a `harmonize_food` fold partner), so the deletion is at source-row grain.
- **Value.** At the last purchase's unit value, where it was in the consumption unit, the deleted filed residual was worth about 127k FCFA on 1,599 rows. This is approximate: units are compared on the labels as filed, not on the hook's folded codes.

Measured cold, previous-previous head `6538bc8b8` → previous head `9b5585cf2` (final2 report) → this head:
- **Phantom purchases on `Jamais` rows**: 11,952 of 12,963 → **305** (2018: 166). The smallest remaining residual is 0.03.
- **Purchased rows**: 107,841 → 83,264 → **83,285**. Produced (17,794) and in-kind (10,579) rows are unchanged.
- **`Expenditure` NA among purchased rows**: 35.3% → 21.0% → **21.1%** (17,546). 3,916 of those are the withheld counts; excluding them, the share is 13,630 of 79,369 (17.2%), against 19.0% in 2018.
- **Purchased `Expenditure`**: 34,464,072 → 29,435,020 FCFA, unchanged in this round. By region:

  | region | before | after |
  |---|---|---|
  | Centrale | 4,999,457 | 3,002,467 |
  | Savanes | 5,633,891 | 4,212,402 |
  | Plateaux | 7,166,796 | 5,922,533 |
  | Kara | 4,274,783 | 4,260,494 |
  | Maritime | 5,836,244 | 5,818,926 |
  | Grand Lomé | 6,552,901 | 6,218,198 |

- **`food_quantities(units='kgs')` 2021-22**: 70,309 → 58,744, unchanged in this round.
- **Derived tables**: the withheld rows are absent, never zeros. Both waves' derived tables are frame-equal to `9b5585cf2`.

**Must read before comparing waves.**
- The doc search's best reading is that the producer applied an under-reporting correction. Its regional multipliers follow, in rank order, each region's drop since 2018 in the number of foods households report (Spearman 0.93 over six regions).
- The doc search's approximate estimate is that without the factor, poverty in Centrale would rise about 20 points from 2018, against the official published +2.9. That figure is the doc search's estimate; it was not measured here.
- So a 2018 against 2021-22 comparison through the library will show a much steeper consumption fall than official statistics. That is what households reported, before the producer's adjustment.
- **The producer's filed values stay recoverable.** `Country('Togo').derivation_inputs('Togo::food_acquired::s07bq03a-regional-factor-divided-out')`, and the same call for the count keys and the deletion, returns each row's `s07bq03a` as filed, with `s00q01`, `s07bq04` and `s07bq05`. `s07bq03a / recover_s07bq03a(...)` is the factor.
- The served quantity no longer reproduces UEMOA's `depan`, which carries the factor. This is by design.

**Disclosed, not fixed** (the second red team's numbers):
- **Grand Lomé's own/gift misfit (N2).** Of the 1,830 positive non-`unité` Grand Lomé rows with own production or a gift, 1,686 land on the 0.01 grid at 1.2, 122 only at 0.9143 and 22 at neither. The predicate divides all of them by 1.2. On the 122, the served purchased quantity is 122.5 units against 216.9 at 0.9143: about 18.5k against 36.9k FCFA at each row's own unit value, so **about 18k FCFA understated, 0.3% of Grand Lomé's 6.22M**. The alternative, dividing by whichever factor lands on the grid, is offered and not applied.
- **The 5x cluster.** 76 of the 305 `Jamais` residuals sit at exactly `s07bq03a / (q4 + q5)` = 5.0175 = 5 x 1.0035, in Kara and Maritime. That is either a reported 5x own production or a unit slip. No 2018 control was run.
- **Purchase-only rows off the grid.** After the division, about 2,200 purchase-only rows sit off the quarter grid beyond 2018's share (Grand Lomé about 770, Kara about 470, Maritime about 450). The cause is not established; Litre and Bouteille are the low units.
- **The cover is not hashed (N5).** The hook and the `inputs` callables read `s00_me_tgo2021.dta` by a constructed path. `food_acquired` is YAML-path and `Wave._input_hash` folds only the declared sources' sidecars, so a re-released cover alone would not invalidate it. This is CLAUDE.md cache limitation (a).

### Cote d'Ivoire 2021-22 `s07bq03a`: H withheld, L flagged
**H rows** are positive `s07bq03a` values off the quarter grid with 4+ decimals, at relative tolerance 1e-6: 20,507 of 303,274. They read as the producer's outlier replacement. The rule is documented: UEMOA/World Bank, EHCVM Document de travail no 4, §2.2, "la valeur aberrante sera remplacée par la médiane dans la région et le milieu de résidence". Its application to this file is not documented anywhere.
- **Ruled: "Withhold all three (Recommended)".** On an H row, consumption, own production and gifts are all NA (20,536 served rows), since none is the household's answer. That is what was already built; this round confirms it on the final head (the CIV wave test and `test_ehcvm_7day` pass) and records the ruling in CONTENTS and the H key's evidence.
- **Why all three.** On H rows, `s07bq04` and `s07bq05` are rescaled too: 2,722 of 2,722 and 2,047 of 2,047 positive values are off the grid, and no on-grid `s07bq03a` row carries an H-like `s07bq04` or `s07bq05`.
- So every row an H source row serves (purchased, produced, in-kind) carries **Quantity NA and Expenditure NA** under `CotedIvoire::food_acquired::s07bq03a-outlier-replaced-withheld`.
- **Collisions.** A served key that an H row shares with another row is withheld whole. Otherwise the core collapse would sum the other row alone and serve it as if whole (this affected 12 rows on the first candidate). The red team sized those partners from the wave test's oracle: **about 12 purchased units, about 6.7k FCFA (0.006% of purchased `Expenditure`), about 2 in-kind units and 0 produced**. Their filed answers are in the 7-day key's inputs only.
- **Counts:** 20,536 served rows (15,769 purchased, 2,722 produced, 2,045 in-kind). They held 3,035,627 FCFA, **2.5%** of purchased `Expenditure` (121,393,491 → 118,357,864).
- **Purchased `Expenditure` NA:** 17.5% → 23.1%.
- **Downstream:** `food_expenditures`, `food_quantities` and `food_prices` drop an NA before summing, so H rows are absent there, never zeros.
- **Large values.** The relative decimal test cannot see four decimals above about 500. H has 0 rows above 100. An absolute test flags 6,126 more rows, of which about 26 change class at a relative 1e-7, so there are at most about 26 hidden H rows. The float32 reading of the rest is an inference, not a round-trip test.

**L rows** are off the grid with ≤ 3 decimals: 25,784 source rows.
- They are served as filed under `CotedIvoire::food_acquired::s07bq03a-partial-triplet-conversion`, a flag with low-to-moderate confidence.
- **Counts:** 25,852 served rows, 1.9% of `Expenditure`.
- **2018-19 control:** 0 H rows at every tolerance, and 2,868 L-shaped rows, so the L flag over-covers by about that baseline.
- **N8:** 12,353 of the 12,354 L purchased groups equal the filed residual. The other differs by 5.0, most likely a served-key collision summed by the core.

No Cote d'Ivoire build input changed in this round: its records are text only, and no Cote d'Ivoire hash moves against `9b5585cf2`.

### Togo 2018 units (#1557): the one change to an older wave
Bare codes 659, 660 and 662 now serve as `Main`, `Quartier` and `Tête`, the labels Togo's 2021-22 `s07b` gives them.
- **Evidence:** the 2021 unit-weight file `ehcvm_nsu_tgo2021.dta` uses the same codes for the same goods: 659 on Banane douce/Plantain, 660 on six meats, 662 on Igname/Manioc/Taro. The 2018 rows carry those goods. The one weak spot: Plantain under 662 in 2018 (21 rows) appears under 662 in neither 2021 file.
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
**Frame-equal to base `e5fdd2fa8`** (final2 report):
- `sample`, `food_acquired`, `food_expenditures` and `food_quantities(units='units')` of every older wave, in all three countries.
- Togo 2018's `food_acquired` and `units` frames are equal once base's three codes are decoded.
- Benin is frame-equal to the previous head in both waves, every table.

**Frame-equal to the previous head `9b5585cf2`** (this round): every Togo 2018 table built (`sample`, `food_acquired`, `food_expenditures`, `food_quantities` in kgs and units, `food_prices`), and all of 2021-22 except the 21 added NA purchased rows. No served value changes and no kg factor changes (0 of 224,254 rows). Togo 2018 serves **68,062.04 kg** on `u` = `kg`, as before.

**Pooled kg factors do move against base** (GH #1487 pools across a country's waves), on top of the movement already disclosed for wiring 2021-22. The second red team split it (N1, its jobs 41251020 and 41251201):
- **Togo 2018:** the +1,118 kg (66,944 → 68,062 against `6538bc8b8`) is the 2018 decode alone, **+624 kg (+0.9%) on 1,875 decoded rows** (`Tête (Moyen/Petit/Grand)` 1,512, `Main` 363, all on the u-pooled `unit` rung, now pooled with 2021-22's labels), plus the 2021-22 changes alone, **+492 kg (+0.7%) on 25,607 other rows** (by rows `Unité` 8,599, `Bol (Grand)` 7,105, `Unité (Moyen)` 2,136; by kg `Unité (Moyen)` +315, `Unité` +129). The interaction is about 1 kg. By rows the 2021-22 effect is larger; by kilograms the decode is. The decode moves no native `Quantity` or `Expenditure`.
- **Cote d'Ivoire 2018-19:** +0.6% (359,122 → 361,326; base 358,449), entirely its 2021-22 slice: old-prev plus new-head equals head exactly. Of that slice, only the H withholding changes values.
- **Benin:** 0.

| | inferred kg, prev → this head | rows that move | median ratio (p10, p90) | `food_prices` rows that move (median ratio) |
|---|---|---|---|---|
| Togo 2018 | 66,944 → 68,062 (+1.7%; base 63,577) | 27,478 of 110,742 | 1.036 (0.992, 1.167) | 14,266 of 67,434 (0.966) |
| Cote d'Ivoire 2018-19 | 359,122 → 361,326 (+0.6%; base 358,449) | 66,494 of 296,942 | 1.032 (0.933, 1.25) | 40,452 of 182,890 (0.969) |
| Benin 2018-19 | 0 moved | | | |

### `derivation_inputs` joins, every new and changed key
Measured at this head, at `(t, i, j)` (strata on `(t, i)`).
- **Togo withheld:** 3,916 served rows to 3,916 input rows, one-to-one, every one NA.
- **Togo divided-out:** 68,744 groups on both sides, every group's summed recovered residual equal to the served quantity. 67,992 of its 69,330 served rows are one-to-one; the rest are `harmonize_food` folds (1,172 pairs told apart by `u`, 166 sums of two source rows in one unit).
- **Togo count-recovered:** 10,037 groups, all equal; 10,035 of 10,039 served rows one-to-one.
- **Togo deletion:** 25,234 input rows, the raw predicate's rows household for household; no served row carries the key.
- **Togo 7-day:** every served keyed `(t, i, j)` is in the inputs.
- **Cote d'Ivoire H/L:** one input row serves up to three rows, one per `s`. 7 H served rows share `(t, i, j)`. Strata: 12,965 of 12,965 re-derived.

### Must-state items (both red teams)
| item | status |
|---|---|
| F1 | fixed (above) |
| F2a, F2b | ruled and applied (above) |
| F3 | cache cost, below; GhanaLSS is a full cold re-warm at both tiers |
| F4 | #1598 (own-country NSU weights) is **deferred, not unruled**. The GO ruling names it inside the food step, and #1598 says to sequence it after the wave wiring. It stays open. The three `CONTENTS.org` files say so. |
| F5 | Benin's 6,138 leavers (`s01q00aa` = non) are dropped, matching `ehcvm_individu`: a de facto choice pending #1565, reversible |
| F6 | Togo crop-state codes 3-5 (`Tubercule`, `Gousse`, `Régime`; 1,032 rows) map to `other_condition` **pending a ruling**; the answer is kept in `condition_native` |
| F7 | the `assets` index `(t, i, j)` is not unique in old and new waves (2021-22: Togo 958, Benin 1,871, Cote d'Ivoire 2,088), because the `harmonize_assets` relabel of `j` runs after the index collapse. Pre-existing, extended here; **filed as #1710** and recorded in all three `CONTENTS.org` |
| F8 | `ehcvm.harvest_totals` / `derive_hh_crop_harvest_total` duplicate Mali's rule as code, per "Extend Mali's derivation", and keep Mali out of the fingerprint; an edit to one does not reach the other |
| N1 | sized (Older waves) |
| N2 | disclosed (Togo) |
| N3, N3b | ruled ("Recover; a 0 becomes NA"); wording and the ~54 disclosure in the count key's assumption |
| N4 | registered deletion, 25,234 rows (Togo) |
| N5 | the cover is not hashed (Togo); CLAUDE.md cache limitation (a) |
| N6 | **open for the maintainer**: `derived-from-data` in `derivations.BASES`, its value and its position after `filename` in the ladder order; the CIV H/L assumptions carry it while their evidence is partly documentary |
| N7 | ruled ("Withhold all three") |
| N8 | recorded (Cote d'Ivoire) |

Also:
- **Shocks:** 2021-22 serves only shocks with follow-ups (Togo 2,666 of 7,229; Benin 4,756 of 10,982; Cote d'Ivoire 7,528), so a cross-wave incidence reads as a false fall. Cote d'Ivoire's damaged keys raise `GrainCollapseWarning` on every read of `shocks` and `plot_inputs`.
- **Unlabelled codes:** Cote d'Ivoire crop codes 56-60 are served as numbers. 2021-22 `crop_production` has no `Quantity_kg`.

### Framework change
`derivations.BASES` gains `derived-from-data`: a regularity measured in the distributed file that no document states. Like `sampling.py`'s use of the term, it is not documentation. `docs/guide/derivations.md` and `tests/test_derivations.py` are updated. The change moves 0 hashes. Its place in the order is N6, open.

### Cache cost
- **This head against base `e5fdd2fa8`** (final2's probe; this round adds no country): `Country._table_cache_hash` and `Wave._input_hash` move:

  | country | table hashes | wave hashes |
  |---|---|---|
  | Togo | 22 of 22 | 44 of 44 |
  | Benin | 22 of 22 | 44 of 44 |
  | Cote d'Ivoire | 21 of 21 | 126 of 126 |
  | GhanaLSS | 21 of 21 | 147 of 147 |

  - **GhanaLSS is a full cold re-warm at both tiers**, through the `Très grand` entry in `_UNIT_SIZE_WORDS` via `nutrition.py`. No GhanaLSS served value changes. The standing hold says no shared-cache rebuild, so the merge stales GhanaLSS everywhere.
  - `build_transforms_fingerprint`: 0 of 40. Every other country: 0.
- **This head against `9b5585cf2`:** Togo 22 of 22 table hashes and 22 of 44 wave hashes (the 2021-22 wave), every other country 0, `build_transforms_fingerprint` 0 of 40. The records commit moves nothing (its probe is byte-identical to the code commit's).

### Tests
The targeted data-gated selection (40 files):
- every test file naming these countries or EHCVM;
- the GH1575 / gh812 strata files;
- sample, sampling, recall and the derivation registries, with the drift tests;
- `test_build_transform_hash.py`, population, panel ids, size pool, shocks and categorical-mapping resolution.

Each run used `-n 8 --dist=loadfile` in a fresh private root, with BLAS threads pinned and `LSMS_BUILD_WORKERS=1`.

| side | targeted | schema (`-k` the three countries) |
|---|---|---|
| this head `ee65db577` | **2,897 passed, 0 failed**, 84 skipped, 4 xfailed | 26 passed |
| `9b5585cf2` (final2) | 2,849 passed, 0 failed, 83 skipped, 4 xfailed | 26 passed |
| base `e5fdd2fa8` | 2,627 passed, 0 failed, 86 skipped, 4 xfailed (without the three 2021-22 wave files) | 26 passed |

- No Togo, Cote d'Ivoire or EHCVM wave test was skipped for want of data.
- Re-pinned on purpose (earlier rounds):
  - `test_ehcvm_7day`: the `AFTER` totals for Togo and CIV 2021-22, the allowed keys, and the in-kind NA for CIV H;
  - both wave oracles (independent re-implementations of the inversion and the H/L split);
  - the CIV strata key scope in `test_gh1575_cotedivoire…` and `test_gh812_ehcvm_strata`;
  - `test_derivations`, which also pins `derived-from-data` (membership only, not order).
  - Earlier still, from the restored wiring: `test_gh675` checks the 2018-19 slice (2021-22 GPS comes from the cover); the Togo nonfood pins are restricted to 2018 (2021-22 is pinned in `test_nonfood_expenditures_schema`); `test_gh323` cluster counts are per wave; `test_sample.PARTIALLY_WIRED_SAMPLE` loses three exemptions.
- Loosened pins, sized by the second red team (§8):
  - `test_civ_2021_wave::test_food_acquired_sums_are_the_files` went from `rel` 1e-6 to 1e-4. The real gaps between served sums and the file's non-H sums are −1.7e-5 (purchased Quantity), −5.7e-5 (purchased Expenditure) and −2.3e-5 (in-kind), from the collision withholding: about 2x headroom.
  - The `>=` checks (`held >= h`, `flagged >= l_rows`) and the 20,000-21,000 / 25,000-26,500 range pins stand as written; the exact counts are above.
  - The Togo re-derive test checks only that filed over recovered lies in the factor set, not the served frame. The served comparison is in the join checks above (exact), so the gap is in test coverage.
- New in this round: a synthetic registry test that a filed-0 `unité` outside Grand Lomé is withheld; a data-gated test that the deletion entry's inputs are exactly the oracle's rows and that no served row carries its key. The wave oracle withholds a recovered 0 and serves it an NA purchased row.
- In-tree parquets after every job: none.

### Scope beyond the rulings (round-1 red team, Q7)
- No panel link: the `E2_` ids cannot meet, and there is no Benin `panel_ids.json`.
- No production-weight change for any existing wave.
- The shared `_UNIT_SIZE_WORDS` edit (F3).
- The option-(d) sale attachment and `harvest-to-date` extended from Mali to all three countries (archived wiring, under the GO). Harvest-from-dispositions only in Benin and CIV.
- A new closing verdict in `.coder/coverage/absent_verdicts.csv`: CIV `plot_labor` 2021-22, `asked-not-distributed`, with C1/C2/C4 evidence.
- 2021-22 `crop_production` has no `Quantity_kg`, unlike Mali 2021-22.

### Not verified
- Every build here is cold. No warm-cache, fresh-process or `assume_cache_fresh` reads, no `Feature()` assembly, no strict levers (`LSMS_*_STRICT`), and not the full test suite: the targeted 40 files and the schema selection.
- Cote d'Ivoire and Benin were not rebuilt in this round. "Unchanged" for them rests on 0 hash movement at both tiers against `9b5585cf2` and no shared-code edit; their numbers are final2's and the red teams'.
- The 2021-22 `food_quantities(units='kgs')` total (58,744) is carried from final2; this round's frames are equal to `9b5585cf2`'s (58,742.6 on `u` = `kg`, 58,743.6 over all rows).
- The `.dta` variable labels for Togo and CIV `s14b` have still not been read by a red team.

### Residuals
- #1598 (NSU weights, three countries);
- #1565 (leavers);
- #1566 (`s12q09`);
- #1705 (Benin link);
- #1710 (`assets` index, F7);
- the F6 ruling; N6; the ten own/gift recovered-0 rows' NA purchased row (a literal reading, above);
- Guinea-Bissau 2021-22;
- Togo's link (not ruled).

Reports: `burst/isa-sm-20261112/reports/ehcvm2-2021-final3/REPORT.md` (this round) and `ehcvm2-2021-final2/REPORT.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
