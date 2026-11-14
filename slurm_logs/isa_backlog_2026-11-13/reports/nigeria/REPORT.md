
## Changes since 2ebdad8b3 (for the delta review)

Commits: `f6361d068` (code, config, tests), `104b314ed` (re-measured registry and `CONTENTS.org`, a new split test, verdict attribution), `2597dda4a` and `0497f9e54` (`CONTENTS.org` only), `3d34550d5` (one registry citation).

1. **W2 centilitre class ("Re-read solids as grams").**
   - The liquids are an explicit per-round list, `nigeria.W2_CL_CLASS_LIQUID_ITEMS`: codes 50, 52, 53 in 2012Q3 and 50, 52 in 2013Q1 (the oils). They keep conv/100, read as litres.
   - Every other item's g_cl = 0 cell is read as conv/1000 under a new key, `Nigeria::food_acquired::w2-cl-class-read-as-grams` (basis derived-from-data). That covers 884 + 637 served rows.
   - Its inputs return `conv`, `g_cl` and `as_grams`. The round keys' inputs now filter on their exact key.
   - Keys are stamped after the collapse as a union of the contributors' keys. No served W2 key mixes the two keys today.
   - `CONTENTS.org` has a per-item evidence table covering every code in the class: each reading against the inference, W3's table and price-implied kg.
   - A new cached-source test pins each held W2 table's class to the liquid list plus the reviewed solids, so both sides of the split are explicit. It ran on the real tables (2 passed, not skipped).
2. **Litres ("Serve tabled litres both sides").** The 71 W3 jerry-can crop rows are released at 1 l = 1 kg. The 9 card-contradicting bundle rows stay held (coordinator default).
3. **#1713 ("Wire W1/W2 PP + verdicts").**
   - W1 and W2 post-planting `individual_education` are served from `sect2_plantingw1` `s2q7` and `sect2_plantingw2` `s2q8`. Each is a per-file `t:` override in that wave's `data_info.yml`; the PH entry is unchanged.
   - Seven PP spellings joined `harmonize_education` under its existing collapse.
   - Three `correctly-empty` rows (W3–W5 PP; checks D1, D2, C4) were added to `.coder/coverage/absent_verdicts.csv`. `latest.csv` was not regenerated.
   - I removed a df_edit hook I had briefly added, because #1292's test pins pass-through of unmapped codes.
4. **Defaults.**
   - `Quantity_kg` is declared `optional: true` in Nigeria's `crop_production`, and the "REPORTED fields only" comment is corrected.
   - The canonical `KgFactor` / `Quantity_kg` notes in `lsms_library/data_info.yml` now admit the shipped-table exception by name.
   - W3 stays on the own-zone column, and the W1-PP / W4 spillover is left as is (disclosed under F4).
5. **F6 stale prose, all fixed.**
   - `CONTENTS.org` line ~215 (the W3 `*_cvn` claim).
   - The base #1518 "does not wire a default table" paragraph is marked superseded.
   - The c8 drop count is reconciled: the held W3 files decode to 11,171 g/cl rows (8,557 + 2,614), of which 11,170 are dropped. The old 13,767 is not reproducible on the held files.
   - Float32 is now noted: both W3 tables store their zone columns as float32 and `conv_national` as float64. The deviation is at most 5.7e-8 relative (median 2e-8).
   - Also corrected: the phase-1 movement table had called W4's partition "unchanged" when 16 rows had changed rung.

## Per issue (cumulative)

- **#1537:** the label-only `Pepper → Fresh Pepper` row is unchanged. All Pepper is code 76 (10,446 food rows, 808 community rows).
- **#1502:** W2 PP/PH and W3 food tables are wired as `Quantity_kg` on the `survey_kg` rung. That is 139,621 rows, 1,521 of them on the grams key.
- **#1518:** `ag_conv_w3` feeds 2016Q1 `crop_production.Quantity_kg`, now 8,507 lines including the jerry cans.
- **#587:** W3 post-harvest blank-size Kg/l rows are served (2016Q1 community prices 23,153 → 26,586 rows). A blank PH unit becomes `Unknown` (0 rows today). Unchanged this round.
- **#1713:** wired and adjudicated as above.

## Measurements

Cold, private roots, base `f138a3d61` against `f6361d068` (job 41275807), plus jobs m8 41275442, m9 41276093, m10 41276128, m11 41276349 and m12 41276356.

`food_quantities(units='kgs')` by round:

| t | before → after |
|---|---|
| 2010Q3 | 191,707 → 192,250 |
| 2011Q1 | 245,539 → 245,539 (unchanged) |
| 2012Q3 | 189,161 → 201,888 (**+6.7%**) |
| 2013Q1 | 147,523 → 158,307 (**+7.3%**) |
| 2015Q3 | 119,919 → 206,721 (+72.4%) |
| 2016Q1 | 108,068 → 189,116 (+75.0%) |
| 2018Q3 | 251,251 → 251,487 |
| 2019Q1 | 214,080 → 214,134 |
| **Total** | **1,467,249 → 1,659,443 (+13.1%)** |

Phase 1 had a total of +17.8%, with W2 at +37.5% and +15.0%.

- **W2 tabled kilograms** went from 138,676 to 80,476 (2012Q3) and from 79,519 to 68,186 (2013Q1) as the re-read took effect.
- **`KgFactorSource` partition:** identical to phase 1, because the re-read changes factor values, not which rows are tabled.
- **`food_prices()`:** all 481,191 rows are kept. Price changes on 10,886 and 12,709 W2 rows (median new/old 0.90 and 0.92); the W2 medians are unchanged. W3 medians go 400 → 261 / 272.
- **What carries W2's rise now** is the g_cl = 1 class: +11,034 and +11,038 kg, mostly tiya and mudu of grains, where the table sits 1.2–1.7x the inference. It is no longer the centilitre class.
- **W2 scores as now read:** price-implied 1.01 / 0.93 (within 2x on 82.4% / 78.1% of rows). Same-label W4 cells 0.92 / 0.94 (within 2x on 72% / 74%).
- **Centilitre class only:**

  | reference | grams reading | cl reading |
  |---|---|---|
  | inference (inferred factor ÷ re-read table; within 2x) | 0.67 (93% / 92%) | cl sits 15x above the inference |
  | W4 cells (6 / 12) | 0.76 / 0.83 | ~8x |
  | price-implied (573 / 338 rows; within 2x) | 0.31 / 0.175 (30% / 17%) | 3.08 / 1.75 (34% / 53%) |

  Kilograms on the class: 64,666 / 12,593 under the cl reading, 6,467 / 1,259 under grams.
- **Crop 2016Q1:** 8,507 lines carry 8,522,018 kg. `harvest_kg` goes 365,707 → 8,887,081. `KgFactorSource`: `none` 12,144 → 3,708, `survey_kg` 0 → 8,507, `metric` 564 → 493.
- **Education:**
  - 2010Q3 gains 17,116 rows and 2012Q3 gains 17,403; the post-harvest quarters are unchanged.
  - Three W2 PP cells hold an undefined code. They are served as `"4.0"`, `"19.0"` and `"20.0"`.
- **`volume_as_mass=False`** moves 0 `survey_kg` rows on the food side, and leaves the 71 crop jerry cans at 14,310 kg.

## Hash census

`f138a3d61` → `104b314ed`:

| fingerprint | moved | country |
|---|---|---|
| `btf` | 0 of 42 | — |
| `fwimp` | 1 of 38 | Nigeria |
| tables | 24 of 555 | all Nigeria |
| waves | 240 of 2,321 | all Nigeria |

`f6361d068` → `104b314ed`: 0 moved.

- **`fwimp`** moved because `nigeria.py` now imports `derivations.union_keys_by_group`. Core's `_normalize_dataframe_index` (a `@build_transform()` root) already folds that function into every `btf`, so this adds no new re-warm trigger.
- **Canonical `data_info.yml`:** no non-Nigeria hash moved, which confirms it is not a corpus input. Nigeria's own movement cannot be separated from it in this census.

## Tests

- At `104b314ed` (job 41276446): **2,529 passed, 16 skipped.** The run covered the Nigeria list, the 15-file extra list (including `test_coverage_matrix`) and `test_gh1288`. The skips are warm-gated cells not built in a private root, the checkout-identity check, and #1287's community contract.
- At `f6361d068`: 1,980 passed, 4 skipped.
- The base arm was not re-run this round. In phase 1 it gave 1,965 / 4 on the main list and 540 / 12 on the extra list.

## Must-states (also in the PR body)

1. W2's rise is now +6.7% / +7.3%, carried by grain mudu and tiya cells. The phase-1 "mostly the cl class" no longer holds.
2. W2 `food_prices` per-kg values move on 10,886 and 12,709 rows (median ×0.90 / ×0.92).
3. 2016Q1 harvest kilograms rise about 24x. Yields and kg-valued production move with them; that is not measured.
4. Crop `survey_kg` here is the World Bank's table at the line's native key, not a total an enumerator wrote down.
5. Tabled litres served as kilograms cannot be declined with `volume_as_mass=False`, on the food side or the crop side.
6. F4: W1-PP and W4 move through the pooled estimate.
   - My deltas: 2010Q3 +543 kg (3,679 factors move, median ×1.13); 2018Q3 +235 kg (308 factors, ×2.50); 2019Q1 +55 kg (213 factors, ×1.93); 16 W4 rows change rung.
   - The red team's scoring at `2ebdad8b3` (not re-scored): a slight worsening on W1-PP, and W4's 521 rows going 392 → 685 kg.
   - The #1452 overlay is not extended to Nigeria.
7. The price-implied reference disagrees with the grams re-read; the inference and W4 agree with it.
8. The jerry cans were served at one litre per can before the release, and their `u` is still `'l'` (rows counting cans).

## Questions for maintainer review

1. **Grams items where a reference prefers the cl reading:**
   - 120 Coffee, 121 Chocolate drinks and 122 Tea, on the inference.
   - 44 (2013Q1), 71 Tomato puree and 84 Local eggs, on the price-implied reference only.
   - 43 Groundnuts, the class's largest item, fits neither reading on price-implied.

   All are read as grams per the ruling. Keep them that way?
2. **Item 53 "Other oils and fats"** is on the liquid list on the inference alone (22 rows), and the name can cover solid fats.
3. **Jerry-can `u = 'l'`:** `_canon_unit` tests "litre" before "jerry". Fixing that changes a key on 71 W3 rows. Fix it?
4. **The three undefined W2 PP education codes.** The canonical note says `Unknown` is the sentinel for unmappable values, but #1292 pins pass-through. Which should win?
5. **The 9 held bundle rows.** W4's evidence suggests the table's `unit_name` is a labelling slip.
6. **Fit of the W2 grams class:** derica sizes sit at about half of both references, and grain mudu/tiya at 1.2–1.7x the inference.
7. **`nigeria.py` docstrings** at about lines 820 and 3383 still carry the superseded 9,427 / 4,341 and 13,768 counts. The file is hashed, so I left them; fix them with the next Nigeria re-warm.

## Notes for Sue

- **Private household-level frames:** `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/private/nigeria/{m1,v1-1c906836a,v1-f6361d068}`.
- **Red-team roots to clean:** `.../private/nigeria-redteam/job-41274078`, `job-41274079`, and `frames/analysis-full.json` there.
- **Three no-op `python3` calls on n0035** in total: two in phase 1, and one this round (`python3 -c 1` left in a shell line). None imported anything or read data.
- **m11's first run (41276285) crashed** while writing its JSON. The partial file is kept as `m11-partial-41276285.json`; the rerun is 41276349.
- **`latest.csv` is stale.** The W1/W2 PP cells will keep reading `dropped`, and W3–W5 PP will read `correctly-empty`, only after the next regrade.
- **Commit `1c906836a` has "(WIP)" in its subject.** Reword it at merge if you want.
- **Shocks and assets** were not touched.

---

