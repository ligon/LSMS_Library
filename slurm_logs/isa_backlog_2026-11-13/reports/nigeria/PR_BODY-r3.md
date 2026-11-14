
**Nigeria: the World Bank's own W2/W3 food and W3 crop conversion tables, Pepper, PH C8 alignment, PP education (GH #1537, #1502, #1518, #587, #1713)**

## Rulings (@ligon)
- **2026-11-12:** "Wire W2 and W3", "Measure PH, then align", "Register tokens, keep Unknown".
- **2026-11-13:** "Re-read solids as grams", "Serve tabled litres both sides", "Wire W1/W2 PP + verdicts", "Read as centilitres" (red team delta D1).

## Changes since 3d34550d5 (for the delta review)

Commits: `cd4416f3a` (D2, D3, D6), `73176e056` (D1), `df2e3b15d` (`CONTENTS.org` only).

Measured cold against `f6361d068`'s frames, which are identical to `3d34550d5`'s (0 hashes moved between them). Jobs 41279394 and 41279736; `reports/nigeria/out/m14/` and `m15/`.

- **D1, "Read as centilitres".**
  - The g_cl = 1 cells of W2 liquids are now read as conv/100 litres at density 1, under the round's own key.
  - The list is explicit (item, unit) cells per round, `nigeria.W2_GRAMS_FLAG_LIQUID_CELLS`: 13 cells in 2012Q3 and 6 in 2013Q1. The items are tinned milk, fresh milk, bottled water, palm wine, pito, honey and butter/margarine.
  - Each listed cell's conv is 0.79–0.96 of its container in centilitres, against the volumes the oil rows imply. Under grams these rows were served at 0.08–0.10 kg per litre.
  - Beverage powders stay grams. The same screen also picks Pepper (76) and Dry Pepper (77); those are solids, whose centilitre reading would be 1.57 kg/l, and they are not listed.
  - Movement: exactly 106 `food_acquired` rows move, each ×10 (64 in 2012Q3, 42 in 2013Q1), and their keys are unchanged.

    | round | `Quantity_kg` on these rows | `food_quantities(units='kgs')` |
    |---|---|---|
    | 2012Q3 | 11.47 → 114.7 kg | 201,888 → 201,991 |
    | 2013Q1 | 1.68 → 16.8 kg | 158,307 → 158,322 |

  - The inference gives 99.0 and 14.6 kg on the same rows.
  - `food_prices()` keeps every row; prices move ×0.1 on 62 and 39 rows, and the medians are unchanged.
  - The table carries an `as_centilitres` column, and the inputs return it.
  - A held-table test pins the list to the screen's liquid hits (it ran on the real tables).
- **D2.** W3 harvest codes 190–192 are served as `u = 'Jerry Can'`, the label W1/W2/W5 cans already use. This goes through a per-frame `unit_override` (`W3_HARVEST_UNIT_OVERRIDE`); `_canon_unit` is not reordered.
  - Exactly 71 rows change `u` (`l` → `Jerry Can`, all 2016Q1).
  - `harvest_kg` and `KgFactorSource` are unchanged, and every other Nigeria table is frame-equal.
  - The two audits that fired on all 71 rows now have nothing to judge:

    | audit | before | after |
    |---|---|---|
    | `survey_kg_off_label` | 54 high of 71 sized | 0 sized |
    | `survey_kg_vs_metric` | 71 of 71 disagree | 0 compared |

  - Residual, not changed: 15 W3 sale records on a 20 or 25 l can still decode `Unit_sold = 'l'`.
- **D3.** The W5 post-planting `correctly-empty` evidence now names `s1q39c`, a leaver's education "as of now". It is non-null on 3,327 rows, all with `s1q4 = NO` (re-measured, job 41279390).
- **D6.** The inputs callables now select rows whose `Derivation` carries the key, splitting on `+`. A compound-key test is added.

## What changes (cumulative)
- **#1537.** Generic Pepper (code 76) takes the Fresh Pepper Aggregate.
- **#1502.** `food_acquired.Quantity_kg` (`survey_kg` rung) comes from each round's shipped table at the row's native key: item, the source's own unit code, and on W3 its own Other text and own zone. 139,621 rows are tabled; metric codes stay on their label rung.
  - Keys `w2-pp-food-conversion`, `w2-ph-food-conversion` and `w3-food-conversion`, plus `w2-cl-class-read-as-grams` for W2's g_cl = 0 solids, read as conv/1000.
  - The W2 liquids are an explicit reviewed list: the g_cl = 0 oils and the g_cl = 1 cells above.
- **#1518.** 2016Q1 `crop_production.Quantity_kg` comes from `ag_conv_w3` at the native key (crop, sized unit, own zone). That is 8,507 lines, including the jerry cans at 1 l = 1 kg.
  - The 9 card-contradicting rows stay held, and `KgFactor` is not written.
  - `Quantity_kg` is declared `optional: true`, and the canonical notes admit the exception.
- **#587.** W3 PH blank-size Kg/l rows are served (+3,433 community-price rows). A blank PH unit becomes `Unknown`.
- **#1713.** W1 and W2 post-planting `individual_education` come from `s2q7` / `s2q8` (+17,116 and +17,403 rows). W3–W5 PP get `correctly-empty` verdicts (D1, D2, C4).

## Movement (cold, base f138a3d61)

| measure | before | after | change |
|---|---|---|---|
| `food_quantities(units='kgs')`, total | 1,467,249 kg | 1,659,561 kg | +13.1% |
| W2, 2012Q3 / 2013Q1 | | | +6.8% / +7.3% |
| W3, 2015Q3 / 2016Q1 | | | +72.4% / +75.0% |
| 2010Q3 | | | +0.3% |
| W4, 2018Q3 / 2019Q1 | | | +0.09% / +0.03% |
| W3 `food_prices()` per-kg median | 400 | 261 / 272 | |
| `harvest_kg`, 2016Q1 | 365,707 | 8,887,081 | about 24x |

`food_prices()` keeps all 481,191 rows.

## Must-state
1. W2's rise is carried by grain mudu and tiya cells, where the table sits 1.2–1.7x the inference.
2. W2 per-kg prices move on 10,886 and 12,709 rows.
3. 2016Q1 harvest kilograms rise about 24x. Yields and kg-valued production move with them; that is not measured.
4. Crop `survey_kg` here is the World Bank's table at the native key, not a total an enumerator wrote down.
5. Tabled litres served as kilograms cannot be declined with `volume_as_mass=False` (measured: 0 `survey_kg` rows move).
6. W1-PP and W4 move through the pooled estimate: 2010Q3 +543 kg, W4 +235 / +55 kg, 16 W4 rows change rung. The red team scored W1-PP as a slight worsening.
7. On the g_cl = 0 solids, the household price-implied reference disagrees with the grams re-read (0.31 / 0.175 under grams, against 3.08 / 1.75 under cl). The inference, W3's table, W4 and container volume all agree with grams.
8. Before this PR the W3 jerry cans were served at one litre per can (645 kg). They are now 14,310 kg, served as `Jerry Can`.
9. The W2 g_cl = 1 liquids were served about 10x low until D1; they are now conv/100 litres.

## For review
- **D4, unruled.** Three W2 post-planting education cells (one each) are served as `"4.0"`, `"19.0"` and `"20.0"`. The canonical note makes `Unknown` the sentinel for an unmappable value, and #1292 pins pass-through. The three rows are left as they are.
- **Grams-class items where a reference prefers cl:** 120 Coffee, 121 Milo and 122 Tea on the inference; 44 (2013Q1), 71 and 84 on price-implied only. The red team's container volumes resolve 44, 71 and 84 to grams.
- **The 9 held bundle rows.** W4's evidence suggests the table's `unit_name` is a labelling slip.
- **W3 sale records** on jerry cans still decode `Unit_sold = 'l'` (15 records).
- **`nigeria.py` docstrings** at about lines 820 and 3383 still carry the superseded 9,427 / 4,341 and 13,768 counts.

## Cache cost
A Nigeria-only re-warm, at both tiers.

| fingerprint | moved |
|---|---|
| `btf` | 0 of 42 |
| `fwimp` | 1 of 38 (Nigeria: the `union_keys_by_group` import, already in every `btf` through `_normalize_dataframe_index`) |
| tables | 24 of 555 |
| waves | 240 of 2,321 |

No non-Nigeria hash moves, which confirms the canonical `data_info.yml` edit is not a corpus input.

## Tests
At `73176e056`: 2,532 passed, 16 skipped, on the Nigeria list, the extra list and `test_gh1288`. The cached W2 split test ran on the real tables (2 passed). The skips are warm-gated cells not built in a private root, the checkout-identity check, and #1287's community contract.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
