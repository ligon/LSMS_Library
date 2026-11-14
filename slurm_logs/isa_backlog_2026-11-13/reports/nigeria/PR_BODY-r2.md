
**Nigeria: the World Bank's own W2/W3 food and W3 crop conversion tables, Pepper, PH C8 alignment, PP education (GH #1537, #1502, #1518, #587, #1713)**

## Rulings (@ligon)
- 2026-11-12: "Wire W2 and W3", "Measure PH, then align", "Register tokens, keep Unknown".
- 2026-11-13: "Re-read solids as grams", "Serve tabled litres both sides", "Wire W1/W2 PP + verdicts".

## What changes
- **#1537.** Generic Pepper (code 76) takes the Fresh Pepper Aggregate.
- **#1502.**
  - `food_acquired.Quantity_kg` (`survey_kg` rung) comes from each round's shipped table at the row's native key: item, the source's own unit code, and on W3 its own Other text and own zone.
  - There are four keys. The W2 round keys and the W3 key cover the table as written. `w2-cl-class-read-as-grams` covers W2's g_cl = 0 cells of non-liquid items, read as conv/1000. The liquids are an explicit reviewed list (the oils) and keep conv/100 as litres.
  - 139,621 rows are tabled; metric codes stay on their label rung.
- **#1518.**
  - `crop_production.Quantity_kg` for 2016Q1 comes from `ag_conv_w3` at the native key: crop, sized unit, own zone. That is 8,507 lines, including the jerry cans at 1 l = 1 kg.
  - The 9 card-contradicting rows stay held, and `KgFactor` is not written.
  - `Quantity_kg` is declared optional, and the canonical notes admit the exception.
- **#587.** W3 PH blank-size Kg/l rows are served (+3,433 community-price rows). A blank PH unit becomes `Unknown`.
- **#1713.**
  - W1 and W2 post-planting `individual_education` come from `s2q7` / `s2q8`: +17,116 and +17,403 rows.
  - W3–W5 PP get `correctly-empty` verdicts (D1, D2, C4).

## Movement (cold, base f138a3d61)
- **`food_quantities(units='kgs')`:** 1,467,249 → 1,659,443 kg (+13.1%). By round: W2 +6.7% / +7.3%, W3 +72.4% / +75.0%, 2010Q3 +0.3%, W4 +0.09% / +0.03%.
- **`food_prices()`:** keeps all 481,191 rows; Price changes on the tabled rounds. W3 per-kg medians go 400 → 261 / 272.
- **`harvest_kg`, 2016Q1:** 365,707 → 8,887,081.

## Must-state
1. W2's rise is carried by grain mudu and tiya cells (table 1.2–1.7x the inference).
2. W2 per-kg prices move on 10,886 and 12,709 rows.
3. 2016Q1 harvest kilograms rise about 24x. Yields and kg-valued production move; that is not measured.
4. Crop `survey_kg` here is the World Bank's table, not an enumerator's written total.
5. Tabled litres served as kilograms cannot be declined with `volume_as_mass=False` (measured: 0 `survey_kg` rows move).
6. W1-PP and W4 move through the pooled estimate: 2010Q3 +543 kg, W4 +235 / +55 kg, 16 W4 rows change rung. The red team scored W1-PP as a slight worsening.
7. The household price-implied reference disagrees with the grams re-read on the centilitre-class solids: 0.31 / 0.175 under grams against 3.08 / 1.75 under cl. The inference and W4 agree with the re-read.
8. The W3 jerry cans were served at one litre per can before this change (645 kg). They are now 14,310 kg, and their served `u` is still `'l'`.

## For review
- The W2 items where a reference prefers the cl reading: 120, 121, 122; 44 (2013Q1), 71, 84; and 43, which fits neither.
- Item 53 on the liquid list.
- Jerry-can `u`.
- The three undefined W2 PP education codes, which pass through.
- The 9 bundle rows.
- Derica sizes at about half of both references.

## Cache cost
A Nigeria-only re-warm, at both tiers.

| fingerprint | moved |
|---|---|
| `btf` | 0 of 42 |
| `fwimp` | 1 of 38 (Nigeria) |
| tables | 24 of 555 |
| waves | 240 of 2,321 |

No other country's hash moves.

## Tests
2,529 passed, 16 skipped, on the Nigeria and extra lists at `104b314ed`. Later commits are documentation and registry prose only.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
