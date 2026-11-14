# GH #587: serve Nigeria's five post-planting community_prices cells (+ a PH C8 decoder fix)

Every Nigeria post-planting (PP) community form prices food in Section C2. `CONTENTS.org` said C8 "is
post-harvest only ... correctly empty", and the forms and files contradict that. This PR serves the five
PP quarters on Nigeria's shared labels, following the six 2026-11-12 GH587 decision-log entries:
- `j` is the PP quarter's `harmonize_food` Preferred Label.
- `u` is the PH base unit, and the native size goes in a new optional `Size` column.

The crosswalk lives in `Nigeria/_/pp_community_prices.py`, so nothing auto-applies it to another Nigeria
table.

## Rulings implemented (decision log, 2026-11-12)

| ruling | what it does here |
|---|---|
| "Develop the item and unit code mapping, then provide community_prices using those harmonized labels" | the PP crosswalk, from each wave's own C2 cards |
| second round: "Base u, size column"; W1 "Register the division"; W5 "Accept quantity of one"; "Reuse PH c8-metric-size" | the price basis by wave (below) |
| "GH587 W1 metric rows with a count": "Hold both groups" | W1 g / ml with N = 1, W1 Kg / l with N > 50 held |
| "GH587 metric units with no recorded package size": "Serve W3 kg/l and W5 g/cl" | W3 Kg / l served as recorded; W5 g / cl divided by `c2q2a` |
| "GH587 label judgements and two follow-ups": "Add a Semovita row"; "Accept all" units; "Measure PH, then align"; "Serve the 63 via c8" | W2 code 15 served as Semovita; W3 161 Palm wine; W2 code 0 dropped; W5's 63 Bottle/Can rows divided by their coded size |
| "GH587 W1 division after red team": "Hold both"; "Flag them, hold g N 2-5"; "Register tokens, keep Unknown" | the W1 N-band split below; `Congo/Mudu`, `ml`, `Tomatoes cup` registered in `u`; blank-unit PP rows stay `Unknown` |

## What is served

**128,701 PP rows** (cold build, private root). `community_prices` is 275,493 rows, of which 146,792 are PH.

| PP t | served | `Derivation` | held, each group its own `rows: 0` registry entry |
|---|---|---|---|
| 2010Q3 | 20,326 | 2,725 `pp-w1-number-of-units`; 3,629 `pp-w1-number-of-units-unvalidated` | 105 `pp-w1-no-count-hold`; 1,043 `pp-w1-metric-package-hold` (963 g, 80 ml, N = 1); 332 `pp-w1-count-as-size-hold` (128 Kg, 204 l, N > 50); 64 `pp-w1-metric-small-count-hold` (61 g, 3 ml, N 2-5); 208 `pp-w1-kg-mid-count-hold` (Kg, N 6-50); 241 `pp-w1-nonmetric-count-as-size-hold` (non-metric, N > 50) |
| 2012Q3 | 21,196 (72 Semovita) | -- | 209 `pp-unrecorded-size-hold` (204 g, 5 cl); `pp-unharmonized-item-hold` 0 (code 0, the sentinel, has no priced row) |
| 2015Q3 | 24,995 (obs 1 18,365, obs 2 6,630) | -- | 12,934 `pp-unrecorded-size-hold` (8,445 g, 4,489 cl) |
| 2018Q3 | 32,246 | 3,156 `pp-c2-metric-size` | none |
| 2023Q3 | 29,938 | 4,619 `pp-c2-metric-size` (63 of them Bottle/Can with `c2q2a` not 1); 208 `pp-w5-metric-unit-quantity` (176 g, 32 cl) | 23 `pp-unrecorded-size-hold` (19 g, 4 cl); 60 `pp-w5-quantity-not-one-hold` (41 Cube, 2 Bottle/Can with no size, 10 Kg, 7 l) |

Against the previous head `e5940eaa9` (129,079 PP rows):
- 513 leave, all 2010Q3: Kg 208, g 61, ml 3 and 241 non-metric rows.
- 135 arrive: the 72 Semovita rows (2012Q3: Kg 67, Mudu 3, Congo 1, Tiya 1) and the 63 W5 Bottle/Can rows,
  now on `cl` under `pp-c2-metric-size`.
- On the 128,566 common rows, the only change is the `Derivation` of 3,629 W1 rows, which moves from
  `pp-w1-number-of-units` to `pp-w1-number-of-units-unvalidated`. No `Price` or `Size` changes.

### Price basis, by wave

**W1** (`sc2q3a` Naira, `sc2q3b` Number of Units, `sc2q3c` unit):
- **N = 1:** served as recorded.
- **N > 1:** Price is `sc2q3a / sc2q3b`, under one of two keys:
  - `pp-w1-number-of-units` on the supported bands: g / ml with N >= 6, l with N 2-50;
  - `pp-w1-number-of-units-unvalidated` on the unvalidated ones: non-metric N 2-50, Kg N 2-5, every
    Unknown-unit row.
- **Held, each group under its own key:**
  - g / ml with N = 1: package prices, about 144x household;
  - g / ml with N 2-5: about 9x;
  - Kg with N 6-50: 0.16-0.33x on three references;
  - Kg / l with N > 50 and non-metric units with N > 50: the count reads as a size;
  - rows with no count.

**W2-W4:** the printed "PLEASE USE A QUANTITY OF ONE (1)"; served as recorded. W4/W5 coded metric sizes are
divided by the PH rule (`pp-c2-metric-size`, the PH function under its own key).

**W3:**
- Kg / l with no size are served as recorded: 1.07x / 1.00x the household unit value.
- g / cl with no size are held: 150x / 57x.

**W2:** g / cl are held. W2 g is mixed at 2.24x. The ruling does not name W2's 5 cl rows, which stay held
with the grams.

**W5:** "quantity of one".
- g / cl rows with no size: Price is `c2q3 / c2q2a` (`pp-w5-metric-unit-quantity`), because there
  `c2q2a` is the package size in g / cl (evidence below).
- The 63 Bottle/Can rows with a coded metric size are divided by that size, whatever their `c2q2a`.
- The other 60 rows recording a `c2q2a` other than 1 are held.

### Separate commit: a live PH defect

`_canon_community_unit` placed two W4/W5 labels wrongly:
- `251. BAG OF 20 SACHETS` was served as `Sack/Bag` (2019Q1 148 rows, 2024Q1 261). It is now
  `Bag of 20 Sachets`.
- W5's `4. CENTLITRES (CL)` was served as `l` (2024Q1, 49 rows). It is now `cl`.

Effect on the served rows:
- The 409 relabelled rows keep their `Price`.
- The 49 cl rows have a blank size, so they now drop under the existing `c8-unrecorded-size-drop`:
  2024Q1 PH goes from 30,064 to 30,015, and W5's drop count from 133 to 182.

The shared harvest decoder `_canon_unit` is unchanged.

### Evidence that `c2q2a` is in g / cl (checked before dividing)

Neither the form nor the manuals say so:
- The printed PP form has no quantity column (Section C2, PDF pp.6-11).
- The PP supervisor manual asks for "the UNIT for ONE quantity ... the price for ONE quantity"
  (PDF pp.22-23).

The evidence is in the file itself:
- **Codebook labels.** `c2q2a` is labelled "Quantity", beside `c2q2b` "Unit of measurement".
- **Conversion factor.** `c2q2_cvn` is 0.001 on every g row and 0.01 on every cl row that carries one
  (196 of the 208).
- **Other-unit text.** `c2q2b_os` is non-blank on all 231 g / cl rows with no size. It states the package:
  a gram figure, or a gram or millilitre figure with a container word.
  - g rows: `c2q2a` equals a number in the text on 175 of 176.
  - cl rows: the text is in millilitres, and `c2q2a` equals that figure / 10 on all 32.
  - One g row is unresolved: its number is garbled, and it is served at about 12x its item median.
- **The 23 rows with no `c2q2a`** have no number in their text, and they stay held.

## W1 Number of Units, band by band (the red team's test and the ruling)

W1's priced rows (measured): 15,015 with N = 1, 7,199 with N > 1 and 105 with no count, 22,319 in all. The
"Register the division" ruling quoted 15,014 / 7,198 (red team F5). Every count is integral, so the bands
are exact.

The references, each calibrated by its own N = 1 rows:
- the household unit value in the same `(v, j, u)` cell (Kg N = 1: 1.05);
- PH 2011Q1's community price in the same cluster (Kg 1.00, l 0.85, non-metric 1.00);
- W1's own N = 1 price for the same `(j, u)`.

Each cell gives the median price / reference, with the share within 2x in brackets; the number after the
slash is the count of matched cells or rows.

| band | rows (measured; the red team's) | household, divided | PH 2011Q1, divided | W1 N = 1, divided | W1 N = 1, undivided | served as |
|---|---|---|---|---|---|---|
| Kg N 2-5 | 547; 547 | 0.75 (63%) / 237 | 0.56 (47%) / 266 | 0.60 (51%) / 534 | 1.6 (53%) | divided, **unvalidated key** |
| Kg N 6-50 | 208; 208 | 0.33 (32%) / 94 | 0.16 (22%) / 72 | 0.155 (16%) / 193 | 2.0 (38%) | **held** |
| Kg N > 50 | 128 | 0.009-0.23 | | | | held (earlier ruling) |
| non-metric N 2-5 | 1,432; 1,432 | -- | 0.40 (36%) / 271 | 0.50 (42%) / 1,240 | 1.67 (32%) | divided, **unvalidated key** |
| non-metric N 6-50 | 1,601; 1,601 | -- | 0.20 (24%) / 233 | 0.14 (17%) / 1,408 | 1.5 (39%) | divided, **unvalidated key** |
| non-metric N > 50 | 241; 241 | -- | 0.087 (17%) / 6 | 0.015 (0%) / 238 | 1.0 (70%) | **held** |
| Unknown N > 1 | 49 (20 / 26 / 3); 49 | -- | -- | 0.38 / 0.23 / 0.005 on 13 / 12 / 2 | 1.1 / 1.75 / 0.99 | divided, **unvalidated key** |
| g N 2-5 + ml N 2-5 | 61 + 3; 61 + 3 | 9.0 (10%) / 21 (undivided 30) | | | | **held** |
| g N 6-50 | 263 | 1.01 (44%) / 90 | | | | divided, original key |
| g N > 50 | 1,754 | 0.75 (58%) / 566 | | | | divided, original key |
| ml N 6-50 | 44 | no matched cell | | | | divided, original key |
| ml N > 50 | 328 | 1.00 (74%) / 27 | | | | divided, original key |
| l N 2-5 | 183 | 1.00 (79%) / 84 | 0.89 (77%) / 53 | 0.83 (56%) / 177 | 2.2 (28%) | divided, original key |
| l N 6-50 | 153 | 0.67 (54%) / 79 | 0.71 (55%) / 40 | 0.12 (33%) / 147 | 1.1 (53%) | divided, original key (mixed) |

Every count I measured equals the red team's.
- **Supported** (`pp-w1-number-of-units`): 2,725. That is g 2,017, ml 372 and l 336, the red team's
  2,078 / 375 / 336 less the 61 + 3 g / ml rows now held.
- **Unvalidated** (`pp-w1-number-of-units-unvalidated`): 3,629 = 3,033 + 547 + 49.
- **Newly held:** 513 = 208 + 241 + 64.

What the bands mean:
- **l N 6-50 is mixed.** It stays on the original key by the ruling ("Supported bands (g other N, ml, l)
  stay under the existing division key").
- **ml N 6-50 cannot be checked:** no household cell matches it.
- **Elasticity.** Within each item and unit, the elasticity of the Naira figure with respect to N is 0.47
  for non-metric units, 0.46 for Kg and 0.41 for Unknown. A true count would give 1; a count that is
  really a size would give 0.
- **The red team's caveat** sits in the unvalidated key's assumptions. The within-W1 reference and the
  elasticity assume N is unrelated to unit size, so the non-metric N 2-50 rows are unvalidated, not refuted.
- **Unknown above N = 50 (3 rows)** shows the signature of the held non-metric N > 50 band: 0.005x
  divided, 0.99x undivided, on 2 matched rows. These rows are divided only because the ruling names all 49
  Unknown-unit rows. Holding them would be a one-line change if the maintainer wants it.
- **The unvalidated key's `rows` selector** is `t: 2010Q3` only, so nominally it spans every 2010Q3 row.
  It stamps only Kg, Unknown and non-metric rows, never g / ml / l (stated in the yml, as F4 asked for
  the PH key).

The registry carries this evidence in each key's `assumptions`, and `CONTENTS.org` has the full table.

## Items

**W3 161 is Palm wine** (already on the branch; confirmed). 77 served rows: 7 at obs 1, 70 at obs 2. The
native label "Palm oil" collides with code 50, and every printed card says Palm wine.

**W2 code 0 is dropped** (already on the branch; confirmed). It is the "No more option" sentinel and has 0
priced rows. It stays in `PP_ITEM_HELD`, so `pp-unharmonized-item-hold` holds 0 rows today. That entry
now spans every PP folder (red team F7), so an off-card item in any wave would be reachable.

**Semovita** ("Add a Semovita row"). The W2 PP card's code 15 is Semovita, and the 2012Q3 household card
has no code 15.

How it is wired:
- The 2012Q3 cell of `harmonize_food` code 15 now names Semovita.
- A label-only row carries Preferred `Semovita` and Aggregate `Other grains and flour`. This is the
  GH #1144 convention, as used by the Groundnuts and Maize (shelled) rows.
- The wave-blind Code contract still reads code 15 as W1's Bread, so the PH C8, crop and input paths are
  unchanged.

**Aggregate `Other grains and flour` is sourced from where Nigeria's own household files record
Semovita.** It comes from the other-specify recode `231. Semovita/semolina` (`item_os_cd` / `item_cd_alt`),
which sits under `item_cd` 23 "Other grains and flour (specify)":

| file | rows | under item_cd 23 |
|---|---|---|
| W4 PP `sect7b_plantingw4` | 60 | 59 (one under 38, other roots and tuber) |
| W4 PH `sect10b_harvestw4` | 29 | 29 |
| W5 PP `sect6b_plantingw5` | 32 | 32 |
| NLSS 2018-19 `sect6b_food_cons` | 69 | 69 |

So `food_acquired` serves W4's Semovita purchases as `Other grains and flour`, and the PP W2 Semovita
price now aggregates with them.

Other countries, for the record:
- GhanaLSS 2016-17 folds "Wheat semolina, suji" into `Wheat Flour` (`GhanaLSS/2016-17/_/categorical_mapping.org:38`, `:679`).
- EHCVM keeps `Semoule de blé` as its own label with no Aggregate (Mali `:391`, Senegal `:83`, Niger `:817`, Burkina `:318`).
- Serbia's COICOP 0106 is "Wheat/Rye Flour and Semolina".

If the maintainer prefers the cross-country reading, the Aggregate is a one-cell change to `Wheat flour`.

**Household files with a code-15 row**, none after W1:
- W1 PP: 2,747 rows; W1 PH: 2,464 rows. All are Bread, which the 2010Q3 / 2011Q1 cells already say.
- W2-W5 household food files and NLSS: 0 rows.

So the Semovita row cannot move `food_acquired`, and the cold build confirms it is frame-equal to base.

Measured Kg medians in W2 PP, in Naira: Semovita 450, Wheat flour 220, Other grains and flour 1,900.

## Units ("Accept all"; "Register tokens, keep Unknown")

`Congo/Mudu`, `ml` and `Tomatoes cup` are now `u` Preferred Labels.
- **Codes:** 1020-1022, following the 1001-1019 convention.
- **No per-wave cell**, so no food or harvest decode reads them; a test pins this.
- **Served rows:** 1,830, 372 and 229, all 2010Q3.

The red team counted 1,834, 375 and 231 before the N-band holds. The new holds take 4 Congo/Mudu (N > 50),
3 ml (N 2-5) and 2 Tomatoes cup (N > 50).

The 399 blank-unit PP rows are served as `u = 'Unknown'` (W1 390, W3 9), the `data_info.yml` sentinel.

Measured on the served frame:
- the only PP `u` outside the `u` Preferred Labels is `Unknown` (399 rows);
- the only PP `j` outside the `harmonize_food` Preferred Labels is `Pepper` (808 rows).

## W5 `c2q2a` other than 1 ("Serve the 63 via c8")

W5 has 123 such rows.
- **Served (63):** Bottle/Can rows with a coded metric size, from 33 CL to 1.5 LITRE. They go through
  `pp-c2-metric-size` (Price = `c2q3` / size), and their `c2q2a` is not used. It restates the coded size
  on only 8 of them.
- **Excluded with their count (60):** 41 Cube, 2 Bottle/Can with no size, 10 Kg and 7 l, under
  `pp-w5-quantity-not-one-hold`.
- **No other unit** among the 123 carries a coded metric size.

All 63 land on `cl`. Divided, against item-unit medians (p10-p90, share within 2x):

| reference | matched | median | p10-p90 | within 2x |
|---|---|---|---|---|
| the same round's other `pp-c2-metric-size` rows | 62 | 0.96x | 0.88-1.07 | 100% |
| PH 2024Q1's `c8-metric-size` | 62 | 0.70x | 0.62-0.95 | 100% |

## Label judgements: ruled 2026-11-12 ("GH587 label judgements and two follow-ups")

These are the rows the wiring PR put up for review, now carried forward with each one's ruling.
- **Items:** "Add a Semovita row". W3 161 is Palm wine and W2 0 is dropped; the other 25 are
  spelling-only and accepted as proposed.
- **Units:** "Accept all".

**Items (29)**

| src | code | native label | served `j` | ruling / note |
|---|---|---|---|---|
| W1 | 65 | fruits: canned | Fruit canned | accepted (spelling / wording) |
| W1 | 66 | fruits: other fruites | Other fruits | accepted |
| W1 | 71 | vegetables: tomatoes puree(canned) | Tomato puree (canned) | accepted |
| W1 | 76 | vegetables : pepper | Pepper | accepted; native label by GH #1144, Aggregate is GH #1537's |
| W1 | 113 | milk and milk products: milk tinned (unsweetend) | Milk tinned (unsweetened) | accepted |
| W1 | 121 | coffee, tea, cocoa and the like beverages:chocolate drinks ( | Chocolate drinks (including Milo) | accepted |
| W1 | 133 | sugar, sweets and confectionary : other sweets confectionary | Other sweets and confectionary | accepted |
| W1 | 154 | non-alcoholic drinks:  fruit juice canned | Fruit juice canned/Pack | accepted |
| W1 | 155 | non-alcoholic drinks:  other non-acoholic drinks | Other non--alcoholic drinks | accepted |
| W2 | 0 | No more option | **not served** (`pp-unharmonized-item-hold`; 0 priced rows) | ruled: dropped, a sentinel |
| W2 | 15 | Semovita | **Semovita** (72 rows) | ruled: "Add a Semovita row"; label-only harmonize_food row, Aggregate `Other grains and flour` |
| W2 | 65 | Fruits (canned) | Fruit canned | accepted |
| W2 | 76 | Pepper | Pepper | accepted; as W1 76 |
| W2 | 153 | Soft drinks (Coca Cola, Sprite, etc) | Soft drinks (Coca Cola, spirit, etc) | accepted |
| W3 | 52 | 52. Groundnuts Oil | Groundnut oil | accepted |
| W3 | 53 | 53. Other oil and fat | Other oils and fats | accepted |
| W3 | 78 | 78. Leaves (Cocoyam, Spinash,etc.) | Leaves (Cocoyam, Spinach, etc.) | accepted |
| W3 | 94 | 94. Wild game/bush meat | Wild game meat | accepted |
| W3 | 147 | 147. Mellon (ground) | Melon (ground) | accepted |
| W3 | 153 | 153. Soft drinks (Coca cola, sprit, etc) | Soft drinks (Coca Cola, spirit, etc) | accepted |
| W3 | 161 | 161. Palm oil | **Palm wine** (77 rows) | ruled: native label collides with code 50; every printed card says Palm wine |
| W4 | 52 | 52. Groundnuts Oil | Groundnut oil | accepted |
| W4 | 53 | 53. Other oil and Fat | Other oils and fats | accepted |
| W4 | 94 | 94. Wild game/bush  meat | Wild game meat | accepted |
| W4 | 133 | 133. Other sweets and confectionery | Other sweets and confectionary | accepted |
| W5 | 52 | 52. Groundnuts Oil | Groundnut oil | accepted |
| W5 | 53 | 53. Other oil and Fat | Other oils and fats | accepted |
| W5 | 94 | 94. Wild game/bush  meat | Wild game meat | accepted |
| W5 | 133 | 133. Other sweets and confectionery | Other sweets and confectionary | accepted |

**Units (47, plus the missing-unit rule), all accepted.** The rows column gives the priced-row counts from
the mapping step.

| src | code | native label | rows | served `u` (+ `Size`) | note |
|---|---|---|---|---|---|
| W1 | 1 | tomatoes cup (0.087 kg) | 233 | `Tomatoes cup` | registered, `u` code 1022 (weightless) |
| W1 | 5 | derica cup (big)  (0.7 kg) | 85 | `Derica` (Size `big`) | |
| W1 | 6 | congo/mudu (1.5 kg) | 1841 | `Congo/Mudu` | registered, `u` code 1020 |
| W1 | 41 | bunch of plantain/ffb: small (5 kg) | 122 | `Bunch` (Size `small`) | |
| W1 | 42 | bunch of plantain/ffb: medium (8 kg) | 164 | `Bunch` (Size `medium`) | |
| W1 | 43 | bunch of plantain/ffb: big (15 kg) | 40 | `Bunch` (Size `big`) | |
| W1 | 51 | tuber of yam: small (3 kg) | 132 | `Tuber` (Size `small`) | |
| W1 | 52 | tuber of yam: medium (5 kg) | 95 | `Tuber` (Size `medium`) | |
| W1 | 53 | tuber of yam: big/large (8 kg) | 42 | `Tuber` (Size `big/large`) | |
| W1 | 61 | bundle of millet, g/corn, sugarcane, vegetable etc: small (1 | 88 | `Bundle` (Size `small`) | |
| W1 | 62 | bundle of millet, g/corn, sugarcane, vegetable etc: medium ( | 18 | `Bundle` (Size `medium`) | |
| W1 | 63 | bundle of millet, g/corn, sugarcane, vegetable etc: big (40  | 11 | `Bundle` (Size `big`) | |
| W1 | 71 | wheel barrow: small (60 kg) | 15 | `Wheelbarrow` (Size `small`) | |
| W1 | 72 | wheel barrow: medium (85 kg) | 36 | `Wheelbarrow` (Size `medium`) | |
| W1 | 73 | wheel barrow: big/large (110 kg) | 9 | `Wheelbarrow` (Size `big/large`) | |
| W1 | 74 | wheel barrow: extra large (150 kg) | 5 | `Wheelbarrow` (Size `extra large`) | |
| W1 | 91 | jerry can, keg, rubber of palm oil: small(10 lt.) | 201 | `Jerry Can` (Size `small`) | |
| W1 | 92 | jerry can, keg, rubber of palm oil: medium (20 lt.) | 105 | `Jerry Can` (Size `medium`) | |
| W1 | 93 | jerry can, keg, rubber of palm oil: big (25 lt.) | 34 | `Jerry Can` (Size `big`) | |
| W1 | 94 | jerry can, keg, rubber of palm oil: large (50 lt.) | 0 | `Jerry Can` (Size `large`) | |
| W1 | 95 | jerry can, keg, rubber of palm oil: drum (200 lt.) | 0 | `Jerry Can` (Size `drum`) | |
| W1 | 104 | millilitre | 460 | `ml` | registered, `u` code 1021 (the decoder read "litre") |
| W2 | 10 | MEDIUM DERCA | 68 | `Derica` (Size `MEDIUM`) | |
| W3 | 10 | 10. BIN/BASKET | 18 + 22 | `Basket` | |
| W3 | 14 | 14. TIN | 30 + 9 | `Derica` | TIN → Derica, the PH fold |
| W3 | 22 | 22. MUDU LARGE | 0 + 2 | `Mudu` (Size `LARGE`) | |
| W3 | 31 | (no native label) | 707 + 53 | `Mudu` (Size `LARGE`) | override; printed card Mudu Large |
| W3 | 32 | 32. DERICA LARGE | 0 + 0 | `Derica` (Size `LARGE`) | |
| W3 | 42 | (no native label) | 66 + 1 | `Derica` (Size `LARGE`) | override; printed card Derica Large |
| W4 | 10 | 10. BIN/BASKET | 152 | `Basket` | |
| W4 | 14 | 14. TIN | 131 | `Derica` | |
| W4 | 100 | 100. BUNCH/BUNDLE | 870 | `Bunch` | |
| W4 | 190 | 190. JERRY CAN/RUBBER | 0 | `Jerry Can` | |
| W4 | 210 | 210. STEMS | 0 | `210. STEMS` | verbatim, GH #1095 |
| W4 | 250 | 250. SACHET | 348 | `Packet/Sachet` | |
| W4 | 251 | 251. BAG OF 20 SACHETS | 103 | `Bag of 20 Sachets` | |
| W4 | 260 | 260. SEEDLING/PLANT | 0 | `260. SEEDLING/PLANT` | verbatim, GH #1095 |
| W5 | 4 | 4. CENTLITRES (CL) | 36 | `cl` | |
| W5 | 10 | 10. BIN/BASKET | 72 | `Basket` | |
| W5 | 40 | 40. DERICA/TIN | 855 | `Derica` | |
| W5 | 100 | 100. BUNCH/BUNDLE | 753 | `Bunch` | |
| W5 | 120 | 120. SACHET | 2887 | `Packet/Sachet` | |
| W5 | 125 | 125. PACKET | 83 | `Packet/Sachet` | |
| W5 | 190 | 190. JERRY CAN/RUBBER | 0 | `Jerry Can` | |
| W5 | 210 | 210. STEMS | 0 | `210. STEMS` | verbatim |
| W5 | 251 | 251. BAG OF 20 SACHETS | 464 | `Bag of 20 Sachets` | |
| W5 | 260 | 260. SEEDLING/PLANT | 0 | `260. SEEDLING/PLANT` | verbatim |
| every source | (missing) | — | W1 390, W3 9 served | `Unknown` (`data_info.yml` sentinel) | ruled "keep Unknown"; aligning PH's drop is a follow-up |

## Must-state

1. **Pepper:** 808 rows pass through `labels='Aggregate'` (2010Q3 396, 2012Q3 412), and
   `LabelPassThroughWarning` reports the same number.
   - The red team measured 817 at `e5940eaa9`. The new W1 holds remove 9 W1 Pepper rows; CONTENTS.org
     said 837, which is now corrected.
   - Pepper's own Aggregate is GH #1537's ruling, a separate batch.
2. **A full Nigeria re-warm.**
   - Every Nigeria table hash moves, because `nigeria.py`, `categorical_mapping.org`, `data_scheme.yml` and
     the two new `_/*.py` files are in every Nigeria table's input hash.
   - Any later edit to `pp_community_prices.py` or `pp_community_price_sources.py` re-warms all of Nigeria.
   - No other country moves (red team: 0 hashes for Tanzania, Uganda or Malawi; this step touches no
     library code).
3. **W2's 5 `cl` rows are held** with W2's grams under `pp-unrecorded-size-hold`. W2 has no size field, and
   no ruling names them.
4. **PH W3 Kg / l asymmetry (follow-up).** PP W3 serves its 4,120 blank-size Kg / l rows; PH W3 still drops
   its 3,433. The ruling is "Measure PH, then align": test PH W3's rows against household unit values and
   serve them if per-unit. That is the next Nigeria batch, not this PR.
5. **PH drops blank-unit rows (follow-up).** PP serves them as `Unknown`. Aligning PH's drop is a follow-up.
6. **Coverage regrade owed.** `.coder/coverage/latest.csv` still grades the five PP `community_prices` cells
   `dropped`. `make matrix` was not run.
7. **One W5 gram row** is served at about 12x its item median under `pp-w5-metric-unit-quantity`. Its text
   number is garbled; it is served as ruled.
8. **The W3 31 / 42 overrides** cover 827 served rows: Mudu Large and Derica Large, absent from the native
   card.
9. **`unit-unplaced`** (an unlabelled unit code off the card) has no registry key. It has 0 rows, and it
   warns (red team F7).

## Measured

All builds are cold, each arm in its own private root:
- base `e5fdd2fa8`;
- previous head `e5940eaa9`;
- candidate `d0ba8ee23`. The final head differs from it only in tests and prose.

**Unchanged:**
- **Other tables:** `sample` (49,089), `household_roster` (287,630), `food_acquired` (595,725),
  `crop_production` (66,090), `plot_inputs` (96,620) and `plot_features` (56,403) pass
  `assert_frame_equal(check_exact=True)` against base. Their `attrs` and their warning and audit sets are
  identical.
- **PH rows:** the 146,792 PH `community_prices` rows are frame-equal to `e5940eaa9`.

**`community_prices` checks:**
- The index is unique; 0 null keys; 0 non-positive or NA prices.
- Site B, Site I and Site Q file 0 reports. Site Q does not screen `community_prices`.
- The warning set is one `ClusterIdentityWarning`, as on the previous head.

**Registry**, through `Country.derivation_inputs`:

| key | kind | inputs | check |
|---|---|---|---|
| `pp-w1-number-of-units` | construction | 2,725 | joins the 2,725 stamped rows exactly; the registered function reproduces `Price` |
| `pp-w1-number-of-units-unvalidated` | construction | 3,629 | same (3,629) |
| `pp-c2-metric-size` | construction | 7,775 (2018Q3 3,156 + 2023Q3 4,619; 63 with `c2q2a` not 1) | same (7,775) |
| `pp-w5-metric-unit-quantity` | construction | 208 | same (208) |
| `pp-w1-no-count-hold` | omission | 105 | unique, disjoint from served, attrs `rows: 0` |
| `pp-w1-metric-package-hold` | omission | 1,043 | same |
| `pp-w1-count-as-size-hold` | omission | 332 (N 52-10,150) | same |
| `pp-w1-metric-small-count-hold` | omission | 64 (N 2-5) | same |
| `pp-w1-kg-mid-count-hold` | omission | 208 (N 6-50) | same |
| `pp-w1-nonmetric-count-as-size-hold` | omission | 241 (N 51-1,200) | same |
| `pp-unrecorded-size-hold` | omission | 13,166 | same |
| `pp-unharmonized-item-hold` | omission | 0 | empty frame, attrs `rows: 0` |
| `pp-w5-quantity-not-one-hold` | omission | 60 | same |

**Assembly:**
- `labels='Aggregate'`: Semovita maps to `Other grains and flour`, and only `Pepper` passes through. The
  Aggregate index is non-unique (21,215 duplicate rows), which is pre-existing and disclosed.
- `Feature('community_prices')(['Nigeria','Tanzania','Uganda'])`:
  - 293,192 rows, unique index;
  - Nigeria 275,493, Tanzania 17,699;
  - Uganda is dropped ("no attribute community_prices"), as on base.

**Tests** (`-n 4 --dist=loadfile`, in private roots). The selection is every `tests/test_*nigeria*.py`,
`test_gh587*`, `test_community_prices_variety.py`, `test_derivations.py` (drift tests in both directions),
`test_build_transform_hash.py`, `test_no_build_input_in_contents_org.py`,
`test_categorical_mapping_resolution.py`, `test_gh1537*` and `test_gh1578*`.

The full selection ran on `4ce666e97` (burst job 41251687). The final head `9e74f2cf9` differs from it only
in registry / CONTENTS.org / ledger prose, so stage 1 was rerun there (job 41251929): 308 passed, 1 skipped,
0 failed.

| run (`4ce666e97`) | files | passed | skipped | failed |
|---|---|---|---|---|
| stage 1, synthetic / config, fail-fast | -- | 308 | 1 | 0 |
| base `e5fdd2fa8` | 45 | 1,370 | 79 | 0 |
| candidate | 47 (+ the two new `test_gh587_nigeria_pp_*`) | 1,518 | 80 | 0 |

Assertions changed (pins moved, none weakened):
- **`test_gh637` `EXPECTED`:** 2010Q3 20,326, 2012Q3 21,196, 2023Q3 29,938.
- **`test_nigeria_cluster_identity.py`:** the new key added to the exact PP key set; the omission test now
  pins each key's exact held count (it was `> 0`), and pins the item-hold at 0.
- **The two GH #1537 pin tests** compare the live harmonize_food table to the frozen baseline fixture
  (sha256-pinned, unchanged) as GH #587 changed it. They also assert that this change moves exactly one
  quarter map entry, 2012Q3 code 15.
- **New synthetic pins:** every W1 class × band; the Semovita resolution and its label-only row; the `u`
  tokens with no per-wave cell; the W5 Bottle/Can rule.

## Red-team findings addressed

| finding | how |
|---|---|
| F1, F2 | ruled; the bands are split as above, with the evidence in the registry and CONTENTS.org |
| F3 | the tokens are registered; `MAPPING.org`'s W1 code 1 row and the stale ledger sentence are corrected; the Pepper count is re-measured |
| F4 | the PH `c8-metric-size` selector is narrowed to `t: [2016Q1, 2019Q1, 2024Q1]` |
| F5 | the measured counts are quoted (above) |
| F6 | the three quoted `c2q2b_os` values are paraphrased in `derivations.yml` and CONTENTS.org |
| F7 | `pp-unharmonized-item-hold` spans every PP folder; `unit-unplaced` is stated as a residual |

## Cost

A Nigeria-wide re-warm. No other country moves and no library code is touched.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
