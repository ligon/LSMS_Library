# GH #587: serve Nigeria's five post-planting community_prices cells (+ a PH C8 decoder fix)

Every Nigeria post-planting (PP) community form prices food in Section C2. `CONTENTS.org` said C8 "is
post-harvest only ... correctly empty"; the forms and the files contradict that. This PR serves the five
PP quarters on Nigeria's shared labels, per the five 2026-11-12 GH587 rulings:
- `j` is the PP quarter's `harmonize_food` Preferred Label.
- `u` is the PH base unit, and the native size goes in a new optional `Size` column.

The crosswalk lives in `Nigeria/_/pp_community_prices.py`, so nothing auto-applies it to another Nigeria
table.

## What is served

**129,079 PP rows** (cold build, private root):

| PP t | served | `Derivation` | held, each group its own `rows: 0` registry entry |
|---|---|---|---|
| 2010Q3 | 20,839 | 6,867 `pp-w1-number-of-units` | 105 `pp-w1-no-count-hold`; 1,043 `pp-w1-metric-package-hold` (963 g, 80 ml, N = 1); 332 `pp-w1-count-as-size-hold` (128 Kg, 204 l, N > 50) |
| 2012Q3 | 21,124 | -- | 209 `pp-unrecorded-size-hold` (204 g, 5 cl); 72 `pp-unharmonized-item-hold` (Semovita) |
| 2015Q3 | 24,995 (obs 1 18,365, obs 2 6,630) | -- | 12,934 `pp-unrecorded-size-hold` (8,445 g, 4,489 cl) |
| 2018Q3 | 32,246 | 3,156 `pp-c2-metric-size` | none |
| 2023Q3 | 29,875 | 4,556 `pp-c2-metric-size`; 208 `pp-w5-metric-unit-quantity` | 23 `pp-unrecorded-size-hold` (19 g, 4 cl); 123 `pp-w5-quantity-not-one-hold` |

**Price basis** (rulings 2026-11-12):

*W1* (`sc2q3a` Naira, `sc2q3b` Number of Units, `sc2q3c` unit):
- N = 1: served as recorded.
- N > 1: Price is `sc2q3a / sc2q3b` (`pp-w1-number-of-units`).
- Held:
  - g / ml with N = 1: package prices, about 144x the household per-gram price.
  - Kg / l with N > 50: there the count reads as a size; divided, these rows sit 50-1000x below the
    household price. N = 50 is still divided.
  - Rows with no count.

*W2-W4*: the printed "PLEASE USE A QUANTITY OF ONE (1)", served as recorded.
- W4/W5 coded metric sizes are divided by the PH rule (`pp-c2-metric-size`, the PH function, its own key).

*W3*:
- Kg / l with no size are served as recorded: 1.07x / 1.00x the household unit value.
- g / cl with no size are held: 150x / 57x.

*W2*: g / cl are held. W2 g is mixed at 2.24x; the ruling does not name W2's 5 cl rows, which stay held with
the grams.

*W5*: "quantity of one".
- On g / cl rows with no size, Price is `c2q3 / c2q2a` (`pp-w5-metric-unit-quantity`), because there `c2q2a`
  is the package size in g / cl. Evidence is in the next section.
- The 123 rows recording another `c2q2a` are held.

**Separate commit, a live PH defect:**
- `251. BAG OF 20 SACHETS` was served as `Sack/Bag`: 2019Q1 148 rows, 2024Q1 261.
- W5's `4. CENTLITRES (CL)` was served as `l`: 2024Q1, 49 rows.

They are now `Bag of 20 Sachets` and `cl`. The 49 cl rows have a blank size, so they drop under the existing
`c8-unrecorded-size-drop`.

## Evidence that `c2q2a` is in g / cl (checked before dividing)

Neither the form nor the manuals state it:
- The printed PP form has no quantity column (Section C2, PDF pp.6-11).
- The PP supervisor manual says "indicate the UNIT for ONE quantity ... record the price for ONE quantity"
  (PDF pp.22-23).
- The PP interviewer manual has no community section, and the BID says only "Section C2: Food Prices -- No
  changes".

The evidence is in the file itself:
- **Codebook labels.** `c2q2a` is labelled "Quantity", beside `c2q2b` "Unit of measurement".
- **Conversion factor.** `c2q2_cvn`, labelled "conversion factor item-unit-size-zone", is 0.001 on every g
  row and 0.01 on every cl row that carries one (196 of the 208).
- **Other-unit text.** `c2q2b_os` is non-blank on all 231 g / cl rows with no size, and it states the package
  ('250G', '750ML BOTTLE'):
  - g rows: `c2q2a` equals a number in the text on 175 of 176 (172 exactly the gram figure, one to float32
    precision).
  - cl rows: the text is in ml, and `c2q2a` equals that figure / 10 on all 32, which is centilitres.
  - One g row is unresolved: its text has a letter inside its number and most plausibly states a package
    about 12x its `c2q2a`, so its served price may be about 12x high. It is served as ruled. No text states a
    unit other than the row's.
- **The 23 rows without `c2q2a`** have no number in their text, and stay held.

## Measured

All builds cold, in private roots:
- Base is `e5fdd2fa8`; the previous head (first wiring plus the decoder fix) is `c541bf49f`.

**Unchanged:**
- PH `community_prices` rows (146,792) are frame-equal to `c541bf49f`.
- `sample`, `household_roster` and `food_acquired` are frame-equal to base.

**PP keyed diff, previous head to this one:**
- 1,375 rows leave: 2010Q3 g 963, ml 80, Kg 128, l 204.
- 4,328 arrive: 2015Q3 Kg 3,075 and l 1,045; 2023Q3 g 176 and cl 32.
- The 124,751 common rows show 0 value differences in `Price`, `Derivation` or `Size`.

**Registry:**
- Each construction's `derivation_inputs` equals its stamped rows (6,867 / 7,712 / 208), and the registered
  function reproduces `Price`.
- Each omission's inputs are disjoint from the served index and unique, and attrs list them at `rows: 0`:
  105 / 1,043 / 332 / 13,166 / 72 / 123.

**Other checks:**
- Every served PP g / cl / ml row carries a derivation key.
- The read-time audits on `community_prices` report 0 Site B (null-read) and 0 Site I (null-index) findings
  in every arm, and the warning set is identical to the previous head. Site Q does not screen
  `community_prices`, so it has nothing to say here.
- `Feature('community_prices')(['Nigeria','Tanzania','Uganda'])` gives 293,570 rows with a unique index.
- `labels='Aggregate'` passes `Pepper` through on 817 rows; it was 837 before W1's holds.

**Tests**: base `e5fdd2fa8` 4,311 passed / 106 skipped / 0 failed; candidate `f79c2f854` 4,453 passed / 106 skipped / 0 failed (every test file naming Nigeria or community_prices, plus `test_gh587*`, `test_derivations.py` (both drift directions), `test_build_transform_hash.py`, `test_no_build_input_in_contents_org.py`, `test_community_prices_variety.py`, `test_feature_canonical_index.py`, `test_coverage_matrix.py`); final head targeted 292 passed / 0 failed.

## For maintainer review: every label judgement row (implemented as MAPPING.org proposes)

Generated from the mapping step's `items.csv` / `units.csv`, taking every row whose flag is not OK. W3 rows cover both
locations; W5 rows that inherit W4's decision are listed under both. "rows" is the priced-row count where the mapping
tables carry it.

**Items (29)**

| src | code | native label | served `j` | alternative / note |
|---|---|---|---|---|
| W1 | 65 | fruits: canned | Fruit canned | spelling / truncation / wording only |
| W1 | 66 | fruits: other fruites | Other fruits | spelling / truncation / wording only |
| W1 | 71 | vegetables: tomatoes puree(canned) | Tomato puree (canned) | spelling / truncation / wording only |
| W1 | 76 | vegetables : pepper | Pepper | Fresh Pepper |
| W1 | 113 | milk and milk products: milk tinned (unsweetend) | Milk tinned (unsweetened) | spelling / truncation / wording only |
| W1 | 121 | coffee, tea, cocoa and the like beverages:chocolate drinks ( | Chocolate drinks (including Milo) | spelling / truncation / wording only |
| W1 | 133 | sugar, sweets and confectionary : other sweets confectionary | Other sweets and confectionary | spelling / truncation / wording only |
| W1 | 154 | non-alcoholic drinks:  fruit juice canned | Fruit juice canned/Pack | spelling / truncation / wording only |
| W1 | 155 | non-alcoholic drinks:  other non-acoholic drinks | Other non--alcoholic drinks | spelling / truncation / wording only |
| W2 | 0 | No more option | **unserved** (`pp-unharmonized-item-hold`) | questionnaire sentinel |
| W2 | 15 | Semovita | **unserved** (`pp-unharmonized-item-hold`) | no harmonize_food row; Code contract would say "Bread" |
| W2 | 65 | Fruits (canned) | Fruit canned | spelling / truncation / wording only |
| W2 | 76 | Pepper | Pepper | Fresh Pepper |
| W2 | 153 | Soft drinks (Coca Cola, Sprite, etc) | Soft drinks (Coca Cola, spirit, etc) | spelling / truncation / wording only |
| W3 | 52 | 52. Groundnuts Oil | Groundnut oil | spelling / truncation / wording only |
| W3 | 53 | 53. Other oil and fat | Other oils and fats | spelling / truncation / wording only |
| W3 | 78 | 78. Leaves (Cocoyam, Spinash,etc.) | Leaves (Cocoyam, Spinach, etc.) | spelling / truncation / wording only |
| W3 | 94 | 94. Wild game/bush meat | Wild game meat | spelling / truncation / wording only |
| W3 | 147 | 147. Mellon (ground) | Melon (ground) | spelling / truncation / wording only |
| W3 | 153 | 153. Soft drinks (Coca cola, sprit, etc) | Soft drinks (Coca Cola, spirit, etc) | spelling / truncation / wording only |
| W3 | 161 | 161. Palm oil | Palm wine | Palm oil |
| W4 | 52 | 52. Groundnuts Oil | Groundnut oil | spelling / truncation / wording only |
| W4 | 53 | 53. Other oil and Fat | Other oils and fats | spelling / truncation / wording only |
| W4 | 94 | 94. Wild game/bush  meat | Wild game meat | spelling / truncation / wording only |
| W4 | 133 | 133. Other sweets and confectionery | Other sweets and confectionary | spelling / truncation / wording only |
| W5 | 52 | 52. Groundnuts Oil | Groundnut oil |  |
| W5 | 53 | 53. Other oil and Fat | Other oils and fats |  |
| W5 | 94 | 94. Wild game/bush  meat | Wild game meat |  |
| W5 | 133 | 133. Other sweets and confectionery | Other sweets and confectionary |  |

**Units (47, plus the missing-unit rule)**

| src | code | native label | rows | served `u` (+ `Size`) | alternative |
|---|---|---|---|---|---|
| W1 | 1 | tomatoes cup (0.087 kg) | 233 | `Tomatoes cup` | tomatoes cup |
| W1 | 5 | derica cup (big)  (0.7 kg) | 85 | `Derica` (Size `big`) | large derica |
| W1 | 6 | congo/mudu (1.5 kg) | 1841 | `Congo/Mudu` | congo/mudu |
| W1 | 41 | bunch of plantain/ffb: small (5 kg) | 122 | `Bunch` (Size `small`) | Bunch Small |
| W1 | 42 | bunch of plantain/ffb: medium (8 kg) | 164 | `Bunch` (Size `medium`) | Bunch Medium |
| W1 | 43 | bunch of plantain/ffb: big (15 kg) | 40 | `Bunch` (Size `big`) | Bunch Big |
| W1 | 51 | tuber of yam: small (3 kg) | 132 | `Tuber` (Size `small`) | Yam Tuber Small |
| W1 | 52 | tuber of yam: medium (5 kg) | 95 | `Tuber` (Size `medium`) | Yam Tuber Medium |
| W1 | 53 | tuber of yam: big/large (8 kg) | 42 | `Tuber` (Size `big/large`) | Yam Tuber Big/Large |
| W1 | 61 | bundle of millet, g/corn, sugarcane, vegetable etc: small (1 | 88 | `Bundle` (Size `small`) | Bundle Small |
| W1 | 62 | bundle of millet, g/corn, sugarcane, vegetable etc: medium ( | 18 | `Bundle` (Size `medium`) | Bundle Medium |
| W1 | 63 | bundle of millet, g/corn, sugarcane, vegetable etc: big (40  | 11 | `Bundle` (Size `big`) | Bundle Big |
| W1 | 71 | wheel barrow: small (60 kg) | 15 | `Wheelbarrow` (Size `small`) | Wheelbarrow Small |
| W1 | 72 | wheel barrow: medium (85 kg) | 36 | `Wheelbarrow` (Size `medium`) | Wheelbarrow Medium |
| W1 | 73 | wheel barrow: big/large (110 kg) | 9 | `Wheelbarrow` (Size `big/large`) | Wheelbarrow Big/Large |
| W1 | 74 | wheel barrow: extra large (150 kg) | 5 | `Wheelbarrow` (Size `extra large`) | Wheelbarrow Extra Large |
| W1 | 91 | jerry can, keg, rubber of palm oil: small(10 lt.) | 201 | `Jerry Can` (Size `small`) | Jerry Can Small |
| W1 | 92 | jerry can, keg, rubber of palm oil: medium (20 lt.) | 105 | `Jerry Can` (Size `medium`) | Jerry Can Medium |
| W1 | 93 | jerry can, keg, rubber of palm oil: big (25 lt.) | 34 | `Jerry Can` (Size `big`) | Jerry Can Big |
| W1 | 94 | jerry can, keg, rubber of palm oil: large (50 lt.) | 0 | `Jerry Can` (Size `large`) | Jerry Can Large |
| W1 | 95 | jerry can, keg, rubber of palm oil: drum (200 lt.) | 0 | `Jerry Can` (Size `drum`) | Jerry Can Drum |
| W1 | 104 | millilitre | 460 | `ml` | ml |
| W2 | 10 | MEDIUM DERCA | 68 | `Derica` (Size `MEDIUM`) | medium derica |
| W3 | 10 | 10. BIN/BASKET | 18 + 22 | `Basket` | bin/basket |
| W3 | 14 | 14. TIN | 30 + 9 | `Derica` | tin |
| W3 | 22 | 22. MUDU LARGE | 0 + 2 | `Mudu` (Size `LARGE`) | large mudu |
| W3 | 31 | (no native label) | 707 + 53 | `Mudu` (Size `LARGE`) | large mudu |
| W3 | 32 | 32. DERICA LARGE | 0 + 0 | `Derica` (Size `LARGE`) | large derica |
| W3 | 42 | (no native label) | 66 + 1 | `Derica` (Size `LARGE`) | large derica |
| W4 | 10 | 10. BIN/BASKET | 152 | `Basket` | bin/basket |
| W4 | 14 | 14. TIN | 131 | `Derica` | tin |
| W4 | 100 | 100. BUNCH/BUNDLE | 870 | `Bunch` | bunch |
| W4 | 190 | 190. JERRY CAN/RUBBER | 0 | `Jerry Can` |  |
| W4 | 210 | 210. STEMS | 0 | `210. STEMS` | Stem |
| W4 | 250 | 250. SACHET | 348 | `Packet/Sachet` | Sachet |
| W4 | 251 | 251. BAG OF 20 SACHETS | 103 | `Bag of 20 Sachets` | Bag of 20 Sachets |
| W4 | 260 | 260. SEEDLING/PLANT | 0 | `260. SEEDLING/PLANT` | Seedling/Plant |
| W5 | 4 | 4. CENTLITRES (CL) | 36 | `cl` |  |
| W5 | 10 | 10. BIN/BASKET | 72 | `Basket` | bin/basket |
| W5 | 40 | 40. DERICA/TIN | 855 | `Derica` | derica |
| W5 | 100 | 100. BUNCH/BUNDLE | 753 | `Bunch` | bunch |
| W5 | 120 | 120. SACHET | 2887 | `Packet/Sachet` | packet/sachet |
| W5 | 125 | 125. PACKET | 83 | `Packet/Sachet` | Packet |
| W5 | 190 | 190. JERRY CAN/RUBBER | 0 | `Jerry Can` |  |
| W5 | 210 | 210. STEMS | 0 | `210. STEMS` | Stem |
| W5 | 251 | 251. BAG OF 20 SACHETS | 464 | `Bag of 20 Sachets` | Bag of 20 Sachets |
| W5 | 260 | 260. SEEDLING/PLANT | 0 | `260. SEEDLING/PLANT` | Seedling/Plant |
| every source | (missing) | — | W1 390, W3 9 served | `Unknown` (data_info.yml sentinel) | dropped, as PH drops a blank unit |

## Also for review

- **One W5 gram row is unresolved.** Its Other-unit text has a letter inside its number and most plausibly
  states a package about 12x its `c2q2a`, so its served price may be about 12x high. It is served as ruled.
- **One omission entry, or three?** The two newly held W1 groups have their own keys. W2 g / cl, W3 g / cl and
  W5's 23 g / cl rows with no `c2q2a` share the existing `pp-unrecorded-size-hold` (the same no-package-size
  condition). Splitting it would change the registry only.
- **W5's 123 `c2q2a`-not-1 rows.** These are 41 CUBE, 65 BOTTLE/CAN, 10 KILOGRAMS and 7 LITRES.
  - CUBE price scales with `c2q2a`.
  - 63 of the BOTTLE/CAN rows have a coded metric size and could be served by the PH division.
- **W3 Kg / l asymmetry.** PH W3 still drops its 3,433 blank-size Kg / l rows, which PP W3 now serves. PH W3
  was not re-measured.
- **PH `c8aq2_a`.** PH W5's same field restates millilitres unconverted on 7 of its 49 cl rows. Those rows are
  dropped, so nothing served depends on it.

## Cost

A Nigeria-wide re-warm, because `_/pp_community_prices.py`, `data_scheme.yml` and the wave scripts are
hashed. No other country moves, and no library code is touched.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
