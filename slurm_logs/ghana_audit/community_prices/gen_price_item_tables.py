"""Generate the per-wave ``harmonize_price_item`` org tables for GhanaLSS community_prices.

The curated item lists below are the CONTENT; this script only formats them
and appends one ``* Community price survey`` section to each wave's
``categorical_mapping.org`` (idempotent: an existing section is replaced).
Commentary is kept ABOVE the ``#+name:`` line (CONTENTS.org: an Org
``#+name:`` attaches to the NEXT element).

Columns: Code | Label | Preferred Label | Food | Unit | Basis | Note
  Food   yes  = on the wave's harmonize_food axis (joins food_acquired.j)
         own  = a food the wave's axis does not name; own label
         no   = non-food; own label
  Unit   the u the form fixes for the item ('' where the file carries a unit
         per row, e.g. 2016-17)
  Basis  number of Unit the form's price refers to ('' where the file carries
         a quantity per row: 1987-88/1988-89 QUAN, 2012-13 s1stkg, 2016-17
         quantity)

Sources: 1987-88 form (GHA_1987_GLSS_Price_Questionnaire_EN.pdf, rendered;
the 1988 file is the same blob), BID §2.3/§6.2; GLSS4 form
(GHA_1998_GLSS_Price_Questionnaire_EN.pdf, text layer); GLSS3 list
RECONSTRUCTED from the GLSS4 form by aligning per-item medians/counts (see
CONTENTS.org); GLSS6 from the .dta value labels of price_sec1/price_sec2.

Usage: gen_price_item_tables.py [wave ...]   (default: all waves defined here)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from lsms_library.paths import countries_root

assert 'wt-glss-prices' in str(countries_root()), countries_root()
ROOT = countries_root() / 'GhanaLSS'

KG, L, BB = 'Kilogram', 'Liter', 'Beer Bottle'

# ---------------------------------------------------------------------------
# 1987-88 (GLSS1) -- 47 items on the form; 28 food (KG), 4 pharmaceutical
# (TABLETS), 2 pharmaceutical + 13 non-food (DESCRIPTION).  QUANn is in the
# file, so Basis is blank except for the multi-unit descriptions.
# j on the 1987-88 harmonize_food axis (58 labels; no Bread / Pepper / Juice /
# Guinea corn / evaporated-milk / specific-oil labels -> own labels spelled as
# the later waves' Preferred Labels).
# ---------------------------------------------------------------------------
W1987 = [
    (1,  'Cassava (fresh)',            'Cassava (fresh)', 'yes', KG, '', ''),
    (2,  'Maize (shelled)',            'Maize',           'yes', KG, '', ''),
    (3,  'Guinea Corn',                'Guinea corn/sorghum', 'own', KG, '', 'no guinea corn label on the 1987-88 axis'),
    (4,  'Millet',                     'Millet',          'yes', KG, '', ''),
    (5,  'Wheat bread',                'Bread',           'own', KG, '', 'axis has Wheat Flour / Biscuit, no Bread'),
    (6,  'Garri',                      'Cassava (flour)', 'yes', KG, '', 'gari = cassava flour, as the axis codes it'),
    (7,  'Yam',                        'Yam',             'yes', KG, '', ''),
    (8,  'Cocoyam',                    'Cocoyam',         'yes', KG, '', ''),
    (9,  'Plantain',                   'Plantain',        'yes', KG, '', ''),
    (10, 'Onion (local)',              'Onion',           'yes', KG, '', ''),
    (11, 'Garden egg',                 'Eggplant',        'yes', KG, '', ''),
    (12, 'Tomato (fresh)',             'Tomato (fresh)',  'yes', KG, '', ''),
    (13, 'Tomato paste',               'Tomato (paste)',  'yes', KG, '', ''),
    (14, 'Pepper (dried)',             'Pepper (dry)',    'own', KG, '', 'no pepper label on the 1987-88 axis'),
    (15, 'Oranges',                    'Orange',          'yes', KG, '', ''),
    (16, 'Pineapple juice',            'Juice',           'own', KG, '', 'axis has only Other Beverages'),
    (17, 'Chicken eggs (ea)',          'Egg',             'yes', 'Piece', '', 'form pre-fills 1 per observation: priced each'),
    (18, 'Chicken (cock)',             'Chicken',         'yes', KG, '', 'weighed live (QUAN median 1.5 kg)'),
    (19, 'Smoked fish (Herring)',      'Fish',            'yes', KG, '', 'the 1987-88 axis has a single Fish label'),
    (20, 'Dried fish (Tilapia)',       'Fish',            'yes', KG, '', ''),
    (21, 'Beef, with bones',           'Beef',            'yes', KG, '', ''),
    (22, 'Palm oil',                   'Oil (palm)',      'yes', KG, '', 'weighed (QUAN median 0.79 kg)'),
    (23, 'Palm kernel oil',            'Oil (palm kernel)', 'own', KG, '', 'axis has only Other Oils'),
    (24, 'Groundnut oil',              'Oil (groundnut)', 'own', KG, '', 'axis has only Other Oils'),
    (25, 'Groundnuts (shelled)',       'Groundnut',       'yes', KG, '', ''),
    (26, 'Akpeteshie (1 quarter)',     'Akpeteshie',      'own', KG, '', 'KG column carries the quarter-bottle fraction (QUAN median 0.25)'),
    (27, 'White sugar',                'Sugar',           'yes', KG, '', ''),
    (28, 'Evaporated milk IDEAL, 170 g', 'Milk',          'yes', KG, '', 'QUAN median 0.17 = the 170 g tin'),
    (29, 'Aspirin',                    'Aspirin',         'no', 'Tablet', '', 'QUAN = number of tablets'),
    (30, 'Paracetamol',                'Paracetamol',     'no', 'Tablet', '', ''),
    (31, 'Nivaquine',                  'Nivaquine (chloroquine)', 'no', 'Tablet', '', ''),
    (32, 'Other anti-malarial tablets', 'Anti-malarial tablets (other)', 'no', 'Tablet', '', ''),
    (33, "Andrew's liver salt (1 packet)", 'Andrews liver salt', 'no', 'Packet', '', ''),
    (34, 'Milk of magnesia (1 bottle)', 'Milk of magnesia', 'no', 'Bottle', '', ''),
    (35, 'Kerosene (1 beer bottle)',   'Kerosene',        'no', BB, '', ''),
    (36, 'Firewood (bunch, 1 ft diameter)', 'Firewood',   'no', 'Bunch', '', ''),
    (37, 'Dry cell battery (1.5 volts)', 'Dry cell battery', 'no', 'Piece', '', ''),
    (38, 'Iron coal pot (standard home size)', 'Iron coal pot', 'no', 'Piece', '', ''),
    (39, 'Hurricane lamp (1)',         'Hurricane lamp',  'no', 'Piece', '', ''),
    (40, 'Matches (Kade, standard size)', 'Matches',      'no', 'Box', '', ''),
    (41, 'Charcoal (small quantity)',  'Charcoal',        'no', KG, '', 'KG-column item (weighed)'),
    (42, 'Soap (Guardian, standard size)', 'Soap (Guardian)', 'no', 'Piece', '', ''),
    (43, 'Local cloth, wax (6 yards)', 'Wax print cloth', 'no', 'Yard', 6, 'QUAN counts 6-yard lots'),
    (44, 'Machete (Cutlass, std size)', 'Cutlass/machete', 'no', 'Piece', '', ''),
    (45, 'Fertilizer (mini bag)',      'Fertilizer',      'no', 'Mini Bag', '', ''),
    (46, 'Metal bucket (std size)',    'Metal bucket',    'no', 'Piece', '', ''),
    (47, 'Plastic bucket (std size)',  'Plastic bucket',  'no', 'Piece', '', ''),
]

# 1988-89 (GLSS2): the shipped questionnaire is the SAME blob as 1987's;
# PRICE.DAT carries one extra code 48 (QUAN median 0.5 in the KG column,
# PRICE1 median 120) that is on no form we hold.
W1988 = W1987 + [
    (48, 'Item 48 (not on the form)', 'Unidentified price item 48', 'no', KG, '',
     'absent from the GLSS1 form the wave ships; KG-column values'),
]

# ---------------------------------------------------------------------------
# 1998-99 (GLSS4) -- the 123-item form.  Foods: KG unless the form fixes a
# container; pharmaceuticals and non-foods: the form's DESCRIPTION.  The file
# ships only the per-unit value (`price` = PRICE/KG), so Basis is what
# NumberOfUnits becomes.  Tuple: code, label, PL_1998, PL_1991, food, unit,
# basis, note.  (PL_1991 feeds the reconstructed GLSS3 table.)
# ---------------------------------------------------------------------------
W1998 = [
    (1,  'Guinea corn',              'Guinea corn/sorghum', 'Guinea corn/sorghum', 'yes', KG, 1, ''),
    (2,  'Maize (shelled)',          'Maize', 'Maize', 'yes', KG, 1, ''),
    (3,  'Millet',                   'Millet', 'Millet', 'yes', KG, 1, ''),
    (4,  'Rice (Local)',             'Rice', 'Rice', 'yes', KG, 1, ''),
    (5,  'Rice (Imported)',          'Rice', 'Rice', 'yes', KG, 1, ''),
    (6,  'Sorghum',                  'Sorghum', 'Sorghum', 'yes', KG, 1, ''),
    (7,  'Wheat bread (White)',      'Bread', 'Bread', 'yes', KG, 1, ''),
    (8,  'Corn dough',               'Maize (flour/dough)', 'Maize (flour/dough)', 'yes', KG, 1, ''),
    (9,  'Kenkey',                   'Kenkey', 'Kenkey', 'yes', KG, 1, ''),
    (10, 'Cocoyam',                  'Cocoyam', 'Cocoyam', 'yes', KG, 1, ''),
    (11, 'Yam',                      'Yam', 'Yam', 'yes', KG, 1, ''),
    (12, 'Plantain',                 'Plantain', 'Plantain', 'yes', KG, 1, ''),
    (13, 'Cassava (fresh)',          'Cassava', 'Cassava', 'yes', KG, 1, ''),
    (14, 'Gari',                     'Cassava (flour)', 'Cassava (flour)', 'yes', KG, 1, 'gari = cassava flour, as the axis codes it'),
    (15, 'Cassava dough',            'Cassava (dough)', 'Cassava (dough)', 'yes', KG, 1, ''),
    (16, 'Konkonte (flour)',         'Konkonte', 'Konkonte', 'yes', KG, 1, ''),
    (17, 'Cowpeas (small beans)',    'Small Bean', 'Small Bean', 'yes', KG, 1, 'the 9b consumption label is Small beans'),
    (18, 'Bambara beans',            'Bambara Bean', 'Bambara Bean', 'yes', KG, 1, ''),
    (19, 'Palmnuts',                 'Palm Nut', 'Palm Nut', 'yes', KG, 1, ''),
    (20, 'Groundnuts (shelled)',     'Groundnut', 'Groundnut', 'yes', KG, 1, ''),
    (21, 'Groundnut oil (1 beer bottle)', 'Oil (groundnut)', 'Oil (groundnut)', 'yes', BB, 1, ''),
    (22, 'Palm kernel oil (1 beer bottle)', 'Oil (palm kernel)', 'Oil (palm kernel)', 'yes', BB, 1, ''),
    (23, 'Palm oil (1 beer bottle)', 'Oil (red palm)', 'Oil (red palm)', 'yes', BB, 1, ''),
    (24, 'Margarine (Blue Band)',    'Margarine', 'Margarine', 'yes', KG, 1, ''),
    (25, 'Avocado pear',             'Avocado', 'Avocado', 'yes', KG, 1, ''),
    (26, 'Banana',                   'Banana', 'Banana', 'yes', KG, 1, ''),
    (27, 'Orange',                   'Orange', 'Orange', 'yes', KG, 1, ''),
    (28, 'Pineapple fresh',          'Pineapple', 'Pineapple', 'yes', KG, 1, ''),
    (29, 'Pineapple juice',          'Juice', 'Juice', 'yes', KG, 1, 'KG-column item on the form'),
    (30, 'Cocoyam leaves',           'Cocoyam Leaves', 'Cocoyam Leaves', 'own98', KG, 1, 'on the 1991-92 axis; the 1998-99 axis has no kontomire label'),
    (31, 'Garden eggs',              'Eggplant/Cucumber', 'Eggplant/Cucumber', 'yes', KG, 1, ''),
    (32, 'Okro',                     'Okra', 'Okra', 'yes', KG, 1, ''),
    (33, 'Onion',                    'Onion', 'Onion', 'yes', KG, 1, ''),
    (34, 'Shallots',                 'Onion', 'Onion', 'yes', KG, 1, 'axis folds Onions and Shallot'),
    (35, 'Pepper (sweet green)',     'Pepper', 'Pepper', 'yes', KG, 1, ''),
    (36, 'Pepper (dried)',           'Pepper (dry)', 'Pepper (dry)', 'yes', KG, 1, ''),
    (37, 'Tomato (fresh)',           'Tomato', 'Tomato', 'yes', KG, 1, ''),
    (38, 'Tomato (paste)',           'Tomato (paste)', 'Tomato Puree', 'own98', KG, 1, 'the 1998-99 axis has no tomato-paste label'),
    (39, 'Fresh beef (with bones)',  'Beef', 'Beef', 'yes', KG, 1, ''),
    (40, 'Goat (fresh)',             'Goat', 'Goat', 'yes', KG, 1, ''),
    (41, 'Fresh mutton',             'Goat', 'Goat', 'yes', KG, 1, 'axis folds Fresh Mutton into Goat; Description keeps it apart'),
    (42, 'Pork',                     'Pork', 'Pork', 'yes', KG, 1, ''),
    (43, 'Bushmeat (smoked) - Grass cutter', 'Other Meat', 'Other Meat', 'yes', KG, 1, 'axis codes Bushmeat as Other Meat'),
    (44, 'Snail (fresh)',            'Snail', 'Snail', 'own98', KG, 1, 'on the 1991-92 axis; not on the 1998-99 axis'),
    (45, 'Live Chicken (Local)',     'Chicken', 'Chicken', 'yes', KG, 1, ''),
    (46, 'Live Chicken (Poultry)',   'Chicken', 'Chicken', 'yes', KG, 1, ''),
    (47, 'Chicken eggs',             'Eggs', 'Eggs', 'yes', KG, 1, 'KG-column item on the GLSS3/4 form (1987 priced each)'),
    (48, 'Evaporated milk (Ideal), 0.170 kg tin', 'Milk (tinned, unsweetened)', 'Milk (tinned, unsweetened)', 'yes', KG, 0.17, 'form pre-fills 0.170 per observation'),
    (49, 'White granulated sugar',   'Sugar', 'Sugar', 'yes', KG, 1, ''),
    (50, 'Salt',                     'Salt', 'Salt', 'yes', KG, 1, ''),
    (51, 'Nescafe (Tin)',            'Coffee', 'Coffee', 'yes', KG, 1, ''),
    (52, 'Nescafe (Sachet)',         'Coffee', 'Coffee', 'yes', KG, 1, ''),
    (53, 'Bournvita (Tin)',          'Chocolate Drink', 'Chocolate Drink', 'yes', KG, 1, ''),
    (54, 'Bournvita (Sachet)',       'Chocolate Drink', 'Chocolate Drink', 'yes', KG, 1, ''),
    (55, 'Milo (Tin)',               'Chocolate Drink', 'Chocolate Drink', 'yes', KG, 1, ''),
    (56, 'Milo (Sachet)',            'Chocolate Drink', 'Chocolate Drink', 'yes', KG, 1, ''),
    (57, 'Chocolate drink',          'Chocolate Drink', 'Chocolate Drink', 'yes', KG, 1, ''),
    (58, 'Tea (Lipton)',             'Tea', 'Tea', 'yes', KG, 1, ''),
    (59, 'Herring (Smoked)',         'Fish (smoked)', 'Fish (smoked)', 'yes', KG, 1, ''),
    (60, 'Herring (Fresh)',          'Fish (fresh and frozen)', 'Fish (fresh and frozen)', 'yes', KG, 1, ''),
    (61, 'Red fish (Fresh)',         'Fish (fresh and frozen)', 'Fish (fresh and frozen)', 'yes', KG, 1, ''),
    (62, 'Dried fish (Tilapia - Koobi)', 'Fish (dried)', 'Fish (dried)', 'yes', KG, 1, ''),
    (63, 'Pilchards (Geisha)',       'Fish (canned)', 'Fish (canned)', 'yes', KG, 1, ''),
    (64, 'Sardine (Titus)',          'Fish (canned)', 'Fish (canned)', 'yes', KG, 1, ''),
    (65, 'Fanta/Coke/Pepsi (standard 300 ml bottle)', 'Soft Drinks', 'Soft Drinks', 'yes', 'Fanta Bottle', 1, 'the standard 300 ml bottle'),
    (66, 'Palm wine (1 beer bottle)', 'Wine', 'Wine', 'yes', BB, 1, 'axis codes Palm Wine as Wine'),
    (67, 'Pito (1 beer bottle)',     'Beer', 'Beer', 'yes', BB, 1, 'axis codes Pito as Beer'),
    (68, 'Akpeteshie (1 beer bottle)', 'Akpeteshie', 'Akpeteshie', 'yes', BB, 1, ''),
    (69, 'Gin (Local) (720 ml)',     'Gin', 'Gin', 'yes', 'Milliliter', 720, 'form basis 720 ml'),
    (70, 'Jam (Specify)',            'Jam', 'Jam', 'yes', KG, 1, ''),
    (71, 'Honey (1 beer bottle)',    'Honey', 'Honey', 'yes', BB, 1, ''),
    (72, 'Aspirin (10 tablets)',     'Aspirin', 'Aspirin', 'no', 'Tablet', 10, ''),
    (73, 'Paracetamol (10 tablets)', 'Paracetamol', 'Paracetamol', 'no', 'Tablet', 10, ''),
    (74, 'Nivaquine/Chloroquine (10 tablets)', 'Nivaquine (chloroquine)', 'Nivaquine (chloroquine)', 'no', 'Tablet', 10, ''),
    (75, 'Vitamin B Complex (10 tablets)', 'Vitamin B complex', 'Vitamin B complex', 'no', 'Tablet', 10, ''),
    (76, 'Penicillin oral 400,000 units (20 tablets)', 'Penicillin (oral)', 'Penicillin (oral)', 'no', 'Tablet', 20, ''),
    (77, 'Terramycin 250 mg (20 capsules)', 'Terramycin', 'Terramycin', 'no', 'Capsule', 20, ''),
    (78, 'Andrews/Starwin liver salt (1 packet of 10 sachets)', 'Andrews liver salt', 'Andrews liver salt', 'no', 'Packet', 1, ''),
    (79, 'Milk of Magnesia (1 large bottle)', 'Milk of magnesia', 'Milk of magnesia', 'no', 'Bottle', 1, ''),
    (80, 'Omo (medium size)',        'Omo (detergent)', 'Omo (detergent)', 'no', 'Piece', 1, 'medium-size packet'),
    (81, 'Guardian Soap (one tablet)', 'Soap (Guardian)', 'Soap (Guardian)', 'no', 'Piece', 1, ''),
    (82, 'Key Soap (one bar)',       'Key soap', 'Key soap', 'no', 'Bar', 1, ''),
    (83, 'Lantern globe/shade (one standard size)', 'Lantern globe', 'Lantern globe', 'no', 'Piece', 1, ''),
    (84, 'Light bulb (White) (60 watts)', 'Light bulb', 'Light bulb', 'no', 'Piece', 1, ''),
    (85, 'Candle (one stick)',       'Candle', 'Candle', 'no', 'Stick', 1, ''),
    (86, 'Dry cell battery (Tiger head) (1.5 volts)', 'Dry cell battery', 'Dry cell battery', 'no', 'Piece', 1, ''),
    (87, 'Matches (one box)',        'Matches', 'Matches', 'no', 'Box', 1, ''),
    (88, 'Iron coal pot (medium size)', 'Iron coal pot', 'Iron coal pot', 'no', 'Piece', 1, ''),
    (89, 'Kerosene (one beer bottle)', 'Kerosene', 'Kerosene', 'no', BB, 1, ''),
    (90, 'Firewood (9 kg bundle)',   'Firewood', 'Firewood', 'no', KG, 9, ''),
    (91, 'Charcoal (one maxi bag of std. size)', 'Charcoal', 'Charcoal', 'no', 'Maxi Bag', 1, ''),
    (92, 'Liquified petroleum gas (medium cylinder)', 'Liquefied petroleum gas', 'Liquefied petroleum gas', 'no', 'Piece', 1, 'medium cylinder'),
    (93, 'Metal bucket (standard size, 34 cm)', 'Metal bucket', 'Metal bucket', 'no', 'Piece', 1, ''),
    (94, 'Plastic bucket (standard size, 32 cm)', 'Plastic bucket', 'Plastic bucket', 'no', 'Piece', 1, ''),
    (95, 'Hurricane lamp (Medium) (Anchor brand)', 'Hurricane lamp', 'Hurricane lamp', 'no', 'Piece', 1, ''),
    (96, 'Fertilizer N.P.K 15-15-15 (one mini bag)', 'Fertilizer (NPK)', 'Fertilizer (NPK)', 'no', 'Mini Bag', 1, ''),
    (97, 'Fertilizer Sulphate of Ammonia (one mini bag)', 'Fertilizer (sulphate of ammonia)', 'Fertilizer (sulphate of ammonia)', 'no', 'Mini Bag', 1, ''),
    (98, 'Mosquito Coil (a packet of 10)', 'Mosquito coil', 'Mosquito coil', 'no', 'Coil', 10, ''),
    (99, 'Toilet Paper (Rose) (one roll)', 'Toilet paper', 'Toilet paper', 'no', 'Roll', 1, ''),
    (100, 'Razor blade (Tatra) (packet of 10)', 'Razor blade', 'Razor blade', 'no', 'Piece', 10, ''),
    (101, 'Tooth paste, Pepsodent (large)', 'Toothpaste', 'Toothpaste', 'no', 'Piece', 1, 'large tube'),
    (102, 'Vim (large size)',        'Vim (scouring powder)', 'Vim (scouring powder)', 'no', 'Piece', 1, ''),
    (103, 'Cutlass (Crocodile brand) (one standard size)', 'Cutlass/machete', 'Cutlass/machete', 'no', 'Piece', 1, ''),
    (104, 'Tobacco leaf (1 kg)',     'Tobacco', 'Tobacco', 'yes', KG, 1, 'on the food axis (9b consumption list)'),
    (105, "'555' Cigarette (a packet of 20)", 'Cigarette', 'Cigarette', 'yes', 'Stick', 20, ''),
    (106, 'Embassy Cigarette (a packet of 20)', 'Cigarette', 'Cigarette', 'yes', 'Stick', 20, ''),
    (107, 'Local Real Wax Print (GTP) (6 yards)', 'Wax print cloth', 'Wax print cloth', 'no', 'Yard', 6, ''),
    (108, 'Kente Cloth (men) (Fathia Fata Nkrumah)', 'Kente cloth (men)', 'Kente cloth (men)', 'no', 'Piece', 1, ''),
    (109, 'Kente Cloth (women) (Fathia Fata Nkrumah)', 'Kente cloth (women)', 'Kente cloth (women)', 'no', 'Piece', 1, ''),
    (110, 'Adinkra (cotton) (medium quality)', 'Adinkra cloth', 'Adinkra cloth', 'no', 'Piece', 1, ''),
    (111, 'Polyester material (one metre, ordinary quality)', 'Polyester material', 'Polyester material', 'no', 'Meter', 1, ''),
    (112, 'Shirt (long sleeves) (65% synthetic 35% cotton, medium quality)', 'Shirt (long sleeves)', 'Shirt (long sleeves)', 'no', 'Piece', 1, ''),
    (113, 'School uniform (boys, 10-12 yrs) (ready made set)', 'School uniform (boys)', 'School uniform (boys)', 'no', 'Set', 1, ''),
    (114, 'School uniform (girls, 10-12 yrs) (ready made set)', 'School uniform (girls)', 'School uniform (girls)', 'no', 'Set', 1, ''),
    (115, 'Rubber Sandals (Local) (pair, charlie wote)', 'Rubber sandals', 'Rubber sandals', 'no', 'Pair', 1, ''),
    (116, 'Vest (cotton) (women) (one medium size)', 'Vest (women)', 'Vest (women)', 'no', 'Piece', 1, ''),
    (117, 'Singlet (cotton) (men) (one medium size)', 'Singlet (men)', 'Singlet (men)', 'no', 'Piece', 1, ''),
    (118, 'Bedsheet (cotton) (single bed)', 'Bedsheet (single)', 'Bedsheet (single)', 'no', 'Piece', 1, ''),
    (119, 'Bedsheet (cotton) (double bed)', 'Bedsheet (double)', 'Bedsheet (double)', 'no', 'Piece', 1, ''),
    (120, 'Singlet (children) (cotton)', 'Singlet (children)', 'Singlet (children)', 'no', 'Piece', 1, ''),
    (121, 'Vest (children) (cotton)', 'Vest (children)', 'Vest (children)', 'no', 'Piece', 1, ''),
    (122, 'Handkerchief (women) (one dozen)', 'Handkerchief (women)', 'Handkerchief (women)', 'no', 'Dozen', 1, ''),
    (123, 'Handkerchief (men) (one dozen)', 'Handkerchief (men)', 'Handkerchief (men)', 'no', 'Dozen', 1, ''),
]

# GLSS3 (1991-92): the wave ships the GLSS4 form under three file names; the
# data file has 117 codes.  Aligning per-item median prices and observation
# counts against GLSS4 (CONTENTS.org, "Community price survey") shows GLSS3's
# list is the GLSS4 list WITHOUT these six GLSS4 codes, renumbered:
G3_MISSING_G4_CODES = {6, 35, 46, 52, 54, 56}
G3_NOTE = {
    5:  'code identification by price level (n=50); Sorghum absent from GLSS3',
    47: 'alignment: GLSS4 code 52 (Nescafe sachet) absent from GLSS3',
    48: 'alignment: GLSS4 code 54 (Bournvita sachet) absent from GLSS3',
    49: 'alignment: GLSS4 code 56 (Milo sachet) absent from GLSS3',
}


def w1991():
    rows = []
    code = 0
    for (c4, label, pl98, pl91, food, unit, basis, note) in W1998:
        if c4 in G3_MISSING_G4_CODES:
            continue
        code += 1
        f = 'yes' if food in ('yes', 'own98') else food
        n = note
        if code in G3_NOTE:
            n = (n + '; ' if n else '') + G3_NOTE[code]
        elif c4 != code:
            n = (n + '; ' if n else '') + f'GLSS4 code {c4}'
        rows.append((code, label, pl91, f, unit, basis, n))
    assert code == 117, code
    return rows


def w1998():
    return [(c, label, pl98, ('own' if food == 'own98' else food), unit, basis, note)
            for (c, label, pl98, pl91, food, unit, basis, note) in W1998]


# ---------------------------------------------------------------------------
# 2012-13 (GLSS6) -- codes and labels are the .dta value labels of
# price_sec1 (fcode 1-102, food) and price_sec2 (nfcode 103-157, non-food);
# the printed GLSS6 form lists 89 + 47 items with different numbering and is
# NOT the coding of the shipped file.  Food basis is the form's KG/LITRE
# column: s1stkg carries the weight/volume, so Basis is blank; Unit says
# which of the two the item is measured in (a per-item judgement: liquids in
# litres).  Non-food prices refer to a free-text description (s2desc) -> Unit
# 'Other Unit', the text goes to Description.
# ---------------------------------------------------------------------------
W2012_FOOD = [
    (1, 'guinea corn/sorghum', 'Guinea corn/sorghum', 'yes', KG),
    (2, 'maize', 'Maize (cob)', 'yes', KG),
    (3, 'millet', 'Millet', 'yes', KG),
    (4, 'rice (local)', 'Rice (local)', 'yes', KG),
    (5, 'rice (imported)', 'Rice (imported)', 'yes', KG),
    (6, 'bread-sugar bread', 'Sugar Bread', 'yes', KG),
    (7, 'biscuits (cookies)', 'Biscuit', 'yes', KG),
    (8, 'flour (wheat)', 'Wheat Flour', 'yes', KG),
    (9, 'maize ground/corn dough', 'Maize (flour/dough)', 'yes', KG),
    (10, 'corned beef', 'Beef (corned)', 'yes', KG),
    (11, 'snail', 'Snail', 'yes', KG),
    (12, 'beef', 'Beef', 'yes', KG),
    (13, 'goat meat', 'Goat', 'yes', KG),
    (14, 'mutton', 'Goat', 'yes', KG),
    (15, 'bushmeat/wild game (grasscutter)', 'Other Meat', 'yes', KG),
    (16, 'chicken', 'Chicken (live)', 'yes', KG),
    (17, 'crustaceans (lobsters, crabs, prawns)', 'Crustaceans', 'yes', KG),
    (18, 'fish (fresh and frozen)', 'Fish (fresh and frozen)', 'yes', KG),
    (19, 'fish (dried-koobi)', 'Fish (dried)', 'yes', KG),
    (20, 'fish (smoked-river fish)', 'Fish (smoked)', 'yes', KG),
    (21, 'fish (smoked herrings)', 'Fish (smoked)', 'yes', KG),
    (22, 'fish (canned-tuna)', 'Fish (canned)', 'yes', KG),
    (23, 'fish (canned-sardines)', 'Fish (canned)', 'yes', KG),
    (24, 'fish (mackerel in tomato sauce)', 'Fish (canned)', 'yes', KG),
    (25, 'milk (fresh)', 'Milk (fresh)', 'yes', L),
    (26, 'milk (powdered)', 'Milk (powdered)', 'yes', KG),
    (27, 'baby milk (lactogen)', 'Milk (powdered)', 'yes', KG),
    (28, 'tinned milk - ideal, peak etc. (evaporated)', 'Milk (evaporated)', 'yes', KG),
    (29, 'tinned milk (sweetened)', 'Milk (tinned, condensed)', 'yes', KG),
    (30, 'chicken eggs', 'Eggs', 'yes', KG),
    (31, 'coconut oil', 'Oil (coconut)', 'yes', L),
    (32, 'ground nut oil', 'Oil (groundnut)', 'yes', L),
    (33, 'vegetable oil (frytol, obaapa)', 'Other Oils', 'yes', L),
    (34, 'palm oil', 'Oil (palm)', 'yes', L),
    (35, 'shea butter', 'Shea Butter', 'yes', KG),
    (36, 'margarine (canned, sachet)', 'Margarine', 'yes', KG),
    (37, 'kolanut', 'Cola Nut', 'yes', KG),
    (38, 'coconut (fresh)', 'Coconut (fresh)', 'yes', KG),
    (39, 'banana', 'Banana', 'yes', KG),
    (40, 'oranges/tangerines', 'Orange', 'yes', KG),
    (41, 'pineapple', 'Pineapple', 'yes', KG),
    (42, 'mango (grafted)', 'Mango', 'yes', KG),
    (43, 'avocado pear', 'Avocado', 'yes', KG),
    (44, 'apples (imported)', 'Other Fruits', 'yes', KG),
    (45, 'cocoyam leaves (kontomire/alefu)', 'Cocoyam Leaves', 'yes', KG),
    (46, 'garden eggs', 'Eggplant', 'yes', KG),
    (47, 'okro', 'Okra', 'yes', KG),
    (48, 'carrots', 'Carrot', 'yes', KG),
    (49, 'pepper (fresh)', 'Pepper', 'yes', KG),
    (50, 'pepper (dried)', 'Pepper', 'yes', KG),
    (51, 'onions/shallot (large/small)', 'Onion', 'yes', KG),
    (52, 'tomatoes (fresh)', 'Tomato (fresh)', 'yes', KG),
    (53, 'tomato paste (canned)', 'Tomato (paste)', 'yes', KG),
    (54, 'garlic', 'Condiments', 'yes', KG),
    (55, 'sugar (cube, st. louis)', 'Sugar', 'yes', KG),
    (56, 'sugar (granulated)', 'Sugar', 'yes', KG),
    (57, 'honey (bottle)', 'Honey', 'yes', L),
    (58, 'ice cream (fan ice, yoghurt)', 'Ice Cream', 'yes', KG),
    (59, 'chocolate (bar)', 'Chocolate', 'yes', KG),
    (60, 'chewing gum (pk, mentos)', 'Other Confectioneries', 'yes', KG),
    (61, 'black pepper', 'Chilli Powder (black pepper)', 'yes', KG),
    (62, 'salt (iodized)', 'Salt', 'yes', KG),
    (63, 'ginger', 'Ginger', 'yes', KG),
    (64, 'maggi cubes', 'Condiments', 'yes', KG),
    (65, 'cassava (fresh)', 'Cassava (fresh)', 'yes', KG),
    (66, 'cassava dough', 'Cassava (dough)', 'yes', KG),
    (67, 'cassava flour (konkonte)', 'Cassava (flour)', 'yes', KG),
    (68, 'cocoyam', 'Cocoyam', 'yes', KG),
    (69, 'plantain', 'Plantain', 'yes', KG),
    (70, 'yam', 'Yam', 'yes', KG),
    (71, 'beans (white)', 'Bean', 'yes', KG),
    (72, 'groundnut (shelled)', 'Groundnut', 'yes', KG),
    (73, 'groundnut (roasted)', 'Groundnut', 'yes', KG),
    (74, 'palm nut fruits', 'Palm Nut', 'yes', KG),
    (75, 'other pulse and nuts specified (e.g. cashew nut, tiger nuts)', 'Other Nut/Seed', 'yes', KG),
    (76, 'gari', 'Cassava (flour)', 'yes', KG),
    (77, 'coffee (nescafe - tin)', 'Coffee', 'yes', KG),
    (78, 'chocolate drinks (milo, bournvita, other specified)', 'Cocoa Powder', 'yes', KG),
    (79, 'pure cocoa powder (without milk, e.g. brown gold)', 'Cocoa Powder', 'yes', KG),
    (80, 'tea (lipton)', 'Tea bags', 'yes', KG),
    (81, 'coca cola / fanta (bottled)', 'Soft Drinks', 'yes', L),
    (82, 'malt drinks', 'Malt Drinks (bottle)', 'yes', L),
    (83, 'fruits juices (don simon multifruita, ceres)', 'Juice', 'yes', L),
    (84, 'mineral water (bottled)', 'Water', 'yes', L),
    (85, 'mineral water (sachet)', 'Water', 'yes', L),
    (86, 'gin', 'Other Spirits', 'yes', L),
    (87, 'whisky', 'Whisky', 'yes', L),
    (88, 'akpeteshie', 'Akpeteshie', 'yes', L),
    (89, 'bitters (alomo, agya appiah, herbafric)', 'Other Spirits', 'yes', L),
    (90, 'palm wine / raffia palm wine, etc.', 'Wine', 'yes', L),
    (91, 'imported wine', 'Wine', 'yes', L),
    (92, 'pito / brukutu, etc.', 'Wine', 'yes', L),
    (93, 'beer (star, club)', 'Beer', 'yes', L),
    (94, 'beer (imported)', 'Beer', 'yes', L),
    (95, 'guinness and other stout', 'Beer', 'yes', L),
    (96, 'cooked rice and stew', 'Cooked Rice and Stew', 'yes', KG),
    (97, 'fufu/tuo with soup', 'Other Prepared Meals', 'yes', KG),
    (98, 'kenkey with fried fish', 'Other Prepared Meals', 'yes', KG),
    (99, 'fast foods (fried rice)', 'Fast Food', 'yes', KG),
    (100, 'fried plantain and beans (red red)', 'Other Prepared Meals', 'yes', KG),
    (101, 'rice and chicken sauce', 'Other Prepared Meals', 'yes', KG),
    (102, 'banku and stew', 'Other Prepared Meals', 'yes', KG),
]
W2012_NONFOOD_LABELS = {
    103: 'cigarette (555)', 104: 'refuse disposal (payment per month)',
    105: 'expenditure on public toilets fees (wc and others)', 106: 'charcoal (bag-mini)',
    107: 'charcoal (loose - heap/container)', 108: 'firewood and other solid fuels',
    109: 'ice block (household cooling and refrigeration only)', 110: 'omo (medium)',
    111: 'washing soaps and powder (arial / kleesoft - small/medium)',
    112: 'bathing/toilet soaps bottled (liquid - sunlight)', 113: 'key soap (bar)',
    114: 'bathing/toilet soaps tablet (solid - sunlight)', 115: 'bleaches (parazone)',
    116: 'disinfectants and cleaners (dettol, camel, etc.)', 117: 'mosquito coil (heaven)',
    118: 'insecticide spray (raid - tin)', 119: 'matches (one box)',
    120: 'toilet roll/paper (rose, orange - one roll)', 121: 'candles (one stick)',
    122: 'pain killers (paracetamol, apc, etc.)', 123: 'antibiotics',
    124: 'anti malaria medicines (artesunate amodiaquine/atc)',
    125: 'condoms (men - one pack of 3 pieces)', 126: 'corrective eye glasses',
    127: 'ghanaian traditional drug (tablet/syrup)', 128: 'petrol (one litre)',
    129: 'diesel (one litre)', 130: 'kerosene', 131: 'gas', 132: 'electricity, centralized supply',
    133: 'maintenance, repair of vehicle, e.g. wheel alignment, change of oil',
    134: 'washing/ packing space services', 135: 'cost of travel by rail (specified distance)',
    136: 'intercity bus fares (stc, neoplan etc.)', 137: 'trotro, taxi and other transport fares',
    138: 'cost of travel by air', 139: 'cost of travel by ferries and canoes',
    140: 'porters (kayaye, male porters etc.)',
    141: 'cost of luggage and items transported unaccompanied (private/stc)',
    142: 'postage (within ghana)', 143: 'postage (outside ghana)', 144: 'national lotteries',
    145: 'exercise books', 146: 'textbooks, story books, pamphlets/dictionaries etc.',
    147: 'graphic and times', 148: 'private newspaper', 149: 'magazines',
    150: 'services of barbers, beauty shops etc. (men)',
    151: 'services of beauty shops including manicure and pedicure (women)',
    152: 'mesh/wigs (human hair)', 153: 'mesh/wigs (synthetic)', 154: 'tooth paste (pepsodent)',
    155: 'razor blades (e.g. lords, bic etc.)', 156: 'combs', 157: 'scent spray',
}
# Cigarette is on the 2012-13 food axis (9b consumption list) -- keep it there.
W2012_NONFOOD_PL = {103: ('Cigarrette', 'yes')}


def _own_label(label: str) -> str:
    """A tidy own label from a value label: sentence case, parentheticals kept."""
    s = re.sub(r'\s+', ' ', label.strip())
    return s[:1].upper() + s[1:]


def w2012():
    rows = [(c, lab, pl, food, unit, '', '') for (c, lab, pl, food, unit) in W2012_FOOD]
    for c, lab in sorted(W2012_NONFOOD_LABELS.items()):
        pl, food = W2012_NONFOOD_PL.get(c, (_own_label(lab), 'no'))
        rows.append((c, lab, pl, food, 'Other Unit', '', 'basis is the free-text s2desc -> Description'))
    return rows


# ---------------------------------------------------------------------------
HEADER = {
    '1987-88': """The GLSS1 price questionnaire (=Documentation/questionnaire/GHA_1987_GLSS_Price_Questionnaire_EN.pdf=,
image-only, rendered and read 2026-09-02) is keyed LOCALITY / CLUSTER and
lists 47 items: 28 foods with a KG + PRICE pair per observation ("Chicken
eggs (ea)" pre-filled 1, priced each), pharmaceuticals 29-32 with TABLETS +
PRICE, and 33-47 with a fixed DESCRIPTION and 1ST/2ND/3RD PRICE.  BID §2.3:
"Prices from up to three vendors are collected for 28 food, 6 pharmaceutical
and 13 other non-food items ... Weighing scales were used to determine the
exact weight of food items."  =PRICE.DAT= carries =QUANn=/=PRICEn= per
observation (BID §6.2), so =Basis= is blank except for the 6-yard cloth lot.
=Preferred Label= is the 1987-88 =harmonize_food= label where the axis names
the item (=Food=yes=); foods the axis does not name keep the later waves'
spelling (=Food=own=); non-foods their own label (=Food=no=).  Unit labels
are =_/unit_labels.org= Preferred Labels; Tablet / Bottle are new.""",
    '1988-89': """The GLSS2 price questionnaire the wave ships
(=Documentation/questionnaire/GHA_1988_GLSS_Price_Questionnaire_EN.pdf=) is
the SAME blob as 1987-88's (identical md5), so the item list below is the
GLSS1 form's 47 items; =PRICE.DAT= carries one extra code 48 (QUAN median
0.5 in the KG column, PRICE1 median 120) that is on no form we hold and is
kept under its own label.  See the 1987-88 table for the reading of the form
and BID §2.3/§6.2 for the file layout.  Two clusters were priced twice (2305
in June and September 1989; 2310 twice in one month) -- those repeat records
become =obs= 4-6; cluster 2726's rows are dated March 1988 (a first-year
price carried into the second-year set, BID p.17).""",
    '1991-92': """GLSS3's price survey is keyed REGION / DISTRICT / NAME OF LOCALITY / EA
("DOES THIS EA INCLUDE MORE THAN ONE LOCALITY?") with three observations
per item, each a weighed KG + PRICE for foods and a fixed DESCRIPTION +
PRICE for pharmaceuticals and non-foods.  =Data/Prices/G3PRICE.DTA= ships
only the per-unit value =p= (= PRICE/KG, e.g. 500/7) with no KG column and
no labels, so =Basis= (the form's stated basis) is what =NumberOfUnits=
becomes.  CAUTION -- all three price-form files this wave ships
(=pdf/G3QPrice.pdf=, =GHA_1991_GLSS_Price_Questionnaire_EN.pdf.pdf=,
=Glss3 qsts and manuals docs/G3QPrice.doc=) carry the 123-item GLSS4 list,
while =G3PRICE.DTA= has codes 1-117.  The list below is RECONSTRUCTED by
aligning per-item median prices and observation counts with GLSS4
(=GhanaLSS/_/CONTENTS.org=, "Community price survey"): GLSS3 = GLSS4 minus
Sorghum (G4 6), Pepper sweet green (G4 35), Live chicken poultry (G4 46) and
the three sachet variants (G4 52/54/56), renumbered.  The tail (G3 66-117 =
G4 72-123) and the anchors (margarine per kg, the 0.170 kg milk tin, the
Nescafe tin, wax print per 6 yards) are unambiguous; the weakest links are
code 5 (Rice imported vs Sorghum, n=50) and codes 47-49, flagged in =Note=.""",
    '1998-99': """GLSS4's price survey (=Documentation/questionnaire/GHA_1998_GLSS_Price_Questionnaire_EN.pdf=,
text layer) is keyed REGION / DISTRICT / NAME OF LOCALITY / EA with three
observations per item: foods 1-71 as a weighed KG + PRICE (a few with a
pre-filled container: 1 beer bottle for the oils / palm wine / pito /
akpeteshie / honey, 0.170 for the evaporated-milk tin, the standard 300 ml
bottle for soft drinks, 720 ml for local gin); pharmaceuticals 72-79 and
non-foods 80-123 with a fixed DESCRIPTION and 1ST/2ND/3RD PRICE.
=Data/Prices/G4PRICE.DTA= ships only the per-unit value =price= (no KG
column, no labels), so =Basis= is the form's stated basis and becomes
=NumberOfUnits=.  =Preferred Label= is the 1998-99 =harmonize_food= label
where the axis names the item; Cocoyam leaves, Tomato paste and Snail are
not on this wave's axis and keep their own label (=Food=own=).""",
    '2012-13': """GLSS6's price survey (=Documentation/QUESTIONNAIRES/GLSS6 Prices Questionnaire.pdf=)
is keyed REGION / DISTRICT / NAME OF LOCALITY / EA / MARKET NUMBER with three
observations per item: foods as KG/LITRE + PRICE, non-foods as a free
DESCRIPTION + PRICE.  The codes and labels below are the Stata value labels
of =Data/PRICES/price_sec1.dta= (=fcode= 1-102) and =price_sec2.dta=
(=nfcode= 103-157) -- the printed form lists 89 + 47 items under a different
numbering and is NOT the coding of the shipped file.  =s1stkg= etc. carry
the weighed/measured quantity, so =Basis= is blank; =Unit= records whether
the KG/LITRE figure is a weight or a volume (liquids in litres -- a per-item
judgement).  Non-food prices refer to the reader's free-text description
(=s2desc=, 3,437 distinct strings): =Unit= is =Other Unit= and the text goes
to =Description=.  The survey stamps prices per EA but collected them at 41
market numbers (323 region/district/market keys for 1,015 EAs; EAs sharing a
market carry identical prices) -- kept native.""",
}

TITLE = '* Community price survey (GH #562)'

# 2016-17 only: the reader's free-text "other unit" spellings -> u axis.
# Lives INSIDE the section so a regeneration keeps it (it was lost once).
UNIT_TABLE_2016 = '''\
When =unit{a,b,c}= is 99 ("Other unit") the reader wrote the unit in
=unito{a,b,c}=; 132 distinct spellings.  The table below folds the ones that
are plain misspellings or case variants of a unit onto the shared axis
(=_/unit_labels.org= Preferred Labels, plus Tablet / Capsule / Bottle /
Milligram / Page / Person / Month / Feet / Inch / Gigabyte which the axis
lacks); anything not listed stays as written (title-cased) so it is visible
as NEW rather than silently dropped.  Matching is case-insensitive on the
stripped text.
#+name: harmonize_price_unit
| Text            | Preferred Label |
|-----------------+-----------------|
| kg              | Kilogram        |
| ml              | Milliliter      |
| mls.            | Milliliter      |
| mml             | Milliliter      |
| cl              | Milliliter      |
| mg              | Milligram       |
| milligram       | Milligram       |
| milligrams      | Milligram       |
| milligramme     | Milligram       |
| milligramd      | Milligram       |
| milligrsms      | Milligram       |
| millllgram      | Milligram       |
| miligram        | Milligram       |
| 9mg             | Milligram       |
| 9milligrams     | Milligram       |
| 200mg           | Milligram       |
| mm              | Millimeter      |
| millimeter      | Millimeter      |
| millimetre      | Millimeter      |
| millimetres     | Millimeter      |
| milimeter       | Millimeter      |
| milimetre       | Millimeter      |
| capsule         | Capsule         |
| capsules        | Capsule         |
| capsuls         | Capsule         |
| casules         | Capsule         |
| tablet          | Tablet          |
| tablets         | Tablet          |
| tablt           | Tablet          |
| strip           | Blister/Strip   |
| blister         | Blister/Strip   |
| gallon          | Gallon          |
| gallo           | Gallon          |
| galon           | Gallon          |
| frytol gallon   | Gallon          |
| bottle          | Bottle          |
| small bottle    | Bottle          |
| can             | Can             |
| bag             | Bag             |
| buckets         | Bucket          |
| pieces of 330ml | Piece           |
| 330ml pieces    | Piece           |
| 325ml pieces    | Piece           |
| page            | Page            |
| 1 page          | Page            |
| 1page           | Page            |
| per page        | Page            |
| per sheet       | Sheet           |
| person          | Person          |
| per person      | Person          |
| per perdon      | Person          |
| passenger       | Person          |
| passengaer      | Person          |
| passenager      | Person          |
| passage         | Person          |
| month           | Month           |
| monthly         | Month           |
| months          | Month           |
| per month       | Month           |
| year            | Year            |
| yearly          | Year            |
| per day         | Day             |
| room            | Room            |
| per room        | Room            |
| feet            | Feet            |
| inch            | Inch            |
| inches          | Inch            |
| gigabyte        | Gigabyte        |
| gigabite        | Gigabyte        |
| gig             | Gigabyte        |
| 4 gigabytes     | Gigabyte        |
| kwh             | Kilowatts       |
| kilowatts       | Kilowatts       |
| watts           | Watts           |
| volt            | Volt            |
| ampere          | Ampere          |
| ampoule         | Ampoule         |
| acre            | Acre            |
| plot            | Plot            |
| kilometre       | Kilometre       |
| service         | Service         |
| dose            | Dose            |
| course          | Course          |
| stamp           | Stamp           |
| olunka          | American Tin    |
| calabash        | Calabash        |
| board           | Board           |
| rod             | Rod             |
| wheel           | Wheel           |
| ounce           | Ounce           |
| carat           | Carat           |
| meter square    | Square Meter    |
| cubic metre     | Cubic Meter     |
| metric cubic    | Cubic Meter     |
| m3              | Cubic Meter     |
'''



def fmt(v):
    if v == '' or v is None:
        return ''
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def org_table(rows):
    cols = ['Code', 'Label', 'Preferred Label', 'Food', 'Unit', 'Basis', 'Note']
    data = [[fmt(x) for x in r] for r in rows]
    widths = [max(len(c), *(len(d[i]) for d in data)) for i, c in enumerate(cols)]
    def line(vals):
        return '| ' + ' | '.join(v.ljust(w) for v, w in zip(vals, widths)) + ' |'
    sep = '|-' + '-+-'.join('-' * w for w in widths) + '-|'
    return '\n'.join([line(cols), sep] + [line(d) for d in data])


def section(wave, rows):
    body = (f"{TITLE}\n{HEADER[wave]}\n\n#+name: harmonize_price_item\n"
            f"{org_table(rows)}\n")
    if wave == '2016-17':
        body += '\n' + UNIT_TABLE_2016
    return body


def write(wave, rows):
    path = ROOT / wave / '_' / 'categorical_mapping.org'
    text = path.read_text()
    new = section(wave, rows)
    if TITLE in text:
        start = text.index(TITLE)
        m = re.search(r'^\* ', text[start + len(TITLE):], flags=re.M)
        end = start + len(TITLE) + m.start() if m else len(text)
        text = text[:start] + new + ('\n' if m else '') + text[end:]
    else:
        text = text.rstrip('\n') + '\n\n' + new
    path.write_text(text)
    print(f'{wave}: {len(rows)} rows -> {path}')


BUILDERS = {
    '1987-88': lambda: W1987,
    '1988-89': lambda: W1988,
    '1991-92': w1991,
    '1998-99': w1998,
    '2012-13': w2012,
}

# ---------------------------------------------------------------------------
# 2016-17 (GLSS7) -- codes are `ln` in g7price.dta, labels the file's own
# `bname` (item) with `itname` the brand line; the form
# (GLSS7_price questionnaire.xlsx) records PRICE / QTY / UOM per observation,
# so Unit and Basis are blank (decoded per row from unit{a,b,c}).  Foods are
# codes 1-297 (cigarette 295, tobacco 296, kola 297 are on the food axis) and
# the restaurant meals 751-758; everything else is non-food.  Every food code
# is placed EXPLICITLY below on the 2016-17 harmonize_food axis; "OTHER
# (SPECIFY)" lines take their block's label.  Non-food rows keep their own
# (title-cased) bname; brand-only names (SONY, GUCCI) recur across blocks and
# are told apart by Description (bname | brand) and obs.
# ---------------------------------------------------------------------------
G7_FOOD = {
    1: 'Rice (local)', 2: 'Rice (imported)',
    8: 'Guinea corn/sorghum', 9: 'Guinea corn/sorghum',
    10: 'Maize (grain)', 11: 'Maize (grain)', 12: 'Maize (grain)',
    13: 'Millet', 14: 'Millet (flour)',
    15: 'Wheat Flour', 16: 'Wheat Flour', 17: 'Wheat Flour',
    18: 'Maize (flour/dough)', 19: 'Maize (flour/dough)',
    20: 'Cerelac (Baby food)', 21: 'Cerelac (Baby food)', 22: 'Cerelac (Baby food)', 23: 'Cerelac (Baby food)',
    24: 'White Oats', 25: 'Cassava (dried)', 26: 'Cassava (dough)', 27: 'Cassava (yellow, flour)',
    28: 'Sugar Bread', 29: 'Bread', 30: 'Bread', 31: 'Bread',
    32: 'Biscuit', 33: 'Biscuit', 34: 'Biscuit', 35: 'Biscuit', 36: 'Biscuit', 37: 'Biscuit', 38: 'Biscuit',
    42: 'Instant Noodle', 43: 'Corflake', 44: 'Macaroni', 45: 'Spaghetti', 46: 'Other Cereal',
    50: 'Beef', 51: 'Beef', 52: 'Beef (leg)', 53: 'Beef (leg)',
    54: 'Pork', 55: 'Pork (rib)', 56: 'Pork (fillet)', 57: 'Pork (feet)',
    58: 'Goat', 59: 'Goat', 60: 'Goat', 61: 'Goat', 62: 'Goat',
    63: 'Chicken (live)', 64: 'Chicken (broiler)', 65: 'Chicken (live)',
    66: 'Chicken (thigh)', 67: 'Chicken (wing)', 68: 'Guineafowl',
    69: 'Beef (corned)', 70: 'Beef (corned)', 71: 'Sausage (beef)', 72: 'Other Meat',
    73: 'Kapla', 74: 'Shrimp', 75: 'Snail', 77: 'Crab',
    78: 'Other Fish', 79: 'Tilapia', 80: 'Fish (smoked, river)', 81: 'Herring (smoked)',
    82: 'Fish (dried)', 83: 'Fish (salted)',
    84: 'Fish (canned)', 85: 'Fish (canned)', 86: 'Fish (canned)', 87: 'Fish (canned)',
    88: 'Tuna (processed)', 89: 'Tuna (processed)',
    90: 'Mackerel (processed)', 91: 'Mackerel (processed)', 92: 'Mackerel (processed)',
    93: 'Mackerel (processed)', 94: 'Mackerel (processed)', 95: 'Mackerel (processed)', 96: 'Mackerel (processed)',
    100: 'Milk (evaporated)', 101: 'Milk (evaporated)', 102: 'Milk (evaporated)', 103: 'Milk (evaporated)',
    104: 'Milk (evaporated)', 105: 'Milk (tinned, condensed)', 106: 'Milk (evaporated)',
    110: 'Milk (powdered)', 111: 'Milk (powdered)', 112: 'Milk (powdered)', 113: 'Milk (powdered)',
    114: 'Milk (powdered)', 115: 'Milk (powdered)', 116: 'Milk (powdered)', 117: 'Milk (powdered)', 118: 'Milk (powdered)',
    122: 'Milk (powdered)', 123: 'Milk (powdered)', 124: 'Milk (powdered)', 125: 'Milk (powdered)',
    126: 'Milk (powdered)', 127: 'Milk (powdered)', 128: 'Milk (powdered)', 129: 'Milk (powdered)', 130: 'Milk (powdered)',
    134: 'Milk (fresh)', 135: 'Milk (powdered)', 136: 'Other Milk Products', 137: 'Ice Cream', 138: 'Yoghurt',
    139: 'Other Milk Products', 140: 'Other Milk Products', 141: 'Eggs', 142: 'Margarine',
    143: 'Oil (coconut)', 144: 'Oil (groundnut)', 145: 'Oil (palm)',
    146: 'Oil (vegetable)', 147: 'Oil (vegetable)', 148: 'Oil (vegetable)', 149: 'Oil (vegetable)',
    153: 'Shea Butter', 154: 'Oil (palm kernel)', 155: 'Coconut (fresh)',
    156: 'Banana', 157: 'Orange', 158: 'Pineapple', 159: 'Mango', 160: 'Mango', 161: 'Watermelon',
    162: 'Avocado', 163: 'Apple', 164: 'Grape', 165: 'Apple',
    166: 'Groundnut', 167: 'Groundnut', 168: 'Palm Nut', 169: 'Lime', 170: 'Pawpaw', 171: 'Canned Fruits',
    172: 'Plantain', 173: 'Cocoyam Leaves', 174: 'Sweet Pepper', 175: 'Carrot', 176: 'Carrot',
    177: 'Eggplant', 178: 'Okra', 179: 'Pepper (fresh)', 180: 'Pepper (dried, red)',
    181: 'Chilli Powder (black pepper)', 182: 'Vinegar', 183: 'Pepper (powder)',
    184: 'Onion', 185: 'Shallot', 186: 'Tomato (fresh)',
    190: 'Tomato (paste)', 191: 'Tomato (paste)', 192: 'Tomato (paste)', 193: 'Tomato (paste)',
    197: 'Tomato (paste)', 198: 'Tomato (paste)', 199: 'Tomato (paste)', 200: 'Tomato (paste)',
    201: 'Garlic', 202: 'Cowpea', 203: 'Cassava (fresh)', 204: 'Cocoyam', 205: 'Yam', 206: 'Water Yam',
    207: 'Sugar (cubed)', 208: 'Sugar (granulated)', 209: 'Honey',
    213: 'Chocolate', 214: 'Chocolate', 215: 'Chocolate',
    219: 'Chewing Gum', 220: 'Chewing Gum', 221: 'Chewing Gum', 222: 'Chewing Gum', 223: 'Chewing Gum',
    227: 'Salt', 228: 'Salt', 229: 'Salt', 230: 'Ginger', 231: 'Vinegar',
    232: 'Condiments', 233: 'Condiments', 234: 'Condiments', 235: 'Condiments',
    239: 'Coffee', 240: 'Cocoa Powder', 241: 'Tea bag',
    242: 'Cocoa (milk powder beverages)', 243: 'Cocoa (milk powder beverages)', 244: 'Cocoa (milk powder beverages)',
    245: 'Cocoa (milk powder beverages)', 246: 'Other Beverages', 247: 'Cocoa (milk powder beverages)',
    248: 'Cocoa (milk powder beverages)',
    250: 'Water', 251: 'Water', 252: 'Water', 256: 'Water', 257: 'Soft Drinks',
    259: 'Malt Drinks (bottle)', 260: 'Malt Drinks (bottle)', 261: 'Malt Drinks (bottle)',
    266: 'Malt Drinks (canned)', 267: 'Malt Drinks (canned)', 268: 'Malt Drinks (canned)',
    273: 'Juice', 274: 'Juice', 275: 'Juice', 276: 'Juice', 277: 'Juice', 278: 'Juice', 279: 'Juice', 280: 'Juice',
    284: 'Gin', 285: 'Whisky', 286: 'Akpeteshie', 287: 'Bitters', 288: 'Schnapps',
    289: 'Wine', 290: 'Wine', 291: 'Beer', 292: 'Beer', 293: 'Beer', 294: 'Beer',
    295: 'Cigarette', 296: 'Tobacco', 297: 'Kola Nut',
    751: 'Pizza', 752: 'Other Prepared Meals', 753: 'Other Prepared Meals', 754: 'Other Prepared Meals',
    756: 'Cooked Rice and Stew', 757: 'Soup', 758: 'Kenkey/Banku',
}
G7_NOTE = {
    27: 'unqualified gari; the axis places Code_8h "Cassava-gari" under the yellow label',
    38: 'other (specify) in the biscuit block', 46: 'other (specify) in the pasta/cereal block',
    58: 'axis folds mutton into Goat', 59: 'axis folds mutton into Goat', 60: 'live sheep; axis has no sheep label',
    61: 'live goat', 89: 'other (specify), tuna block', 96: 'other (specify), mackerel block',
    106: 'other (specify), tinned-milk block', 118: 'other (specify), milk-sachet block',
    130: 'other (specify), powdered-milk block', 135: 'infant formula, as 2012-13',
    136: 'Fan Milk (frozen milk product)', 193: 'other (specify), canned tomato paste',
    200: 'other (specify), sachet tomato paste', 215: 'other (specify), chocolate block',
    223: 'other (specify), chewing-gum block', 235: 'other (specify), stock-cube block',
    246: 'chocolate-flavoured milk drink', 252: 'other (specify), bottled water',
    261: 'other (specify), bottled malt', 268: 'other (specify), canned malt',
    280: 'other (specify), juice block', 289: 'axis codes Palm Wine as Wine', 294: 'axis codes pito as Beer',
    752: 'restaurant dish', 753: 'restaurant dish', 754: 'restaurant dish', 757: 'axis codes fufu and light soup as Soup',
}


def w2016():
    from lsms_library.local_tools import get_dataframe, df_from_orgfile
    g7 = get_dataframe(str(ROOT / '2016-17' / 'Data' / 'g7price.dta'), convert_categoricals=False)
    items = (g7.groupby('ln')['bname'].agg(lambda s: s.mode().iloc[0] if len(s.mode()) else '')
               .reset_index())
    hf = df_from_orgfile(ROOT / '2016-17' / '_' / 'categorical_mapping.org', name='harmonize_food',
                         to_numeric=False)
    hf.columns = [c.strip() for c in hf.columns]
    food_axis = {str(x).strip().lower() for x in hf['Preferred Label'] if str(x).strip()}
    # Non-food own labels must not land on the food axis (ln 771 "ORANGE" is a
    # toilet roll, ln 812/820 "APPLE" a wrist watch) and a brand name that
    # recurs across blocks (SONY 640/669, SHARP 663/706/720, RAID 534/541) or
    # an "OTHER (SPECIFY)" line must not fold unrelated products onto one j:
    # such labels get the item code appended so each code keeps its own j.
    labels = {int(r['ln']): re.sub(r'\s+', ' ', str(r['bname']).strip()) for _, r in items.iterrows()}
    nonfood_counts = {}
    for code, lab in labels.items():
        if code and lab and code not in G7_FOOD:
            nonfood_counts[lab.lower()] = nonfood_counts.get(lab.lower(), 0) + 1
    rows = []
    for code, lab in sorted(labels.items()):
        if code == 0 or lab == '':
            continue
        note = G7_NOTE.get(code, '')
        if code in G7_FOOD:
            pl, food = G7_FOOD[code], 'yes'
        else:
            assert code > 297 and not (751 <= code <= 758), f'2016-17 food code {code} ({lab}) not placed'
            own = _own_label(lab.lower())
            key = lab.lower()
            if key.startswith('other (specify)'):
                pl = f'Other (specify) [ln {code}]'
                note = (note + '; ' if note else '') + 'other (specify) line of a non-food block'
            elif key in food_axis or nonfood_counts[key] > 1:
                pl = f'{own} [ln {code}]'
                why = 'name is a food label' if key in food_axis else 'brand name recurs across blocks'
                note = (note + '; ' if note else '') + f'{why}; code appended to keep its own j'
            else:
                pl = own
            food = 'no'
        rows.append((code, lab, pl, food, '', '', note))
    return rows


HEADER['2016-17'] = """GLSS7's price survey (=Documentation/GLSS7_price questionnaire.xlsx=, cover
=COVER PAGE _PRICE.docx=) is keyed REGION / DISTRICT / CLUSTER / MARKET NAME
and records, per item and BRAND line, three observations each with PRICE,
QTY and UOM.  =Data/g7price.dta= carries =ln= (the item code), =bname= (the
item), =itname= (the brand line), =price{a,b,c}=, =quantity{a,b,c}=,
=unit{a,b,c}= (its own value labels: the =unit_9b= list plus 72=Service,
75=Visit) and =unito{a,b,c}= (other-unit text).  So =Unit= and =Basis= are
blank here -- both are decoded per row -- and the brand goes to
=Description=.  Foods are codes 1-297 (cigarette, tobacco and kola are on
the food axis) plus the restaurant meals 751-758; every one is placed
explicitly on the 2016-17 =harmonize_food= axis (the price code scheme is
NOT the =Code_9b= consumption scheme: 0 of 644 codes coincide).  Non-foods
keep their own label.  Two clusters (70002, 70909) carry a mis-keyed sibling
EA's rows (the price file's region contradicts the household cover for 11
cluster/region pairs) -- kept native, told apart by =obs=."""

BUILDERS['2016-17'] = w2016


def main(waves):
    for w in waves or list(BUILDERS):
        rows = BUILDERS[w]()
        codes = [r[0] for r in rows]
        assert len(codes) == len(set(codes)), f'{w}: duplicate codes'
        write(w, rows)


if __name__ == '__main__':
    main(sys.argv[1:])
