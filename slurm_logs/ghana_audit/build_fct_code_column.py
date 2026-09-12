#!/usr/bin/env python
"""Add the ``FCT Code`` column to ``GhanaLSS/_/food_items.org#food_label`` (GH #563).

The map is CURATED, not generated: it is a judgement about which West African
Food Composition Table 2019 food best represents each GhanaLSS harmonised food
label.  It is written here, in one reviewable place, rather than being
recomputed by a fuzzy matcher at build time.

Provenance of each entry, recorded in ``SOURCE`` below:

``anchor``
    Taken from the producers' own public companion sheet, "Ghana Food
    Expenditure Data with Aggregate Labels + FCT Codes"
    (docs.google.com/spreadsheets/d/1qZhbq5gpAmCsYH1ixUn0Ix_Cb5YKSIpU-Pc2_hh2lhU,
    retrieved 2026-09-04).  NOTE that sheet is keyed to **GhanaSPS** waves
    (2009-10 / 2013-14 / 2017-18), not GhanaLSS's, and only 39 of GhanaLSS's
    195 Preferred Labels appear in it verbatim.

``anchor-departed``
    The companion sheet has an entry, but it is a poor proxy where the WAFCT
    carries the actual food.  Each departure is listed with what the sheet
    said, so the disagreement is auditable rather than silent.

``derived``
    No entry in the companion sheet; chosen here against the WAFCT food list.

Curation rules applied throughout:

* Prefer the ``raw`` / unprepared form.  ``food_acquired`` records food
  ACQUIRED, not food as eaten, so the WAFCT's ``* (as part of a recipe)`` and
  ``boiled/grilled/steamed`` rows -- and its Burkina Faso mixed dishes -- are
  never used.  This is the same reasoning that leaves datasheets 07/08/09
  (yield, retention, mixed dishes) unapplied.
* Prefer ``unfortified`` where the WAFCT splits a food by fortification;
  fortification is not recorded by the GLSS instrument, and the unfortified
  value is the conservative base.
* An "Other X" residual label is mapped to a representative member of its
  class, never to a nutrient-dense outlier.
* Non-foods (tobacco, cigarettes, chewing gum) get NO code: they carry mass
  but no dietary nutrients, and inventing one would put calories into the
  household's intake.

Run:  python slurm_logs/ghana_audit/build_fct_code_column.py
"""
import re
from pathlib import Path

import pandas as pd

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

#: Preferred Label -> (FCT Code, provenance, note)
#: ``note`` is required for every 'anchor-departed' entry.
MAP = {
    # ---- cereals & cereal products -------------------------------------
    'Maize (flour/dough)':   ('01_081', 'derived', ''),
    'Maize (grain)':         ('01_057', 'derived', ''),
    'Maize (cob)':           ('04_076', 'derived', 'fresh maize on the cob is the "Maize, seeds, fresh, raw" row'),
    'Maize':                 ('01_014', 'derived', ''),
    'Maize (dough)':         ('01_099', 'derived', ''),
    'Maizena':               ('01_081', 'derived', 'corn starch; nearest is refined maize flour'),
    'Rice':                  ('01_037', 'derived', ''),
    'Rice (imported)':       ('01_037', 'derived', ''),
    'Rice (local)':          ('01_065', 'derived', ''),
    'Millet':                ('01_095', 'anchor-departed', 'sheet says 01_018 (a single named variety, n=1); 01_095 is the generic decorticated pearl millet'),
    'Millet (flour)':        ('01_063', 'derived', ''),
    'Guinea Corn':           ('01_039', 'anchor', ''),
    'Guinea Corn/Sorghum':   ('01_039', 'derived', ''),
    'Sorghum':               ('01_039', 'derived', ''),
    'Wheat Flour':           ('01_043', 'derived', ''),
    'Other Flours':          ('01_043', 'derived', ''),
    'Flour':                 ('01_043', 'anchor', ''),
    'Bread':                 ('01_046', 'anchor-departed', 'sheet says 01_047 (bread "for toasting"); 01_046 is plain white wheat bread'),
    'Sugar Bread':           ('01_045', 'derived', 'sweetened white bread/rolls'),
    'Biscuit':               ('01_188', 'derived', ''),
    'Biscuits':              ('01_188', 'anchor', ''),
    'Corflake':              ('01_182', 'derived', ''),
    'Other Cereal':          ('01_182', 'derived', ''),
    'Other Cereals':         ('01_182', 'anchor', ''),
    'Other Grains':          ('01_039', 'derived', ''),
    'Macaroni':              ('01_052', 'derived', ''),
    'Spaghetti':             ('01_052', 'derived', ''),
    'Macaroni/Spaghetti':    ('01_052', 'derived', ''),
    'Instant Noodle':        ('01_077', 'derived', ''),
    'White Oat':             ('01_133', 'derived', 'oats; the WAFCT carries oats only as porridge (01_133)'),
    'White Oats':            ('01_133', 'derived', 'as above'),
    'Couscous':              ('01_043', 'derived', 'wheat couscous; nearest raw basis is wheat flour'),
    'Kenkey':                ('01_099', 'derived', 'fermented maize dough'),
    'Banku':                 ('01_099', 'anchor-departed', 'sheet says 02_051 (boiled cassava, a recipe row); banku is fermented maize dough'),
    'Kapla':                 ('01_099', 'derived', 'maize-dough preparation'),

    # ---- starchy roots, tubers -----------------------------------------
    'Cassava':               ('02_001', 'anchor', ''),
    'Cassava (fresh)':       ('02_001', 'derived', ''),
    'Cassava (dried)':       ('02_002', 'derived', ''),
    'Cassava (flour)':       ('02_004', 'derived', ''),
    'Cassava (white, flour)': ('02_004', 'derived', ''),
    'Cassava (yellow, flour)': ('02_040', 'derived', ''),
    'Cassava (dough)':       ('02_041', 'derived', 'fermented cassava paste'),
    'Cassava Dough':         ('02_041', 'anchor-departed', 'sheet says 02_004 (flour); 02_041 is the fermented paste'),
    'Gari':                  ('02_039', 'anchor', ''),
    'Yam':                   ('02_019', 'anchor', ''),
    'Water Yam':             ('02_017', 'derived', ''),
    'Cocoyam':               ('02_005', 'anchor', ''),
    'Taro':                  ('02_015', 'derived', ''),
    'Plantain':              ('02_042', 'anchor', ''),
    'Sweet Potato':          ('02_013', 'derived', ''),
    'Potato/Sweet Potato':   ('02_009', 'derived', 'combined label; potato is the more common purchase'),
    'Potato':                ('02_009', 'derived', ''),
    'Other Tubers':          ('02_015', 'derived', ''),
    'Other Starchy Staples': ('02_015', 'anchor', ''),
    'Tiger Nut':             ('02_024', 'derived', ''),

    # ---- legumes --------------------------------------------------------
    'Cowpea':                ('03_004', 'derived', ''),
    'Cowpea Beans':          ('03_004', 'anchor', ''),
    'Bean':                  ('03_004', 'derived', ''),
    'Other Beans':           ('03_004', 'anchor', ''),
    'Broad Bean':            ('03_024', 'derived', ''),
    'Bambara Bean':          ('03_001', 'derived', ''),
    'Soybean':               ('03_008', 'derived', ''),
    'Soya Beans':            ('03_008', 'anchor-departed', 'sheet says 03_004 (cowpea); the WAFCT has soya bean at 03_008'),
    'Other Pulses':          ('03_004', 'derived', ''),
    'Other Pulses And Nuts': ('03_004', 'anchor-departed', 'sheet says 06_008 (dikanut); cowpea represents the class far better'),
    'Dawadawa':              ('03_009', 'anchor', ''),
    'Pigeon Pea':            ('03_032', 'derived', ''),

    # ---- vegetables ------------------------------------------------------
    'Tomato':                ('04_021', 'derived', ''),
    'Tomato (fresh)':        ('04_021', 'derived', ''),
    'Tomatoes':              ('04_021', 'anchor', ''),
    'Tomato (paste)':        ('04_066', 'derived', ''),
    'Tomato Puree':          ('04_066', 'anchor', ''),
    'Onion':                 ('04_018', 'derived', ''),
    'Onions':                ('04_018', 'anchor', ''),
    'Shallot':               ('04_018', 'derived', 'no shallot row; fresh onion is the nearest'),
    'Garlic':                ('04_015', 'derived', ''),
    'Okra':                  ('04_017', 'anchor-departed', 'sheet says 04_004 (okra LEAVES); the fruit (04_017) is what is eaten as okro'),
    'Okro':                  ('04_017', 'derived', 'as above'),
    'Pepper':                ('04_046', 'anchor-departed', 'sheet says 04_049 (sweet green pepper); Ghanaian "pepper" is chilli (04_046)'),
    'Pepper (fresh)':        ('04_046', 'derived', ''),
    'Pepper (dried)':        ('13_006', 'derived', ''),
    'Pepper (dried, red)':   ('13_006', 'derived', ''),
    'Pepper (powder)':       ('13_006', 'derived', ''),
    'Sweet Pepper':          ('04_047', 'derived', ''),
    'Chilli Powder (black pepper)': ('13_006', 'derived', ''),
    'Black Pepper':          ('13_014', 'anchor', ''),
    'Eggplant':              ('04_074', 'derived', 'garden eggs are the native eggplant (04_074)'),
    'Eggplant/Cucumber':     ('04_074', 'derived', ''),
    'Garden Eggs':           ('04_074', 'anchor-departed', 'sheet says 08_005 (a CHICKEN EGG) -- a false-friend match on "eggs"'),
    'Eggplant Leaf':         ('04_013', 'derived', ''),
    'Cabbage':               ('04_005', 'anchor', ''),
    'Carrot':                ('04_006', 'derived', ''),
    'Carrots':               ('04_006', 'anchor', ''),
    'Spinach':               ('04_057', 'derived', ''),
    'Cocoyam Leaves':        ('04_009', 'anchor', ''),
    'Jute Leaf':             ('04_038', 'derived', ''),
    'Other Leafy Vegetable': ('04_131', 'derived', 'the WAFCT publishes an explicit "green leafy vegetable, average"'),
    'Other Leafy Vegetables': ('04_131', 'derived', 'as above'),
    'Other Vegetable':       ('04_005', 'derived', ''),
    'Other Vegetables':      ('04_005', 'anchor', ''),
    'Lettuce':               ('04_040', 'derived', ''),
    'Pumpkin':               ('04_055', 'derived', ''),
    'Mushroom':              ('04_005', 'anchor-departed', 'sheet says 04_005 (cabbage) for wild mushrooms; kept, the WAFCT has no mushroom row'),

    # ---- fruits ----------------------------------------------------------
    'Orange':                ('05_016', 'anchor', ''),
    'Banana':                ('05_003', 'anchor-departed', 'sheet says 05_048 (UNRIPE green banana); ripe (05_003) is what is eaten'),
    'Mango':                 ('05_015', 'anchor-departed', 'sheet says 05_036 (deep orange flesh); 05_015 is the general ripe orange-flesh mango'),
    'Pawpaw':                ('05_017', 'anchor', ''),
    'Pineapple':             ('05_018', 'anchor', ''),
    'Avocado':               ('05_002', 'anchor', ''),
    'Watermelon':            ('05_022', 'anchor-departed', 'sheet says 05_002 (AVOCADO) -- a transcription error in the sheet; 05_022 is watermelon'),
    'Water Melon':           ('05_022', 'derived', 'as above'),
    'Apple':                 ('05_026', 'anchor-departed', 'sheet says 05_047; 05_026 is apple with skin, as purchased'),
    'Grape':                 ('05_051', 'derived', ''),
    'Lime':                  ('05_014', 'derived', 'lemon/lime'),
    'Lemon':                 ('05_014', 'derived', ''),
    'Other Fruits':          ('05_017', 'anchor', ''),
    'Canned Fruits':         ('05_017', 'derived', ''),
    'Baobab':                ('05_004', 'derived', ''),

    # ---- nuts, seeds -----------------------------------------------------
    'Groundnut':             ('06_010', 'anchor-departed', 'sheet says 06_023 (one Ghanaian sample, n=1); 06_010 is the generic shelled dried groundnut'),
    'Groundnuts':            ('06_010', 'derived', 'as above'),
    'Coconut (fresh)':       ('06_002', 'anchor-departed', 'sheet says 06_004 (immature kernel); 06_002 is the mature kernel normally sold'),
    'Coconut':               ('06_002', 'derived', ''),
    'Coconut (dried)':       ('06_005', 'derived', ''),
    'Palm Nut':              ('06_029', 'anchor', ''),
    'Kola Nut':              ('06_019', 'derived', ''),
    'Cola Nut':              ('06_019', 'anchor', ''),
    'Cashew':                ('06_001', 'derived', ''),
    'Shea Nut':              ('06_016', 'anchor', ''),
    'Agushie Seed':          ('06_009', 'derived', 'egusi melon seed; nearest WAFCT row is the dried oil-seed 06_009'),
    'Other Nuts/Seeds':      ('06_010', 'derived', ''),
    'Other Oil Seeds':       ('06_009', 'derived', ''),
    'Sesame':                ('06_009', 'derived', ''),

    # ---- meat, poultry ---------------------------------------------------
    'Beef':                  ('07_014', 'anchor', ''),
    'Beef (steak)':          ('07_016', 'derived', 'lean beef cut'),
    'Beef (leg)':            ('07_016', 'derived', ''),
    'Beef (face)':           ('07_014', 'derived', ''),
    'Beef (offal)':          ('07_021', 'derived', 'tripe is the commonest Ghanaian offal'),
    'Beef (corned)':         ('07_025', 'anchor-departed', 'sheet says 07_014 (fresh beef); the WAFCT has canned corned beef at 07_025'),
    'Corned Beef':           ('07_025', 'derived', 'as above'),
    'Goat':                  ('07_069', 'derived', ''),
    'Goat Meat':             ('07_069', 'anchor', ''),
    'Mutton':                ('07_072', 'anchor', ''),
    'Pork':                  ('07_006', 'anchor-departed', 'sheet says 07_005 (40% fat); 07_006 (20% fat) is the moderate default'),
    'Pork (feet)':           ('07_005', 'derived', ''),
    'Pork (rib)':            ('07_005', 'derived', ''),
    'Pork (fillet)':         ('07_071', 'derived', ''),
    'Pork (shoulder)':       ('07_006', 'derived', ''),
    'Sausage (pork)':        ('07_063', 'derived', ''),
    'Sausage (beef)':        ('07_063', 'derived', ''),
    'Sausage (chicken)':     ('07_063', 'derived', ''),
    'Chicken':               ('07_030', 'anchor-departed', 'sheet says 07_039 (chicken GIBLETS); 07_030 is chicken meat'),
    'Chicken (live)':        ('07_030', 'derived', ''),
    'Chicken (frozen)':      ('07_030', 'derived', ''),
    'Chicken (breast)':      ('07_031', 'derived', 'light meat'),
    'Chicken (thigh)':       ('07_030', 'derived', 'dark meat'),
    'Chicken (wing)':        ('07_030', 'derived', ''),
    'Chicken (gizzard)':     ('07_039', 'derived', ''),
    'Guinea Fowl':           ('07_070', 'derived', ''),
    'Other Poultry':         ('07_030', 'derived', ''),
    'Other Domestic Poultry': ('07_030', 'derived', ''),
    'Snail':                 ('07_083', 'derived', ''),
    'Bush Meat':             ('07_027', 'anchor-departed', 'sheet says 07_005 (PORK); game meat (07_027) is what bush meat is'),
    'Other Meat':            ('07_069', 'anchor-departed', 'sheet says 07_005 (fatty pork); goat (07_069) better represents the Ghanaian residual'),
    'Game Birds':            ('07_070', 'derived', ''),

    # ---- eggs -------------------------------------------------------------
    'Egg':                   ('08_001', 'derived', ''),
    'Eggs':                  ('08_001', 'anchor-departed', 'sheet says 08_005 (local-breed egg); 08_001 is the generic chicken egg'),
    'Other Poultry Eggs':    ('08_008', 'derived', ''),

    # ---- fish -------------------------------------------------------------
    'Fish':                  ('09_003', 'anchor', ''),
    'Fish (fresh and frozen)': ('09_003', 'derived', ''),
    'Fish (smoked)':         ('09_053', 'derived', 'smoked/dried whole fish'),
    'Fish (smoked, sea)':    ('09_053', 'derived', ''),
    'Fish (smoked, river)':  ('09_053', 'derived', ''),
    'Fish (dried)':          ('09_053', 'derived', ''),
    'Fish (salted)':         ('09_053', 'derived', ''),
    'Fish (fried)':          ('09_003', 'derived', 'acquired basis; the raw fillet is the right density'),
    'Fish (canned)':         ('09_110', 'derived', ''),
    'Canned Fish':           ('09_110', 'anchor-departed', 'sheet says 09_003 (raw mackerel); canned tuna in water (09_110) is the canned form'),
    'Herring (smoked)':      ('09_053', 'derived', ''),
    'Salmon (smoked)':       ('09_053', 'derived', 'GLSS "salmon" is smoked marine fish, not Salmo salar'),
    'Mackerel (processed)':  ('09_109', 'derived', ''),
    'Tuna (processed)':      ('09_111', 'derived', ''),
    'Tilapia':               ('09_041', 'derived', ''),
    'Crab':                  ('09_055', 'anchor-departed', 'sheet says 09_003 (mackerel); the WAFCT has crab flesh at 09_055'),
    'Shrimp':                ('09_059', 'derived', ''),
    'Other Fish':            ('09_003', 'derived', ''),
    'Fish And Shellfish':    ('09_003', 'derived', ''),

    # ---- milk -------------------------------------------------------------
    'Milk (fresh)':          ('10_029', 'anchor', ''),
    'Milk (powder)':         ('10_002', 'anchor', ''),
    'Milk (powdered)':       ('10_002', 'derived', ''),
    'Milk (evaporated)':     ('10_016', 'derived', ''),
    'Milk (tinned, condensed/unsweetened)': ('10_016', 'derived', ''),
    'Tinned Milk':           ('10_016', 'anchor-departed', 'sheet says 10_018 (CAMEL milk); canned evaporated cow milk (10_016) is what is sold'),
    'Baby Milk':             ('10_011', 'anchor-departed', 'sheet says 10_028 (fresh curd cheese); infant formula (10_011) is baby milk'),
    'Yoghurt':               ('10_005', 'derived', ''),
    'Cheese':                ('10_028', 'derived', ''),
    'Other Milk Products':   ('10_028', 'anchor', ''),
    'Baby Food':             ('10_011', 'anchor', ''),
    'Cerelac (Baby food)':   ('10_011', 'derived', ''),

    # ---- fats and oils ----------------------------------------------------
    'Oil (palm)':            ('11_004', 'anchor-departed', 'sheet maps every oil to 11_006 MARGARINE; the WAFCT has red palm oil at 11_004'),
    'Palm Oil':              ('11_004', 'derived', 'as above'),
    'Oil (palm kernel)':     ('11_012', 'anchor-departed', 'sheet says 11_006 (margarine); palm kernel oil is 11_012'),
    'Palm Kernel Oil':       ('11_012', 'derived', 'as above'),
    'Oil (groundnut)':       ('11_003', 'anchor-departed', 'sheet says 11_006 (margarine); groundnut oil is 11_003'),
    'Groundnut Oil':         ('11_003', 'derived', 'as above'),
    'Oil (coconut)':         ('11_002', 'anchor-departed', 'sheet says 11_006 (margarine); coconut oil is 11_002'),
    'Coconut Oil':           ('11_002', 'derived', 'as above'),
    'Oil (vegetable)':       ('11_009', 'derived', 'soya oil is the commonest bottled vegetable oil'),
    'Other Vegetable Oils':  ('11_009', 'anchor-departed', 'sheet says 11_006 (margarine); a vegetable OIL is not margarine'),
    'Other Oils':            ('11_009', 'derived', ''),
    'Shea Butter':           ('11_008', 'anchor-departed', 'sheet says 11_006 (margarine); the WAFCT has shea butter at 11_008'),
    'Margarine':             ('11_006', 'anchor', ''),
    'Margarine/Butter':      ('11_006', 'anchor', ''),
    'Butter':                ('11_011', 'derived', ''),
    'Animals Fat':           ('11_011', 'derived', ''),

    # ---- beverages ---------------------------------------------------------
    'Water':                 ('12_019', 'derived', 'tap water: zero energy, but mapping it keeps the mass accounted for'),
    'Soft Drinks':           ('12_024', 'anchor-departed', 'sheet says 12_012 (fruit juice); a carbonated drink (12_024) is the soft drink'),
    'Juice':                 ('12_012', 'derived', ''),
    'Malt Drinks (bottle)':  ('12_024', 'derived', ''),
    'Malt Drinks (canned)':  ('12_024', 'derived', ''),
    'Tea':                   ('12_008', 'anchor-departed', 'sheet says 12_012 (juice); tea infusion is 12_008'),
    'Tea bag':               ('12_008', 'derived', ''),
    'Coffee':                ('12_009', 'derived', ''),
    'Cocoa Powder':          ('13_028', 'derived', 'fortified malt/cocoa beverage powder'),
    'Cocoa (milk powder beverages)': ('12_017', 'derived', ''),
    'Beer':                  ('12_001', 'derived', ''),
    'Wine':                  ('12_006', 'derived', 'palm wine is the WAFCT wine row'),
    'Akpeteshie':            ('12_006', 'derived', 'distilled palm wine; the WAFCT has no spirit row'),
    'Gin':                   ('12_006', 'derived', 'as above'),
    'Whisky':                ('12_006', 'derived', 'as above'),
    'Schnapps':              ('12_006', 'derived', 'as above'),
    'Bitters':               ('12_006', 'derived', 'as above'),
    'Other Spirits':         ('12_006', 'derived', 'as above'),
    'Other Alcoholic Beverages': ('12_006', 'derived', ''),
    'Alcoholic Beverages':   ('12_006', 'anchor', ''),
    'Other Beverages':       ('12_024', 'anchor', ''),
    'Palm Wine':             ('12_006', 'derived', ''),

    # ---- miscellaneous -----------------------------------------------------
    'Sugar':                 ('13_002', 'anchor', ''),
    'Sugar (granulated)':    ('13_002', 'derived', ''),
    'Sugarcane':             ('13_002', 'anchor-departed', 'sheet says 04_005 (CABBAGE); sugar (13_002) is far closer for cane'),
    'Honey':                 ('13_001', 'anchor', ''),
    'Salt':                  ('13_015', 'anchor', ''),
    'Condiment':             ('13_008', 'derived', 'the bouillon cube is the dominant Ghanaian condiment'),
    'Condiments':            ('13_008', 'derived', ''),
    'Other Condiments/Spices': ('13_016', 'anchor', ''),
    'Other Spices':          ('13_016', 'anchor', ''),
    'Curry Power':           ('13_022', 'derived', ''),
    'Ginger':                ('04_082', 'anchor', ''),
    'Vinegar':               ('13_003', 'derived', ''),
    'Chocolate':             ('13_021', 'anchor-departed', 'sheet says 13_001 (HONEY); the WAFCT has milk chocolate at 13_021'),
    'Ice Cream':             ('13_021', 'anchor-departed', 'sheet says 13_001 (honey); a milk confection (13_021) is nearer than honey'),
    'Other Confectionaries': ('13_021', 'anchor-departed', 'sheet says 13_001 (honey); 13_021 represents the sweets class'),
    'Chewing Gum':           (None, 'derived', 'not a food: contributes no dietary nutrients'),
    'Prekese':               ('13_016', 'anchor', ''),
    'Jam':                   ('13_023', 'derived', ''),
    'Yeast':                 ('13_017', 'derived', ''),
    'Potash':                ('13_024', 'derived', ''),

    # ---- non-foods: deliberately unmapped ------------------------------------
    'Tobacco':               (None, 'derived', 'not a food'),
    'Other Tobacco':         (None, 'derived', 'not a food'),
    'Cigarette':             (None, 'derived', 'not a food'),

    # ---- prepared meals: deliberately unmapped -------------------------------
    # The WAFCT's mixed-dish rows are Burkinabe recipes, not Ghanaian ones, and
    # datasheet 09 is not applied (see fct_west_africa.org).  Mapping a Ghanaian
    # restaurant meal onto a named Burkinabe dish would be a fabrication.
    'Cooked Meals':          (None, 'derived', 'prepared dish: no defensible WAFCT row (see fct_west_africa.org, sheets 07/08/09)'),
    'Cooked Rice and Stew':  (None, 'derived', 'as above'),
    'Restaurants':           (None, 'derived', 'as above'),
}


def main():
    org = countries_root() / 'GhanaLSS' / '_' / 'food_items.org'
    lab = df_from_orgfile(org, name='food_label')
    labels = lab['Preferred Label'].astype(str).str.strip()

    unknown = sorted(set(MAP) - set(labels))
    if unknown:
        print(f'NOTE: {len(unknown)} mapped labels are not in food_label '
              f'(harmless -- they cover sibling spellings): {unknown[:8]}...')

    codes = labels.map(lambda s: (MAP.get(s) or (None,))[0])
    lab = lab.copy()
    lab['FCT Code'] = codes.fillna('')

    # Validate every code against the FCT actually shipped.
    fct = df_from_orgfile(countries_root() / 'GhanaLSS' / '_' / 'fct_west_africa.org',
                          name='fct_west_africa')
    known = set(fct['FCT Code'].astype(str))
    bad = sorted({c for c in lab['FCT Code'] if c and c not in known})
    if bad:
        raise SystemExit(f'FCT codes not present in fct_west_africa.org: {bad}')

    n = int((lab['FCT Code'] != '').sum())
    print(f'food_label: {len(lab)} labels, {n} carry an FCT Code, '
          f'{len(lab)-n} blank')

    _rewrite(org, lab)
    print(f'rewrote {org}')


def _rewrite(org, lab):
    """Replace the food_label table in-place, preserving everything else."""
    text = org.read_text().split('\n')
    start = next(i for i, s in enumerate(text)
                 if s.strip().lower() == '#+name: food_label')
    i = start + 1
    while not text[i].lstrip().startswith('|'):
        i += 1
    first = i
    while i < len(text) and text[i].lstrip().startswith('|'):
        i += 1
    last = i

    cols = list(lab.columns)
    rows = [[('' if pd.isna(v) else str(v)).replace('|', '/') for v in rec]
            for rec in lab.itertuples(index=False)]
    w = [max(len(c), *(len(r[k]) for r in rows)) for k, c in enumerate(cols)]
    out = ['| ' + ' | '.join(c.ljust(w[k]) for k, c in enumerate(cols)) + ' |',
           '|-' + '-+-'.join('-' * x for x in w) + '-|']
    out += ['| ' + ' | '.join(r[k].ljust(w[k]) for k in range(len(cols))) + ' |'
            for r in rows]
    org.write_text('\n'.join(text[:first] + out + text[last:]))


if __name__ == '__main__':
    main()
