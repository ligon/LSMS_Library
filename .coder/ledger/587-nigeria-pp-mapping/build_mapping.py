"""GH #587 step 1: proposed PP C2 item/unit crosswalk for Nigeria (UNWIRED).

Source-only.  Reads: the sealed PP header cards in
``countries/Nigeria/_/pp_community_price_sources.py`` (via ``ast``, not import),
Nigeria ``categorical_mapping.org`` (harmonize_food, u), the existing Nigeria
resolvers in ``nigeria.py`` (pure functions over those tables), the printed
questionnaire lists in ``printed_items.json`` (parsed from the PDFs fetched by
``get_data_file``), and the per-code row counts in ``occupancy.json`` (copied
from the sealed prior audit; no new data read).

Writes items.csv, units.csv, sizes.csv and MAPPING.org into OUT.
Nothing here is a build input or is read by the library.
"""
import ast
import json
import os
import re
import sys

import pandas as pd

W = os.environ['PYTHONPATH']
sys.path.insert(0, W)
from lsms_library.local_tools import df_from_orgfile                      # noqa: E402
from lsms_library.countries.Nigeria._ import nigeria as ng                 # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = sys.argv[1] if len(sys.argv) > 1 else HERE
NG = f'{W}/lsms_library/countries/Nigeria/_'
for node in ast.parse(open(f'{NG}/pp_community_price_sources.py').read()).body:
    if isinstance(node, ast.Assign) and node.targets[0].id == 'SOURCES':
        SOURCES = ast.literal_eval(node.value)
PRINTED = json.load(open(f'{HERE}/printed_items.json'))
OCC = json.load(open(f'{HERE}/occupancy.json'))
HF = df_from_orgfile(f'{NG}/categorical_mapping.org', name='harmonize_food')
HF['code'] = pd.to_numeric(HF['Code'], errors='coerce')
UT = df_from_orgfile(f'{NG}/categorical_mapping.org', name='u')
U_PREFERRED = set(str(x).strip() for x in UT['Preferred Label'])
PREFERRED = set(str(x).strip() for x in HF['Preferred Label'] if str(x).strip())

SRC_ORDER = ['W1', 'W2', 'W3-first', 'W3-second', 'W4', 'W5']
T = {k: ng.PP_QUARTER[SOURCES[k]['folder']] for k in SRC_ORDER}
HHQ = {'W1': '2010Q3', 'W2': '2012Q3', 'W3-first': '2015Q3', 'W3-second': '2015Q3', 'W4': '2018Q3'}
PRINTED_PP = {'W1': 'W1-PP-printed', 'W2': 'W2-PP', 'W3-first': 'W3-PP-first',
              'W3-second': 'W3-PP-second', 'W4': 'W4-PP', 'W5': 'W5-PP'}
PRINTED_PH = {'W3-first': 'W3-PH-all', 'W3-second': 'W3-PH-all', 'W4': 'W4-PH', 'W5': 'W5-PH'}
PP_DOC = {'W1': 'Community_Questionnaire_Final (1).pdf', 'W2': 'GHS_Panel_PP_Community.pdf',
          'W3-first': 'PP_W3_Community_Quest.pdf', 'W3-second': 'PP_W3_Community_Quest.pdf',
          'W4': 'pp_w4_community_quest.pdf', 'W5': 'ghs_panel_w5_pp_community_questionnaire.pdf'}

# --------------------------------------------------------------------------
# label normalisation (documented in the ledger, section 3)
# --------------------------------------------------------------------------
W1_CATEGORY = re.compile(
    r'^(grain and flours|starchy roots[.,] tubers (?:&|and) plantain|pulses, nuts and seeds|oil and fats|'
    r'fruits|vegetables|poultry and poultry products|meat|fish and seafood|milk and milk products|'
    r'coffee, tea, cocoa and the like beverages|sugar, sweets and confectionary|other miscellaneous foods|'
    r'non-alcoholic drinks|alcoholic drinks \(bottle and tot\))\s*:?\s*', re.I)


def item_norm(label):
    s = re.sub(r'^\s*\d+\.\s*', '', str(label)).strip()
    s = W1_CATEGORY.sub('', s).casefold().replace('­', '')
    s = re.sub(r'\betc\b', ' ', s)
    return ' '.join(re.sub(r'[^0-9a-z]+', ' ', s).split())


UNIT_ALIAS = {'kilograms': 'kg', 'kilogram': 'kg', 'grams': 'g', 'gram': 'g', 'litres': 'l', 'litre': 'l',
              'centilitres': 'cl', 'centilitre': 'cl', 'millilitre': 'ml', 'pieces': 'piece', 'sachets': 'sachet'}


def unit_tokens(label):
    s = re.sub(r'^\s*\d+\.\s*', '', str(label)).casefold()
    s = re.sub(r'\([^)]*\d[^)]*\)?', ' ', s)          # stated weights '(20 kg)', '(1.5 kg)'
    toks = [UNIT_ALIAS.get(t, t) for t in re.findall(r'[0-9a-z]+', s)]
    return frozenset(toks)


# --------------------------------------------------------------------------
# ITEMS
# --------------------------------------------------------------------------
QRES = {q: ng.harmonized_food_labels(t=q) for q in ('2010Q3', '2012Q3', '2015Q3', '2018Q3')}
CODE_CONTRACT = ng.harmonized_food_labels(dirs=[f'{NG}/'])     # PH W3-W5 C8 contract

ITEM_SUBSTANTIVE = {
    ('W1', 76): ('Pepper', 'Fresh Pepper',
                 'Native and household 2010Q3 card both say unqualified "pepper"; the quarter-aware food '
                 'resolver serves the native name Pepper (GH #1144 ruling: add no attribute the source did '
                 'not record), which is NOT a harmonize_food Preferred Label (GH #1537 holds it).  PH '
                 'community W1/W2 (_W1W2_PRICE_ITEM 106) and the wave-blind Code contract serve Fresh '
                 'Pepper.  Choose one for community_prices.'),
    ('W2', 76): ('Pepper', 'Fresh Pepper', 'As W1 code 76 (W2 PP native, printed p.6 and 2012Q3 all say "Pepper").'),
    ('W3', 161): ('Palm wine', 'Palm oil',
                  'Native header label is "161. Palm oil", but code 161 is Palm wine in the printed PP '
                  'card (PDF p.10, 1st location; p.16, 2nd), the printed PH W3 card (PDF p.23), the '
                  'household 2015Q3 card and the Code contract, and it sits in the ALCOHOLIC DRINKS '
                  'block.  Palm oil is code 50 on the same card.  Recommend Palm wine; the alternative '
                  'would make code 161 collide with code 50 on (t,v,j,u,unknown_variety,obs).'),
}
ITEM_UNMAPPABLE = {
    ('W2', 0): 'Questionnaire sentinel "No more option" -- not a food item.  Whether any W2 row carries '
               'code 0 is not established (the sealed audit reports 0 rows outside the card, and 0 is on it).',
    ('W2', 15): 'Semovita: on the W2 PP native and printed card (PDF p.5) but harmonize_food has no row '
                'meaning Semovita (2012Q3 cell for code 15 is blank).  The wave-blind Code contract would '
                'serve "Bread" (code 15 is Bread in W1) -- wrong.  Needs a new harmonize_food row or stays '
                'unserved.',
}
ITEM_NOTES = {
    ('W1', 43): 'Native "groundnuts" = 2010Q3 "Groundnuts" (GH #1144 native name, label-only Preferred row).  PH '
                'community W1/W2 serves Groundnuts (shelled) (_W1W2_PRICE_ITEM 61).',
    ('W2', 43): 'As W1 code 43.',
    ('W2', 21): 'Native "Maize (Shelled)" = 2012Q3 "Maize (shelled)" (GH #1144 label-only Preferred row).',
    ('W1', 20): 'Wave-blind Code contract would serve "Maize (unshelled, on the cob)" -- wrong for W1 (GH #1144).',
    ('W1', 44): 'Wave-blind Code contract would serve "Groundnuts (shelled)" -- wrong for W1.',
    ('W1', 77): 'Wave-blind Code contract would serve "Dry Pepper" -- wrong for W1.',
    ('W1', 78): 'Wave-blind Code contract would serve "Leaves (...)" -- wrong for W1.',
    ('W1', 114): 'Wave-blind Code contract would serve "Cheese (wara)" -- wrong for W1.',
    ('W2', 22): 'Wave-blind Code contract would serve "Maize (shelled, off the cob)" -- wrong for W2.',
    ('W2', 44): 'Wave-blind Code contract would serve "Groundnuts (shelled)" -- wrong for W2.',
    ('W2', 77): 'Wave-blind Code contract would serve "Dry Pepper" -- wrong for W2.',
    ('W3', 601): 'Printed PP card prints the code as "60A" (PDF p.7/p.13); native 601.',
    ('W4', 148): 'Native-only vs the held printed PP list (which prints 140 Condiments instead); = Preferred.',
}


def group(src):
    return 'W3' if src.startswith('W3') else src


item_rows = []
for src in SRC_ORDER:
    s = SOURCES[src]
    card = s['report_value_label_sets'][s['variable_value_label_associations']['item_cd']]
    w4 = {r['code']: r['label'] for r in SOURCES['W4']['report_value_label_sets']['ITEM_CD']}
    for r in card:
        c, lab = r['code'], r['label']
        g = group(src)
        hrow = HF[HF.code == c]
        p_at_code = str(hrow['Preferred Label'].iloc[0]).strip() if len(hrow) else ''
        if src == 'W5':
            comparator, cmp_lab = 'W4 PP native card (W5 card byte-identical)', w4.get(c, '')
            qres = QRES['2018Q3'].get(c, '')
        else:
            q = HHQ[src]
            comparator = f'harmonize_food[{q}]'
            cmp_lab = str(hrow[q].iloc[0]).strip() if len(hrow) else ''
            cmp_lab = '' if cmp_lab in ('nan', '---') else cmp_lab
            qres = QRES[q].get(c, '')
        pp_pr = PRINTED[PRINTED_PP[src]].get(str(c), ['', ''])
        ph_pr = PRINTED.get(PRINTED_PH.get(src, ''), {}).get(str(c), ['', ''])
        same = bool(cmp_lab) and (item_norm(lab) == item_norm(cmp_lab))
        same_pref = bool(p_at_code) and item_norm(lab) == item_norm(p_at_code) and qres == p_at_code
        alt, note = '', ITEM_NOTES.get((g, c), '')
        if (g, c) in ITEM_UNMAPPABLE:
            proposed, basis, flag, note = '', 'none', 'UNMAPPABLE', ITEM_UNMAPPABLE[(g, c)]
        elif (g, c) in ITEM_SUBSTANTIVE:
            proposed, alt, note = ITEM_SUBSTANTIVE[(g, c)]
            basis, flag = 'judgement', 'JUDGEMENT'
        elif same or same_pref:
            proposed, flag = qres, 'OK'
            basis = f'same-code-same-label-as-{comparator.split(" ")[0] if src == "W5" else comparator}'
            if src == 'W5':
                basis = 'same-code-same-label-as-W4-PP-card (= harmonize_food[2018Q3])'
        else:
            # same code, household label differs only by spelling / truncation / qualifier wording
            proposed, basis, flag = qres, 'judgement-spelling', 'JUDGEMENT'
            note = (note + ' ' if note else '') + f'Same code; label differs from {comparator} "{cmp_lab}" only ' \
                   'by spelling, truncation (Stata 60-char) or wording; meaning unchanged.'
        assert proposed == '' or proposed in PREFERRED or proposed in ('Pepper',), (src, c, proposed)
        item_rows.append(dict(
            source=src, t=T[src], obs=s['obs'], native_code=c, native_label=lab,
            proposed_j=proposed, alternative_j=alt, basis=basis, flag=flag, note=note.strip(),
            comparator=comparator, comparator_label=cmp_lab,
            food_quarter_resolver_j=qres, ph_code_contract_j=CODE_CONTRACT.get(c, ''),
            ph_w1w2_scheme_label_at_same_code=(ng._W1W2_PRICE_ITEM.get(c, '') if src in ('W1', 'W2') else ''),
            printed_pp_label=pp_pr[0], printed_pp_locator=(f'{PP_DOC[src]} PDF p.{pp_pr[1]}' if pp_pr[1] else ''),
            printed_ph_label=ph_pr[0],
            proposed_is_harmonize_food_preferred=(proposed in PREFERRED)))
items = pd.DataFrame(item_rows)
# W5's card is byte-identical to W4's (checked here), so W5 inherits W4's decision row by row.
_w4 = items[items.source == 'W4'].set_index('native_code')
_w5 = items.source == 'W5'
assert (items.loc[_w5].set_index('native_code').native_label == _w4.native_label).all()
for col in ('proposed_j', 'alternative_j', 'flag', 'note'):
    items.loc[_w5, col] = items.loc[_w5, 'native_code'].map(_w4[col]).values
items.loc[_w5, 'basis'] = ('same-code-same-label-as-W4-PP-card; W4 row: '
                           + items.loc[_w5, 'native_code'].map(_w4['basis']).values)

# --------------------------------------------------------------------------
# UNITS
# --------------------------------------------------------------------------
P_W2 = {1: 'KILOGRAMS (kg)', 2: 'GRAMS (g)', 3: 'LITRE (l)', 4: 'CENTILITRE (cl)', 5: 'MUDU', 6: 'OLODO',
        7: 'CONGO', 8: 'PAINT RUBBER', 9: 'LARGE DERICA', 10: 'MEDIUM DERICA', 11: 'SMALL DERICA',
        12: 'MILK CUP', 13: 'CIGARETTE CUP', 14: 'TIYA', 15: 'KOBIOWU'}
P_W3 = {1: 'Kilograms (Kg)', 2: 'Grams (g)', 3: 'Litres (l)', 4: 'Centilitres (cl)', 10: 'Bin/basket',
        11: 'Paint Rubber', 12: 'Milk cup', 13: 'Cigarette cup', 14: 'Tin', 20: 'Congo small', 21: 'Congo large',
        30: 'Mudu Small', 31: 'Mudu Large', 40: 'Derica Small', 41: 'Derica Medium', 42: 'Derica Large',
        43: 'Derica Very Large', 50: 'Tiya Small', 51: 'Tiya Medium', 52: 'Tiya Large', 60: 'Kobiowu Small',
        61: 'Kobiowu Medium', 62: 'Kobiowu Large', 70: 'Bowl Small', 71: 'Bowl Medium', 72: 'Bowl Large',
        80: 'Piece Small', 81: 'Piece Medium', 82: 'Piece Large', 90: 'Heap Small', 91: 'Heap Medium',
        92: 'Heap Large', 100: 'Bunch Small', 101: 'Bunch Medium', 102: 'Bunch Large', 110: 'Stalk Small',
        111: 'Stalk Medium', 112: 'Stalk Large', 120: 'Packet/sachet Small', 121: 'Packet/sachet Medium',
        122: 'Packet/sachet Large', 900: 'Other (specify)'}
P_W45 = {1: 'Kilograms (Kg)', 3: 'Litres (l)', 10: 'Bin/basket', 11: 'Paint Rubber', 12: 'Milk cup',
         13: 'Cigarette cup', 14: 'Tin', 20: 'Congo', 30: 'Mudu', 40: 'Derica', 50: 'Tiya', 60: 'Kobiowu',
         70: 'Bowl', 80: 'Piece', 90: 'Heap', 100: 'Bunch/Bundle', 110: 'Stalk', 120: 'Packet/sachet',
         130: 'Sack/bag', 140: 'Basket', 150: 'Basin', 160: 'Bundle', 170: 'Wheelbarrow', 180: 'Pick-up',
         190: 'Jerry can/rubber', 200: 'Loaf', 210: 'Stems', 211: 'Tuber', 240: 'Bottle/can', 250: 'Sachet',
         251: 'Bag of 20 sachets', 260: 'Seedling/plant', 900: 'Other (specify)'}
P_PH_W3 = {22: 'Mudu Large', 32: 'Derica Large', 31: 'Derica Medium', 42: 'Tiya Large'}  # PH_W3 PDF p.30
PRINTED_U = {'W1': {}, 'W2': P_W2, 'W3-first': P_W3, 'W3-second': P_W3, 'W4': P_W45, 'W5': P_W45}
PRINTED_U_LOC = {'W1': 'none: C2 PDF pp.5-8 print an empty "Unit Codes" column and no key',
                 'W2': 'GHS_Panel_PP_Community.pdf PDF p.5-7 (UNIT CODE box)',
                 'W3-first': 'PP_W3_Community_Quest.pdf PDF p.20', 'W3-second': 'PP_W3_Community_Quest.pdf PDF p.20',
                 'W4': 'pp_w4_community_quest.pdf PDF p.13', 'W5': 'ghs_panel_w5_pp_community_questionnaire.pdf PDF p.12'}
HH_U = {'W1': '2010Q3', 'W2': '2012Q3', 'W3-first': '2015Q3', 'W3-second': '2015Q3', 'W4': '2018Q3'}

J, OKF, UN = 'JUDGEMENT', 'OK', 'UNMAPPABLE'
# (group, code): (u_base, u_sized_alt, basis, flag, note)
UNIT_CURATED = {
    ('W1', 1): ('', 'tomatoes cup', 'none', UN,
                'No u row means a tomato cup.  Recorded unit, so NOT Unknown.  Options: a new weightless spelling '
                '"tomatoes cup" (needs a u decision), or the recorded label verbatim per the GH #1095 ruling -- '
                'which would put "(0.087 kg)" in u, a stated weight _parse_explicit_metric can read (cf. #1035).'),
    ('W1', 2): ('Milk cup', 'milk cup', 'label-match', OKF, 'Stated weight (0.175 kg) stays in the native label only.'),
    ('W1', 3): ('Cigarette cup', 'cigarette cup', 'label-match', OKF, 'Stated weight (0.23 kg) stays native only.'),
    ('W1', 4): ('Derica', 'small derica', 'label-match', OKF, '"derica cup (small)"; stated 0.35 kg native only.'),
    ('W1', 5): ('Derica', 'large derica', 'label-match', J,
                '"derica cup (big)" -> sized alternative reads big as large (W3 card has no "big" derica); '
                'base label unaffected.  Stated 0.7 kg native only.'),
    ('W1', 6): ('Congo/Mudu', 'congo/mudu', 'none', J,
                'One W1 code names TWO units ("congo/mudu (1.5 kg)"); W2-W5 carry congo and mudu as separate '
                'codes.  _canon_community_unit returns Congo (first substring hit), discarding mudu.  1,841 '
                'rows -- the largest W1 local unit.  Options: Congo, Mudu, or a combined token (new u spelling).'),
    ('W1', 7): ('Tiya', 'tiya', 'label-match', OKF, 'Stated weight (3.0 kg) native only.'),
    ('W1', 101): ('Kg', 'Kg', 'label-match', OKF, 'Code 101 here; the household W1 card spells Kg as code 1.'),
    ('W1', 102): ('g', 'g', 'label-match', OKF, 'Household W1 card: code 2.'),
    ('W1', 103): ('l', 'l', 'label-match', OKF, 'Household W1 card: code 3.'),
    ('W1', 104): ('ml', 'ml', 'label-match', J,
                  '_canon_community_unit returns "l" for "millilitre" (substring "litre") -- a 1000x error.  '
                  'Proposed "ml", the spelling food_acquired W1 serves (u[2010-11] code 4); ml is not a u '
                  'Preferred Label.  460 rows.'),
    ('W1', 105): ('Piece', 'piece', 'label-match', OKF, 'Household W1-PP card: code 5.'),
    ('W1', 106): ('other specify', 'other', 'label-match', OKF, 'Household W1-PP card: code 6 "other".'),
    ('W2', 10): ('Derica', 'medium derica', 'judgement-spelling', J,
                 'Native "MEDIUM DERCA" (typo); printed p.5 and u[2012-13] say medium derica.  '
                 '_canon_community_unit returns NA on the typo.'),
    ('W3', 10): ('Basket', 'bin/basket', 'same-code-same-label-as-u[2015-16]', J,
                 'Inherited PH rule folds "bin/basket" to Basket; u Preferred for code 10 is "bin/basket".'),
    ('W3', 14): ('Derica', 'tin', 'same-code-same-label-as-u[2015-16]', J,
                 'Inherited PH rule (_canon_unit: tl == "tin" -> Derica).  u Preferred for code 14 is "tin", and '
                 'PH W1/W2 community serves "tin" (_W1W2_PRICE_UNIT).  Choose Derica (PH W3-W5) or tin.'),
    ('W3', 22): ('Mudu', 'large mudu', 'judgement', J,
                 'Native-only "22. MUDU LARGE": absent from the printed PP card (p.20) and from u[2015-16]; it is '
                 'the PH W3 printed card\'s number for Mudu Large (PH_W3 PDF p.30).  2 rows (2nd location).  '
                 'u Preferred for code 22 is "Basket Medium" (W1) -- never join on the bare code.'),
    ('W3', 31): ('Mudu', 'large mudu', 'judgement', J,
                 'Code used by the data (707 + 53 rows) but absent from the native label set; printed PP card '
                 '(p.20) and u[2015-16] both say Mudu Large.  The PH W3 printed card uses 31 for Derica Medium '
                 '(p.30) -- a third layout.  Not Unknown: the unit was recorded.'),
    ('W3', 32): ('Derica', 'large derica', 'judgement', J,
                 'Native-only "32. DERICA LARGE" = PH W3 printed card\'s Derica Large (p.30); 0 rows.  u Preferred '
                 'for code 32 is "Basin Medium" (W1).'),
    ('W3', 42): ('Derica', 'large derica', 'judgement', J,
                 'Used by the data (66 + 1 rows), absent from the native label set; printed PP card and u[2015-16] '
                 'say Derica Large.  PH W3 printed uses 42 for Tiya Large.'),
    ('W4', 10): ('Basket', 'bin/basket', 'same-code-same-label-as-u[2018Q3]', J, 'As W3 code 10.'),
    ('W4', 14): ('Derica', 'tin', 'same-code-same-label-as-u[2018Q3]', J, 'As W3 code 14.'),
    ('W4', 100): ('Bunch', 'bunch', 'judgement', J,
                  '"BUNCH/BUNDLE"; PH rule and the GH #1033 food decode serve bunch; bundle is code 160.'),
    ('W4', 250): ('Packet/Sachet', 'Sachet', 'same-code-same-label-as-u[2018Q3]', J,
                  'Inherited PH rule folds SACHET into Packet/Sachet; u Preferred 250 is "Sachet".'),
    ('W4', 251): ('Bag of 20 Sachets', 'Bag of 20 Sachets', 'same-code-same-label-as-u[2018Q3]', J,
                  '_canon_community_unit returns Sack/Bag (substring "bag") -- wrong unit.  Proposed u Preferred '
                  '251.  Rows: W4 103, W5 464.  The same rule runs on PH C8 W4/W5 labels; whether PH serves any '
                  'code-251 row as Sack/Bag is UNVERIFIED (no PH data read here).'),
    ('W4', 160): ('Bundle', '', 'label-match', OKF, 'No u[2018Q3] entry; u Preferred Bundle (1003).  0 rows.'),
    ('W4', 180): ('Pick-up', '', 'label-match', OKF, 'u Preferred Pick-up (1015).  0 rows.'),
    ('W4', 190): ('Jerry Can', '', 'label-match', J, '"JERRY CAN/RUBBER" -> Jerry Can (PH rule); 0 rows.'),
    ('W4', 210): ('', '', 'none', UN, '"STEMS": no u row (plot_inputs serves "Stem" via _INPUT_UNIT_LABELS).  0 rows; '
                  'if one appears, the GH #1095 ruling keeps the recorded label.'),
    ('W4', 260): ('', '', 'none', UN, '"SEEDLING/PLANT": no u row.  0 rows; as code 210.'),
    ('W5', 2): ('g', '', 'label-match', OKF, 'Native-only vs the held printed PP card (p.12) and the W4 PP card.  195 rows.'),
    ('W5', 4): ('cl', '', 'label-match', J,
                'Native label "4. CENTLITRES (CL)" (typo).  _canon_community_unit returns "l" (substring "litres") '
                '-- a 100x error.  36 rows.  Native-only vs printed card.  The PH W5 C8 native card is said to '
                'share these labels; whether PH serves code 4 as l is UNVERIFIED here.'),
    ('W5', 40): ('Derica', 'derica', 'judgement', J,
                 '"DERICA/TIN" (printed: 40 Derica, 14 Tin; W4 PP card: 40 DERICA).  Precedent: GH #1033 ruling '
                 '(2026-11-04) serves W4-PH food "40. DERICA/TIN" as derica.  855 rows.'),
    ('W5', 120): ('Packet/Sachet', 'packet/sachet', 'judgement', J,
                  '"SACHET" (printed: 120 Packet/sachet, 250 Sachet; W4 PP: 120 PACKET/SACHET).  Precedent: GH #1033 '
                  'serves W4-PH food "120. SACHET" as packet/sachet.  2,887 rows.'),
    ('W5', 125): ('Packet/Sachet', 'Packet', 'label-match', J,
                  '"PACKET": native-only; PH rule folds it into Packet/Sachet, so 120 and 125 serve one u; u Preferred '
                  '125 is "Packet" (W4 food serves it apart).  83 rows.'),
}
for code, lab in ((270, 'Cube'), (280, 'Metric Cup'), (290, 'Wrap'), (301, 'Tea Spoon'), (302, 'Table Spoon'),
                  (310, 'Crate'), (320, 'Slice'), (330, 'Whole Animal')):
    UNIT_CURATED[('W5', code)] = (lab, lab, 'label-match', OKF,
                                  'Native-only (absent from the held printed PP card and the W4 PP card); u Preferred '
                                  f'{code}; the PH community alias (_COMMUNITY_UNIT_LABELS) gives the same label.')
UNIT_INHERIT_W4 = (10, 14, 100, 160, 180, 190, 210, 250, 251, 260)


def hh_label(src, c):
    return ng.food_unit_labels(HH_U[src]).get(c, '') if src in HH_U else ''


unit_rows = []
for src in SRC_ORDER:
    s = SOURCES[src]
    ucol = s['operand_columns']['unit']
    setname = s['variable_value_label_associations'][ucol]
    card = {r['code']: r['label'] for r in s['report_value_label_sets'][setname]}
    occ = {r['code']: r['rows'] for r in OCC.get(f'{src}:unit', [])}
    w4card = {r['code']: r['label'] for r in SOURCES['W4']['report_value_label_sets']['C2Q2']}
    g = group(src)
    codes = sorted(set(card) | {c for c in occ if c not in card})
    for c in codes:
        lab = card.get(c, '')
        rule = str(ng._canon_community_unit(lab)) if lab else ''
        rule = '' if rule == '<NA>' else rule
        hh = hh_label(src, c)
        if src == 'W5':
            comparator, cmp_lab = 'W4 PP native card', w4card.get(c, '')
        elif src == 'W1' and 11 <= c <= 95:
            comparator, cmp_lab = 'u[2010-11] (W1-PH food card, CONTENTS.org L504-512)', hh
        elif src == 'W1':
            comparator, cmp_lab = 'none (W1-PP household card 1-6 is a different code space)', ''
        else:
            comparator, cmp_lab = f'u[{"2015-16" if src.startswith("W3") else HH_U[src]}]', hh
        key = (g, c)
        if src == 'W5' and key not in UNIT_CURATED and c in UNIT_INHERIT_W4 and w4card.get(c) == lab:
            key = ('W4', c)
        if key in UNIT_CURATED:
            ub, us, basis, flag, note = UNIT_CURATED[key]
            if key[0] == 'W4' and src == 'W5':
                note = 'Same code+label as W4 PP card. ' + note
                basis = basis.replace('u[2018Q3]', 'W4-PP-card') if basis.startswith('same') else basis
        else:
            same = bool(cmp_lab) and unit_tokens(lab) == unit_tokens(cmp_lab)
            if src == 'W1' and 11 <= c <= 95:
                same = True       # same SP10BQ2B card; household spelling is its weightless clean-up
            ub, us = rule, (hh if src != 'W5' else hh_label('W4', c) if w4card.get(c) == lab else '')
            if same:
                basis = f'same-code-same-label-as-{"W4-PP-card" if src == "W5" else comparator.split(" ")[0]}'
                flag, note = OKF, ''
                if src == 'W1':
                    note = 'Stated weight stays in the native label only (GH #1035 precedent: W1 labels weightless).'
            else:
                raise AssertionError(f'uncurated unit row {src} {c} {lab!r} vs {cmp_lab!r} rule={rule!r}')
        assert ub == '' or ub in U_PREFERRED or ub in ('ml', 'Congo/Mudu'), (src, c, ub)
        unit_rows.append(dict(
            source=src, t=T[src], unit_var=ucol, native_label_set=setname, native_code=c, native_label=lab,
            in_native_card=(c in card), rows_with_code=occ.get(c, ''),
            proposed_u=ub, sized_alternative_u=us, basis=basis, flag=flag, note=note,
            rule_canon_community_unit=rule, comparator=comparator, comparator_label=cmp_lab,
            printed_pp_label=PRINTED_U[src].get(c, ''), printed_pp_locator=PRINTED_U_LOC[src],
            printed_ph_w3_label=(P_PH_W3.get(c, '') if src.startswith('W3') else ''),
            proposed_is_u_preferred=(ub in U_PREFERRED)))
    unit_rows.append(dict(source=src, t=T[src], unit_var=ucol, native_label_set=setname, native_code='(missing)',
                          native_label='', in_native_card=False, rows_with_code='see report',
                          proposed_u='Unknown', sized_alternative_u='Unknown', basis='sentinel (data_info.yml u)',
                          flag=OKF, note='No unit recorded -> the canonical missing-unit sentinel; never a pooled factor.',
                          rule_canon_community_unit='', comparator='lsms_library/data_info.yml L62-93',
                          comparator_label='Unknown', printed_pp_label='', printed_pp_locator='',
                          printed_ph_w3_label='', proposed_is_u_preferred=False))
units = pd.DataFrame(unit_rows)

# printed-only unit codes (no data row can carry them: 0 rows outside the native card in the sealed audit)
printed_only = []
for src in SRC_ORDER:
    s = SOURCES[src]
    ucol = s['operand_columns']['unit']
    card = {r['code'] for r in s['report_value_label_sets'][s['variable_value_label_associations'][ucol]]}
    occ = {r['code'] for r in OCC.get(f'{src}:unit', [])}
    for c, lab in PRINTED_U[src].items():
        if c not in card and c not in occ:
            printed_only.append(dict(source=src, printed_code=c, printed_label=lab))

# --------------------------------------------------------------------------
# SIZES (W4 c2q2c, W5 c2q2c)
# --------------------------------------------------------------------------
P_SIZE = {0: 'Small', 1: 'Medium', 2: 'Large', 3: 'Very large', 4: '10 Litres', 5: '20 Litres', 6: '25 Litres',
          7: '50 Litres', 10: '25kg', 11: '50kg', 12: '100kg', 20: '33cl', 21: '35cl', 22: '50cl', 23: '60cl',
          24: '75cl', 25: '1 Litre', 26: '1.5 Litre', 99: 'One size only'}
size_rows = []
for src in ('W4', 'W5'):
    s = SOURCES[src]
    sc = s['operand_columns']['size']
    sn = s['variable_value_label_associations'][sc]
    card = {r['code']: r['label'] for r in s['report_value_label_sets'][sn]}
    occ = {r['code']: r['rows'] for r in OCC.get(f'{src}:size', [])}
    for c in sorted(card):
        lab = card[c]
        q = ng._c8_size_metric_grams(lab)
        if ng._C8_SIZE_ONE_SIZE_ONLY.match(lab):
            kind, handling = 'one-size', 'u unchanged; the questionnaire\'s own statement that the price is of one unit.'
        elif q is not None:
            kind = 'metric'
            handling = (f'u = the container label; Price is the price of ONE container of the stated size.  '
                        f'Dividing by {q:g} {"cl" if re.search(r"(?i)CL|LITRE", lab) else "g"} '
                        '(the PH c8-metric-size rule) is NOT applied: not authorized for PP.')
        elif c in (0, 1, 2, 3):
            kind = 'ordinal'
            handling = ('u = base label (size not in u, as PH C8).  Sized alternative: nigeria.sized_food_unit '
                        'prefix for W4_SIZED_UNIT_CODES.  The size is retained as NativeSizeCode/Label.')
        else:
            kind, handling = 'other', 'review'
        size_rows.append(dict(source=src, t=T[src], size_var=sc, native_label_set=sn, native_code=c,
                              native_label=lab, rows_with_code=occ.get(c, ''),
                              printed_pp_label=P_SIZE.get(c, ''), kind=kind,
                              metric_quantity_g_or_cl=('' if q is None else q), proposed_handling=handling,
                              flag=('OK' if kind in ('ordinal', 'one-size') else 'JUDGEMENT')))
sizes = pd.DataFrame(size_rows)

items.to_csv(f'{OUT}/items.csv', index=False)
units.to_csv(f'{OUT}/units.csv', index=False)
sizes.to_csv(f'{OUT}/sizes.csv', index=False)
pd.DataFrame(printed_only).to_csv(f'{OUT}/units_printed_only.csv', index=False)


def bucket(row):
    if row['flag'] == 'UNMAPPABLE':
        return 'unmappable'
    if row['basis'].startswith('same-code'):
        if row['flag'] == 'OK':
            return 'identity'
        return 'judgement' if 'judgement' in row['basis'] else 'identity-but-judgement'
    if row['basis'] == 'label-match':
        return 'label-match' if row['flag'] == 'OK' else 'label-match-but-judgement'
    if row['basis'].startswith('sentinel'):
        return 'sentinel'
    return 'judgement'


summary = {}
for name, df in (('items', items), ('units', units)):
    d = df.assign(bucket=df.apply(bucket, axis=1))
    summary[name] = d.groupby(['source', 'bucket']).size().unstack(fill_value=0)
    print(name); print(summary[name].to_string()); print()
json.dump({k: v.to_dict() for k, v in summary.items()}, open(f'{OUT}/COUNTS.json', 'w'), indent=1, default=str)

# --------------------------------------------------------------------------
# MAPPING.org -- compact reviewable tables (NOT a build input: lives under .coder/)
# --------------------------------------------------------------------------
def org_escape(x):
    return str(x).replace('|', '\\vert{}')


lines = ['#+title: GH #587 Nigeria PP C2 proposed item/unit crosswalk (UNWIRED; not a build input)', '',
         'Generated by =build_mapping.py= in this directory; see =../587-nigeria-pp-mapping.md= for the',
         'comparators, the normalisation rule and every judgement.  Nothing reads this file: it is under',
         '=.coder/=, outside every build-input glob.', '',
         'Flags: OK = mechanical (same code and same label as the named comparator, or an exact label match',
         'to an existing Preferred/u label); JUDGEMENT = needs a maintainer look; UNMAPPABLE = no defensible',
         'existing label.  =j= targets =harmonize_food= Preferred Labels as the food path resolves them for',
         'the PP quarter; =u= targets the size-agnostic base vocabulary PH =community_prices= serves today',
         '(=_canon_community_unit=); the sized alternative is the =food_acquired= per-round spelling.', '']
lines += ['* Counts', '', '| table | source | bucket | n |', '|-+-+-+-|']
for name, tab in summary.items():
    for src_ in tab.index:
        for b in tab.columns:
            if tab.loc[src_, b]:
                lines.append(f'| {name} | {src_} | {b} | {tab.loc[src_, b]} |')
lines.append('')
for src in SRC_ORDER:
    lines += [f'* {src} ({T[src]}, obs {SOURCES[src]["obs"]}) items', '',
              '| code | native label | proposed j | alternative | basis | flag | comparator label | PH code contract | note |',
              '|-+-+-+-+-+-+-+-+-|']
    for _, r in items[items.source == src].iterrows():
        lines.append('| ' + ' | '.join(org_escape(x) for x in (
            r.native_code, r.native_label, r.proposed_j, r.alternative_j, r.basis, r.flag,
            r.comparator_label, r.ph_code_contract_j, r.note)) + ' |')
    lines += ['', f'* {src} ({T[src]}) units  (variable {SOURCES[src]["operand_columns"]["unit"]})', '',
              '| code | native label | rows | proposed u | sized alt. | PH rule output | basis | flag | comparator label | printed PP label | note |',
              '|-+-+-+-+-+-+-+-+-+-+-|']
    for _, r in units[units.source == src].iterrows():
        lines.append('| ' + ' | '.join(org_escape(x) for x in (
            r.native_code, r.native_label, r.rows_with_code, r.proposed_u, r.sized_alternative_u,
            r.rule_canon_community_unit, r.basis, r.flag, r.comparator_label, r.printed_pp_label, r.note)) + ' |')
    lines.append('')
lines += ['* W4 / W5 SIZE codes (variable c2q2c)', '',
          '| src | code | native label | rows | printed PP label | kind | flag | handling |', '|-+-+-+-+-+-+-+-|']
for _, r in sizes.iterrows():
    lines.append('| ' + ' | '.join(org_escape(x) for x in (r.source, r.native_code, r.native_label, r.rows_with_code,
                                                             r.printed_pp_label, r.kind, r.flag,
                                                             r.proposed_handling)) + ' |')
lines += ['', '* Printed-only unit codes (absent from the native card; no data row can carry them)', '',
          '| src | code | printed label |', '|-+-+-|']
for r in printed_only:
    lines.append(f"| {r['source']} | {r['printed_code']} | {r['printed_label']} |")
open(f'{OUT}/MAPPING.org', 'w').write('\n'.join(lines) + '\n')
print('printed-only unit codes', printed_only)
