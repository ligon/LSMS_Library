"""Guatemala ENCOVI 2000 -- shocks (Capitulo 3 "Situaciones Adversas").

Two source files:
  - ECV05H03.DTA (RESUMEN): per-household occurrence roster, WIDE.  Two si/no
    families of event flags -- p03a01a..p03a01m (13 "general"/community events)
    and p03a06a..p03a06p (15 "particular"/household events).  These are the
    authoritative *occurrence* flags (one column per event type per household).
  - ECV06H03.DTA (DETALLE): one row per (household, problem).  `item` is the
    event code (101-113 general, 201-215 particular -- aligned 1:1 with the
    RESUMEN columns); `perdida` is the impact type (1=income, 2=patrimonio/
    assets, 3=both, 4=none); `accion` is the single coping action taken.

Design (canonical `shocks`, index (t, i, Shock)):
  - One row per (household, experienced shock).  The row set is the UNION of
    RESUMEN-experienced (hh,item) pairs and DETALLE (hh,item) rows -- they
    agree to within ~67 pairs (DETALLE drops some RESUMEN-flagged shocks for
    which no detail was collected, and has 1 extra).  Using the union keeps
    every experienced shock as a row.
  - `Shock` is the item code mapped to an English label (SHOCK_LABELS below).
  - AffectedIncome / AffectedAssets come from DETALLE.perdida.  ENCOVI's
    impact item distinguishes income vs. patrimonio (assets) only -- there is
    NO production/consumption breakdown -- so AffectedProduction /
    AffectedConsumption are not derivable and are left out (NaN if read).
  - `Experienced` (bool) carries the RESUMEN occurrence flag (always True for
    rows in this table by construction; included for parity with the Liberia
    occurrence-only precedent and to mark the ~67 detail-less shocks).
  - HowCoped0 is the DETALLE `accion` mapped to English text.  ENCOVI records
    a SINGLE coping action per problem (no up-to-3 battery), so HowCoped1 /
    HowCoped2 do not exist in this instrument and are omitted.

i = hogar (matches household_roster / sample); v is NOT baked in -- it is
joined from sample() at API time.
"""
import sys
sys.path.append('../../../_/')
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet, format_id

# item code -> canonical English Shock label.  Codes & Spanish source labels
# come from ECV06H03 `item` value labels; the RESUMEN p03a01*/p03a06* columns
# carry the same event set (mapped to the same codes via RESUMEN_TO_ITEM).
SHOCK_LABELS = {
    101: 'Earthquake',
    102: 'Drought',
    103: 'Floods',
    104: 'Storms',
    105: 'Hurricane',
    106: 'Pests',
    107: 'Landslides',
    108: 'Forest fires',
    109: 'Business closures',
    110: 'Mass layoffs',
    111: 'General price rise',
    112: 'Public protests',
    113: 'Other general problem',
    201: 'Loss of employment',
    202: 'Drop in household income',
    203: 'Family business failure',
    204: 'Illness or serious accident',
    205: 'Death of a worker',
    206: 'Death of other household member',
    207: 'Abandonment by household head',
    208: 'Fire of dwelling/business',
    209: 'Crime',
    210: 'Land dispute',
    211: 'Family disputes',
    212: 'Loss of cash/in-kind aid',
    213: 'Drop in prices of business products',
    214: 'Loss of harvest',
    215: 'Other particular problem',
}

# RESUMEN si/no column -> DETALLE item code (1:1, in questionnaire order).
RESUMEN_TO_ITEM = {
    'p03a01a': 101, 'p03a01b': 102, 'p03a01c': 103, 'p03a01d': 104,
    'p03a01e': 105, 'p03a01f': 106, 'p03a01g': 107, 'p03a01h': 108,
    'p03a01i': 109, 'p03a01j': 110, 'p03a01k': 111, 'p03a01l': 112,
    'p03a01m': 113,
    'p03a06a': 201, 'p03a06b': 202, 'p03a06c': 203, 'p03a06d': 204,
    'p03a06e': 205, 'p03a06f': 206, 'p03a06g': 207, 'p03a06h': 208,
    'p03a06i': 209, 'p03a06j': 210, 'p03a06k': 211, 'p03a06l': 212,
    'p03a06m': 213, 'p03a06n': 214, 'p03a06p': 215,
}

# accion code -> English coping-action label (from ECV06H03 `accion` labels).
COPING_LABELS = {
    1: 'Spent savings or investments',
    2: 'Pawned goods',
    3: 'Mortgaged house or land',
    4: 'Collected on insurance',
    5: 'Worked more than those already working',
    6: 'Other household members went out to work',
    7: 'Borrowed money from a private bank',
    8: 'Borrowed money from a state bank',
    9: 'Borrowed money from a relative',
    10: 'Borrowed money from a friend',
    11: 'Borrowed money from a moneylender',
    12: 'Borrowed money at work',
    13: 'Sold house or land',
    14: 'Sold animals',
    15: 'Sold appliances/equipment/machines',
    16: 'Sold jewelry',
    17: 'Sold harvest in advance',
    18: 'Help from government bodies',
    19: 'Help from private entities',
    20: 'Help from international entities',
    21: 'Help from NGOs',
    22: 'Help from neighbors',
    23: 'Stopped consuming some products',
    24: 'Did nothing',
    25: 'Help from family/relatives',
    98: 'Other',
}

# perdida code -> (AffectedIncome, AffectedAssets)
PERDIDA_AFFECTED = {
    1: (True, False),   # los ingresos que reciben normalmente
    2: (False, True),   # del patrimonio
    3: (True, True),    # de ingresos y patrimonio
    4: (False, False),  # no ha significado ninguna perdida
}

# --- RESUMEN: occurrence (long) -------------------------------------------
R = get_dataframe('../Data/ECV05H03.DTA', convert_categoricals=False)
occ_rows = []
for col, code in RESUMEN_TO_ITEM.items():
    hh = R.loc[R[col] == 1, 'hogar']
    for h in hh:
        occ_rows.append((h, code))
occ = pd.DataFrame(occ_rows, columns=['hogar', 'item'])
occ['Experienced'] = True

# --- DETALLE: impact + coping (already long) ------------------------------
D = get_dataframe('../Data/ECV06H03.DTA', convert_categoricals=False)
D = D[['hogar', 'item', 'perdida', 'accion']].copy()
aff = D['perdida'].map(PERDIDA_AFFECTED)
D['AffectedIncome'] = aff.map(lambda x: x[0] if isinstance(x, tuple) else pd.NA)
D['AffectedAssets'] = aff.map(lambda x: x[1] if isinstance(x, tuple) else pd.NA)
D['HowCoped0'] = D['accion'].map(COPING_LABELS)
D = D.drop(columns=['perdida', 'accion'])

# --- union of (hogar, item); DETALLE supplies impact/coping ---------------
df = occ.merge(D, on=['hogar', 'item'], how='outer')
# Any (hogar,item) only in DETALLE was still an experienced shock.
df['Experienced'] = df['Experienced'].fillna(True).astype('boolean')

df['Shock'] = df['item'].map(SHOCK_LABELS)
df = df.dropna(subset=['Shock'])

df['i'] = df['hogar'].apply(format_id)
df['t'] = '2000'

out = df[['t', 'i', 'Shock', 'Experienced',
          'AffectedIncome', 'AffectedAssets', 'HowCoped0']].copy()
out['AffectedIncome'] = out['AffectedIncome'].astype('boolean')
out['AffectedAssets'] = out['AffectedAssets'].astype('boolean')
out = out.set_index(['t', 'i', 'Shock']).sort_index()

to_parquet(out, 'shocks.parquet')
