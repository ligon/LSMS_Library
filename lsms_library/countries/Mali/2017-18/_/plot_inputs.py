"""Build plot_inputs (item-level ag inputs) for Mali EACI 2017-18.

GAP 2 (parity loop).  One row per (t, i, plot, input[, crop]).

Sources (cultivation passage 1; post-harvest passage 2):
  - eaci17_s11cp1.dta  seed/cultivation roster, keyed
                       (grappe, exploitation, field, parcel, crop)
  - eaci17_s07dp2.dta  plot-fertilizer / pesticide roster (one row per plot,
                       Urea/DAP/NPK/other-inorganic + manure/compost/other-
                       organic + pesticide/fungicide/herbicide/other slots)
  - eaci17_s07bp2.dta  household input-purchase roster (type, qty, value;
                       NO plot grain) -> Purchased / Quantity_purchased

i = (grappe, exploitation) — the 2017-18 household key.
plot = "{field}_{parcel}" (s11cq01_s11cq02 for seed; s7dq01_s7dq02 for
fertilizer) — the SAME plot id as crop_production / plot_features.

REPORTED item-level columns only — Quantity / u / Purchased /
Quantity_purchased / Improved / crop.  NO seed_kg / nitrogen_kg / any-use
flags / fertilizer totals.

Variable map traced from MLI_EACI2.do (WB harmonised input section):
  seed: crop=s11cq03 improved=s11cq10 qty=s11cq11a unit=s11cq11b
        paid?=s11cq12 amount=s11cq13  (purchase IS asked here)
  inorganic fert: flag=s7dq22; Urea qty/unit=s7dq26a1/a2, DAP=s7dq26b1/b2,
        NPK=s7dq26c1/c2, other=s7dq26d1/d2
  organic fert: manure flag=s7dq05 acq=s7dq08 qty/unit=s7dq09a/b;
        compost flag=s7dq10 acq=s7dq14 qty/unit=s7dq15a/b;
        other-org flag=s7dq16 acq=s7dq20 qty/unit=s7dq21a/b
  pesticide: flag=s7dq27; pesticide qty/unit=s7dq30a1/a2, fungicide=
        s7dq30b1/b2, herbicide=s7dq30c1/c2, other=s7dq30d1/d2
  purchases (s7b): type=s7bq01 bought?=s7bq02 qty=s7bq09a value=s7bq09c
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, plot_inputs_finalize

WAVE = '2017-18'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['exploitation']])),
                    axis=1)


def _plot(df, fcol, pcol):
    f = df[fcol].astype('Int64').astype('string')
    p = df[pcol].astype('Int64').astype('string')
    return (f + '_' + p).where(f.notna() & p.notna(), pd.NA)


def _yes(series):
    s = series.astype('string').str.strip()
    return s.map({'Oui': True, 'Non': False}).astype('boolean')


pieces = []

# --- seeds (s11c): per plot-crop; purchase IS asked (s11cq12/s11cq13) ---
s11c = get_dataframe('../Data/eaci17_s11cp1.dta').copy()
s11c['i'] = _hhid(s11c)
improved_raw = s11c['s11cq10'].astype('string').str.strip()
improved = pd.Series(pd.NA, index=s11c.index, dtype='boolean')
improved = improved.mask(improved_raw.eq('Locales'), False)
improved = improved.mask(improved_raw.str.startswith('Améliorées').fillna(False), True)
paid = _yes(s11c['s11cq12'])
seed = pd.DataFrame({
    't': WAVE,
    'i': s11c['i'],
    'plot': _plot(s11c, 's11cq01', 's11cq02'),
    'input': 'seed',
    'crop': s11c['s11cq03'],
    'u': s11c['s11cq11b'],
    'Quantity': pd.to_numeric(s11c['s11cq11a'], errors='coerce'),
    'Purchased': paid,
    # s11cq13 is the AMOUNT PAID (value, CFA), not a quantity — do not put it
    # in Quantity_purchased (which is a kg/native-unit quantity).  Left NA;
    # seed purchase value is recoverable from the raw module if needed.
    'Quantity_purchased': pd.NA,
    'Improved': improved,
})
pieces.append(seed)

# --- plot fertilizer / pesticide (s7d): one row per used input slot ---
s7d = get_dataframe('../Data/eaci17_s07dp2.dta').copy()
s7d['i'] = _hhid(s7d)
s7d['plot'] = _plot(s7d, 's7dq01', 's7dq02')


def _fert_slot(code, qty_col, unit_col, purchased=None):
    return pd.DataFrame({
        't': WAVE,
        'i': s7d['i'],
        'plot': s7d['plot'],
        'input': code,
        'crop': pd.NA,
        'u': s7d[unit_col] if unit_col else pd.NA,
        'Quantity': pd.to_numeric(s7d[qty_col], errors='coerce'),
        'Purchased': purchased if purchased is not None
                     else pd.Series(pd.NA, index=s7d.index, dtype='boolean'),
        'Quantity_purchased': pd.NA,
        'Improved': pd.NA,
    })


pieces.append(_fert_slot('urea', 's7dq26a1', 's7dq26a2'))
pieces.append(_fert_slot('dap', 's7dq26b1', 's7dq26b2'))
pieces.append(_fert_slot('npk', 's7dq26c1', 's7dq26c2'))
pieces.append(_fert_slot('other_inorganic', 's7dq26d1', 's7dq26d2'))

_achat = lambda c: s7d[c].astype('string').str.strip().eq('Achat')
manure_p = pd.Series(pd.NA, index=s7d.index, dtype='boolean').mask(
    s7d['s7dq08'].notna(), _achat('s7dq08'))
compost_p = pd.Series(pd.NA, index=s7d.index, dtype='boolean').mask(
    s7d['s7dq14'].notna(), _achat('s7dq14'))
otherorg_p = pd.Series(pd.NA, index=s7d.index, dtype='boolean').mask(
    s7d['s7dq20'].notna(), _achat('s7dq20'))
pieces.append(_fert_slot('manure', 's7dq09a', 's7dq09b', purchased=manure_p))
pieces.append(_fert_slot('compost', 's7dq15a', 's7dq15b', purchased=compost_p))
pieces.append(_fert_slot('other_organic', 's7dq21a', 's7dq21b', purchased=otherorg_p))

pieces.append(_fert_slot('pesticide', 's7dq30a1', 's7dq30a2'))
pieces.append(_fert_slot('fungicide', 's7dq30b1', 's7dq30b2'))
pieces.append(_fert_slot('herbicide', 's7dq30c1', 's7dq30c2'))
pieces.append(_fert_slot('other_phytosanitary', 's7dq30d1', 's7dq30d2'))

# --- household input purchases (s7b): qty per type; HOUSEHOLD grain ---
s7b = get_dataframe('../Data/eaci17_s07bp2.dta').copy()
s7b['i'] = _hhid(s7b)


def _s7b_type(label):
    s = str(label)
    if 'Urée' in s:
        return 'urea'
    if 'DAP' in s:
        return 'dap'
    if 'NPK' in s:
        return 'npk'
    if s.startswith('Engrais inorganiques') or s.startswith('Autres engrais inorganiques'):
        return 'other_inorganic'
    if 'Fumure' in s:
        return 'manure'
    if 'Compost' in s:
        return 'compost'
    if s.startswith('Engrais organiques'):
        return 'other_organic'
    if 'Pesticides' in s:
        return 'pesticide'
    if 'Fongicides' in s:
        return 'fungicide'
    if 'Herbicides' in s:
        return 'herbicide'
    if 'phytosanitaires' in s:
        return 'other_phytosanitary'
    return pd.NA


s7b['input'] = s7b['s7bq01'].astype('string').map(_s7b_type)
purch = s7b[s7b['input'].notna()].copy()
purch['bought'] = _yes(purch['s7bq02']).loc[purch.index]
purch['qty'] = pd.to_numeric(purch['s7bq09a'], errors='coerce')
hh_purch = purch.groupby(['i', 'input'], as_index=False).agg(
    Purchased=('bought', 'max'),
    Quantity_purchased=('qty', lambda s: s.sum(min_count=1)),
)

df = pd.concat(pieces, ignore_index=True)
# The HH-grain purchase (s7b) ANNOTATES a plot APPLICATION row — gate on a
# reported plot-level application Quantity so a purchased-but-unapplied
# fertilizer does not manufacture a content-free row on every plot.  The seed
# rows already carry their own paid-for question (s11cq12), kept where present.
df = df.merge(hh_purch, on=['i', 'input'], how='left', suffixes=('', '_hh'))
applied = df['Quantity'].notna()
df['Purchased'] = df['Purchased'].astype('boolean').where(
    df['Purchased'].notna(),
    df['Purchased_hh'].astype('boolean').where(applied, pd.NA))
df['Quantity_purchased'] = df['Quantity_purchased'].where(
    df['Quantity_purchased'].notna(),
    df['Quantity_purchased_hh'].where(applied))
df = df.drop(columns=['Purchased_hh', 'Quantity_purchased_hh'])

df = plot_inputs_finalize(df)

assert len(df) > 0, "plot_inputs 2017-18 produced no rows"
assert df.index.is_unique, "Non-unique (t,i,plot,input,crop) in plot_inputs 2017-18"

to_parquet(df, 'plot_inputs.parquet')
