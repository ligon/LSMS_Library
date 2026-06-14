"""Build plot_inputs (item-level ag inputs) for Mali EACI 2014-15.

GAP 2 (parity loop).  One row per (t, i, plot, input[, crop]).

Sources (post-harvest passage 2; cultivation passage 1):
  - EACIS1E_p2.dta   seed roster, keyed (grappe, menage, field, parcel, crop)
  - EACIS2C_p2.dta   plot-fertilizer / pesticide roster (one row per plot,
                     with separate Urea/DAP/NPK/other-inorganic and
                     manure/compost/other-organic and pesticide/fungicide/
                     herbicide/other-phytosanitary slots)

plot = "{field}_{parcel}" (s1eq01_s1eq02 for seed; s2cq01_s2cq02 for
fertilizer) — the SAME plot id as crop_production / plot_features.

REPORTED item-level columns only — Quantity / u / Purchased /
Quantity_purchased / Improved / crop.  NO seed_kg / nitrogen_kg / any-use
flags / fertilizer totals (those are transformations over these rows).

Variable map traced from MLI_EACI1.do (WB harmonised input section):
  seed: crop=s1eq03b improved=s1eq04 qty=s1eq05a unit=s1eq05b
        purchased-mode is not asked here; the WB code assumes seeds were
        bought (seeds_amount_purchased_kg = seed_kg), so Purchased is left
        NA at item grain (honest missing) rather than force-true.
  inorganic fert: flag=s2cq21; Urea qty/unit=s2cq25a/b, DAP=s2cq25c/d,
        NPK=s2cq25e/f, other=s2cq25g/h
  organic fert: manure flag=s2cq04 acq=s2cq07 qty/unit=s2cq08a/b;
        compost flag=s2cq09 acq=s2cq13 qty/unit=s2cq14a/b;
        other-org flag=s2cq15 acq=s2cq19 qty/unit=s2cq20a/b
  pesticide: flag=s2cq26; pesticide qty/unit=s2cq29a/b, fungicide=s2cq29c/d,
        herbicide=s2cq29e/f, other=s2cq29g/h
  Purchased (organic): acquisition mode == 'Achat'.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, plot_inputs_finalize

WAVE = '2014-15'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['menage']])), axis=1)


def _plot(df, fcol, pcol):
    f = df[fcol].astype('Int64').astype('string')
    p = df[pcol].astype('Int64').astype('string')
    return (f + '_' + p).where(f.notna() & p.notna(), pd.NA)


def _yes(series):
    """'Oui'/'Non' (or NA) -> nullable bool."""
    s = series.astype('string').str.strip()
    out = s.map({'Oui': True, 'Non': False})
    return out.astype('boolean')


pieces = []

# --- seeds (s1e): per plot-crop ---
s1e = get_dataframe('../Data/EACIS1E_p2.dta').copy()
s1e['i'] = _hhid(s1e)
# improved: WB recode s1eq04 (2/5 -> improved, 1=Locales -> not improved).
improved_raw = s1e['s1eq04'].astype('string').str.strip()
improved = pd.Series(pd.NA, index=s1e.index, dtype='boolean')
improved = improved.mask(improved_raw.eq('Locales'), False)
improved = improved.mask(improved_raw.str.startswith('Améliorées').fillna(False), True)
seed = pd.DataFrame({
    't': WAVE,
    'i': s1e['i'],
    'plot': _plot(s1e, 's1eq01', 's1eq02'),
    'input': 'seed',
    'crop': s1e['s1eq03b'],
    'u': s1e['s1eq05b'],
    'Quantity': pd.to_numeric(s1e['s1eq05a'], errors='coerce'),
    'Purchased': pd.NA,            # not asked at item grain in 2014-15 s1e
    'Quantity_purchased': pd.NA,
    'Improved': improved,
})
pieces.append(seed)

# --- plot fertilizer / pesticide (s2c): one row per used input slot ---
s2c = get_dataframe('../Data/EACIS2C_p2.dta').copy()
s2c['i'] = _hhid(s2c)
s2c['plot'] = _plot(s2c, 's2cq01', 's2cq02')


def _fert_slot(code, qty_col, unit_col, purchased=None):
    """Build a (plot, input) fertilizer/pesticide slot frame."""
    return pd.DataFrame({
        't': WAVE,
        'i': s2c['i'],
        'plot': s2c['plot'],
        'input': code,
        'crop': pd.NA,             # fertilizer/pesticide applied at plot grain
        'u': s2c[unit_col] if unit_col else pd.NA,
        'Quantity': pd.to_numeric(s2c[qty_col], errors='coerce'),
        'Purchased': purchased if purchased is not None
                     else pd.Series(pd.NA, index=s2c.index, dtype='boolean'),
        'Quantity_purchased': pd.NA,
        'Improved': pd.NA,
    })


# inorganic: Urea / DAP / NPK / other-inorganic (purchase signal -> s2d below)
pieces.append(_fert_slot('urea', 's2cq25a', 's2cq25b'))
pieces.append(_fert_slot('dap', 's2cq25c', 's2cq25d'))
pieces.append(_fert_slot('npk', 's2cq25e', 's2cq25f'))
pieces.append(_fert_slot('other_inorganic', 's2cq25g', 's2cq25h'))

# organic: manure / compost / other-organic, with Purchased from acq mode.
_achat = lambda c: s2c[c].astype('string').str.strip().eq('Achat')
manure_p = pd.Series(pd.NA, index=s2c.index, dtype='boolean').mask(
    s2c['s2cq07'].notna(), _achat('s2cq07'))
compost_p = pd.Series(pd.NA, index=s2c.index, dtype='boolean').mask(
    s2c['s2cq13'].notna(), _achat('s2cq13'))
otherorg_p = pd.Series(pd.NA, index=s2c.index, dtype='boolean').mask(
    s2c['s2cq19'].notna(), _achat('s2cq19'))
pieces.append(_fert_slot('manure', 's2cq08a', 's2cq08b', purchased=manure_p))
pieces.append(_fert_slot('compost', 's2cq14a', 's2cq14b', purchased=compost_p))
pieces.append(_fert_slot('other_organic', 's2cq20a', 's2cq20b', purchased=otherorg_p))

# phytosanitary: pesticide / fungicide / herbicide / other
pieces.append(_fert_slot('pesticide', 's2cq29a', 's2cq29b'))
pieces.append(_fert_slot('fungicide', 's2cq29c', 's2cq29d'))
pieces.append(_fert_slot('herbicide', 's2cq29e', 's2cq29f'))
pieces.append(_fert_slot('other_phytosanitary', 's2cq29g', 's2cq29h'))

# --- household inorganic-fertilizer purchases (s2d): qty/value per type ---
# s2d records purchases at the HOUSEHOLD grain (no plot/parcel).  We use it
# only to set Purchased=True and Quantity_purchased on the matching inorganic
# input rows for households that report a purchase of that type — joined at
# (i, input).  We do NOT fabricate a plot allocation: the purchased quantity
# is broadcast as a household-level signal onto every plot row of that input
# type (a transformations.py rollup can recover the exact HH total).
s2d = get_dataframe('../Data/EACIS2D_p2.dta').copy()
s2d['i'] = _hhid(s2d)
# Map the (truncated) s2dq01 fertilizer-type label -> harmonize_input Code.
_S2D_TYPE = {
    'Engrais inorganiques - Ur': 'urea',
    'Engrais inorganiques - DA': 'dap',
    'Engrais inorganiques - NP': 'npk',
    'Engrais inorganiques - Co': 'other_inorganic',
    'Engrais inorganiques - PN': 'other_inorganic',
    'Engrais inorganiques - KC': 'other_inorganic',
    'Engrais inorganiques - Ni': 'other_inorganic',
    'Autres engrais inorganiqu': 'other_inorganic',
    'Engrais organiques - Fumu': 'manure',
    'Engrais organiques - Comp': 'compost',
    'Engrais organiques - Sabu': 'other_organic',
}
s2d['input'] = s2d['s2dq01'].astype('string').str.strip().map(_S2D_TYPE)
bought = _yes(s2d['s2dq02'])
purch = s2d[s2d['input'].notna()].copy()
purch['bought'] = bought.loc[purch.index]
purch['qty'] = pd.to_numeric(purch['s2dq09a'], errors='coerce')
# collapse to (i, input): any purchase -> True; sum reported purchased qty.
hh_purch = purch.groupby(['i', 'input'], as_index=False).agg(
    Purchased=('bought', 'max'),
    Quantity_purchased=('qty', lambda s: s.sum(min_count=1)),
)

df = pd.concat(pieces, ignore_index=True)

# Join the household-grain purchase signal (s2d) onto input rows by
# (i, input).  The HH purchase ANNOTATES a plot APPLICATION row — it does not
# create one: gate the merged Purchased / Quantity_purchased on the row having
# a reported plot-level application Quantity (so a fertilizer a household
# bought but whose plot-level application went unrecorded does not manufacture
# a content-free row on every plot).  Organic Purchased already set from the
# plot acquisition mode is kept where present.
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

assert len(df) > 0, "plot_inputs 2014-15 produced no rows"
assert df.index.is_unique, "Non-unique (t,i,plot,input,crop) in plot_inputs 2014-15"

to_parquet(df, 'plot_inputs.parquet')
