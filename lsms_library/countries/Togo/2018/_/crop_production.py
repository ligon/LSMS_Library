"""Build crop_production for Togo EHCVM 2018 (item-level), cloned from the
Niger 2018-19 EHCVM template.  SELF-CONTAINED: the map/finish helpers are
inlined here, so this script does NOT import togo or niger.

Single source file: ../Data1/s16c_me_tgo2018.dta (agriculture crop/harvest
module).  One row per reported (field, parcel, crop) harvest record.
Harvest qty + unit (s16cq12a / s16cq12b), sold qty + unit (s16cq16a /
s16cq16b), sale value (s16cq17), and the intercrop flag (s16cq07) are ALL
recorded at this plot-crop grain in 2018, so no cross-file join is needed.

*** TOGO PATH / WAVE QUIRKS: the EHCVM agriculture module lives in
2018/Data1/ (NOT 2018/Data/, which holds only _forEthan extracts); the
wave dir is named 2018 (NOT 2018-19); the code suffix is tgo2018; t='2018'.
Togo also ships bespoke Togo_survey2018_* files — DO NOT use those; this
uses the standardized EHCVM module s16c_me_tgo2018.dta. ***

Index = (t, i, plot, crop, u); plot = "{s16cq02}_{s16cq03}" aligns with
plot_features' plot_id.  `i` is Togo's composite household id (grappe + '0'
+ zero-padded menage, inlined verbatim from togo.i() / 2018/_/livestock.py —
NO 'E_' prefix), matching sample() and plot_features (t='2018').

MAPPING NOTE: Togo has NO `harmonize_food` table (its food labels live in
food_items.org keyed on Code, not Original Label), so the crop labels pass
through as their raw French Preferred Labels ('Maïs', 'Sorgho', ...) — the
same identity-passthrough the Niger helper applies to any unmapped label.
The `u` unit table resolves at the library level (../../../_/), so units are
harmonized through it where present.  perennial / planting_month are not
recorded by EHCVM s16c, so they are omitted (parity with Niger).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet, format_id


def i(value):
    """Composite household id from (grappe, menage), matching Togo's
    sample().  Inlined VERBATIM from togo.i() / 2018/_/livestock.py: grappe
    + '0' separator + zero-padded (2-digit) menage.  NO 'E_' prefix."""
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def _maybe_map(tablename):
    """Load a {Original Label -> Preferred Label} dict, or return {} (identity
    passthrough) if the table is absent for Togo.  harmonize_food is absent
    here, so its labels stay raw; `u` resolves at the library level."""
    try:
        return tools.get_categorical_mapping(
            tablename=tablename, idxvars='Original Label',
            **{'Preferred Label': 'Preferred Label'})
    except (FileNotFoundError, KeyError):
        return {}


def _label_map(series, label_map):
    """Map a string-label Series through label_map; unmapped labels pass
    through unchanged (kept visible)."""
    lab = series.astype('string')
    return lab.map(lambda x: label_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _finish_crop_production(df, t):
    """Common tail (inlined from niger._finish_crop_production): coerce
    numeric columns, build the (t, i, plot, crop, u) index, drop rows with no
    crop recorded."""
    for col in ['Quantity', 'Quantity_sold', 'Value_sold']:
        df[col] = pd.to_numeric(df.get(col), errors='coerce').astype('Float64')
    if 'harvest_month' not in df.columns:
        df['harvest_month'] = pd.NA
    df['harvest_month'] = pd.to_numeric(df['harvest_month'], errors='coerce').astype('Float64')
    if 'intercropped' not in df.columns:
        df['intercropped'] = pd.NA
    df['intercropped'] = df['intercropped'].astype('boolean')
    df['t'] = t
    df['crop'] = df['crop'].astype('string')
    df['u'] = df['u'].astype('string')
    df = df[df['crop'].notna()]
    keep = ['t', 'i', 'plot', 'crop', 'u', 'Quantity',
            'Quantity_sold', 'Value_sold', 'harvest_month', 'intercropped']
    df = df[[c for c in keep if c in df.columns]]
    df = df.set_index(['t', 'i', 'plot', 'crop', 'u'])
    return df


# convert_categoricals=True so crop / unit value labels arrive as the strings
# the harmonize_food / u tables key on; the codes view is used for the
# plot/parcel sequence numbers (so format_id sees integers, not labels).
src = get_dataframe('../Data1/s16c_me_tgo2018.dta', convert_categoricals=True)
src_codes = get_dataframe('../Data1/s16c_me_tgo2018.dta', convert_categoricals=False)

crop_map = _maybe_map('harmonize_food')
unit_map = _maybe_map('u')

hh = src.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                     index=['grappe', 'menage'])), axis=1)
field = src_codes['s16cq02'].apply(format_id)
parcel = src_codes['s16cq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

# intercrop: s16cq07 == 'Association de cultures' -> True, 'Pure' -> False
intercropped = src['s16cq07'].astype('string').map(
    {'Association de cultures': True, 'Pure': False})

df = pd.DataFrame({
    'i':             hh.values,
    'plot':          plot.values,
    'crop':          _label_map(src['s16cq04'], crop_map).values,
    'u':             _label_map(src['s16cq12b'], unit_map).values,
    'Quantity':      src['s16cq12a'].values,
    'Quantity_sold': src['s16cq16a'].values,
    'Value_sold':    src['s16cq17'].values,
    'intercropped':  intercropped.values,
})

df = _finish_crop_production(df, '2018')

assert len(df) > 0, 'crop_production 2018 produced no rows'
to_parquet(df, 'crop_production.parquet')
