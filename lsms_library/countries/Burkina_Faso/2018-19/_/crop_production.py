"""Build crop_production for Burkina Faso EHCVM 2018-19 (GAP 1, item-level).

Self-contained clone of Niger/2018-19/_/crop_production.py (no
``import niger``): the helper logic is inlined and the crop / unit maps are
read from Burkina's own categorical_mapping.org (harmonize_food + u).

Single source file: s16c_me_bfa2018.dta (agriculture crop/harvest module).
One row per reported (field, parcel, crop) harvest record.  Harvest qty +
unit (s16cq12a / s16cq12b), sold qty (s16cq16a), sale value (s16cq17), and
the intercrop flag (s16cq07) are recorded at this plot-crop grain.

ENCODING: loaded with the DEFAULT (utf-8) encoding so the French crop / unit
labels keep their accents and match the harmonize_food / u table keys.

Index = (t, i, plot, crop, u); plot = "{s16cq02}_{s16cq03}" aligns with
plot_features' plot_id.  i = EHCVM composite via burkina_faso.ehcvm_i
(reconciles 100% with sample()).  Unmapped crop / unit labels pass through
unchanged (kept visible; the GAP-1 reported-only rule).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import (get_dataframe, to_parquet, format_id,
                                       get_categorical_mapping)
from burkina_faso import ehcvm_i


def _crop_maps():
    crop_map = get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    unit_map = get_categorical_mapping(
        tablename='u', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    return crop_map, unit_map


def _crop_labels(source_labels, crop_map):
    """Map a crop column (string labels) through harmonize_food.  Unmapped
    labels pass through unchanged."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: crop_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _unit_labels(unit_series, unit_map):
    """Map a unit column (string labels) through the u table.  Unmapped
    labels pass through unchanged."""
    u = unit_series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _finish_crop_production(df, t):
    """Tag t, build the (t, i, plot, crop, u) index, coerce numerics, drop
    rows with no crop recorded."""
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


# convert_categoricals=True (default utf-8 encoding) so crop / unit value
# labels arrive as the accented strings harmonize_food / the u table key on.
src = get_dataframe('../Data/s16c_me_bfa2018.dta', convert_categoricals=True)
src_codes = get_dataframe('../Data/s16c_me_bfa2018.dta', convert_categoricals=False)

crop_map, unit_map = _crop_maps()

hh = src_codes.apply(lambda r: ehcvm_i(r['grappe'], r['menage']), axis=1)
field = src_codes['s16cq02'].apply(format_id)
parcel = src_codes['s16cq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

# intercrop: s16cq07 'Association de cultures' -> True, 'Pure' -> False
intercropped = src['s16cq07'].map({'Association de cultures': True, 'Pure': False})

df = pd.DataFrame({
    'i':             hh.values,
    'plot':          plot.values,
    'crop':          _crop_labels(src['s16cq04'], crop_map).values,
    'u':             _unit_labels(src['s16cq12b'], unit_map).values,
    'Quantity':      src['s16cq12a'].values,
    'Quantity_sold': src['s16cq16a'].values,
    'Value_sold':    src['s16cq17'].values,
    'intercropped':  intercropped.values,
})

df = _finish_crop_production(df, '2018-19')

assert len(df) > 0, 'crop_production 2018-19 produced no rows'
to_parquet(df, 'crop_production.parquet')
