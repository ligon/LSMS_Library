"""Build crop_production for Senegal EHCVM 2018-19 (GAP 1, item-level).

Cloned from the Niger EHCVM template (Niger/2018-19/_/crop_production.py).
Single source file: s16c_me_sen2018.dta (agriculture crop/harvest module).
One row per reported (field, parcel, crop) harvest record.  Harvest qty +
unit (s16cq12a / s16cq12b), sold qty (s16cq16a), sale value (s16cq17), and
the intercrop flag (s16cq07) are ALL recorded at this plot-crop grain in
2018-19, so no cross-file join is needed.

Index = (t, i, plot, crop, u); plot = "{s16cq02}_{s16cq03}" aligns with
plot_features' plot_id.  REPORTED values only — harvest_kg / yield /
main_crop / value-shares are transformations, never columns here.

This script is SELF-CONTAINED — it inlines the Senegal household-id
formatter (matching sample() / livestock.py: grappe + '0' + zero-padded
menage, NO 'E_' prefix), the crop / unit label mappers, and the finishing
tail rather than importing them, so it has no cross-country / wave-module
import coupling.

`crop` joins food via harmonize_food Preferred Labels (so a crop that is
also a consumed food shares food_acquired's Preferred Label); `u` via the u
table.  Both mappers PASS UNMAPPED LABELS THROUGH unchanged (kept visible,
flagged by the sanity checker) — exactly the Niger template behavior;
Senegal's harmonize_food / u tables cover only a subset of the s16c crop /
unit labels, so the rest surface in raw French.
"""
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet, format_id


def i(value):
    """Senegal EHCVM household id from (grappe, menage): grappe + '0' +
    zero-padded (2-digit) menage.  Matches sample().i / mapping.i /
    livestock.py (NO 'E_' prefix).  Built positionally (``.iloc``)."""
    g = tools.format_id(value.iloc[0])
    m = tools.format_id(value.iloc[1], zeropadding=2)
    if g is None or m is None:
        return None
    return g + '0' + m


def _crop_maps():
    crop_map = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    unit_map = tools.get_categorical_mapping(
        tablename='u', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    return crop_map, unit_map


def _crop_labels(source_labels, crop_map):
    """Map a crop column (string labels from convert_categoricals=True)
    through ``harmonize_food`` (keyed on Original Label).  Unmapped labels
    pass through unchanged (kept visible, flagged by the sanity checker)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: crop_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _unit_labels(unit_series, unit_map):
    """Map a harvest-unit column (string labels) through the ``u`` table.
    Unmapped labels pass through unchanged."""
    u = unit_series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _finish_crop_production(df, t):
    """Common tail: tag t, build the (t, i, plot, crop, u) index, coerce
    numeric columns, and guarantee the full schema column set.  Drops
    placeholder rows with no crop recorded."""
    for col in ['Quantity', 'Quantity_sold', 'Value_sold']:
        df[col] = pd.to_numeric(df.get(col), errors='coerce').astype('Float64')
    # harvest_month: not recorded in the EHCVM s16c module; NaN here (carried
    # for cross-wave schema parity).
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
# that harmonize_food / the u table key on; the *_codes view keeps the integer
# field / parcel ids that format_id zero-pads.
src = get_dataframe('../Data/s16c_me_sen2018.dta', convert_categoricals=True)
src_codes = get_dataframe('../Data/s16c_me_sen2018.dta', convert_categoricals=False)

crop_map, unit_map = _crop_maps()

hh = src_codes.apply(lambda r: i(pd.Series([r['grappe'], r['menage']])), axis=1)
field = src_codes['s16cq02'].apply(format_id)
parcel = src_codes['s16cq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

# intercrop: s16cq07 == 'Association de cultures' -> True, 'Pure' -> False
intercropped = src['s16cq07'].astype('string').map(
    {'Association de cultures': True, 'Pure': False})

df = pd.DataFrame({
    'i':             hh.values,
    'plot':          plot.values,
    'crop':          _crop_labels(src['s16cq04'], crop_map).values,
    'u':             _unit_labels(src['s16cq12b'], unit_map).values,
    'Quantity':      src_codes['s16cq12a'].values,
    'Quantity_sold': src_codes['s16cq16a'].values,
    'Value_sold':    src_codes['s16cq17'].values,
    'intercropped':  intercropped.values,
})

df = _finish_crop_production(df, '2018-19')

assert len(df) > 0, 'crop_production 2018-19 produced no rows'
to_parquet(df, 'crop_production.parquet')
