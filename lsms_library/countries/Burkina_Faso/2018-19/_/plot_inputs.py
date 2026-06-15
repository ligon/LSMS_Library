"""Build plot_inputs for Burkina Faso EHCVM 2018-19 (GAP 2, item-level).

Self-contained clone of Niger/2018-19/_/plot_inputs.py (no ``import
niger``): helpers inlined; the input / unit / seed-crop maps read from
Burkina's own categorical_mapping.org (harmonize_input, u,
harmonize_seed_crop — copied from Niger).

Single source file: s16b_me_bfa2018.dta — the household agricultural-input
roster, one row per input-type the household was asked about.  Columns:
  s16bq01   input type (organic / inorganic fert / phyto / seeds-by-crop)
  s16bq02   USED this input? (1=Oui) — application gate (all Oui in 2018)
  s16bq03a / s16bq03b   quantity used + native unit
  s16bq05   purchased? (Oui / Non)
  s16bq07a  quantity purchased (native unit)

ENCODING: loaded with the DEFAULT (utf-8) encoding so the accented French
input / unit labels match the harmonize_input / u / harmonize_seed_crop keys.

The EHCVM roster has NO crop column — the seed's crop is embedded in the
input-type label ('Semences de petit mil'), resolved to the harmonize_food
crop label via harmonize_seed_crop.  Non-seed input rows carry the CROP_NA
sentinel.  i = EHCVM composite via burkina_faso.ehcvm_i (reconciles 100%
with sample()).  No plot column; grain is (t, i, input, crop, u).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import (get_dataframe, to_parquet,
                                       get_categorical_mapping)
from burkina_faso import ehcvm_i


# Sentinel for the `crop` index level on input rows that are NOT
# crop-specific.  A non-null token keeps every reported row through the
# framework's groupby(level=...).first() de-dup (which drops NaN keys).
CROP_NA = '(not crop-specific)'


def _input_maps():
    input_map = get_categorical_mapping(
        tablename='harmonize_input', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    unit_map = get_categorical_mapping(
        tablename='u', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    seed_crop_map = get_categorical_mapping(
        tablename='harmonize_seed_crop', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    return input_map, unit_map, seed_crop_map


def _input_labels(source_labels, input_map):
    lab = source_labels.astype('string')
    return lab.map(lambda x: input_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _seed_crop_labels(source_labels, seed_crop_map):
    """Resolve the crop embedded in a seed-type label ('Semences de petit
    mil' -> 'Mil') via harmonize_seed_crop.  NA for non-seed input rows."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: seed_crop_map.get(x, pd.NA) if pd.notna(x) else pd.NA).astype('string')


def _unit_labels(unit_series, unit_map):
    u = unit_series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _finish_plot_inputs(df, t):
    """Coerce numerics, fill the CROP_NA / 'Manquant' sentinels so the index
    is fully non-null, drop rows with no input identity, build
    (t, i, input, crop, u)."""
    for col in ['Quantity', 'Quantity_purchased']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
    if 'Purchased' not in df.columns:
        df['Purchased'] = pd.NA
    df['Purchased'] = df['Purchased'].astype('boolean')
    df['t'] = t
    df['input'] = df['input'].astype('string')
    if 'crop' not in df.columns:
        df['crop'] = pd.NA
    df['crop'] = df['crop'].astype('string').fillna(CROP_NA)
    df['u'] = df['u'].astype('string').fillna('Manquant')
    df = df[df['input'].notna()]
    keep = ['t', 'i', 'input', 'crop', 'u',
            'Quantity', 'Purchased', 'Quantity_purchased']
    df = df[[c for c in keep if c in df.columns]]
    df = df.set_index(['t', 'i', 'input', 'crop', 'u'])
    return df


src = get_dataframe('../Data/s16b_me_bfa2018.dta', convert_categoricals=True)
srcn = get_dataframe('../Data/s16b_me_bfa2018.dta', convert_categoricals=False)

input_map, unit_map, seed_crop_map = _input_maps()

# Keep only inputs the household actually applied (s16bq02 == 1 = Oui).
applied = srcn['s16bq02'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = srcn.apply(lambda r: ehcvm_i(r['grappe'], r['menage']), axis=1)

# purchased: s16bq05 Oui/Non (labels with convert_categoricals)
purchased = src['s16bq05'].map({'Oui': True, 'Non': False})

df = pd.DataFrame({
    'i':                  hh.values,
    'input':              _input_labels(src['s16bq01'], input_map).values,
    'crop':               _seed_crop_labels(src['s16bq01'], seed_crop_map).values,
    'u':                  _unit_labels(src['s16bq03b'], unit_map).values,
    'Quantity':           pd.to_numeric(srcn['s16bq03a'], errors='coerce').values,
    'Purchased':          purchased.values,
    'Quantity_purchased': pd.to_numeric(srcn['s16bq07a'], errors='coerce').values,
})

df = _finish_plot_inputs(df, '2018-19')

assert len(df) > 0, 'plot_inputs 2018-19 produced no rows'
to_parquet(df, 'plot_inputs.parquet')
