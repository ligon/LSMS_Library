"""Build plot_inputs for Benin EHCVM 2018-19 (GAP 2, item-level).

Single source file: s16b_me_ben2018.dta — the household agricultural-input
roster, one row per input-type the household was asked about.  Columns:
  s16bq01   input type (organic / inorganic fert / phyto / seeds-by-crop)
  s16bq02   USED this input? (Oui / Non) — application gate (all Oui in 2018)
  s16bq03a / s16bq03b   quantity used + native unit
  s16bq05   purchased? (Oui / Non)
  s16bq07a  quantity purchased

We keep s16bq02==1 (applied) rows.  The EHCVM roster has NO crop column — the
seed's crop is embedded in the input-type label ('Semences de petit mil'),
resolved to the canonical crop label via harmonize_seed_crop.  Non-seed input
rows carry crop NaN (filled with the '(not crop-specific)' sentinel in the
tail).  ``i`` is the Benin EHCVM composite id via ``benin.i()`` (100% sample
intersection verified).  No plot column; grain is (t, i, input, crop, u).

This script is SELF-CONTAINED: the label mappers and the schema tail (cloned
from the Niger EHCVM reference) are inlined; only ``benin.i()`` is imported so
the household id cannot drift from ``sample()``.  harmonize_input and
harmonize_seed_crop were copied verbatim from Niger into Benin's
categorical_mapping.org (EHCVM s16b labels are standardized across EHCVM
countries; every Benin s16bq01 label is covered).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from benin import i as benin_i


# Sentinel for the `crop` index level on input rows that are NOT
# crop-specific (every fertilizer / pesticide row, and any seed row whose
# crop could not be resolved).  A non-null token keeps all reported rows and
# makes "no crop dimension" explicit; it is deliberately not a food/crop label
# so it never collides with the harmonize_food `crop` values seed rows carry.
CROP_NA = '(not crop-specific)'


def _label_map(tablename):
    """Load a ``{Original Label -> Preferred Label}`` dict; {} if absent."""
    try:
        return tools.get_categorical_mapping(
            tablename=tablename, idxvars='Original Label',
            **{'Preferred Label': 'Preferred Label'})
    except Exception:
        return {}


def _input_labels(source_labels, input_map):
    """Map an input-type column (string labels) through harmonize_input.
    Unmapped labels pass through unchanged (kept visible, flagged)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: input_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _seed_crop_labels(source_labels, seed_crop_map):
    """Resolve the crop embedded in a seed-type label ('Semences de maïs' ->
    'Maïs en grain') via harmonize_seed_crop.  Returns NA for any label not in
    the seed-crop table (i.e. non-seed input rows)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: seed_crop_map.get(x, pd.NA) if pd.notna(x) else pd.NA).astype('string')


def _unit_labels(unit_series, unit_map):
    """Map a unit column (string labels) through the ``u`` table; passthrough
    for units not in the table."""
    u = unit_series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _finish_plot_inputs(df, t):
    """Common tail: coerce numeric columns, guarantee the full schema column
    set, build the (t, i, input, crop, u) index.  Drops rows with no input
    identity.  The `crop` and `u` index levels are filled with sentinels
    wherever absent so the index is fully non-null and no row is lost to the
    framework's NaN-key duplicate collapse."""
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


src = get_dataframe('../Data/s16b_me_ben2018.dta', convert_categoricals=True)
srcn = get_dataframe('../Data/s16b_me_ben2018.dta', convert_categoricals=False)

input_map = _label_map('harmonize_input')
seed_crop_map = _label_map('harmonize_seed_crop')
unit_map = _label_map('u')

# Keep only inputs the household actually applied (s16bq02 == 1 = Oui).
applied = srcn['s16bq02'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = srcn.apply(lambda r: benin_i(pd.Series([r['grappe'], r['menage']])),
                axis=1)

# purchased: s16bq05 Oui/Non (loaded as labels with convert_categoricals)
purchased = src['s16bq05'].astype('string').map({'Oui': True, 'Non': False})

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
