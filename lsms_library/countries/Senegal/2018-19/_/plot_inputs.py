"""Build plot_inputs for Senegal EHCVM 2018-19 (GAP 2, item-level).

Cloned from the Niger EHCVM template (Niger/2018-19/_/plot_inputs.py).
Single source file: s16b_me_sen2018.dta — the household agricultural-input
roster, one row per input-type the household was asked about.  Columns:
  s16bq01   input type (organic / inorganic fert / phyto / seeds-by-crop)
  s16bq02   USED this input? (1=Oui) — application gate (all 1 in 2018)
  s16bq03a / s16bq03b   quantity used + native unit
  s16bq05   purchased? (Oui / Non)
  s16bq07a  quantity purchased (native unit)

We keep s16bq02==1 (applied) rows.  The EHCVM roster has NO crop column —
the seed's crop is embedded in the input-type label ('Semences de petit
mil'), resolved to the harmonize_food crop label via harmonize_seed_crop.
Non-seed input rows carry the CROP_NA sentinel.  Grain (t, i, input, crop,
u); no plot column (EHCVM records inputs at the household x input grain).

This script is SELF-CONTAINED — it inlines the Senegal household-id
formatter (matching sample() / livestock.py: grappe + '0' + zero-padded
menage, NO 'E_' prefix), the input / seed-crop / unit label mappers, and
the finishing tail.  The harmonize_input / harmonize_seed_crop tables were
copied from Niger into Senegal's categorical_mapping.org (with the
Senegal-specific 'Semences d'arachide' -> Arachide seed slot added).
"""
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet


# See _finish_plot_inputs: a non-null token keeps non-crop-specific rows from
# being silently dropped by the framework's NaN-key duplicate collapse.
CROP_NA = '(not crop-specific)'


def i(value):
    """Senegal EHCVM household id from (grappe, menage): grappe + '0' +
    zero-padded (2-digit) menage.  Matches sample() / livestock.py (NO
    'E_' prefix).  Built positionally (``.iloc``)."""
    g = tools.format_id(value.iloc[0])
    m = tools.format_id(value.iloc[1], zeropadding=2)
    if g is None or m is None:
        return None
    return g + '0' + m


def _input_maps():
    """Load the harmonize_input (input-type) and u (unit) string->label maps,
    plus the harmonize_seed_crop map (EHCVM seed-label -> crop).  Keyed on
    Original Label."""
    input_map = tools.get_categorical_mapping(
        tablename='harmonize_input', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    unit_map = tools.get_categorical_mapping(
        tablename='u', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    seed_crop_map = tools.get_categorical_mapping(
        tablename='harmonize_seed_crop', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    return input_map, unit_map, seed_crop_map


def _input_labels(source_labels, input_map):
    """Map an input-type column through harmonize_input.  Unmapped labels
    pass through unchanged (kept visible, flagged by the sanity checker)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: input_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _unit_labels(unit_series, unit_map):
    """Map a native-unit column through the ``u`` table.  Unmapped labels
    pass through unchanged."""
    u = unit_series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _seed_crop_labels(source_labels, seed_crop_map):
    """Resolve the crop embedded in a seed-type label ('Semences de petit
    mil' -> 'Mil') via harmonize_seed_crop.  Returns NA for any label not in
    the seed-crop table (i.e. non-seed input rows)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: seed_crop_map.get(x, pd.NA) if pd.notna(x) else pd.NA).astype('string')


def _finish_plot_inputs(df, t):
    """Common tail: coerce numeric columns, guarantee the full schema column
    set, build the (t, i, input, crop, u) index.  Drops rows with no input
    identity.  The `crop` index level is filled with the CROP_NA sentinel
    wherever no crop applies; the native unit `u` with the 'Manquant' label —
    so neither index level is null and no row is lost to the framework's
    NaN-key duplicate collapse."""
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


src = get_dataframe('../Data/s16b_me_sen2018.dta', convert_categoricals=True)
srcn = get_dataframe('../Data/s16b_me_sen2018.dta', convert_categoricals=False)

input_map, unit_map, seed_crop_map = _input_maps()

# Keep only inputs the household actually applied (s16bq02 == 1 = Oui).
applied = srcn['s16bq02'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = srcn.apply(lambda r: i(pd.Series([r['grappe'], r['menage']])), axis=1)

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
