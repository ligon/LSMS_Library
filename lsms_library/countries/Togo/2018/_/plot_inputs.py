"""Build plot_inputs for Togo EHCVM 2018 (item-level), cloned from the Niger
2018-19 EHCVM template.  SELF-CONTAINED: the map/finish helpers are inlined
here, so this script does NOT import togo or niger.

Single source file: ../Data1/s16b_me_tgo2018.dta — the household
agricultural-input roster, one row per input-type the household was asked
about.  Columns:
  s16bq01   input type (organic / inorganic fert / phyto / seeds-by-crop)
  s16bq02   USED this input? (1=Oui / 2=Non) — application gate
  s16bq03a / s16bq03b   quantity used + native unit
  s16bq05   purchased? (Oui / Non)
  s16bq07a  quantity purchased (native unit)

*** TOGO PATH / WAVE QUIRKS: source in 2018/Data1/; wave dir 2018; code
suffix tgo2018; t='2018'; standardized module (NOT Togo_survey2018_*). ***

We keep s16bq02==1 (applied) rows.  The EHCVM roster has NO crop column —
the seed's crop is embedded in the input-type label ('Semences de petit
mil'), resolved to a crop label via harmonize_seed_crop (copied from Niger
into Togo's categorical_mapping.org).  Non-seed input rows carry the CROP_NA
sentinel.  `i` is Togo's composite household id (grappe + '0' + zero-padded
menage; NO 'E_' prefix), matching sample() (t='2018').  No plot column;
grain is (t, i, input, crop, u).

MAPPING: harmonize_input + harmonize_seed_crop were copied verbatim from
Niger into Togo/_/categorical_mapping.org (the EHCVM s16b labels are
identical across EHCVM countries; every Togo input label is covered).  `u`
resolves at the library level.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet


# Sentinel for the `crop` index level on input rows that are NOT
# crop-specific (every fertilizer / pesticide row, and any seed row whose
# crop could not be resolved).  A non-null token keeps every reported row
# through the framework's NaN-key duplicate collapse (groupby().first()
# drops NaN grouping keys).  Inlined verbatim from niger.CROP_NA.
CROP_NA = '(not crop-specific)'
# Missing-unit fill (a reported input may lack a recorded unit, e.g. a count
# of bags); keeps the `u` index level non-null.  Matches niger's 'Manquant'.
UNIT_NA = 'Manquant'


def i(value):
    """Composite household id from (grappe, menage), matching Togo's
    sample().  Inlined VERBATIM from togo.i() / 2018/_/livestock.py: grappe
    + '0' separator + zero-padded (2-digit) menage.  NO 'E_' prefix."""
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def _maybe_map(tablename):
    """Load a {Original Label -> Preferred Label} dict, or {} (identity) if
    the table is absent for Togo."""
    try:
        return tools.get_categorical_mapping(
            tablename=tablename, idxvars='Original Label',
            **{'Preferred Label': 'Preferred Label'})
    except (FileNotFoundError, KeyError):
        return {}


def _input_labels(series, input_map):
    """Map an input-type column through harmonize_input; unmapped labels pass
    through unchanged (kept visible)."""
    lab = series.astype('string')
    return lab.map(lambda x: input_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _seed_crop_labels(series, seed_crop_map):
    """Resolve the crop embedded in a seed-type label via harmonize_seed_crop.
    Returns NA for any label not in the seed-crop table (non-seed rows)."""
    lab = series.astype('string')
    return lab.map(lambda x: seed_crop_map.get(x, pd.NA) if pd.notna(x) else pd.NA).astype('string')


def _unit_labels(series, unit_map):
    """Map a native-unit column through the `u` table; unmapped labels pass
    through unchanged."""
    u = series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _finish_plot_inputs(df, t):
    """Common tail (inlined from niger._finish_plot_inputs): coerce numeric
    columns, fill the CROP_NA / UNIT_NA sentinels so every index level is
    non-null, drop rows with no input identity, build the
    (t, i, input, crop, u) index."""
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
    df['u'] = df['u'].astype('string').fillna(UNIT_NA)
    df = df[df['input'].notna()]
    keep = ['t', 'i', 'input', 'crop', 'u',
            'Quantity', 'Purchased', 'Quantity_purchased']
    df = df[[c for c in keep if c in df.columns]]
    df = df.set_index(['t', 'i', 'input', 'crop', 'u'])
    return df


src = get_dataframe('../Data1/s16b_me_tgo2018.dta', convert_categoricals=True)
srcn = get_dataframe('../Data1/s16b_me_tgo2018.dta', convert_categoricals=False)

input_map = _maybe_map('harmonize_input')
unit_map = _maybe_map('u')
seed_crop_map = _maybe_map('harmonize_seed_crop')

# Keep only inputs the household actually applied (s16bq02 == 1 = Oui).
applied = srcn['s16bq02'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = src.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                     index=['grappe', 'menage'])), axis=1)

# purchased: s16bq05 Oui/Non (labels via convert_categoricals)
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

df = _finish_plot_inputs(df, '2018')

assert len(df) > 0, 'plot_inputs 2018 produced no rows'

# GH #323 -- ENFORCE the declared grain.  `crop` is an index level and
# harmonize_input collapses every seed label onto the single input 'Seed', so a
# NON-INJECTIVE harmonize_seed_crop makes two DISTINCT reported seed line-items
# land on one index tuple, whereupon the framework's groupby().first() silently
# discards one.  That is what happened until 2026-07-13: four labels shared an
# 'Autre crop' catch-all and 165 rows -- real reported quantities, e.g. the
# 16 Charrette of market-purchased "Plants/boutures de tubercules" that grappe
# 101 / menage 9 reported alongside 3 Charrette of own-production "Autres
# semences" -- vanished with no warning.  The CROP_NA / UNIT_NA sentinels above
# show the author knew the framework drops rows on a duplicated index and
# guarded the NaN case; the catch-all bucket has the identical effect and was
# missed.
#
# A comment is not a guard.  This assertion is: if any future label (or a
# re-widened bucket) re-introduces a collision, the BUILD FAILS here, loudly,
# instead of quietly shipping short.
dups = df.index[df.index.duplicated()]
assert dups.empty, (
    f"plot_inputs 2018: {len(dups)} duplicate tuple(s) on the declared index "
    f"(t, i, input, crop, u) -- e.g. {list(dups[:3])}.  Two distinct reported "
    f"input line-items are colliding on one key; the framework would silently "
    f"drop one via groupby().first().  Most likely a non-injective "
    f"harmonize_seed_crop in _/categorical_mapping.org (see the note there). "
    f"Split the offending Preferred Label rather than letting a row disappear."
)

to_parquet(df, 'plot_inputs.parquet')
