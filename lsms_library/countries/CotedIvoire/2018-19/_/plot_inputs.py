"""Build plot_inputs for CotedIvoire EHCVM 2018-19 (GAP 2, item-level).

Single source file: ../Data/Menage/s16b_me_CIV2018.dta — the household
agricultural-input roster, one row per input-type the household used.
Columns (verified identical to Niger's EHCVM 2018-19 s16b):
  s16bq01   input type (organic / inorganic fert / phyto / seeds-by-crop)
  s16bq02   USED this input? (1=Oui) — application gate (all 1 here)
  s16bq03a / s16bq03b   quantity used + native unit
  s16bq05   purchased? (Oui / Non)
  s16bq07a  quantity purchased (native unit)

We keep s16bq02==1 (applied) rows.  The EHCVM roster has NO crop column —
the seed's crop is embedded in the input-type label ('Semences de riz'),
resolved to the crop label via harmonize_seed_crop (cloned from Niger into
CotedIvoire's categorical_mapping.org).  Non-seed input rows carry the
CROP_NA sentinel.  Grain (t, i, input, crop, u).

i is CotedIvoire's EHCVM composite id (grappe + zero-padded(3) menage; NO
'E_' prefix — CotedIvoire predates the standard EHCVM list), inlined here so
this wave script is self-contained.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id, get_categorical_mapping


# Sentinel for the `crop` index level on non-crop-specific input rows.  A
# partly-null index level would be silently dropped by the framework's
# canonical-index de-dup (groupby().first() drops NaN keys); a non-null token
# keeps every reported row.  Mirrors niger.py:CROP_NA.
CROP_NA = '(not crop-specific)'


def _i(grappe, menage):
    g = format_id(grappe)
    m = format_id(menage, zeropadding=3)
    if g is None or m is None:
        return None
    return g + m


def _label_map(tablename):
    """Load a {Original Label -> Preferred Label} dict, returning {} if the
    table is absent (pass-through)."""
    try:
        return get_categorical_mapping(
            tablename=tablename, idxvars='Original Label',
            **{'Preferred Label': 'Preferred Label'})
    except (FileNotFoundError, KeyError):
        return {}


def _input_labels(source_labels, input_map):
    """Map an input-type column through harmonize_input.  Unmapped labels
    pass through unchanged."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: input_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _seed_crop_labels(source_labels, seed_crop_map):
    """Resolve the crop embedded in a seed-type label ('Semences de riz' ->
    'Riz Paddy') via harmonize_seed_crop.  Returns NA for any non-seed label
    (not in the seed-crop table)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: seed_crop_map.get(x, pd.NA) if pd.notna(x) else pd.NA).astype('string')


def _finish_plot_inputs(df, t):
    """Common tail: coerce numeric columns, guarantee the full schema set,
    fill the crop / u index levels with sentinels so no row is lost to the
    framework's NaN-key collapse, build the (t, i, input, crop, u) index.
    Mirrors niger.py:_finish_plot_inputs."""
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
    df = df[df['input'].notna() & df['i'].notna()]
    keep = ['t', 'i', 'input', 'crop', 'u',
            'Quantity', 'Purchased', 'Quantity_purchased']
    df = df[[c for c in keep if c in df.columns]]
    df = df.set_index(['t', 'i', 'input', 'crop', 'u'])
    return df


src = get_dataframe('../Data/Menage/s16b_me_CIV2018.dta', convert_categoricals=True)
srcn = get_dataframe('../Data/Menage/s16b_me_CIV2018.dta', convert_categoricals=False)

input_map = _label_map('harmonize_input')
unit_map = _label_map('u')
seed_crop_map = _label_map('harmonize_seed_crop')

# Keep only inputs the household actually applied (s16bq02 == 1 = Oui).
applied = srcn['s16bq02'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = srcn.apply(lambda r: _i(r['grappe'], r['menage']), axis=1)

# purchased: s16bq05 Oui/Non (labels with convert_categoricals)
purchased = src['s16bq05'].astype('string').map({'Oui': True, 'Non': False})

df = pd.DataFrame({
    'i':                  hh.values,
    'input':              _input_labels(src['s16bq01'], input_map).values,
    'crop':               _seed_crop_labels(src['s16bq01'], seed_crop_map).values,
    'u':                  _input_labels(src['s16bq03b'], unit_map).values,
    'Quantity':           pd.to_numeric(srcn['s16bq03a'], errors='coerce').values,
    'Purchased':          purchased.values,
    'Quantity_purchased': pd.to_numeric(srcn['s16bq07a'], errors='coerce').values,
})

df = _finish_plot_inputs(df, '2018-19')

assert len(df) > 0, 'plot_inputs 2018-19 produced no rows'

# GH #323 -- ENFORCE the declared grain.  `crop` is an index level and
# harmonize_input collapses every seed label onto the single input 'Seed', so a
# NON-INJECTIVE harmonize_seed_crop makes two DISTINCT reported seed line-items
# land on one index tuple, whereupon the framework's groupby().first() silently
# discards one.  That is what happened until 2026-07-12: four labels shared an
# 'Autre crop' catch-all, and 9 rows -- real reported quantities, e.g. 350 kg of
# market-purchased "Semences d'autres céréales" in grappe 744 / menage 12 --
# vanished with no warning.  The CROP_NA sentinel above shows the author knew
# the framework drops rows on a duplicated index and guarded the NaN case; the
# catch-all bucket has the identical effect and was missed.
#
# A comment is not a guard.  This assertion is: if any future label (or a
# re-widened bucket) re-introduces a collision, the BUILD FAILS here, loudly,
# instead of quietly shipping short.
dups = df.index[df.index.duplicated()]
assert dups.empty, (
    f"plot_inputs 2018-19: {len(dups)} duplicate tuple(s) on the declared index "
    f"(t, i, input, crop, u) -- e.g. {list(dups[:3])}.  Two distinct reported "
    f"input line-items are colliding on one key; the framework would silently "
    f"drop one via groupby().first().  Most likely a non-injective "
    f"harmonize_seed_crop in _/categorical_mapping.org (see the note there). "
    f"Split the offending Preferred Label rather than letting a row disappear."
)

to_parquet(df, 'plot_inputs.parquet')
