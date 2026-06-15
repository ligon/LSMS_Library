"""Build plot_inputs for Guinea-Bissau EHCVM 2018-19 (GAP 2, item-level).

Cloned from Niger/2018-19/_/plot_inputs.py but SELF-CONTAINED: the label
maps, the i() builder, the CROP_NA sentinel, and the
(t, i, input, crop, u) finish tail are inlined here so this script does
not import the niger module.

Single source file: s16b_me_gnb2018.dta — the household agricultural-input
roster, one row per input-type the household was asked about.  Columns:
  s16bq01   input type (organic / inorganic fert / phyto / seeds-by-crop)
  s16bq02   USED this input? (1=Sim / 2=Não) — application gate
  s16bq03a / s16bq03b   quantity used + native unit
  s16bq05   purchased? (Sim / Não)
  s16bq07a  quantity purchased (native unit)

We keep s16bq02==1 (applied) rows.  The EHCVM roster has NO crop column —
the seed's crop is embedded in the input-type label ('Sementes de arroz'),
resolved via harmonize_seed_crop where that table exists.  Non-seed input
rows carry the CROP_NA sentinel.  i = EHCVM composite via the Guinea-Bissau
helper (grappe + '0' + zero-padded menage, NO 'E_' prefix).  No plot
column; grain is (t, i, input, crop, u).

LABEL NOTE (silent-failure gotcha): Guinea-Bissau's categorical_mapping.org
has NEITHER harmonize_input NOR harmonize_seed_crop (the input value labels
are Portuguese: 'Sementes de arroz', 'Adubos inorgânicos - ureia', ...).  A
bare get_categorical_mapping on a missing table silently returns the FIRST
org table (`u`), which would mis-map inputs to unit labels.  We guard with
_table_exists(): if the named table is absent the map is empty, so input
labels pass through unchanged (raw Portuguese, visible / flagged) and seed
crops resolve to the CROP_NA sentinel.  The `u` table DOES exist for
Guinea-Bissau, so units harmonize normally.  Purchased labels are
Portuguese ('Sim' / 'Não').
"""
import os

import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet


# Sentinel for the `crop` index level on input rows that are NOT
# crop-specific.  A non-null token keeps all reported rows through the
# framework's NaN-key de-dup collapse (groupby().first() drops NaN keys).
CROP_NA = '(not crop-specific)'

_ORG = os.path.join(os.path.dirname(__file__), '..', '..', '_',
                    'categorical_mapping.org')


def _table_exists(tablename):
    """True iff categorical_mapping.org declares a ``#+name:`` /
    ``#+NAME:`` header for ``tablename``.  Guards against
    get_categorical_mapping's silent fall-through to the first org table."""
    target = ('#+name: ' + tablename).lower()
    try:
        with open(_ORG, encoding='utf-8') as fh:
            for line in fh:
                if line.strip().lower() == target:
                    return True
    except OSError:
        return False
    return False


def i(value):
    """Guinea-Bissau EHCVM household id from (grappe, menage).  No 'E_'
    prefix (single EHCVM wave).  Inlined for self-containment."""
    if isinstance(value, pd.Series):
        grappe = tools.format_id(value.iloc[0])
        menage = tools.format_id(value.iloc[1], zeropadding=2)
        if grappe is None or menage is None:
            return None
        return grappe + '0' + menage
    return tools.format_id(value)


def _safe_map(tablename):
    """Load a {Original Label -> Preferred Label} dict only if the table
    really exists; otherwise return {} (identity passthrough)."""
    if not _table_exists(tablename):
        return {}
    return tools.get_categorical_mapping(
        tablename=tablename, idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})


def _input_labels(source_labels, input_map):
    """Map an input-type column through input_map.  Unmapped labels pass
    through unchanged (kept visible, flagged by the sanity checker)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: input_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _seed_crop_labels(source_labels, seed_crop_map):
    """Resolve the crop embedded in a seed-type label via seed_crop_map.
    Returns NA for any label not in the table (non-seed rows / unresolved
    seed crops); _finish fills those with the CROP_NA sentinel."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: seed_crop_map.get(x, pd.NA) if pd.notna(x) else pd.NA).astype('string')


def _unit_labels(unit_series, unit_map):
    """Map a unit column through the u table.  Unmapped labels pass
    through unchanged."""
    u = unit_series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _finish_plot_inputs(df, t):
    """Coerce numeric columns, guarantee the schema column set, fill the
    crop / u index levels with non-null tokens where absent, drop rows with
    no input identity, build the (t, i, input, crop, u) index."""
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


src = get_dataframe('../Data/s16b_me_gnb2018.dta', convert_categoricals=True)
srcn = get_dataframe('../Data/s16b_me_gnb2018.dta', convert_categoricals=False)

input_map = _safe_map('harmonize_input')        # absent in GNB -> passthrough
unit_map = _safe_map('u')                        # present in GNB
seed_crop_map = _safe_map('harmonize_seed_crop')  # absent in GNB -> sentinel

# Keep only inputs the household actually applied (s16bq02 == 1 = Sim).
applied = srcn['s16bq02'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = src.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                     index=['grappe', 'menage'])), axis=1)

# purchased: 'Sim' / 'Não' (loaded as labels with convert_categoricals)
purchased = src['s16bq05'].astype('string').map({'Sim': True, 'Não': False})

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
