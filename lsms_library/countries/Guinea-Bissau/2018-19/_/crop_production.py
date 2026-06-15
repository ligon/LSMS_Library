"""Build crop_production for Guinea-Bissau EHCVM 2018-19 (GAP 1, item-level).

Cloned from Niger/2018-19/_/crop_production.py but SELF-CONTAINED: the
label maps, the i() builder, and the (t, i, plot, crop, u) finish tail are
inlined here so this script does not import the niger module.

Single source file: s16c_me_gnb2018.dta (agriculture crop/harvest module).
One row per reported (field, parcel, crop) harvest record.  Harvest qty +
unit (s16cq12a / s16cq12b), sold qty + unit (s16cq16a / s16cq16b), sale
value (s16cq17), and the intercrop flag (s16cq07) are ALL recorded at this
plot-crop grain in 2018-19, so no cross-file join is needed.

Index = (t, i, plot, crop, u); plot = "{s16cq02}_{s16cq03}" aligns with
plot_features' plot_id.  i = EHCVM composite via the Guinea-Bissau helper
(grappe + '0' + zero-padded menage, NO 'E_' prefix).

LABEL NOTE (silent-failure gotcha): Guinea-Bissau's categorical_mapping.org
has NO harmonize_food table (the EHCVM crop value labels are Portuguese:
'Cajueiro', 'Arroz paddy', 'Mancarra', ...).  A bare get_categorical_mapping
on a missing table silently returns the FIRST org table (here `u`), which
would mis-map crops to unit labels.  We guard with _table_exists(): if the
named table is absent the map is empty and the raw (Portuguese) crop label
passes through unchanged — visible and flagged by the sanity checker, never
silently corrupted.  The `u` table DOES exist for Guinea-Bissau (Portuguese
units: Kg / Saco grande / Bacia / ...), so units harmonize normally.  The
intercrop labels are Portuguese ('Associação de culturas' / 'Pura').
"""
import os

import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet, format_id


_ORG = os.path.join(os.path.dirname(__file__), '..', '..', '_',
                    'categorical_mapping.org')


def _table_exists(tablename):
    """True iff categorical_mapping.org declares a ``#+name:`` /
    ``#+NAME:`` header for ``tablename``.  Guards against
    get_categorical_mapping's silent fall-through to the first org table
    when the requested table is absent."""
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


def _crop_labels(source_labels, crop_map):
    """Map a crop column (string labels) through crop_map.  Unmapped labels
    pass through unchanged (kept visible, flagged by the sanity checker)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: crop_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _unit_labels(unit_series, unit_map):
    """Map a unit column (string labels) through the u table.  Unmapped
    labels pass through unchanged."""
    u = unit_series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _finish_crop_production(df, t):
    """Tag t, build the (t, i, plot, crop, u) index, coerce numeric columns,
    guarantee the schema column set, drop rows with no crop recorded."""
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


# convert_categoricals=True so crop / unit value labels arrive as the
# strings the (Portuguese) label tables key on; codes for plot ids come from
# the convert_categoricals=False frame so format_id sees integers.
src = get_dataframe('../Data/s16c_me_gnb2018.dta', convert_categoricals=True)
src_codes = get_dataframe('../Data/s16c_me_gnb2018.dta', convert_categoricals=False)

crop_map = _safe_map('harmonize_food')   # absent in GNB -> identity passthrough
unit_map = _safe_map('u')                # present in GNB

hh = src.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                     index=['grappe', 'menage'])), axis=1)
field = src_codes['s16cq02'].apply(format_id)
parcel = src_codes['s16cq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

# intercrop: 'Associação de culturas' (code 2) -> True, 'Pura' (code 1) -> False
intercropped = src['s16cq07'].astype('string').map(
    {'Associação de culturas': True, 'Pura': False})

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
