"""Build crop_production for Benin EHCVM 2018-19 (GAP 1, item-level).

Single source file: s16c_me_ben2018.dta (agriculture crop/harvest module).
One row per reported (field, parcel, crop) harvest record.  Harvest qty +
unit (s16cq12a / s16cq12b), sold qty + unit (s16cq16a / s16cq16b), sale
value (s16cq17), and the intercrop flag (s16cq07) are ALL recorded at this
plot-crop grain in 2018-19, so no cross-file join is needed.

Index = (t, i, plot_id, crop, u, condition); plot_id = "{s16cq02}_{s16cq03}" aligns with
plot_features' plot_id.  `i` is the Benin EHCVM composite household id built
with ``benin.i()`` from a (grappe, menage) Series, so it matches ``sample().i``
natively (100% intersection verified).

This script is SELF-CONTAINED: the generic crop-label / unit-label mapping and
the schema tail (cloned from the Niger EHCVM reference) are inlined below; only
the load-bearing ``i()`` household-id constructor is imported from Benin's own
``benin`` module so it cannot drift from ``sample()``.

CROP LABEL NOTE: Niger resolves the crop label through ``harmonize_food``
(keyed on Original Label).  Benin has no ``harmonize_food`` table, so the
mapping is loaded if present and otherwise the raw French crop label passes
through unchanged (Niger's ``_crop_labels`` already passes unmapped labels
through verbatim) — the native label IS the item identity.  ``u`` likewise
maps through Benin's ``u`` table with passthrough for the harvest-specific
units (Sac (100 Kg), Bassine, ...) not in that consumption-unit table.

``condition`` is the harvest's own "Etat du produit" (s16cq12c, GH #1436), read
by code and folded from five answers to three tokens; the column
``condition_native`` keeps the answer as recorded.
"""
import sys

sys.path.append('../../_/')
import pandas as pd
from lsms_library.ehcvm import crop_condition_codes, crop_condition_labels

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from benin import i as benin_i


def _label_map(tablename):
    """Load a ``{Original Label -> Preferred Label}`` dict from
    ``categorical_mapping.org``.  Returns {} if the table is absent so the
    caller's passthrough leaves labels unchanged."""
    try:
        return tools.get_categorical_mapping(
            tablename=tablename, idxvars='Original Label',
            **{'Preferred Label': 'Preferred Label'})
    except (FileNotFoundError, KeyError):   # absent only, as CotedIvoire/Togo (GH #1036 r1)
        return {}


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


def _crop_conditions(codes):
    """Map raw "Etat du produit" codes (``convert_categoricals=False``) through
    ``harmonize_crop_condition_ehcvm1`` (GH #1436).  The table is read strictly:
    a missing table must not serve every row as the sentinel.  A blank or
    off-scheme code becomes ``unknown_condition``, a real value and never NA,
    because a null index key is deleted by the next ``groupby`` (GH #323 3b)."""
    table = tools.get_categorical_mapping(
        tablename='harmonize_crop_condition_ehcvm1', idxvars='Code',
        **{'Preferred Label': 'Preferred Label'})
    table = {int(code): token for code, token in table.items()}
    assert sorted(table) == [1, 2, 3, 4, 5], f'harmonize_crop_condition_ehcvm1: {table}'
    return crop_condition_codes(pd.to_numeric(codes, errors='coerce'), table).fillna('unknown_condition')


def _crop_condition_labels(labels):
    """The state as the survey recorded it, for ``condition_native``: *labels* is
    the column read with ``convert_categoricals=True``.  A blank stays NA, never a
    sentinel, because a column has no NaN-key hazard (ruling @ligon 2026-11-04,
    GH #1436)."""
    return crop_condition_labels(labels)


def _finish_crop_production(df, t):
    """Common tail: tag t, build the (t, i, plot_id, crop, u, condition) index, coerce
    numeric columns, and guarantee the full schema column set."""
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
    # Drop placeholder rows with no crop recorded (a parcel listed but no
    # crop grown / reported on the line).
    df = df[df['crop'].notna()]
    keep = ['t', 'i', 'plot_id', 'crop', 'u', 'condition', 'Quantity',
            'Quantity_sold', 'Value_sold', 'harvest_month', 'intercropped',
            'condition_native']
    df = df[[c for c in keep if c in df.columns]]
    df['condition'] = df['condition'].astype('string')
    df['condition_native'] = df['condition_native'].astype('string')
    df = df.set_index(['t', 'i', 'plot_id', 'crop', 'u', 'condition'])
    return df


# convert_categoricals=True so crop / unit value labels arrive as the
# strings harmonize_food / the u table key on.
src = get_dataframe('../Data/s16c_me_ben2018.dta', convert_categoricals=True)
src_codes = get_dataframe('../Data/s16c_me_ben2018.dta', convert_categoricals=False)

crop_map = _label_map('harmonize_food')
unit_map = _label_map('u')

hh = src_codes.apply(lambda r: benin_i(pd.Series([r['grappe'], r['menage']])),
                     axis=1)
field = src_codes['s16cq02'].apply(format_id)
parcel = src_codes['s16cq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

# intercrop: s16cq07 == 'Association de cultures' -> True, 'Pure' -> False
intercropped = src['s16cq07'].astype('string').map(
    {'Association de cultures': True, 'Pure': False})

df = pd.DataFrame({
    'i':             hh.values,
    'plot_id':          plot.values,
    'crop':          _crop_labels(src['s16cq04'], crop_map).values,
    'u':             _unit_labels(src['s16cq12b'], unit_map).values,
    # By code; the fold is ruled, see categorical_mapping.org (GH #1436).
    'condition':     _crop_conditions(src_codes['s16cq12c']).values,
    # What the fold cannot show: the answer as recorded, by label.
    'condition_native': _crop_condition_labels(src['s16cq12c']).values,
    'Quantity':      src['s16cq12a'].values,
    'Quantity_sold': src['s16cq16a'].values,
    'Value_sold':    src['s16cq17'].values,
    'intercropped':  intercropped.values,
})

df = _finish_crop_production(df, '2018-19')

assert len(df) > 0, 'crop_production 2018-19 produced no rows'
to_parquet(df, 'crop_production.parquet')
