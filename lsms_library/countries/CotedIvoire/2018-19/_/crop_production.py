"""Build crop_production for CotedIvoire EHCVM 2018-19 (GAP 1, item-level).

Single source file: ../Data/Menage/s16c_me_CIV2018.dta (agriculture
crop/harvest module).  One row per reported (field, parcel, crop) harvest
record.  Harvest qty + unit (s16cq12a / s16cq12b), sold qty (s16cq16a), sale
value (s16cq17), and the intercrop flag (s16cq07) are ALL recorded at this
plot-crop grain in 2018-19, so no cross-file join is needed.  This is the
CotedIvoire analogue of Niger's EHCVM 2018-19 s16c crop module; the column
scheme verified identical.

Index = (t, i, plot_id, crop, u, condition); plot_id = "{s16cq02}_{s16cq03}" aligns with
plot_features' plot_id.

i is CotedIvoire's EHCVM composite household id.  CotedIvoire PREDATES the
standard EHCVM list (CLAUDE.md) and uses a DIFFERENT id scheme from the
Niger / Senegal / Mali EHCVM siblings: NO 'E_' prefix and NO '0' separator —
just ``format_id(grappe) + format_id(menage, zeropadding=3)`` (e.g.
grappe=1, menage=3 -> '1003'), matching sample().i.  Inlined here (NOT
imported from niger / cotedivoire) to keep this wave script self-contained.

CROP LABELS: CotedIvoire has NO harmonize_food table (its food harmonization
is code-keyed via food_items, not a string label table; and its forest/cash
crops — Cacao, Anacarde, Hévéa, Café — are absent from the Sahelian Niger
table).  So crop labels pass through UNCHANGED (the harmonized French label,
kept visible and flagged by the sanity checker) where no map applies — the
same pass-through behavior Niger's _crop_labels uses for unmapped labels.
If a country-level harmonize_food is added later it is picked up
automatically.  Units map through the existing CotedIvoire `u` table.

``condition`` is the harvest's own "Etat du produit" (s16cq12c, GH #1436), read
by code and folded onto the canonical tokens; the column ``condition_native``
keeps the answer as recorded.  This file has seven answers where the other EHCVM
files have five: the vague 2 instrument added 6 Fève and 7 Autre à préciser, and
the .dta does not label them.  See categorical_mapping.org.
"""
import pandas as pd
from lsms_library.ehcvm import crop_condition_codes, crop_condition_labels

from lsms_library.local_tools import get_dataframe, to_parquet, format_id, get_categorical_mapping


def _i(grappe, menage):
    """CotedIvoire EHCVM household id: format_id(grappe) +
    format_id(menage, zeropadding=3).  Inlined copy of cotedivoire.i() so
    this wave script is self-contained.  Returns None if either part is
    missing so the _finish gate drops the row."""
    g = format_id(grappe)
    m = format_id(menage, zeropadding=3)
    if g is None or m is None:
        return None
    return g + m


def _label_map(tablename):
    """Load a {Original Label -> Preferred Label} dict from
    categorical_mapping.org, returning {} (pass-through) if the table is
    absent.  Mirrors niger.py:_crop_maps but tolerant of a missing table."""
    try:
        return get_categorical_mapping(
            tablename=tablename, idxvars='Original Label',
            **{'Preferred Label': 'Preferred Label'})
    except (FileNotFoundError, KeyError):
        return {}


def _map_labels(source_labels, label_map):
    """Map a string-label column through ``label_map``.  Unmapped labels pass
    through unchanged (kept visible, flagged by the sanity checker)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: label_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _crop_conditions(codes):
    """Map raw "Etat du produit" codes (``convert_categoricals=False``) through
    ``harmonize_crop_condition_ehcvm1`` (GH #1436).  The table is read strictly:
    a missing table must not serve every row as the sentinel.  A blank or
    off-scheme code becomes ``unknown_condition``, a real value and never NA,
    because a null index key is deleted by the next ``groupby`` (GH #323 3b)."""
    table = get_categorical_mapping(
        tablename='harmonize_crop_condition_ehcvm1', idxvars='Code',
        **{'Preferred Label': 'Preferred Label'})
    table = {int(code): token for code, token in table.items()}
    assert sorted(table) == [1, 2, 3, 4, 5, 6, 7], f'harmonize_crop_condition_ehcvm1: {table}'
    return crop_condition_codes(pd.to_numeric(codes, errors='coerce'), table).fillna('unknown_condition')


def _crop_condition_labels(labels, codes):
    """The state as the survey recorded it, for ``condition_native``: *labels* is
    the column read with ``convert_categoricals=True``, *codes* the same column
    read without.  The .dta labels codes 1-5 only, so 6 and 7 arrive as bare
    numbers and take the table's Source Label, as the vague 2 instrument prints
    them (CIV-EHCVM1-QNR-MEN-CAPI-VAGUE2.pdf, p. 197).  A blank stays NA, never a
    sentinel, because a column has no NaN-key hazard (ruling @ligon 2026-11-04,
    GH #1436)."""
    instrument = get_categorical_mapping(
        tablename='harmonize_crop_condition_ehcvm1', idxvars='Code',
        **{'Source Label': 'Source Label'})
    instrument = {int(code): label for code, label in instrument.items()}
    codes = pd.to_numeric(codes, errors='coerce').astype('Int64')
    out = crop_condition_labels(labels)
    return out.where(~codes.isin([6, 7]), codes.map(instrument).astype('string'))


def _finish_crop_production(df, t):
    """Common tail: tag t, build the (t, i, plot_id, crop, u, condition) index, coerce
    numeric columns, guarantee the schema column set.  Mirrors
    niger.py:_finish_crop_production."""
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
    df = df[df['crop'].notna() & df['i'].notna()]
    keep = ['t', 'i', 'plot_id', 'crop', 'u', 'condition', 'Quantity',
            'Quantity_sold', 'Value_sold', 'harvest_month', 'intercropped',
            'condition_native']
    df = df[[c for c in keep if c in df.columns]]
    df['condition'] = df['condition'].astype('string')
    df['condition_native'] = df['condition_native'].astype('string')
    df = df.set_index(['t', 'i', 'plot_id', 'crop', 'u', 'condition'])
    return df


# convert_categoricals=True so crop / unit value labels arrive as the strings
# harmonize_food (if present) / the u table key on.  Codes file for the
# field/parcel ids and the (numeric) intercrop fallback.
src = get_dataframe('../Data/Menage/s16c_me_CIV2018.dta', convert_categoricals=True)
src_codes = get_dataframe('../Data/Menage/s16c_me_CIV2018.dta', convert_categoricals=False)

crop_map = _label_map('harmonize_food')
unit_map = _label_map('u')

hh = src_codes.apply(lambda r: _i(r['grappe'], r['menage']), axis=1)
field = src_codes['s16cq02'].apply(format_id)
parcel = src_codes['s16cq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

# intercrop: s16cq07 == 'Association de cultures' -> True, 'Pure' -> False
intercropped = src['s16cq07'].astype('string').map(
    {'Association de cultures': True, 'Pure': False})

df = pd.DataFrame({
    'i':             hh.values,
    'plot_id':          plot.values,
    'crop':          _map_labels(src['s16cq04'], crop_map).values,
    'u':             _map_labels(src['s16cq12b'], unit_map).values,
    # By code; the fold is ruled, see categorical_mapping.org (GH #1436).
    'condition':     _crop_conditions(src_codes['s16cq12c']).values,
    # What the fold cannot show: the answer as recorded, by label.
    'condition_native': _crop_condition_labels(src['s16cq12c'], src_codes['s16cq12c']).values,
    'Quantity':      pd.to_numeric(src_codes['s16cq12a'], errors='coerce').values,
    'Quantity_sold': pd.to_numeric(src_codes['s16cq16a'], errors='coerce').values,
    'Value_sold':    pd.to_numeric(src_codes['s16cq17'], errors='coerce').values,
    'intercropped':  intercropped.values,
})

df = _finish_crop_production(df, '2018-19')

assert len(df) > 0, 'crop_production 2018-19 produced no rows'
to_parquet(df, 'crop_production.parquet')
