"""Build crop_production for CotedIvoire EHCVM 2018-19 (GAP 1, item-level).

Single source file: ../Data/Menage/s16c_me_CIV2018.dta (agriculture
crop/harvest module).  One row per reported (field, parcel, crop) harvest
record.  Harvest qty + unit (s16cq12a / s16cq12b), sold qty (s16cq16a), sale
value (s16cq17), and the intercrop flag (s16cq07) are ALL recorded at this
plot-crop grain in 2018-19, so no cross-file join is needed.  This is the
CotedIvoire analogue of Niger's EHCVM 2018-19 s16c crop module; the column
scheme verified identical.

Index = (t, i, plot, crop, u); plot = "{s16cq02}_{s16cq03}" aligns with
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
"""
import pandas as pd

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


def _finish_crop_production(df, t):
    """Common tail: tag t, build the (t, i, plot, crop, u) index, coerce
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
    keep = ['t', 'i', 'plot', 'crop', 'u', 'Quantity',
            'Quantity_sold', 'Value_sold', 'harvest_month', 'intercropped']
    df = df[[c for c in keep if c in df.columns]]
    df = df.set_index(['t', 'i', 'plot', 'crop', 'u'])
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
    'plot':          plot.values,
    'crop':          _map_labels(src['s16cq04'], crop_map).values,
    'u':             _map_labels(src['s16cq12b'], unit_map).values,
    'Quantity':      pd.to_numeric(src_codes['s16cq12a'], errors='coerce').values,
    'Quantity_sold': pd.to_numeric(src_codes['s16cq16a'], errors='coerce').values,
    'Value_sold':    pd.to_numeric(src_codes['s16cq17'], errors='coerce').values,
    'intercropped':  intercropped.values,
})

df = _finish_crop_production(df, '2018-19')

assert len(df) > 0, 'crop_production 2018-19 produced no rows'
to_parquet(df, 'crop_production.parquet')
