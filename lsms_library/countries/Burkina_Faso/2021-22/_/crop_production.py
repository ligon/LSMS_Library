"""Build crop_production for Burkina Faso EHCVM 2021-22 (GH #1436, #824, #1437).

The panel component only (WB 6224): the cross-section (6277) ships no agriculture
module we hold (GH #1276).  The instrument splits the module 2018-19 kept together:

  s16c_me_bfa2021.dta  one row per (field, parcel, crop), plus 1,118 rows of households
                       with no field.  Harvest s16cq16a, unit s16cq16b, state s16cq16d.
                       No line number, and (household, field, parcel, crop) repeats on
                       two pairs of DISTINCT lines (different roster text, intercrop
                       answer, area share and seed), so the line is production_obs, the
                       constructed `row:<source_fingerprint>:<n>` (Niger 2021-22's rule,
                       GH #1360; ruling @ligon 2026-11-13, "Add production_obs").
  s16d_me_bfa2021.dta  one record per (household, crop), no plot.  A SALE is a record
                       with a positive s16dq05a or s16dq06: 1,548, every one answering
                       s16dq04 'Oui'.  No exact repeats.

THE HARVEST (GH #1437).  s16cq11 'Oui' on 7,663 of the 8,476 crop rows, and on those rows
s16cq12 to s16cq16d -- the whole block after the question -- are blank, 0 of 7,663.  The
printed form routes Oui to 16C.16 (_/CONTENTS.org, 2026-11-09).  So, as for Mali, Benin and
Cote d'Ivoire 2021-22 (ruling @ligon 2026-11-13, "Extend Mali's rule"): where every s16c
line of a household's crop is finished, none records a quantity, and the s16d
dispositions (consumed, given, sold, in stock) share one unit, their sum is the harvest,
keyed Burkina_Faso::crop_production::harvest-from-dispositions, on the single line or on
the crop's one Farm-level row.  A crop whose dispositions span units stays NA.  A recorded
Quantity (179 rows, all unfinished) is served as recorded, keyed harvest-to-date: it is a
PARTIAL harvest, a median half of the plot still to harvest, and is no yield.

SALES take option (d) (GH #824, @ligon 2026-11-03, ruled for Mali 2021-22).  A sale goes
on the household's single s16c line of the crop, keyed
Burkina_Faso::crop_production::hh-crop-sale-single-row; otherwise on ONE row with plot_id
= 'Farm-level', u = 'Unknown', and no key.  No sale is split, summed or dropped.  The
match is by CROP CODE, not label: the two files label one crop list differently (see
mapping.py, "THE IDENTITY OF A CROP IS ITS CODE"), and by label 332 sales would be
sent to Farm-level for want of a line.  The sale's state s16dq05d is Condition_sold, by
the 2018-19 fold (GH #1528), with the answer in condition_sold_native.

Units: every unit field is decoded by code as one UML list (codes 1-7 are labelled alike in
all five fields; that a bare code above 7 is one unit across fields is assumed, not shown)
and served through the read-path `u` table (mapping._served_units), so u, Unit_sold and
the dispositions share one vocabulary.  A missing harvest unit is `Unknown`, a missing
sold unit NA.  condition is s16cq16d through harmonize_crop_condition_ehcvm1 (the same
five codes as 2018-19's s16cq12c), the answer as recorded in condition_native (GH #1436).
intercropped is s16cq07.
harvest_month has no source in either file and stays NA, as in 2018-19.

Not served, for want of a ruling that names this country: s16cq16c (the line's kilograms)
as Quantity_kg and s16dq05c (the sale's kilograms) as Quantity_sold_kg.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.derivations import union_keys
from lsms_library.ehcvm import (crop_condition_codes, crop_condition_labels,
                                derive_hh_crop_harvest_total, derive_hh_crop_sale_attachment,
                                hh_crop_records, is_crop_sale, stamp_harvest_to_date)
from lsms_library.local_tools import get_categorical_mapping, get_dataframe, to_parquet
from burkina_faso import i as country_i
from mapping import (CROP_CODES_FROM_TEXT, CROP_OBS_NA, FARM_LEVEL, KEY_CROP_FROM_TEXT,
                     KEY_HARVEST_DISPOSITIONS, KEY_HARVEST_TO_DATE, KEY_HH_CROP_SALE,
                     _crop_household_ids, _crop_plot_ids, _crop_row_observations,
                     _crop_tables, _dispositions, _served_units)

T = '2021-22'
UNIT_NA = 'Unknown'
CONDITION_NA = 'unknown_condition'
SOURCE = '../Data/s16c_me_bfa2021.dta'
INDEX = ['t', 'i', 'plot_id', 'crop', 'u', 'condition', 'production_obs']


def _same(a, b):
    """Equal totals, up to the order floating-point sums were taken in."""
    return abs(float(a) - float(b)) <= 1e-9 * max(1.0, abs(float(b)))


def _fold_table():
    """harmonize_crop_condition_ehcvm1, strictly: the same five codes and labels as 2018-19."""
    table = get_categorical_mapping(tablename='harmonize_crop_condition_ehcvm1', idxvars='Code',
                                    **{'Preferred Label': 'Preferred Label'})
    table = {int(code): token for code, token in table.items()}
    assert sorted(table) == [1, 2, 3, 4, 5], f'harmonize_crop_condition_ehcvm1: {table}'
    return table


def _conditions(codes, field):
    """A state answer by code through the fold table.  A code outside it is refused,
    not served as the sentinel; a blank stays NA here."""
    table = _fold_table()
    num = pd.to_numeric(codes, errors='coerce')
    outside = num.notna() & ~num.isin(list(table))
    assert not outside.any(), f'{field}: {int(outside.sum())} code(s) outside the table'
    return crop_condition_codes(num, table)


def _stamp(frame, mask, key):
    out = frame.copy()
    derivation = out['Derivation'].astype('string').copy()
    derivation[mask] = pd.array([union_keys((value, key)) for value in derivation[mask]],
                                dtype='string')
    out['Derivation'] = derivation
    return out


src = get_dataframe(SOURCE, convert_categoricals=True)
codes = get_dataframe(SOURCE, convert_categoricals=False)
rec = get_dataframe('../Data/s16d_me_bfa2021.dta', convert_categoricals=True)
rec_codes = get_dataframe('../Data/s16d_me_bfa2021.dta', convert_categoricals=False)
RAW_VALUE_SOLD = pd.to_numeric(rec_codes['s16dq06'], errors='coerce').sum()

crop_table, unit_table = _crop_tables(src, codes, rec, rec_codes)
# The zero-based row of the held release, taken before any filter (GH #1360).
observations = _crop_row_observations(len(codes), SOURCE, script_path=__file__)
observations.index = codes.index

keep = codes['s16cq04'].notna().to_numpy()
src, codes, observations = src[keep], codes[keep], observations[keep]
assert not codes[['s16cq02', 's16cq03']].isna().any(axis=None), 'a crop row with no plot'
hh = _crop_household_ids(codes)
assert (hh == pd.Series([country_i(pd.Series([g, m])) for g, m in
                         zip(codes['grappe'], codes['menage'])], index=codes.index)).all(), \
    'crop_production 2021-22: the household id drifted from burkina_faso.i'
harvest = pd.DataFrame({
    'i': hh.to_numpy(),
    'plot_id': _crop_plot_ids(codes).astype(str).to_numpy(),
    'production_obs': observations.to_numpy(),
    # The crop CODE while sales and totals are attached; replaced by its label below.
    'crop': pd.to_numeric(codes['s16cq04']).astype('int64').astype('string').to_numpy(),
    'u': _served_units(codes['s16cq16b'], unit_table).to_numpy(),
    'Quantity': pd.to_numeric(codes['s16cq16a'], errors='coerce').to_numpy(),
    'intercropped': src['s16cq07'].astype('string').str.strip().map(
        {'Association de cultures': True, 'Pure': False}).astype('boolean').to_numpy(),
    'finished': src['s16cq11'].astype('string').to_numpy(),
    'condition': _conditions(codes['s16cq16d'], 's16cq16d').fillna(CONDITION_NA).to_numpy(),
    'condition_native': crop_condition_labels(src['s16cq16d']).to_numpy(),
})

rec_codes = hh_crop_records(rec_codes)
rec_codes = rec_codes[rec_codes['s16dq01'].notna()]
rec = rec.loc[rec_codes.index]
is_sale = is_crop_sale(rec_codes['s16dq05a'], rec_codes['s16dq06']).to_numpy()
sold, sold_codes = rec[is_sale], rec_codes[is_sale]
sales = pd.DataFrame({
    'i': _crop_household_ids(sold_codes).to_numpy(),
    'crop': pd.to_numeric(sold_codes['s16dq01']).astype('int64').astype('string').to_numpy(),
    'Quantity_sold': pd.to_numeric(sold_codes['s16dq05a'], errors='coerce').to_numpy(),
    'Unit_sold': _served_units(sold_codes['s16dq05b'], unit_table).to_numpy(),
    'Value_sold': pd.to_numeric(sold_codes['s16dq06'], errors='coerce').to_numpy(),
})
# The sale's own state (GH #1528), carried on whichever row the sale lands on.
sale_state = pd.DataFrame({
    'i': sales['i'].to_numpy(), 'crop': sales['crop'].to_numpy(),
    'Condition_sold': _conditions(sold_codes['s16dq05d'], 's16dq05d').to_numpy(),
    'condition_sold_native': crop_condition_labels(sold['s16dq05d']).to_numpy(),
})
lines = harvest.groupby(['i', 'crop']).size()
assert sales.set_index(['i', 'crop']).index.isin(lines.index).all(), \
    'crop_production 2021-22: a sale with no s16c line of its crop code'

df = derive_hh_crop_sale_attachment(harvest, sales, KEY_HH_CROP_SALE)
carries = (df['Quantity_sold'].notna() | df['Value_sold'].notna()).to_numpy()
df = df.merge(sale_state, on=['i', 'crop'], how='left', validate='many_to_one')
for col in ('Condition_sold', 'condition_sold_native'):
    df[col] = df[col].astype('string').where(carries, pd.NA)
recorded = pd.to_numeric(df['Quantity'], errors='coerce').astype('float64').sum()
df = stamp_harvest_to_date(df, KEY_HARVEST_TO_DATE)
df = derive_hh_crop_harvest_total(df, _dispositions(rec, rec_codes, unit_table),
                                  KEY_HARVEST_DISPOSITIONS)
df = df.drop(columns='finished')
# A Farm-level row is no s16c line: it carries the canonical sentinel.
farm = df['plot_id'].eq(FARM_LEVEL).fillna(False).to_numpy()
df['production_obs'] = df['production_obs'].astype('string').where(~farm, CROP_OBS_NA)

# Conservation: every sale is on exactly one served row, unchanged; no recorded harvest moved.
on_a_row = df[df['Quantity_sold'].notna() | df['Value_sold'].notna()]
assert len(on_a_row) == len(sales), 'crop_production 2021-22: a sale moved or doubled'
assert _same(on_a_row['Value_sold'].sum(), sales['Value_sold'].sum())
assert _same(on_a_row['Quantity_sold'].sum(), sales['Quantity_sold'].sum())
# The raw file's whole s16dq06 column, before any filter, is what the table serves.
assert _same(df['Value_sold'].sum(), RAW_VALUE_SOLD), \
    'crop_production 2021-22: Value_sold is not the s16d sum'
# Every answered sale state is on its sale's row, and on no other row.
assert int(df['condition_sold_native'].notna().sum()) == int(
    sale_state['condition_sold_native'].notna().sum()), 'crop_production 2021-22: a sold state moved'
derived = df['Derivation'].astype('string').str.contains(
    KEY_HARVEST_DISPOSITIONS, regex=False).fillna(False).to_numpy()
assert _same(pd.to_numeric(df.loc[~derived, 'Quantity'], errors='coerce').sum(), recorded), \
    'crop_production 2021-22: a recorded harvest moved'

# The crop label, and the key of a label read from the roster text (mapping.py).
from_text = df['crop'].astype(int).isin(list(CROP_CODES_FROM_TEXT)).to_numpy()
df = _stamp(df, from_text, KEY_CROP_FROM_TEXT)
df['crop'] = pd.array([crop_table[int(c)] for c in df['crop']], dtype='string')

for col in ('Quantity', 'Quantity_sold', 'Value_sold'):
    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
df['harvest_month'] = pd.array([pd.NA] * len(df), dtype='Float64')
df['intercropped'] = df['intercropped'].astype('boolean')
df['t'] = T
df['u'] = df['u'].astype('string').fillna(UNIT_NA)
df['condition'] = df['condition'].astype('string').fillna(CONDITION_NA)
for col in ('crop', 'Unit_sold', 'Derivation', 'condition_native', 'Condition_sold',
            'condition_sold_native', 'production_obs'):
    df[col] = df[col].astype('string')
# `str`, as the 2018-19 builder writes it: a different dtype here would retype the
# 2018-19 rows of the concatenated country table (measured: str -> string).
df['plot_id'] = df['plot_id'].astype(str)
# The 2018-19 contract: a sold unit is NA, never the sentinel.
df['Unit_sold'] = df['Unit_sold'].where(df['Unit_sold'] != UNIT_NA, pd.NA)
df = df.set_index(INDEX)[
    ['Quantity', 'Quantity_sold', 'Value_sold', 'Unit_sold', 'harvest_month', 'intercropped',
     'condition_native', 'Condition_sold', 'condition_sold_native', 'Derivation']]

# THE GUARD.  The declared index is unique, so the framework collapses nothing.  On the
# key WITHOUT production_obs, two pairs of s16c lines repeat (measured: 2 groups, 4 rows);
# a keyed row -- a sale, a derived or recorded harvest, a text-derived label -- may never
# sit in such a group, and a group may never carry a quantity, so no consumer that groups
# on the plot-crop key can sum a derived value with another line's.
assert df.index.is_unique, 'crop_production 2021-22 repeats a served key'
lines_key = df.index.droplevel('production_obs')
dup = lines_key.duplicated(keep=False)
keyed = df['Derivation'].notna().to_numpy()
assert not (dup & keyed).any(), 'crop_production 2021-22: a keyed row shares its plot-crop key'
assert df.loc[dup, 'Quantity'].isna().all() and df.loc[dup, 'Quantity_sold'].isna().all(), \
    'crop_production 2021-22: a repeated plot-crop line carries a quantity'
assert int(dup.sum()) <= 4, f'crop_production 2021-22: {int(dup.sum())} rows repeat a plot-crop key'
assert len(df) >= len(src), 'crop_production 2021-22 lost a source row'
to_parquet(df, 'crop_production.parquet')
