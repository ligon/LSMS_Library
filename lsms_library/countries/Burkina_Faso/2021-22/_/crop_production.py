"""Build crop_production for Burkina Faso EHCVM 2021-22 (GH #1436, #824, #1437).

The panel component only (WB 6224): the cross-section (6277) ships no agriculture
module we hold (GH #1276).  The instrument splits the module 2018-19 kept together:

  s16c_me_bfa2021.dta  one row per (field, parcel, crop), plus 1,118 rows of households
                       with no field.  Harvest s16cq16a, unit s16cq16b, state s16cq16d.
  s16d_me_bfa2021.dta  one record per (household, crop), no plot.  A SALE is a record
                       with a positive s16dq05a or s16dq06: 1,548, every one answering
                       s16dq04 'Oui'.  No exact repeats.

THE HARVEST IS BLANK ON EVERY FINISHED ROW (GH #1437).  s16cq11 'Oui' on 7,663 of the
8,476 crop rows, and on those rows s16cq12 to s16cq16d -- the whole block after the
question -- are blank, 0 of 7,663 filled.  The printed form routes Oui to 16C.16
(_/CONTENTS.org, 2026-11-09).  A recorded Quantity exists on 179 rows, all unfinished:
it is served as recorded, keyed Burkina_Faso::crop_production::harvest-to-date, a part of
the season.  Nothing is derived for a finished row: harvest-from-dispositions is ruled
for Mali, Benin and Cote d'Ivoire, not for this country.

SALES take option (d) (GH #824, @ligon 2026-11-03, ruled for Mali 2021-22).  A sale goes
on the household's single s16c line of the crop, keyed
Burkina_Faso::crop_production::hh-crop-sale-single-row; otherwise on ONE row with plot_id
= 'Farm-level', u = 'Unknown', and no key.  No sale is split, summed or dropped.  The
match is by CROP CODE, not label: the two files label one crop list differently (see
mapping.py, "THE IDENTITY OF A CROP IS ITS CODE"), and by label 332 sales would be
sent to Farm-level for want of a line.

u is s16cq16b and Unit_sold s16dq05b, each by its own file's labels through the `u`
table; a code the file leaves bare keeps its source-qualified identity; a missing harvest
unit is `Unknown`, a missing sold unit NA.  condition is s16cq16d through
harmonize_crop_condition_ehcvm1 (the same five codes as 2018-19's s16cq12c), the answer
as recorded in condition_native (GH #1436).  intercropped is s16cq07.

Not served here, each for want of a ruling that names this country: s16cq16c (the line's
kilograms, 108 rows) as Quantity_kg; s16dq05c (the sale's kilograms) as
Quantity_sold_kg; s16dq05d (the sale's state) as Condition_sold.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.ehcvm import (crop_condition_codes, crop_condition_labels,
                                derive_hh_crop_sale_attachment, hh_crop_records,
                                is_crop_sale, stamp_harvest_to_date)
from lsms_library.local_tools import get_categorical_mapping, get_dataframe, to_parquet
from burkina_faso import i as country_i
from mapping import (KEY_HARVEST_TO_DATE, KEY_HH_CROP_SALE, _code_labels, _crop_household_ids,
                     _crop_label_table, _crop_maps, _crop_plot_ids, _unit_labels)

T = '2021-22'
UNIT_NA = 'Unknown'
CONDITION_NA = 'unknown_condition'


def _same(a, b):
    """Equal totals, up to the order floating-point sums were taken in."""
    return abs(float(a) - float(b)) <= 1e-9 * max(1.0, abs(float(b)))


def _conditions(codes):
    """s16cq16d by code through the 2018-19 table, which carries the same five codes
    under the same labels (_/CONTENTS.org, GH #1436).  Strict: a code outside the table
    is refused, not served as the sentinel."""
    table = get_categorical_mapping(tablename='harmonize_crop_condition_ehcvm1', idxvars='Code',
                                    **{'Preferred Label': 'Preferred Label'})
    table = {int(code): token for code, token in table.items()}
    assert sorted(table) == [1, 2, 3, 4, 5], f'harmonize_crop_condition_ehcvm1: {table}'
    num = pd.to_numeric(codes, errors='coerce')
    outside = num.notna() & ~num.isin(list(table))
    assert not outside.any(), f's16cq16d: {int(outside.sum())} code(s) outside the table'
    return crop_condition_codes(num, table).fillna(CONDITION_NA)


src = get_dataframe('../Data/s16c_me_bfa2021.dta', convert_categoricals=True)
codes = get_dataframe('../Data/s16c_me_bfa2021.dta', convert_categoricals=False)
rec = get_dataframe('../Data/s16d_me_bfa2021.dta', convert_categoricals=True)
rec_codes = get_dataframe('../Data/s16d_me_bfa2021.dta', convert_categoricals=False)
RAW_VALUE_SOLD = pd.to_numeric(rec_codes['s16dq06'], errors='coerce').sum()

food_map, unit_map = _crop_maps()
crop_table = _crop_label_table(_code_labels(src['s16cq04'], codes['s16cq04'], 's16cq04'),
                               _code_labels(rec['s16dq01'], rec_codes['s16dq01'], 's16dq01'),
                               food_map)

keep = codes['s16cq04'].notna().to_numpy()
src, codes = src[keep], codes[keep]
assert not codes[['s16cq02', 's16cq03']].isna().any(axis=None), 'a crop row with no plot'
hh = _crop_household_ids(codes)
assert (hh == pd.Series([country_i(pd.Series([g, m])) for g, m in
                         zip(codes['grappe'], codes['menage'])], index=codes.index)).all(), \
    'crop_production 2021-22: the household id drifted from burkina_faso.i'
harvest = pd.DataFrame({
    'i': hh.to_numpy(),
    'plot_id': _crop_plot_ids(codes).to_numpy(),
    # The crop CODE while sales are attached; replaced by its served label below.
    'crop': pd.to_numeric(codes['s16cq04']).astype('int64').astype('string').to_numpy(),
    'u': _unit_labels(src['s16cq16b'], codes['s16cq16b'], 's16cq16b', unit_map).to_numpy(),
    'Quantity': pd.to_numeric(codes['s16cq16a'], errors='coerce').to_numpy(),
    'intercropped': src['s16cq07'].astype('string').str.strip().map(
        {'Association de cultures': True, 'Pure': False}).astype('boolean').to_numpy(),
    'finished': src['s16cq11'].astype('string').to_numpy(),
    'condition': _conditions(codes['s16cq16d']).to_numpy(),
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
    'Unit_sold': _unit_labels(sold['s16dq05b'], sold_codes['s16dq05b'], 's16dq05b',
                              unit_map).to_numpy(),
    'Value_sold': pd.to_numeric(sold_codes['s16dq06'], errors='coerce').to_numpy(),
})
lines = harvest.groupby(['i', 'crop']).size()
assert sales.set_index(['i', 'crop']).index.isin(lines.index).all(), \
    'crop_production 2021-22: a sale with no s16c line of its crop code'

df = derive_hh_crop_sale_attachment(harvest, sales, KEY_HH_CROP_SALE)
recorded = df['Quantity'].sum()
df = stamp_harvest_to_date(df, KEY_HARVEST_TO_DATE).drop(columns='finished')

# Conservation: every sale is on exactly one served row, unchanged, and no harvest moved.
on_a_row = df[df['Quantity_sold'].notna() | df['Value_sold'].notna()]
assert len(on_a_row) == len(sales), 'crop_production 2021-22: a sale moved or doubled'
assert _same(on_a_row['Value_sold'].sum(), sales['Value_sold'].sum())
assert _same(on_a_row['Quantity_sold'].sum(), sales['Quantity_sold'].sum())
# The raw file's whole s16dq06 column, before any filter, is what the table serves.
assert _same(df['Value_sold'].sum(), RAW_VALUE_SOLD), \
    'crop_production 2021-22: Value_sold is not the s16d sum'
assert _same(df['Quantity'].sum(), recorded), 'crop_production 2021-22: a harvest moved'

df['crop'] = pd.array([crop_table[int(c)] for c in df['crop']], dtype='string')
for col in ('Quantity', 'Quantity_sold', 'Value_sold'):
    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
df['harvest_month'] = pd.array([pd.NA] * len(df), dtype='Float64')
df['intercropped'] = df['intercropped'].astype('boolean')
df['t'] = T
df['u'] = df['u'].astype('string').fillna(UNIT_NA)
df['condition'] = df['condition'].astype('string').fillna(CONDITION_NA)
for col in ('crop', 'Unit_sold', 'Derivation', 'condition_native'):
    df[col] = df[col].astype('string')
# `str`, as the 2018-19 builder writes it: a different dtype here would retype the
# 2018-19 rows of the concatenated country table (measured: str -> string).
df['plot_id'] = df['plot_id'].astype(str)
# The 2018-19 contract: a sold unit is NA, never the sentinel.
df['Unit_sold'] = df['Unit_sold'].where(df['Unit_sold'] != UNIT_NA, pd.NA)
df = df.set_index(['t', 'i', 'plot_id', 'crop', 'u', 'condition'])[
    ['Quantity', 'Quantity_sold', 'Value_sold', 'Unit_sold', 'harvest_month',
     'intercropped', 'condition_native', 'Derivation']]

# s16c repeats (household, field, parcel, crop) on two pairs of lines that record no
# harvest; on the served key they are one row each, and the framework's audited
# collapse (GH #323) reports the pair whose intercrop answer disagrees.  Counted here so
# a third pair, or one that carries a quantity, fails instead.
dup = df.index.duplicated(keep=False)
assert int(dup.sum()) <= 4 and df.loc[dup, 'Quantity'].isna().all(), \
    f'crop_production 2021-22: {int(dup.sum())} rows repeat a served key'
assert len(df) >= len(src), 'crop_production 2021-22 lost a source row'
to_parquet(df, 'crop_production.parquet')
