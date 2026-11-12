"""Build crop_production for Togo EHCVM 2021-22 (GH #1697).

The instrument splits the module 2018 kept together:

  s16c_me_tgo2021.dta  one row per (field, parcel, crop), plus 2,690 rows of households
                       with no field.  Harvest s16cq16a, unit s16cq16b, filled on 10,325
                       of the 10,877 crop rows.  The file is unique on (household, field,
                       parcel, crop), so no observation level is needed.
  s16d_me_tgo2021.dta  one record per (household, crop), no plot, repeated once per
                       harvest line (1,111 exact repeats, removed).  A SALE is a record
                       with a positive s16dq05a or s16dq06: 5,976 of 9,214.

SALES take option (d), as ruled for Mali 2021-22 (GH #824, @ligon 2026-11-03).  One
harvest row for the crop: the sale goes on it, keyed
Togo::crop_production::hh-crop-sale-single-row.  More: one row with plot_id =
'Farm-level', u = 'Unknown'.  No sale is split, summed or dropped.

A recorded harvest of an unfinished crop (s16cq11 = Non, 111 rows) is served as recorded,
keyed Togo::crop_production::harvest-to-date: a part of the season.

u is s16cq16b, in the same vocabulary as the sold unit s16dq05b; a missing unit is
`Unknown`.  s16cq16b_bis decodes the 670 `Autre à préciser` rows and is not served
(_/CONTENTS.org).  Nor is s16cq16c or s16dq05c: each is a total in kg.

condition is s16cq16d, read by code through this wave's own eight-code table, with the
answer as recorded in condition_native (GH #1436).  A blank state is `unknown_condition`
and NA, as is a Farm-level row, whose sale state is out of scope (GH #1462).

`Récolte nulle` (258 rows) stays NA with u = `Unknown`, as ruled for Mali: s16cq16a and
s16cq16b are blank there, and the answer is not written in as a zero.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.ehcvm import (derive_hh_crop_sale_attachment, hh_crop_records,
                                is_crop_sale, served_unit_labels, stamp_harvest_to_date)
import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import (KEY_HARVEST_TO_DATE, KEY_HH_CROP_SALE, crop_labels, household_ids,
                     plot_ids)

UNIT_NA = 'Unknown'
CONDITION_NA = 'unknown_condition'


def _units(series):
    """Each unit label as the API serves it; NA stays NA."""
    raw = series.astype('string').str.strip()
    known = raw.dropna().unique()
    served = dict(zip(known, served_unit_labels('Togo', known)))
    return raw.map(served).astype('string')


def _conditions(codes):
    """The canonical token of each s16cq16d code.  Strict: a missing table must not
    serve every row as the sentinel."""
    table = tools.get_categorical_mapping(
        tablename='harmonize_crop_condition_ehcvm2', idxvars='Code', dirs=['./'],
        **{'Preferred Label': 'Preferred Label'})
    table = {int(code): token for code, token in table.items()}
    assert sorted(table) == list(range(1, 9)), f'harmonize_crop_condition_ehcvm2: {table}'
    return pd.to_numeric(codes, errors='coerce').astype('Int64').map(table).astype('string')


src = get_dataframe('../Data/s16c_me_tgo2021.dta', convert_categoricals=True)
codes = get_dataframe('../Data/s16c_me_tgo2021.dta', convert_categoricals=False)
keep = src['s16cq04'].notna().values
src, codes = src[keep], codes[keep]
assert not src[['s16cq02', 's16cq03']].isna().any(axis=None), 'a crop row with no plot'
harvest = pd.DataFrame({
    'i': household_ids(src).astype('string').values,
    'plot_id': plot_ids(src, 's16cq02', 's16cq03').values,
    'crop': crop_labels(src['s16cq04']).values,
    'u': _units(src['s16cq16b']).values,
    'Quantity': pd.to_numeric(src['s16cq16a'], errors='coerce').values,
    'intercropped': src['s16cq07'].astype('string').str.strip().map(
        {'Association de cultures': True, 'Pure': False}).astype('boolean').values,
    'finished': src['s16cq11'].astype('string').values,
    'condition': _conditions(codes['s16cq16d']).values,
    'condition_native': src['s16cq16d'].astype('string').str.strip().values,
})

records = hh_crop_records(get_dataframe('../Data/s16d_me_tgo2021.dta', convert_categoricals=True))
sold = records[is_crop_sale(records['s16dq05a'], records['s16dq06'])]
sales = pd.DataFrame({
    'i': household_ids(sold).astype('string').values,
    'crop': crop_labels(sold['s16dq01']).values,
    'Quantity_sold': pd.to_numeric(sold['s16dq05a'], errors='coerce').values,
    'Unit_sold': _units(sold['s16dq05b']).values,
    'Value_sold': pd.to_numeric(sold['s16dq06'], errors='coerce').values,
})

df = derive_hh_crop_sale_attachment(harvest, sales, KEY_HH_CROP_SALE)
recorded = df['Quantity'].sum()
df = stamp_harvest_to_date(df, KEY_HARVEST_TO_DATE).drop(columns='finished')

# Conservation: every sale is on exactly one served row, unchanged, and no harvest moved.
on_a_row = df[df['Quantity_sold'].notna() | df['Value_sold'].notna()]
assert len(on_a_row) == len(sales), 'crop_production 2021-22: a sale moved or doubled'
assert on_a_row['Value_sold'].sum() == sales['Value_sold'].sum()
assert on_a_row['Quantity_sold'].sum() == sales['Quantity_sold'].sum()
assert df['Quantity'].sum() == recorded, 'crop_production 2021-22: a harvest moved'

for col in ('Quantity', 'Quantity_sold', 'Value_sold'):
    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
df['harvest_month'] = pd.array([pd.NA] * len(df), dtype='Float64')
df['intercropped'] = df['intercropped'].astype('boolean')
df['t'] = '2021-22'
df['u'] = df['u'].astype('string').fillna(UNIT_NA)
df['condition'] = df['condition'].astype('string').fillna(CONDITION_NA)
for col in ('crop', 'plot_id', 'Unit_sold', 'Derivation', 'condition_native'):
    df[col] = df[col].astype('string')
df = df.set_index(['t', 'i', 'plot_id', 'crop', 'u', 'condition'])[
    ['Quantity', 'Quantity_sold', 'Unit_sold', 'Value_sold', 'harvest_month',
     'intercropped', 'condition_native', 'Derivation']]

assert df.index.is_unique, 'crop_production 2021-22 repeats a key'
assert len(df) >= len(src), 'crop_production 2021-22 lost a source row'
to_parquet(df, 'crop_production.parquet')
