"""Build crop_production for Benin EHCVM 2021-22 (GH #1697).

The instrument splits the module 2018-19 kept together:

  s16c_me_ben2021.dta  one row per (field, parcel, crop), 8,934, unique.  Harvest
                       s16cq16a, unit s16cq16b, state s16cq16d, filled on 381 rows: 324
                       of the 970 unfinished harvests and 57 of the 7,964 finished ones.
  s16d_me_ben2021.dta  one record per (household, crop), no plot, repeated once per harvest
                       line (627 exact repeats of 8,328, removed).  A SALE is a record
                       with a positive s16dq05a or s16dq06: 5,064 of 7,701.

So Benin has Mali's shape (GH #1437) and not Togo's: the instrument sends a finished
harvest to the quantity (16C.11, PDF p. 87), and the file has it on 57 of 7,964.

THE HARVEST, on the ruling for that shape (GH #1437, @ligon 2026-11-04).  Where every s16c
row of a household's crop is finished, none records a quantity, and the s16d dispositions
(consumed, given, sold, in stock) share one unit, their sum is the harvest, keyed
Benin::crop_production::harvest-from-dispositions, on the single row or on the crop's one
Farm-level row.  A crop whose dispositions span units stays NA: no factor exists.  The
ruling names Mali and Burkina Faso; that it reaches Benin is flagged for confirmation in
the PR, and this step is one commit.

SALES take option (d), as ruled for Mali 2021-22 (GH #824, @ligon 2026-11-03).  One harvest
row for the crop: the sale goes on it, keyed Benin::crop_production::hh-crop-sale-single-row.
More: one row with plot_id = 'Farm-level', u = 'Unknown'.  No sale is split, summed or
dropped.

THE CROP IS READ BY CODE: s16dq01 labels only codes 1-47 and 65 (mapping.crop_labels).

A recorded harvest of an unfinished crop (324 rows) is served as recorded, keyed
Benin::crop_production::harvest-to-date: a part of the season.

u is s16cq16b, in the same vocabulary as the sold unit s16dq05b; a missing unit is
`Unknown`.  Neither s16cq16c nor s16dq05c is served: the questionnaire asks the kilograms of
ONE unit and the label says a total, and the file holds both shapes.

condition is s16cq16d, read by code through this wave's own three-code table, with the
answer as recorded in condition_native (GH #1436).  A blank state is `unknown_condition` and
NA, as is a Farm-level row, whose sale state is out of scope (GH #1462).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.ehcvm import (derive_hh_crop_harvest_total, derive_hh_crop_sale_attachment,
                                is_crop_sale, stamp_harvest_to_date)
import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import (KEY_HARVEST_DISPOSITIONS, KEY_HARVEST_TO_DATE, KEY_HH_CROP_SALE,
                     crop_labels, dispositions, household_ids, plot_ids, sale_records,
                     unit_labels)

UNIT_NA = 'Unknown'
CONDITION_NA = 'unknown_condition'
WAVE = '2021-22'


def _conditions(codes):
    """The canonical token of each s16cq16d code.  Strict: a missing table must not
    serve every row as the sentinel."""
    table = tools.get_categorical_mapping(
        tablename='harmonize_crop_condition_ehcvm2', idxvars='Code', dirs=['./'],
        **{'Preferred Label': 'Preferred Label'})
    table = {int(code): token for code, token in table.items()}
    assert sorted(table) == [1, 2, 3], f'harmonize_crop_condition_ehcvm2: {table}'
    return pd.to_numeric(codes, errors='coerce').astype('Int64').map(table).astype('string')


src = get_dataframe('../Data/s16c_me_ben2021.dta', convert_categoricals=True)
codes = get_dataframe('../Data/s16c_me_ben2021.dta', convert_categoricals=False)
keep = codes['s16cq04'].notna().values
src, codes = src[keep], codes[keep]
assert not codes[['s16cq02', 's16cq03']].isna().any(axis=None), 'a crop row with no plot'
harvest = pd.DataFrame({
    'i': household_ids(codes).astype('string').values,
    'plot_id': plot_ids(codes, 's16cq02', 's16cq03').values,
    'crop': crop_labels(codes['s16cq04']).values,
    'u': unit_labels(src['s16cq16b']).values,
    'Quantity': pd.to_numeric(codes['s16cq16a'], errors='coerce').values,
    'intercropped': src['s16cq07'].astype('string').str.strip().map(
        {'Association de cultures': True, 'Pure': False}).astype('boolean').values,
    'finished': src['s16cq11'].astype('string').values,
    'condition': _conditions(codes['s16cq16d']).values,
    'condition_native': src['s16cq16d'].astype('string').str.strip().values,
})

records = sale_records(WAVE)
sold = records[is_crop_sale(records['s16dq05a'], records['s16dq06'])]
sales = pd.DataFrame({
    'i': sold['i'].values,
    'crop': sold['crop'].values,
    'Quantity_sold': pd.to_numeric(sold['s16dq05a'], errors='coerce').values,
    'Unit_sold': unit_labels(sold['s16dq05b']).values,
    'Value_sold': pd.to_numeric(sold['s16dq06'], errors='coerce').values,
})

df = derive_hh_crop_sale_attachment(harvest, sales, KEY_HH_CROP_SALE)
recorded = pd.to_numeric(df['Quantity'], errors='coerce').astype('float64').sum()
df = stamp_harvest_to_date(df, KEY_HARVEST_TO_DATE)
df = derive_hh_crop_harvest_total(df, dispositions(records), KEY_HARVEST_DISPOSITIONS)
df = df.drop(columns='finished')

# Conservation: every sale is on exactly one served row, unchanged, and no harvest moved.
on_a_row = df[df['Quantity_sold'].notna() | df['Value_sold'].notna()]
assert len(on_a_row) == len(sales), 'crop_production 2021-22: a sale moved or doubled'
assert on_a_row['Value_sold'].sum() == sales['Value_sold'].sum()
assert on_a_row['Quantity_sold'].sum() == sales['Quantity_sold'].sum()
derived = df['Derivation'].astype('string').str.contains(
    'harvest-from-dispositions', regex=False).fillna(False)
assert df.loc[~derived, 'Quantity'].sum() == recorded, 'crop_production 2021-22: a recorded harvest moved'

for col in ('Quantity', 'Quantity_sold', 'Value_sold'):
    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
df['harvest_month'] = pd.array([pd.NA] * len(df), dtype='Float64')
df['intercropped'] = df['intercropped'].astype('boolean')
df['t'] = WAVE
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
