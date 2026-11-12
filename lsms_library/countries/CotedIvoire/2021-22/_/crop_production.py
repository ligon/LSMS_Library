"""Build crop_production for CotedIvoire EHCVM 2021-22 (GH #1697).

The instrument splits the module 2018-19 kept together:

  s16c_me_civ2021.dta  one row per (field, parcel, crop), plus 5,397 rows of households
                       with no field.  s16cq16a and its unit s16cq16b are filled on 3,595
                       of the 21,737 crop rows, every one of them unfinished: no finished
                       row records a quantity.  So this wave is Mali's shape (GH #1437),
                       not Togo's.  The file is unique on (household, field, parcel,
                       crop), so no observation level is needed.
  s16d_me_civ2021.dta  one record per (household, crop), no plot, 14,171 of them, with no
                       exact repeats (unlike Togo's).  A SALE is a record with a positive
                       s16dq05a or s16dq06: 8,467.

SALES take option (d), as ruled for Mali 2021-22 (GH #824, @ligon 2026-11-03).  One
harvest row for the crop: the sale goes on it, keyed
CotedIvoire::crop_production::hh-crop-sale-single-row.  More: one row with plot_id =
'Farm-level', u = 'Unknown'.  No sale is split, summed or dropped.

THE HARVEST takes the rule ruled for Mali (GH #1437, @ligon 2026-11-04).  A recorded
quantity on an unfinished row (s16cq11 = Non) is served as recorded, keyed
CotedIvoire::crop_production::harvest-to-date: a part of the season.  Where every s16c
row of a household's crop is finished, none records a quantity, and the s16d dispositions
share one unit, their sum is the harvest, keyed
CotedIvoire::crop_production::harvest-from-dispositions, on the single row or on the crop's
one Farm-level row.  A crop whose dispositions span units stays NA.

u is s16cq16b; a missing unit is `Unknown`.  s16cq16c and s16dq05c are totals in kg, as in
Mali, and not served.  s16d's unit columns label codes 1 to 7 and hold other codes of
section 7B's list: mapping.s16d_units.

condition is s16cq16d, read by code through this wave's own five-code table, with the
answer as recorded in condition_native (GH #1436).  The file states it on the rows with a
recorded quantity, so every other row carries `unknown_condition` and NA, and that includes
a derived harvest, whose dispositions have states of their own in s16d, not read here.

Crop codes 56 to 60 (3,946 rows) have no value label and are served as their numbers:
_/CONTENTS.org, "2021-22 wired".
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.ehcvm import (derive_hh_crop_harvest_total, derive_hh_crop_sale_attachment,
                                DISPOSITIONS as DISPOSITION_VARIABLES, is_crop_sale,
                                served_unit_labels,
                                stamp_harvest_to_date)
import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import (KEY_HARVEST_DISPOSITIONS, KEY_HARVEST_TO_DATE, KEY_HH_CROP_SALE, crop_labels,
                     household_ids, plot_ids, s16d_units)

UNIT_NA = 'Unknown'
CONDITION_NA = 'unknown_condition'


def _units(series):
    """Each unit label as the API serves it; NA stays NA."""
    raw = series.astype('string').str.strip()
    known = raw.dropna().unique()
    served = dict(zip(known, served_unit_labels('CotedIvoire', known)))
    return raw.map(served).astype('string')


def _conditions(codes):
    """The canonical token of each s16cq16d code.  Strict: a missing table must not
    serve every row as the sentinel."""
    table = tools.get_categorical_mapping(
        tablename='harmonize_crop_condition_ehcvm2', idxvars='Code', dirs=['./'],
        **{'Preferred Label': 'Preferred Label'})
    table = {int(code): token for code, token in table.items()}
    assert sorted(table) == list(range(1, 6)), f'harmonize_crop_condition_ehcvm2: {table}'
    return pd.to_numeric(codes, errors='coerce').astype('Int64').map(table).astype('string')


src = get_dataframe('../Data/s16c_me_civ2021.dta', convert_categoricals=True)
codes = get_dataframe('../Data/s16c_me_civ2021.dta', convert_categoricals=False)
keep = codes['s16cq04'].notna().values
src, codes = src[keep], codes[keep]
assert not codes[['s16cq02', 's16cq03']].isna().any(axis=None), 'a crop row with no plot'
harvest = pd.DataFrame({
    'i': household_ids(codes).astype('string').values,
    'plot_id': plot_ids(codes, 's16cq02', 's16cq03').values,
    'crop': crop_labels(src['s16cq04']).values,
    'u': _units(src['s16cq16b']).values,
    'Quantity': pd.to_numeric(codes['s16cq16a'], errors='coerce').values,
    'intercropped': src['s16cq07'].astype('string').str.strip().map(
        {'Association de cultures': True, 'Pure': False}).astype('boolean').values,
    'finished': src['s16cq11'].astype('string').values,
    'condition': _conditions(codes['s16cq16d']).values,
    'condition_native': src['s16cq16d'].astype('string').str.strip().values,
})

records = get_dataframe('../Data/s16d_me_civ2021.dta', convert_categoricals=True)
record_codes = get_dataframe('../Data/s16d_me_civ2021.dta', convert_categoricals=False)
keep = record_codes['s16dq01'].notna().values
records, record_codes = records[keep], record_codes[keep]
assert not record_codes.duplicated(['grappe', 'menage', 's16dq01']).any(), 's16d: two records of a crop'
record_ids = household_ids(record_codes).astype('string').values
record_crops = crop_labels(records['s16dq01']).values

sold = is_crop_sale(record_codes['s16dq05a'], record_codes['s16dq06']).values
sales = pd.DataFrame({
    'i': record_ids[sold],
    'crop': record_crops[sold],
    'Quantity_sold': pd.to_numeric(record_codes['s16dq05a'], errors='coerce').values[sold],
    'Unit_sold': s16d_units(record_codes['s16dq05b']).values[sold],
    'Value_sold': pd.to_numeric(record_codes['s16dq06'], errors='coerce').values[sold],
})

df = derive_hh_crop_sale_attachment(harvest, sales, KEY_HH_CROP_SALE)

# Conservation: every sale is on exactly one served row, unchanged, and no harvest moved.
on_a_row = df[df['Quantity_sold'].notna() | df['Value_sold'].notna()]
assert len(on_a_row) == len(sales), 'crop_production 2021-22: a sale moved or doubled'
assert on_a_row['Value_sold'].sum() == sales['Value_sold'].sum()
assert on_a_row['Quantity_sold'].sum() == sales['Quantity_sold'].sum()

recorded = pd.to_numeric(df['Quantity'], errors='coerce').astype('float64').sum()
df = stamp_harvest_to_date(df, KEY_HARVEST_TO_DATE)

dispositions = pd.DataFrame({'i': record_ids, 'crop': record_crops})
for var in DISPOSITION_VARIABLES:
    dispositions[f'{var}a'] = pd.to_numeric(record_codes[f'{var}a'], errors='coerce').values
    dispositions[f'{var}b'] = s16d_units(record_codes[f'{var}b']).values
df = derive_hh_crop_harvest_total(df, dispositions, KEY_HARVEST_DISPOSITIONS).drop(columns='finished')

# Nothing recorded moved: every other Quantity is the file's.
derived = df['Derivation'].astype('string').str.contains(
    'harvest-from-dispositions', regex=False).fillna(False).astype(bool)
assert abs(df.loc[~derived, 'Quantity'].sum() - recorded) < 1e-6 * recorded, (
    'crop_production 2021-22: a harvest moved')

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
