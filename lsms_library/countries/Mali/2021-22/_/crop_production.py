"""Build crop_production for Mali EHCVM 2021-22 (GH #824, option (d)).

The instrument splits the module 2018-19 kept together:

  s16c_me_mli2021.dta  one row per (field, parcel, crop).  Quantity = s16cq16a,
                       u = s16cq16b, filled on 309 of 7,247 rows: 287 of 2,010
                       unfinished harvests and 22 of 5,237 finished ones.
  s16d_me_mli2021.dta  one row per (household, crop), no plot.  Quantity_sold =
                       s16dq05a, Unit_sold = s16dq05b, Value_sold = s16dq06.
                       Unlike Niger's, this file does not repeat its lines: 5,068
                       rows on 5,068 keys.

A SALE is a record with a positive s16dq05a or s16dq06: 1,140, of 5,068.  The
other 3,928 records answer s16dq04 'Non' and hold no quantity, unit or value,
so they serve nothing and no zero is written.

A sale goes on the household's single harvest row for the crop where there is
exactly one (1,070; the Niger 2021-22 rule after GH #1360, and the key
Mali::crop_production::hh-crop-sale-single-row).  Otherwise it
goes on ONE row with plot_id = 'Farm-level' (70: 58 of crops on two rows and 12
on three or more).  No sale is split, summed or dropped.  s16dq05c is an equivalent
TOTAL in kg, so KgFactor_sold stays NA.

THE HARVEST (GH #1437, ruling 2026-11-04).  The instrument sends a finished harvest
to the quantity, and the file has it on 22 of 5,237.  Where every s16c row of a
household's crop is finished, none records a quantity, and the s16d dispositions
(consumed, given, sold, in stock) share one unit, their sum is the harvest, keyed
Mali::crop_production::harvest-from-dispositions, on the single row or on the crop's
one Farm-level row.  A crop whose dispositions span units stays NA: no factor exists.
A recorded Quantity on an unfinished row is served as recorded, keyed
Mali::crop_production::harvest-to-date, a window and not a season.

THE STATE (GH #1436).  condition is s16cq16d, "Etat du produit", by code through
harmonize_crop_condition_ehcvm1, and condition_native the answer as recorded.  The file
states it on exactly the 309 rows with a Quantity.  Every other row carries the sentinel,
and that includes a derived harvest: its dispositions have states of their own in s16d,
not read here, and they fold differently on 166 of the 3,360 totals (derivations.yml).

See mali.py, "crop_production: the EHCVM waves".
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import (derive_hh_crop_harvest_total, derive_hh_crop_sale_attachment,
                  ehcvm_crop_condition_labels, ehcvm_crop_conditions,
                  ehcvm_crop_labels, ehcvm_dispositions, ehcvm_household_ids,
                  ehcvm_intercropped, ehcvm_is_sale, ehcvm_plot_ids, ehcvm_unit_labels,
                  finish_ehcvm_crop_production, stamp_harvest_to_date)

src = get_dataframe('../Data/s16c_me_mli2021.dta', convert_categoricals=True)
src_codes = get_dataframe('../Data/s16c_me_mli2021.dta', convert_categoricals=False)
harvest = pd.DataFrame({
    'i':            ehcvm_household_ids(src).astype('string').values,
    'plot_id':      ehcvm_plot_ids(src).values,
    'crop':         ehcvm_crop_labels(src['s16cq04']).values,
    'u':            ehcvm_unit_labels(src['s16cq16b']).values,
    'condition':    ehcvm_crop_conditions(src_codes['s16cq16d']).values,
    'condition_native': ehcvm_crop_condition_labels(src['s16cq16d']).values,
    'Quantity':     src['s16cq16a'].values,
    'intercropped': ehcvm_intercropped(src['s16cq07']).values,
    'finished':     src['s16cq11'].values,
})

records = get_dataframe('../Data/s16d_me_mli2021.dta', convert_categoricals=True)
sold = records[ehcvm_is_sale(records['s16dq05a'], records['s16dq06'])]
sales = pd.DataFrame({
    'i':             ehcvm_household_ids(sold).astype('string').values,
    'crop':          ehcvm_crop_labels(sold['s16dq01']).values,
    'Quantity_sold': pd.to_numeric(sold['s16dq05a'], errors='coerce').values,
    'Unit_sold':     ehcvm_unit_labels(sold['s16dq05b']).values,
    'Value_sold':    pd.to_numeric(sold['s16dq06'], errors='coerce').values,
})

df = derive_hh_crop_sale_attachment(harvest, sales)

# Conservation: every sale record is on exactly one served row, unchanged.
on_a_row = df[df['Quantity_sold'].notna() | df['Value_sold'].notna()]
assert len(on_a_row) == len(sales), 'crop_production 2021-22: a sale moved or doubled'
assert on_a_row['Value_sold'].sum() == sales['Value_sold'].sum()
assert on_a_row['Quantity_sold'].sum() == sales['Quantity_sold'].sum()

recorded = pd.to_numeric(df['Quantity'], errors='coerce').astype('float64').sum()
df = stamp_harvest_to_date(df)
df = derive_hh_crop_harvest_total(df, ehcvm_dispositions(records))

# Nothing recorded moved: every other Quantity is the file's.
derived = df['Derivation'].astype('string').str.contains(
    'harvest-from-dispositions', regex=False).fillna(False)
assert df.loc[~derived, 'Quantity'].sum() == recorded, 'crop_production 2021-22: a recorded harvest moved'

df = finish_ehcvm_crop_production(df, '2021-22')

assert len(df) >= len(src), 'crop_production 2021-22 lost a source row'
to_parquet(df, 'crop_production.parquet')
