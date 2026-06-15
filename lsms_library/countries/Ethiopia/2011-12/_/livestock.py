#!/usr/bin/env python
"""Build livestock for Ethiopia ESS 2011-12 (Wave 1; GAP 4).

Item-level livestock at (t, i, animal).  One row per (household, canonical
species), carrying only REPORTED §8 roster fields.  This is the pre-collapse
roster the WB code reads then throws away down to a single engaged-y/n
binary (recode pp_saq13 ... collapse-max).

W1 source / specifics (sect8a_ls_w1.dta -- the long-format roster, one row
per holder x animal code):
  - animal code   ls_s8aq00  (1=Cattle .. 14=Beehives)
  - head count    ls_s8aq13a (grand total owned by the HH, Total column)
  - head acquired ls_s8aq44a (amount PURCHASED in last 12 months, Total)
  - head sold     ls_s8aq46a (amount of SALES in last 12 months, Total)
  - sale value    ls_s8aq60  (total value of sales in last 12 months, Birr)
All transaction columns are in the SAME file as the count (txn=None).
W1 reports NO current herd value -> Value carries the reported sale value,
which is the only monetary livestock field the §8 roster records this wave.
i = household_id (matches sample().i for W1).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import livestock_for_wave


count = get_dataframe('../Data/sect8a_ls_w1.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', holder_id='holder_id',
    animal_code='ls_s8aq00',
    head_count='ls_s8aq13a',
    head_acquired='ls_s8aq44a',
    head_sold='ls_s8aq46a',
    sale_value='ls_s8aq60',
)

df = livestock_for_wave('2011-12', count, None, colmap)

assert len(df) > 0, "livestock 2011-12 produced no rows"
to_parquet(df, 'livestock.parquet')
