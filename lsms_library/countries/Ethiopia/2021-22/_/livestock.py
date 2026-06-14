#!/usr/bin/env python
"""Build livestock for Ethiopia ESS 2021-22 (Wave 5; GAP 4).

Item-level livestock at (t, i, animal); see Ethiopia/_/livestock.py.

W5 splits the §8 roster across two files joined on (holder_id, ls_code),
structurally identical to W4:
  - sect8_1_ls_w5.dta  current head count:
      animal code  ls_code        (1=Bulls .. 16=Bee Colony)
      head count   ls_s8_1q01     (currently keep)
  - sect8_2_ls_w5.dta  transactions (per the same ls_code):
      head acquired ls_s8_2q04    (purchased alive in last 12 months)
      head sold     ls_s8_2q13    (sold alive in last 12 months)
      sale value    ls_s8_2q14    (total income from sales, Birr)
No current herd value asked -> Value = reported sale income.
i = household_id.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import livestock_for_wave


count = get_dataframe('../Data/sect8_1_ls_w5.dta', convert_categoricals=False)
txn   = get_dataframe('../Data/sect8_2_ls_w5.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', holder_id='holder_id',
    animal_code='ls_code',
    head_count='ls_s8_1q01',
    t_animal_code='ls_code', t_holder_id='holder_id',
    head_acquired='ls_s8_2q04',
    head_sold='ls_s8_2q13',
    sale_value='ls_s8_2q14',
)

df = livestock_for_wave('2021-22', count, txn, colmap)

assert len(df) > 0, "livestock 2021-22 produced no rows"
to_parquet(df, 'livestock.parquet')
