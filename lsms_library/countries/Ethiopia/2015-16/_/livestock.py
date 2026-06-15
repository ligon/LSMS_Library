#!/usr/bin/env python
"""Build livestock for Ethiopia ESS 2015-16 (Wave 3; GAP 4).

Item-level livestock at (t, i, animal); see Ethiopia/_/livestock.py.

W3 splits the §8 roster across two files joined on (holder_id, ls_code):
  - sect8_1_ls_w3.dta  current head count:
      animal code  ls_code           (1=Bulls .. 23=Bee Colony; finer scheme)
      head count   ls_sec_8_1q01     (currently own and keep)
  - sect8_2_ls_w3.dta  transactions (per the same ls_code):
      head acquired ls_sec_8_2aq04   (bought alive in last 12 months)
      head sold     ls_sec_8_2aq13   (sold alive in last 12 months)
      sale value    ls_sec_8_2aq14   (total income from sales, Birr)
The finer ls_code (Bulls/Oxen/Cows/... ; Goats-He/She/Kids; etc.) folds to
canonical species via the wave-keyed harmonize_species table and is summed
within household.  No current herd value is asked -> Value = reported sale
income (the only monetary §8 field).
i = household_id2 (matches sample().i / plot_features for W3).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import livestock_for_wave


count = get_dataframe('../Data/sect8_1_ls_w3.dta', convert_categoricals=False)
txn   = get_dataframe('../Data/sect8_2_ls_w3.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id2', holder_id='holder_id',
    animal_code='ls_code',
    head_count='ls_sec_8_1q01',
    t_animal_code='ls_code', t_holder_id='holder_id',
    head_acquired='ls_sec_8_2aq04',
    head_sold='ls_sec_8_2aq13',
    sale_value='ls_sec_8_2aq14',
)

df = livestock_for_wave('2015-16', count, txn, colmap)

assert len(df) > 0, "livestock 2015-16 produced no rows"
to_parquet(df, 'livestock.parquet')
