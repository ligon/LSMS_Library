#!/usr/bin/env python
"""Build livestock for Ethiopia ESS 2013-14 (Wave 2; GAP 4).

Item-level livestock at (t, i, animal); see the country-level
Ethiopia/_/livestock.py docstring and the 2011-12 wave script for the
construct.  W2 §8 roster (sect8a_ls_w2.dta) is structurally identical to
W1: a single long-format file (one row per holder x animal code) carrying
the count AND the transactions, with the same ls_s8aq* column names.

  - animal code   ls_s8aq00  (1=Cattle .. 14=Beehives)
  - head count    ls_s8aq13a (grand total owned, Total column)
  - head acquired ls_s8aq44a (purchased in last 12 months, Total)
  - head sold     ls_s8aq46a (sales in last 12 months, Total)
  - sale value    ls_s8aq60  (total value of sales, Birr)
i = household_id2 (matches sample().i / plot_features for W2).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import livestock_for_wave


count = get_dataframe('../Data/sect8a_ls_w2.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id2', holder_id='holder_id',
    animal_code='ls_s8aq00',
    head_count='ls_s8aq13a',
    head_acquired='ls_s8aq44a',
    head_sold='ls_s8aq46a',
    sale_value='ls_s8aq60',
)

df = livestock_for_wave('2013-14', count, None, colmap)

assert len(df) > 0, "livestock 2013-14 produced no rows"
to_parquet(df, 'livestock.parquet')
