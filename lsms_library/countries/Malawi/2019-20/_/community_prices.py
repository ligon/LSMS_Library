"""Build community_prices for Malawi IHS5 2019-20 (GAP C).

Item-level (t, v, j, u) reference-price feature from the Community
questionnaire Module CK ("Reference prices of selected items", codes
1-51).  IHS5 ships a Cross_Sectional half (bare ea_id) and a Panel half
(ea_id), concatenated into the single 2019-20 wave -- the two halves carry
DISJOINT EA ids (the panel EAs are a subset visited in both rounds).

Source columns are renamed in IHS5: com_ck00a item code (1-51), cka
availability (1=Yes/2=No), ckb surveyed price (MK), ckc number of units
(a free-text string -- coerced to numeric, junk -> NaN), ckd unit code
(1-21).  ea_id is the cluster id (== sample().v keyspace).

v, j, u and column conventions match the IHS3/IHPS waves; v is NATIVE
(no household i).  See lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _price_block, assemble_community_prices


WAVE = '2019-20'

pieces = []
for fn in ('../Data/Cross_Sectional/com_ck.dta',
           '../Data/Panel/com_ck_19.dta'):
    ck = get_dataframe(fn, convert_categoricals=False)
    pieces.append(
        _price_block(ck, ea_col='ea_id', item_col='com_ck00a',
                     price_col='ckb', nunits_col='ckc',
                     unit_col='ckd', avail_col='cka', t=WAVE))

df = assemble_community_prices(WAVE, pieces)

to_parquet(df, 'community_prices.parquet')
