"""Build community_prices for Malawi IHPS 2013-14 (GAP C).

Item-level (t, v, j, u) reference-price feature from the Community
questionnaire Module K ("Reference prices of selected items").  Source:
  * COM_MOD_K_13 -- one row per (EA, item).  ea_id is the cluster id
    (== sample().v keyspace), com_ck00a the item code as the STRING
    'CK<n>' (CK1, CK22, ...; stripped to the bare 1-51 integer code by
    malawi._strip_ck), com_ck00c availability (1=Yes/2=No), com_ck00b1 the
    surveyed price (MK), com_ck00b2 the number of units, com_ck00b3 the
    unit code (1-21).

The IHPS price list is a reduced subset (CK1, CK3, CK4, CK22-CK50 -- it
omits the perishable foods CK5-CK21 and the ganyu wage CK51).  v, j, u and
column conventions match the IHS3/IHS5 waves; see
lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _price_block, assemble_community_prices


WAVE = '2013-14'

ck = get_dataframe('../Data/COM_MOD_K_13.dta', convert_categoricals=False)

piece = _price_block(ck, ea_col='ea_id', item_col='com_ck00a',
                     price_col='com_ck00b1', nunits_col='com_ck00b2',
                     unit_col='com_ck00b3', avail_col='com_ck00c',
                     t=WAVE, strip_ck=True)

df = assemble_community_prices(WAVE, [piece])

to_parquet(df, 'community_prices.parquet')
