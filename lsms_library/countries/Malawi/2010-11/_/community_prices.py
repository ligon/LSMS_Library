"""Build community_prices for Malawi IHS3 2010-11 (GAP C).

Item-level (t, v, j, u) reference-price feature from the Community
questionnaire Module CK ("Reference prices of selected items", codes
1-51).  Source (Full_Sample/Community):
  * com_ck -- one row per (EA, item).  ea_id is the community cluster id
    (== sample().v keyspace), com_ck00a the item code (1-51),
    com_ck00c the in-market availability (1=Yes/2=No), com_ck00b1 the
    surveyed price in MK, com_ck00b2 the number of units that price refers
    to, com_ck00b3 the unit code (1-21).

v = format_id(ea_id) (NATIVE -- there is no household i, so the framework's
_join_v_from_sample does not apply); j on harmonize_price_item ->
harmonize_food / harmonize_crop Preferred Labels; u on harmonize_price_unit
-> shared `u` Preferred Labels.  Reported columns only (Price,
NumberOfUnits, Available); per-unit price and cross-cluster medians are
transformations.  See lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _price_block, assemble_community_prices


WAVE = '2010-11'

ck = get_dataframe('../Data/Full_Sample/Community/com_ck.dta',
                   convert_categoricals=False)

piece = _price_block(ck, ea_col='ea_id', item_col='com_ck00a',
                     price_col='com_ck00b1', nunits_col='com_ck00b2',
                     unit_col='com_ck00b3', avail_col='com_ck00c', t=WAVE)

df = assemble_community_prices(WAVE, [piece])

to_parquet(df, 'community_prices.parquet')
