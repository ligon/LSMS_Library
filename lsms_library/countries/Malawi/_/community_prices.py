"""Concatenate wave-level community_prices parquets for Malawi (GAP C).

Each buildable wave's ``Malawi/<wave>/_/community_prices.py`` produces a
parquet indexed (t, v, j, u) with the reported item-level columns
(Price, NumberOfUnits, Available) from the Community/Market questionnaire
Module CK reference-price section.  This script concatenates them.

Only three of the five waves collected the Module CK price section:
2010-11 (IHS3), 2013-14 (IHPS) and 2019-20 (IHS5).  2016-17 (IHS4) ran a
community questionnaire but DROPPED the price module (no com_ck file in
either the Cross_Sectional or Panel half), and 2004-05 (IHS2) has no
community price questionnaire at all -- both are absent here (reported, not
imputed).

v is the community EA cluster id on the SAME keyspace as sample().v (an
EA visited in exactly one community interview per wave), so a surveyed
price joins the households of that cluster.  There is no household i, so
the framework's _join_v_from_sample does not apply; v is declared in the
index by the wave scripts.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2010-11', '2013-14', '2019-20']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/community_prices.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built (DVC raises
        # PathMissingError here, not FileNotFoundError).
        continue
    pieces.append(df)

assert pieces, "community_prices: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/community_prices.parquet')
