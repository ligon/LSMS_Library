"""Concatenate wave-level community_prices for Niger (GAP C, item-level).

Each wave's ``Niger/<wave>/_/community_prices.py`` writes a parquet indexed by
``(t, v, j, u, obs)`` carrying the REPORTED community-questionnaire surveyed
price (and the native price-per-quantity basis).  This script concatenates the
waves that HAVE an EA-level community price module: only the two ECVMA waves
(2011-12, 2014-15).  The EHCVM waves (2018-19, 2021-22) ship no cluster-level
community price questionnaire — their region/market price survey
(ehcvm_prix, not at the grappe grain) and the ehcvm_nsu median are NOT this
feature — so they have no wave script / parquet and are silently skipped here.

``v`` is the community questionnaire's EA (grappe) formatted into the sample()
v keyspace; it IS in the index (cluster-level feature, no household i), so the
framework does NOT join it from sample().  No ``id_walk`` here; the framework
runs it in ``_finalize_result`` on every read.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/community_prices.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for community_prices (no .py script / no parquet —
        # the EHCVM waves have no EA-level community price module).  DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, 'community_prices: no wave-level parquets found'

p = pd.concat(pieces)

to_parquet(p, '../var/community_prices.parquet')
