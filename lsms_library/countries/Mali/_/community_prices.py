"""Concatenate wave-level community_prices for Mali (GAP C; parity loop).

Each EACI wave's ``Mali/<wave>/_/community_prices.py`` writes a parquet
indexed by ``(t, v, j, u)`` with the reported item-level community price
columns (Price, Quantity).  The community price questionnaire lives in the
2014-15 EACI wave ONLY (EACIS04_p1/p2); the 2017-18 EACI wave dropped the
community questionnaire and the EHCVM waves (2018-19, 2021-22) collect prices
only at the region x milieu IHPC grain (no grappe key), so they contribute
nothing.

``v`` is NATIVE (the community questionnaire's grappe, the same keyspace as
``sample().v``) — the framework's ``_join_v_from_sample`` is for tables that
carry household ``i`` and does NOT fire here.  ``id_walk`` is left to
``_finalize_result``, matching the crop_production / plot_features pattern.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['2014-15', '2017-18', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/community_prices.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired (no .py script / no parquet); DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "community_prices: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, v, j, u) after concat"

to_parquet(p, '../var/community_prices.parquet')
