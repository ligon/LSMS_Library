"""Concatenate wave-level community_prices parquets for GhanaLSS (GH #562, 3a).

Each wave's ``GhanaLSS/<wave>/_/community_prices.py`` builds that wave's OWN
market/community price survey into the canonical shape documented in
``_/glss_prices.py``: index (t, v, j, u, obs), reported columns Price,
NumberOfUnits, Description.  This script only concatenates them.

Six of the seven waves ship a price file (PRICE.DAT for GLSS1/2, G3PRICE /
G4PRICE for GLSS3/4, PRICES/price_sec* for GLSS6, g7price for GLSS7).
2005-06 (GLSS5) fielded the same price questionnaire (G5QPrice.pdf) but no
price dataset is distributed -- adjudicated ``asked-not-distributed`` in
.coder/coverage/absent_verdicts.csv; absent here (reported, not imputed).

v is the price survey's own cluster id on the SAME keyspace as sample().v
and is NATIVE in the index: there is no household i, so the framework's
_join_v_from_sample does not apply.  No cross-wave id_walk is needed --
clusters are wave-specific (1xxx ... 7xxxx, CONTENTS.org).
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['1987-88', '1988-89', '1991-92', '1998-99', '2012-13', '2016-17']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/community_prices.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave parquet not built yet (the framework's wave path builds it on
        # demand; a direct country build may run first).
        continue
    pieces.append(df)

assert pieces, "community_prices: no wave-level parquets found"

p = pd.concat(pieces)
assert p.index.is_unique, "community_prices: (t,v,j,u,obs) not unique after concat"

to_parquet(p, '../var/community_prices.parquet')
