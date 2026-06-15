"""Concatenate wave-level community_prices for Tanzania NPS
(parity-loop GAP C -- OURS-ONLY; maintainer priority).

Each buildable wave's ``Tanzania/<wave>/_/community_prices.py`` produces a
parquet at grain (t, v, j, u) with the single reported column Price (the
village-market unit price).  This script concatenates the per-wave parquets.

There is NO id_walk here: the grain carries no household ``i`` (community
prices are a cluster-level instrument), and ``v`` is the community
questionnaire's OWN native cluster id (interview__key), not a household-panel
id, so the panel id-remap does not apply.

Only 2019-20 (NPS-SDD Extended Panel) and 2020-21 (NPS Y5 Refresh Panel) carry
a community price module: the 2008-15 multi-round folder has only the household
upd4_hh_* modules on disk -- no community / CM_SEC_F source file -- so those
four NPS rounds have no community prices to wire (same source-availability
deferral as plot_features / crop_production / livestock, GH #167, for a
different reason: here the community questionnaire files were not retained in
the multi-round panel release, not the agriculture ones).
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2019-20', '2020-21']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/community_prices.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built.  DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "community_prices: no wave-level parquets found"

p = pd.concat(pieces)
assert p.index.is_unique, "community_prices: (t,v,j,u) not unique after concat"

to_parquet(p, '../var/community_prices.parquet')
