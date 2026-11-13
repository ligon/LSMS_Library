"""Concatenate wave-level community_cluster_xwalk for Tanzania NPS (issue #113).

Each buildable wave's ``Tanzania/<wave>/_/community_cluster_xwalk.py`` produces
a parquet at grain (t, v) -- one row per community price cluster
(``interview__key``) -- mapping it to the household survey cluster
(``sample().v``) where the (region, ward) + interview-date match resolves
uniquely, with a region-level fallback otherwise.  This script concatenates the
per-wave parquets.

There is NO id_walk here: the grain carries no household ``i``, and ``v`` is the
community questionnaire's OWN native cluster id (interview__key), not a
household-panel id.  Only 2019-20 and 2020-21 carry a community questionnaire
(see ``community_prices`` for the same wave-scope note).
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2019-20', '2020-21']

# GH #1287: declarations belong to logical waves; readers use physical folders.
country = Country('Tanzania', preload_panel_ids=False)
declared_folders = {
    wave.wave_folder
    for wave in (country[year] for year in country.waves)
    if 'community_cluster_xwalk' in wave.data_scheme
}

pieces = []
for t in WAVES:
    if t not in declared_folders:
        continue
    fn = f'../{t}/_/community_cluster_xwalk.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as exc:
        # Each folder reaching this reader declares a producer (ledger #1287 §8).
        # Refuse partial country output when a declared build cannot be read.
        raise RuntimeError(
            f'Tanzania/community_cluster_xwalk: could not load declared folder {t} ({fn})'
        ) from exc
    pieces.append(df)

assert pieces, "community_cluster_xwalk: no wave-level parquets found"

p = pd.concat(pieces)
assert p.index.is_unique, "community_cluster_xwalk: (t, v) not unique after concat"

to_parquet(p, '../var/community_cluster_xwalk.parquet')
