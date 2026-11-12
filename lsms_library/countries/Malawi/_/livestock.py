"""Concatenate wave-level livestock parquets for Malawi (GAP 4).

Each buildable wave's ``Malawi/<wave>/_/livestock.py`` produces a parquet
indexed (t, i, animal) with the canonical item-level columns (HeadCount,
HeadAcquired, HeadSold, ValuePerAnimal, HeadCount12MonthsAgo, RaisedOrOwned).  This
script concatenates them.
The country cache stores household IDs after the existing single-pass id_walk
(GH #1288 / CLAUDE "Panel ID Transitive Chains"). The unchanged writer retains
id_converted, so _finalize_result does not walk it again. Other Malawi country
compilers retain their own raw-ID contracts. ``v`` is NOT joined: 'livestock' is in the framework
_no_v_join set (a household-level holding, not a cluster-keyed roster), so
the grain stays (t, i, animal) with no cluster level.

Only the four IHS3+ IHPS waves are buildable (2010-11, 2013-14, 2016-17,
2019-20) -- the same waves as crop_production / plot_inputs / plot_features,
and the World Bank Plotcrop panel's waves 1-4.  2004-05 (IHS2) is DEFERRED:
its agricultural file is household-level aggregated, with no Module R
livestock roster.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, id_walk, to_parquet


country = Country('Malawi')
assert 'livestock' in country.data_scheme, "livestock: not declared for Malawi"

pieces = []
# Reuse actual table discovery, including native scripts and YAML declarations.
# Ledger: malawi-compiler-declarations-20261028.md §§2–5.
for t in country.waves:
    wave = country[t]
    if 'livestock' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/livestock.parquet'
    # A declared output that cannot be read is a failed build, not absence.
    df = get_dataframe(fn)
    pieces.append(df)

assert pieces, "livestock: no wave-level parquets found"

p = pd.concat(pieces)

# Complete known-native input, before the unchanged writer (ledger1288 §§3–5).
# Keep the ordinary no-map/already-converted behavior; never invent a map.
if 'i' in p.index.names and not p.attrs.get('id_converted'):
    updated_ids = country.updated_ids
    if updated_ids is not None:
        p = id_walk(p, updated_ids, state='raw')

to_parquet(p, '../var/livestock.parquet')
