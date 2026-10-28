"""Concatenate wave-level plot_features parquets for Malawi (GH #167).

Each buildable wave's ``Malawi/<wave>/_/plot_features.py`` produces a
parquet indexed (t, i, plot_id) with the canonical columns.  This script
concatenates them.  Cross-wave id_walk (panel-id chaining) and the join
of the cluster id ``v`` are applied by the framework at API time in
_finalize_result; no id_walk is run in this compiler. Malawi's committed
country-level updated_ids.json is the existing framework input (GH #894);
this concatenator preserves the wave-native household IDs.

2004-05 (IHS2) is DEFERRED: it has no standard plot roster.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet


country = Country('Malawi')
assert 'plot_features' in country.data_scheme, "plot_features: not declared for Malawi"

pieces = []
# Reuse actual table discovery, including native scripts and YAML declarations.
# Ledger: malawi-compiler-declarations-20261028.md §§2–5.
for t in country.waves:
    wave = country[t]
    if 'plot_features' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/plot_features.parquet'
    # A declared output that cannot be read is a failed build, not absence.
    df = get_dataframe(fn)
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/plot_features.parquet')
