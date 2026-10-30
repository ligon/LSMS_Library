"""Concatenate wave-level food_coping parquets for Malawi (GH #332).

Each buildable wave's ``Malawi/<wave>/_/food_coping.py`` produces a
parquet indexed (t, i, Strategy) with the integer ``Days`` column.  This
script concatenates them.  Cross-wave id_walk (panel-id chaining) and the
join of the cluster id ``v`` are applied by the framework at API time in
_finalize_result -- as for plot_features, no id_walk is run here.

2004-05 (IHS2) is ABSENT: its Module H is a 3-day food-consumption recall
(item-level), not the rCSI coping battery introduced in IHS3.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet


country = Country('Malawi')
assert 'food_coping' in country.data_scheme, "food_coping: not declared for Malawi"

pieces = []
# Reuse actual table discovery, including native scripts and YAML declarations.
# Ledger: malawi-compiler-declarations-20261028.md §§2–5.
for t in country.waves:
    wave = country[t]
    if 'food_coping' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/food_coping.parquet'
    # A declared output that cannot be read is a failed build, not absence.
    df = get_dataframe(fn)
    pieces.append(df)

assert pieces, "food_coping: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/food_coping.parquet')
