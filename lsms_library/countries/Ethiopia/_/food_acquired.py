"""Concatenate wave-level food_acquired data for Ethiopia.

Wave-level scripts (each ``Ethiopia/<wave>/_/food_acquired.py``) call
``ethiopia.food_acquired()``, which delegates to
``food_acquired_to_canonical()`` and produces canonical-form parquets
with index ``[t, i, j, u, s]`` (``s in {'purchased', 'produced'}``) and
columns ``[Quantity, Expenditure]`` (Phase 3 of GH #169).  This script
just concatenates them across waves and applies cross-wave id_walk and
the food-label rename.

The pre-Phase-3 implementation expected wide-form wave parquets with
``units`` / ``units_purchased`` columns and did
``df['t'] = t`` followed by ``groupby(['j','t','i','units','units_purchased'])``;
that broke once the wave-level reshape moved ``t`` into the index and
collapsed the unit columns.  Replaced 2026-05-07.
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet

import pandas as pd
import numpy as np
from ethiopia import change_id, Waves, harmonized_food_labels


def fix_food_labels():
    # Unit #0 (2026-06-14): food labels are now resolved to canonical
    # Preferred Labels at the WAVE-script level inside ethiopia.food_acquired
    # (via harmonize_food_union_map), so this country-level rename is an
    # idempotent no-op for the already-resolved waves.  We still build the
    # union map from the migrated harmonize_food table (in
    # categorical_mapping.org) so any wave parquet predating the migration
    # gets harmonized on concat.  The raw food_items.org reads are gone.
    D = {}
    for w in Waves.keys():
        D.update(harmonized_food_labels(fn='./categorical_mapping.org', key=w))
    return D


def id_walk(df, wave, waves):
    use_waves = list(waves.keys())
    T = use_waves.index(wave)
    for t in use_waves[T::-1]:
        if len(waves[t]):
            df = change_id(df, '../%s/Data/%s' % (t, waves[t][0]), *waves[t][1:])
        else:
            df = change_id(df)
    return df


p = []
for t in Waves.keys():
    df = get_dataframe('../' + t + '/_/food_acquired.parquet').squeeze()
    # Wave parquet already has canonical index [t, i, j, u, s] and
    # columns [Quantity, Expenditure] from food_acquired_to_canonical().
    df1 = id_walk(df, t, Waves)
    p.append(df1)

p = pd.concat(p)

# fix_food_labels() returns a {wave-specific code -> Preferred Label}
# dict; canonical level for food item codes is 'j'.
p = p.rename(index=fix_food_labels(), level='j')

to_parquet(p, '../var/food_acquired.parquet')
