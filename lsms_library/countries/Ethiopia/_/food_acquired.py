"""Concatenate wave-level food_acquired data for Ethiopia.

Wave-level scripts (each ``Ethiopia/<wave>/_/food_acquired.py``) call
``ethiopia.food_acquired()``, which delegates to
produces canonical-form parquets with index ``[t, i, j, u, s]``
(``s in {'purchased', 'produced', 'inkind'}``) and columns
``[Quantity, Expenditure]`` (Phase 3 of GH #169; the third source and the
direct reads landed with GH #874, 2026-09-12 -- before that the wave
scripts read only Q2/Q3/Q4 and delegated to
``build_transforms.food_acquired_to_canonical``, whose contract is the
``purchased = Quantity - Produced`` residual split).  This script just
concatenates them across waves and applies cross-wave id_walk and the
food-label rename.

The pre-Phase-3 implementation expected wide-form wave parquets with
``units`` / ``units_purchased`` columns and did
``df['t'] = t`` followed by ``groupby(['j','t','i','units','units_purchased'])``;
that broke once the wave-level reshape moved ``t`` into the index and
collapsed the unit columns.  Replaced 2026-05-07.
"""
import sys
sys.path.append('../../_/')
from lsms_library import Country
from lsms_library.local_tools import get_dataframe, id_walk, to_parquet

import pandas as pd
import numpy as np
from ethiopia import Waves, harmonized_food_labels


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


country = Country('Ethiopia', preload_panel_ids=False)
updated_ids = country.updated_ids
p = []
seen_folders = set()
for t in country.waves:
    wave = country[t]
    folder = wave.wave_folder
    if folder in seen_folders:
        continue
    seen_folders.add(folder)
    # Reuse actual Wave declarations, including YAML-only/null entries
    # (ledger #1287-ethiopia-food-country-selector §§2–4).
    if 'food_acquired' not in wave.data_scheme:
        continue
    fn = f'../{folder}/_/food_acquired.parquet'
    try:
        df = get_dataframe(fn).squeeze()
    except Exception as exc:
        raise RuntimeError(
            f'Ethiopia food_acquired: declared wave {t!r} '
            f'(folder {folder!r}) could not be read from {fn!r}'
        ) from exc
    # Wave parquet already has canonical index [t, i, j, u, s] and
    # columns [Quantity, Expenditure] from food_acquired_to_canonical().
    # Match the framework's native-contribution stage: household is i,
    # food is j. The obsolete local walk mapped the latter as household.
    # A source-native frame uses one substitution, never transitive closure;
    # an explicitly converted frame must not be walked again (GH #1245).
    if ('i' in df.index.names and not df.attrs.get('id_converted')
            and updated_ids is not None):
        df = id_walk(df, updated_ids, state='raw')
    p.append(df)

p = pd.concat(p)

# fix_food_labels() returns a {wave-specific code -> Preferred Label}
# dict; canonical level for food item codes is 'j'.
p = p.rename(index=fix_food_labels(), level='j')

to_parquet(p, '../var/food_acquired.parquet')
