"""Concatenate wave-level food_coping data for Ethiopia (GH #332, Family B).

Each wave's ``Ethiopia/<wave>/_/food_coping.py`` produces a parquet with
index ``(t, i, Strategy)`` and column ``Days`` (0-7), reshaped from the ESS
Section 7 Coping Strategies Index battery (hh_s7q02_{a..h}).  Only the rCSI
day-count waves are wired: W1 (2011-12), W2 (2013-14), W3 (2015-16).

W4 (2018-19) §8 is a different rCSI/coping battery (s8q02a..h) and W5
(2021-22) §8 is FAO FIES (wired separately as ``food_security``); neither
declares ``food_coping``, so they are skipped before reading below.

The wave parquets carry the wave-native household id matching ``sample().i``
(``household_id2`` for W2/W3, ``household_id`` for W1).  ``id_walk`` converts
to the panel-canonical id scheme; it is idempotent (sets
``attrs['id_converted']``) so ``_finalize_result`` will not re-apply it.
"""
import json
import os

import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import data_root, get_dataframe, id_walk, to_parquet


country = Country('Ethiopia', preload_panel_ids=False)
pieces = []
seen_folders = set()
for t in country.waves:
    wave = country[t]
    folder = wave.wave_folder
    if folder in seen_folders:
        continue
    seen_folders.add(folder)
    # Output presence is not a declaration: missing declared outputs must
    # reach the reader, while genuinely unwired folders remain skipped.
    if 'food_coping' not in wave.data_scheme:
        continue
    # The wave scripts write their parquet under data_root() (to_parquet
    # redirects there), so read it back from that canonical location.
    # Reading the in-tree relative path via get_dataframe would trigger a
    # slow DVCFS sidecar-tree walk when the file is absent in-tree.
    fn = os.path.join(str(data_root('Ethiopia')), folder, '_', 'food_coping.parquet')
    df = get_dataframe(fn)
    pieces.append(df)

assert pieces, "food_coping: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/food_coping.parquet')
