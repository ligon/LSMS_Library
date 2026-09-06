#!/usr/bin/env python
"""
Compile data on reported labor earnings into a (t, i) panel.

Each wave's ``earnings.parquet`` is a single ``Earnings`` column indexed
by household, so concatenating the waves under a ``t`` key builds the
panel directly.

Do NOT go back to the older ``stack()`` / ``pd.DataFrame(dict)`` /
``stack()`` round trip (GH #772).  It cost three defects at once:

  * the measure came out named ``earnings`` -- not the ``Earnings`` that
    ``data_scheme.yml`` declares and that ``lsms_library.currency``
    registers as this table's monetary column -- and the stacked column
    axis was left behind as a stray ``level_1`` column;

  * under pandas 3 ``DataFrame.stack`` no longer drops all-null rows (the
    legacy implementation did, via its ``dropna=True`` default), so
    aligning the waves as a dict of Series materialised the full
    (union of household ids) x (waves) cross product: 129,160 rows, i.e.
    16,145 ids x 8 waves, against 22,447 real household-wave observations.
    Every household got a NaN row in every wave it was not surveyed in.
    Two committed artefacts date this precisely: the coverage snapshot
    taken 2026-06-26 records per-wave slices of 1467 / 2889 / 2618 / 2805
    / 3119 / 3305 / 3180 / 3064 -- the wave parquet row counts, summing to
    22,447 -- while ``tests/fixtures/uganda_baseline.json``, regenerated
    under the pandas 3 floor (PR #710, 2026-08-22), records 129,160;

  * ``.squeeze()`` collapses a length-1 Series to a *scalar*, so a wave
    yielding exactly one household would have broadcast that one value
    across every household in the panel.
"""
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe

import sys
import json
import pandas as pd

from uganda import Waves, id_walk

x = {}

for t in Waves:
    print(t, file=sys.stderr)
    df = get_dataframe('../'+t+'/_/earnings.parquet')
    # The wave scripts group on the household id; older ones labelled
    # that index 'j', which in this library means a good, not a
    # household.  Normalise here so a stale wave parquet still builds.
    df.index.name = 'i'
    x[t] = df['Earnings']

x = pd.concat(x, names=['t', 'i']).to_frame('Earnings')

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

to_parquet(x, '../var/earnings.parquet')
