#!/usr/bin/env python
"""
Compile data on reported household enterprise income into a (t, i) panel.

Each wave's ``enterprise_income.parquet`` carries one row per household
with the six enterprise columns, so concatenating the waves under a ``t``
key builds the panel directly.

Do NOT go back to the older ``stack()`` / ``pd.DataFrame(dict)`` /
``stack()`` / ``unstack()`` round trip -- it is the same defect fixed in
``earnings.py`` for GH #772.  Under pandas 3 ``DataFrame.stack`` no longer
drops all-null rows (the legacy implementation did, via its
``dropna=True`` default), so aligning the waves as a dict of Series
materialised the full (union of household ids) x (waves) cross product:
*78,040 rows, 64,105 of them entirely null* -- 82.1% fabricated -- against
13,935 real household-wave observations.

That divergence was reachable in normal use.  ``income.py`` lists this
table as a Makefile prerequisite, so building ``income`` regenerates
``var/enterprise_income.parquet`` through this script, while a direct
``Country('Uganda').enterprise_income()`` call on a cache without it goes
through the framework's wave concatenation instead and returns 13,935.
Same API call, two different answers depending on cache provenance.

``.squeeze()`` is also gone: it returns a *scalar* for a length-1 Series,
which ``pd.DataFrame(dict)`` then broadcasts across every row.
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
    df = get_dataframe('../'+t+'/_/enterprise_income.parquet')
    # The wave scripts group on the household id but label that index 'j',
    # which in this library means a good, not a household.  Normalise here
    # so a stale wave parquet still builds and id_walk is not left to guess.
    df.index.name = 'i'
    x[t] = df

x = pd.concat(x, names=['t', 'i'])

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

to_parquet(x, '../var/enterprise_income.parquet')
