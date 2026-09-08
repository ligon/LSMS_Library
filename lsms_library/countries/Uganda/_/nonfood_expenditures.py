#!/usr/bin/env python
"""Concatenate wave-level nonfood_expenditures data for Uganda.

Wave-level scripts (each ``Uganda/<wave>/_/nonfood_expenditures.py``) call
``uganda.nonfood_expenditures()``, stamp ``t``, and produce canonical long
parquets with index ``[t, i, j]`` and a single column ``Expenditure``.  This
script concatenates them across waves and applies cross-wave ``id_walk`` --
mirroring the sibling ``Uganda/_/food_acquired.py`` exactly.

WHAT THIS USED TO DO, AND WHY IT STOPPED (GH #817).  It read the (then wide)
wave parquets and pivoted them into ONE item-by-household matrix:
``stack('i') -> unstack('i') -> .T.groupby('i').sum().T -> fillna(0)``, then
stamped ``x['m'] = 'Uganda'`` into the index.  Three things were wrong with
that, none of them survey facts:

  * the wide shape is not a table the rest of the library can consume --
    ``labels=``, ``currency=``, the coverage grader's ``j`` checks and
    ``Feature()`` assembly are all written against a long ``j`` level, and
    Togo's ``nonfood_expenditures`` (long) could not be stacked with it;
  * ``fillna(0)`` fabricated an expenditure of 0 for every (household, item)
    pair the survey never recorded, so "not reported" and "reported zero"
    became the same number (the sparsity convention
    ``transformations.food_expenditures_from_acquired`` documents for food);
  * ``m`` must not be baked into a cached parquet -- it is added on demand by
    ``_add_market_index()`` when the caller passes ``market=`` (CLAUDE.md,
    "``other_features`` is obsolete").

The ``Aggregate Label`` rename that sat here commented out went with them: it
was scaffolding for an aggregation that never landed, and aggregating ``j`` is
now the ``labels=`` kwarg's job, not this script's.
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import Waves, id_walk


p = []
for t in Waves.keys():
    # Wave parquet already has canonical index [t, i, j] and the single
    # column [Expenditure] from uganda.nonfood_expenditures().
    p.append(get_dataframe('../' + t + '/_/nonfood_expenditures.parquet'))

p = pd.concat(p)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

# id_walk remaps household ids WITHIN each wave, so two distinct old ids can
# land on one new id and make (t, i, j) non-unique -- a collision the wide
# shape used to hide by overwriting a column.  Core would collapse it with
# groupby().first() and destroy the smaller row (GH #323), so say so here
# instead.
dups = p.index.duplicated().sum()
assert not dups, (
    f'{dups} duplicate (t, i, j) rows after id_walk: two household ids in one '
    f'wave map to the same updated id.  Fix the mapping in updated_ids.json; '
    f'do NOT reduce here.')

to_parquet(p, '../var/nonfood_expenditures.parquet')
