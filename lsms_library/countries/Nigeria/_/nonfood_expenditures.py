#!/usr/bin/env python
"""Concatenate wave-level nonfood_expenditures data for Nigeria.

Wave-level scripts (each ``Nigeria/<wave>/_/nonfood_expenditures.py``) read the
post-planting (``sect8*``) and post-harvest (``sect11*``) non-food modules and
produce canonical long parquets with index ``[t, i, j]`` and a single column
``Expenditure``.  ``t`` is per ROUND (``2010Q3``, ``2011Q1``, ...) -- Nigeria is
post-planting / post-harvest and the two rounds in each wave directory are
never pooled (``.claude/skills/add-feature/pp-ph/SKILL.md``).  This script only
concatenates them.

WHAT THIS USED TO DO, AND WHY IT STOPPED (GH #817).  The wave parquets were
WIDE (one column per item, ``m`` in the index) and this script re-pivoted them
with ``x.columns.name = 'i'; x.T.groupby('i').sum().T`` to fold items that
shared a label.  The library cannot consume that shape: ``labels=``,
``currency=``, the coverage grader's ``j`` checks and ``Feature()`` assembly are
all written against a long ``j`` level, and Togo's ``nonfood_expenditures``
(already long) could not be stacked with it.  Folding items that share a label
now happens where it belongs -- in the wave scripts' own groupby over the long
``(t, i, j)`` key.

The ``aggregate_items.json`` rename that sat here commented out is NOT
scaffolding to revive: ``aggregate_items.json``'s ``Aggregated Label`` is a
FOOD map (``Eggs``, ``Apples``, ``Avocado pear``, ...), used by
``food_quantities.py`` and ``unitvalues.py``.  It was never going to aggregate
non-food items, and aggregating ``j`` is the ``labels=`` kwarg's job anyway.
"""
import numpy as np
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

X = []
for t in ['2010-11', '2012-13', '2015-16', '2018-19']:
    # Wave parquet already has canonical index [t, i, j] and the single
    # column [Expenditure]; t is the ROUND label, not the wave directory.
    X.append(get_dataframe('../%s/_/nonfood_expenditures.parquet' % t))

x = pd.concat(X, axis=0)

# Eliminate infinities.  Kept from the pre-#817 script: the wave-level
# positivity filter passes +inf (inf > 0), so this is still the only place it
# would be caught.  Measured 0 occurrences across all eight rounds.
x = x.replace([np.inf, -np.inf], np.nan)
x = x.dropna(subset=['Expenditure'])

assert x.index.is_unique, (
    f'{int(x.index.duplicated().sum())} duplicate (t, i, j) rows across waves '
    f'-- two wave directories are claiming the same round.')

to_parquet(x, '../var/nonfood_expenditures.parquet')
