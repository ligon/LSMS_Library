#!/usr/bin/env python
"""Shocks for Tanzania 2008-15 (multi-round file covering NPS rounds 1-4).

Source: ``upd4_hh_r.dta`` -- the harmonised §R "Shocks" module, one row per
(panel line, round, shock type).

``i`` IS ``r_hhid`` -- the household, NOT ``UPHI`` (GH #637).
------------------------------------------------------------------------
The upd4 household-level modules are keyed on the panel-tracking LINE
(``UPHI``), not the household: a household-round arrives once per DESCENDANT
line, so the ancestor's answers are replicated 1..11 times.  ``sample.py``
documents the same replication on the cover page (29,250 rows -> 16,540
household-rounds).

This script used to set ``i = UPHI``.  That made ``(t, i, Shock)`` unique --
but only by keying the table on the wrong entity.  ``UPHI`` is a line index
("1".."14985"); ``r_hhid`` is the household id used by ``sample``,
``household_roster`` and every other Tanzania table (14-digit in R1, 16-digit
in R2, ``NNNN-NNN`` in R3/R4).  Measured on a cold build, the two namespaces
shared **zero** values: shocks' 5,587 / 6,875 / 7,745 / 3,634 distinct ``i``
per round overlapped ``household_roster``'s by 0, while 2019-20 and 2020-21
(which key on sdd_hhid / y5_hhid) overlapped by 100% (502/502, 2,552/2,552).
So every 2008-15 shocks row was un-joinable to its own household, ``id_walk``
matched no key in ``updated_ids.json`` (which is r_hhid-keyed), and the
replicated lines survived as separate rows -- 74,341 rows standing for 39,724
household-shock facts, inflating any shock count by ~1.9x.

Keying on ``r_hhid`` re-introduces the line replication as duplicate index
entries, which the ``.first()`` below collapses.  That collapse is
VALUE-PRESERVING here, not a guess: over the whole module (449,435 rows,
134,035 duplicate ``(round, r_hhid, hr_00)`` groups) **not one group differs on
any of the nine hr_* columns** -- hr_01..hr_06_3 are byte-identical across a
household-round's lines.  The de-replicated table is 39,724 rows and all 13,252
of its (t, i) pairs are known to ``sample()`` (0 unknown).
"""
from lsms_library.local_tools import to_parquet, get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np

# Shock dataset (multi-round file covering rounds 1-4)
df = get_dataframe('../Data/upd4_hh_r.dta')

# Filter for households that experienced the shock
df = df[df['hr_01'] == 'YES']

# Map combined effect variable to separate AffectedIncome and AffectedAssets
effect_income_map = {
    'INCOME LOSS': True,
    'Income loss': True,
    'income loss': True,
    'ASSET LOSS': False,
    'Asset loss': False,
    'asset loss': False,
    'LOSS OF BOTH': True,
    'Loss of both': True,
    'loss of both': True,
    'NEITHER': False,
    'Neither': False,
    'neither': False,
}

effect_assets_map = {
    'INCOME LOSS': False,
    'Income loss': False,
    'income loss': False,
    'ASSET LOSS': True,
    'Asset loss': True,
    'asset loss': True,
    'LOSS OF BOTH': True,
    'Loss of both': True,
    'loss of both': True,
    'NEITHER': False,
    'Neither': False,
    'neither': False,
}

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

shocks = pd.DataFrame({
    # i = r_hhid, the household -- see the module docstring.  NOT UPHI.
    'i': df.r_hhid.values.tolist(),
    't': df['round'].values.tolist(),
    'Shock': df.hr_00.values.tolist(),
    'AffectedIncome': df.hr_03.map(effect_income_map).values.tolist(),
    'AffectedAssets': df.hr_03.map(effect_assets_map).values.tolist(),
    'HowCoped0': df.hr_06_1.values.tolist(),
    'HowCoped1': df.hr_06_2.values.tolist(),
    'HowCoped2': df.hr_06_3.values.tolist(),
})

shocks = shocks.replace({'t': round_match})

# Convert household ID to string (same spelling as household_roster.py /
# sample.py -- r_hhid is not numeric in rounds 3-4, so never .astype(int)).
shocks['i'] = shocks['i'].astype(str)

shocks = shocks.set_index(['t', 'i', 'Shock'])

# Collapse the UPHI line replication.  Measured value-preserving: 0 of the
# 134,035 duplicate (round, r_hhid, hr_00) groups in upd4_hh_r differ on any
# hr_* column, so .first() de-replicates rather than choosing.  ASSERT it
# rather than trust it -- the same discipline sample.py:76 applies to the same
# replication -- so that a future re-release which makes the lines disagree
# fails loudly instead of silently keeping one line's answer.
if not shocks.index.is_unique:
    _dup = shocks.index.duplicated(keep=False)
    _nun = (shocks[_dup].groupby(level=shocks.index.names, observed=True)
            .nunique(dropna=False))
    assert (_nun.max() <= 1).all(), (
        'Tanzania 2008-15 shocks: a household-round\'s UPHI lines disagree; '
        'the .first() collapse would pick arbitrarily (GH #637).')
    shocks = shocks.groupby(level=shocks.index.names).first()

to_parquet(shocks, 'shocks.parquet')
