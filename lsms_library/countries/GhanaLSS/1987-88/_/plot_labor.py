#!/usr/bin/env python
"""GhanaLSS 1987-88 plot_labor -- Section 9 Part D, hired and exchange labour.

Index ``(t, i, stage, source, worker)``.  NO plot level: Section 9 has no farm
or plot roster anywhere, and ``plot_features`` carries a C4-validated
``not-asked`` verdict for both panel waves.  Dropping the plot level has
precedent in the EHCVM countries' ``plot_inputs`` and EthiopiaRHS's
``crop_production``.

This is NOT a bespoke table and NOT a new one.  Every country in the corpus
already holds hired labour in ``plot_labor`` under ``source='hired'``, and
GhanaSPS already declares a six-level ``plot_labor``
``(t, i, plot_id, season, stage, source)`` whose ``stage`` vocabulary is the
one reused here.  ``plot_labor`` has no canonical ``index_info`` or ``Columns``
entry (GH #569), so the corpus shape is convention, and that convention already
runs from four index levels to six.

TWO SOURCES, both household-grain and both 12-month recall:

  Q41-42  PAID labour.  "How much was SPENT for the following kinds of PAID
          labor during the past 12 months?" over a grid of
          CLEARING LAND / PLANTING / HARVESTING / OTHER  x  MALE / FEMALE /
          CHILD.  The cells are AMOUNTS, not person-days -- which is why this
          survey cannot fill the corpus's usual ``PersonDays``/``Wage`` pair
          and serves ``Cost`` instead (the same column the GLSS ``plot_inputs``
          introduced).  -> source='hired'.

  Q45-46  UNPAID labour EXCHANGE.  "Have the members of your household taken
          part in any exchange of unpaid labor?" / "How many MAN DAYS of labor
          has the household RECEIVED in this way?"  A single household total
          with no stage and no worker type.  -> source='exchange',
          ``PersonDays``.

SENTINELS, and why they are here rather than a NaN.  Q46 has neither a stage
nor a worker type, but both are DECLARED INDEX LEVELS -- and a NaN on a
declared index level is deleted by the first ``groupby`` (CLAUDE.md, "Site I").
So those rows carry ``stage='unspecified'`` and ``worker='all'``, the landed
Niger ``'Unknown'`` pattern from GH #842.  They are sentinels, not measurements:
the survey did not ask, and nothing is imputed.

``source='exchange'`` is the form's own word ("exchange of unpaid labor").
GhanaSPS's ``plot_labor`` carries a ``'communal'`` source which MAY be the same
institution (nnoboa), but that has not been verified here and the two are
deliberately not merged.

ZERO ROWS ARE DROPPED, with a count printed, following the GhanaSPS wave
scripts.  The Q42 grid instructs "IF NOTHING SPENT, WRITE ZERO", so a zero is a
real answer -- but 12 cells per household is mostly zeros, and carrying them
would let one country's zeros dominate a cross-country ``Feature('plot_labor')``.
The count below is the audit trail.
"""
import sys
sys.path.append('../../_')
from mapping import i as i_helper
import numpy as np
import pandas as pd
sys.path.append('../../../_/')
from lsms_library.local_tools import format_id, get_dataframe, to_parquet

t = '1987-88'

#: Q42 grid rows -> the GhanaSPS `stage` vocabulary.  'other' is not in that
#: vocabulary; the form's fourth row is literally "OTHER".
STAGE = {1: 'clearing_and_land_preparation', 2: 'planting',
         3: 'harvesting', 4: 'other'}

#: Q42 grid columns.  A mixed sex/age classification, as the form asks it.
WORKER = {'M': 'male', 'F': 'female', 'C': 'child'}

df = get_dataframe('../Data/Y09D3B.DAT')
num = lambda c: pd.to_numeric(df[c], errors='coerce') if c in df else pd.Series(np.nan, index=df.index)

rows = []
for code, stage in STAGE.items():
    for letter, worker in WORKER.items():
        rows.append(pd.DataFrame({
            't': t,
            'i': df['HID'].apply(i_helper),
            'stage': stage,
            'source': 'hired',
            'worker': worker,
            'Cost': num(f'CRLAB{letter}{code}'),
            'PersonDays': np.nan,
        }))

# Q45-46: unpaid labour exchange RECEIVED, one household total.
rows.append(pd.DataFrame({
    't': t,
    'i': df['HID'].apply(i_helper),
    'stage': 'unspecified',
    'source': 'exchange',
    'worker': 'all',
    'Cost': np.nan,
    'PersonDays': num('CREXLABD'),
}))

f = pd.concat(rows, ignore_index=True)

_before = len(f)
_zero = ((f['Cost'].fillna(0) == 0) & (f['PersonDays'].fillna(0) == 0))
f = f[~_zero]
print(f'{t}: {_before} candidate (stage, source, worker) cells -> {len(f)} kept; '
      f'dropped {int(_zero.sum())} with neither a cost nor person-days '
      f'(the Q42 grid says "IF NOTHING SPENT, WRITE ZERO", so most are real zeros)')

f = f.set_index(['t', 'i', 'stage', 'source', 'worker']).sort_index()
assert f.index.is_unique, (
    't, i, stage, source, worker is not unique: '
    f'{f.index[f.index.duplicated()].tolist()[:5]}')

to_parquet(f[['Cost', 'PersonDays']], 'plot_labor.parquet')
