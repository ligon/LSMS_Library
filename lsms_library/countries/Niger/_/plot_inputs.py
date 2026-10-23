"""Concatenate wave-level plot_inputs for Niger (GAP 2, item-level).

Each wave's ``Niger/<wave>/_/plot_inputs.py`` writes a parquet indexed by
``(t, i, input, crop, u)`` with the reported per-input columns.  This
script concatenates the four waves (2011-12, 2014-15, 2018-19, 2021-22),
each of which has a household agricultural-input roster.  ``v`` is NOT
baked in -- the framework joins it from ``sample()`` at API time.

As in the crop_production / plot_features siblings, this does NOT apply
``id_walk`` here; the framework runs it in ``_finalize_result`` on every
read.  The ``crop`` and ``u`` levels carry non-null sentinels
(``(not crop-specific)``, ``Unknown``), never NaN.

The declared index is meant to be UNIQUE: one row per reported household x
input line (GH #323).  The two EHCVM wave scripts enforce that before they
write (GH #1123).  The two ECVMA waves still are NOT unique -- a household
that names the same crop in two of the seven seed slots, in one unit, lands
twice on one key (2011-12: 73 lines, 2014-15: 45), and the framework keeps
one observed row with a GrainCollapseWarning.  That is an open defect, not
the design; see ``_/CONTENTS.org``.

EVERY wave in WAVES has a ``plot_inputs.py`` script, so a missing wave
parquet means that script failed or was never run -- never "not wired".  It
is refused, not skipped (GH #1123): under ``LSMS_BUILD_BACKEND=make`` this
script is also the fallback the framework runs after a failed ``make``, and
skipping the absent wave there served the other three waves with no Python
warning.  The parquet is looked up where ``to_parquet`` writes it (under
``data_root()``), as ``Ethiopia/_/food_coping.py`` does.
"""
from pathlib import Path

import pandas as pd

from lsms_library.local_tools import data_root, get_dataframe, to_parquet


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

unwired = [t for t in WAVES if not Path(f'../{t}/_/plot_inputs.py').exists()]
assert not unwired, f'Niger plot_inputs: WAVES lists {unwired} with no wave script'

pieces = []
for t in WAVES:
    fn = Path(data_root('Niger')) / t / '_' / 'plot_inputs.parquet'
    if not fn.exists():
        raise FileNotFoundError(
            f'Niger plot_inputs: wave {t} declares plot_inputs '
            f'(../{t}/_/plot_inputs.py) but its parquet was not built; refusing '
            f'to concatenate the other waves without it (GH #1123).  Run that '
            f'wave script and read its error.')
    pieces.append(get_dataframe(fn))

p = pd.concat(pieces)

to_parquet(p, '../var/plot_inputs.parquet')
