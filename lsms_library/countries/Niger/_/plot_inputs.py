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
warning.  The refusal is ``niger.concat_wave_parquets``, shared with the
four sibling concatenators (crop_production, livestock, plot_labor,
people_last7days).
"""
from lsms_library.local_tools import to_parquet
from niger import concat_wave_parquets


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

p = concat_wave_parquets('plot_inputs', WAVES)

to_parquet(p, '../var/plot_inputs.parquet')
