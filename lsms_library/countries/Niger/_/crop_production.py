"""Concatenate wave-level crop_production for Niger (GAP 1, item-level).

Each wave's ``Niger/<wave>/_/crop_production.py`` writes a parquet indexed
by ``(t, i, plot, crop, u)`` with the reported harvest columns.  This
script concatenates the four waves (2011-12, 2014-15, 2018-19, 2021-22),
each of which has a crop/harvest module.  ``v`` is NOT baked in — the
framework joins it from ``sample()`` at API time.

As in the plot_features sibling, this does NOT apply ``id_walk`` here;
the framework runs it in ``_finalize_result`` on every read.  The
``(t, i, plot, crop, u)`` index is intentionally NON-unique: a single
(plot, crop) may carry several reported harvest lines (different units /
records), and collapsing them would be a sum — forbidden by the
item-level / reported-values discipline.  Duplicate rate stays well under
the diagnostics tolerance.

EVERY wave in WAVES has a ``crop_production.py`` script, so a missing wave
parquet means that script failed or was never run -- never "not wired". It
is refused, not skipped (``niger.concat_wave_parquets``, GH #1123, extended
from plot_inputs on 2026-10-23): under ``LSMS_BUILD_BACKEND=make`` this
script is also the fallback the framework runs after a failed ``make``, and
skipping the absent wave there would serve the other waves with no Python
warning.
"""
from lsms_library.local_tools import to_parquet
from niger import concat_wave_parquets


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

p = concat_wave_parquets('crop_production', WAVES)

to_parquet(p, '../var/crop_production.parquet')
