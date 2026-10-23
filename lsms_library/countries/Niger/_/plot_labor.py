"""Concatenate wave-level plot_labor for Niger (GAP 3, item-level).

Each wave's ``Niger/<wave>/_/plot_labor.py`` writes a parquet indexed by
``(t, i, plot, source)`` with the reported per-(plot, source) labor columns
(PersonDays, Wage).  This script concatenates the four waves (2011-12,
2014-15, 2018-19, 2021-22), each of which has a plot-labor module.  ``v`` is
NOT baked in — the framework joins it from ``sample()`` at API time.

As in the crop_production / plot_inputs / livestock siblings, this does NOT
apply ``id_walk`` here; the framework runs it in ``_finalize_result`` on
every read.  The ``(t, i, plot, source)`` index is unique within each wave
(``_finish_plot_labor`` sums the labor strata onto the (plot, source) grain),
so no rows are lost to the framework's canonical-index de-dup collapse.

EVERY wave in WAVES has a ``plot_labor.py`` script, so a missing wave
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

p = concat_wave_parquets('plot_labor', WAVES)

to_parquet(p, '../var/plot_labor.parquet')
