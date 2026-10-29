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

``niger.concat_wave_parquets`` discovers registered logical waves and checks
their actual native-script/YAML declarations before output access (GH #1287).
Undeclared waves skip; declared missing/unreadable outputs fail before a
country write (GH #1123). Make-producer routing is a separate boundary; see
the helper's docstring and the country ledger.
"""
from lsms_library.local_tools import to_parquet
from niger import concat_wave_parquets



p = concat_wave_parquets('plot_labor')

to_parquet(p, '../var/plot_labor.parquet')
