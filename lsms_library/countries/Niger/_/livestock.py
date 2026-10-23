"""Concatenate wave-level livestock for Niger (GAP 4, item-level).

Each wave's ``Niger/<wave>/_/livestock.py`` writes a parquet indexed by
``(t, i, animal)`` with the reported per-species columns (HeadCount,
HeadAcquired, HeadSold).  This script concatenates the four waves
(2011-12, 2014-15, 2018-19, 2021-22), each of which has a livestock
roster.  ``v`` is NOT baked in: 'livestock' is in the framework
``_no_v_join`` set, so the household-level rows carry no cluster level and
the framework does not join one from ``sample()``.

As in the crop_production / plot_inputs siblings, this does NOT apply
``id_walk`` here; the framework runs it in ``_finalize_result`` on every
read.  Each wave's ``_finish_livestock`` already sums the ECVMA animal
sub-type lines (bœuf/vache/... -> Cattle) onto one row per (household,
canonical species), so the ``(t, i, animal)`` index is UNIQUE within each
wave and the concatenation stays unique across waves (the four ``t`` values
are disjoint).  No rows are lost to the framework's canonical-index de-dup.

EVERY wave in WAVES has a ``livestock.py`` script, so a missing wave parquet
means that script failed or was never run -- never "not wired". It is
refused, not skipped (``niger.concat_wave_parquets``, GH #1123, extended
from plot_inputs on 2026-10-23): under ``LSMS_BUILD_BACKEND=make`` this
script is also the fallback the framework runs after a failed ``make``, and
skipping the absent wave there would serve the other waves with no Python
warning.
"""
from lsms_library.local_tools import to_parquet
from niger import concat_wave_parquets


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

p = concat_wave_parquets('livestock', WAVES)

to_parquet(p, '../var/livestock.parquet')
