"""Concatenate wave-level plot_inputs for Niger (GAP 2, item-level).

Each wave's ``Niger/<wave>/_/plot_inputs.py`` writes a parquet indexed by
``(t, i, input, crop, u, input_obs)`` with the reported per-input columns.  This
script concatenates the four waves (2011-12, 2014-15, 2018-19, 2021-22),
each of which has a household agricultural-input roster.  ``v`` is NOT
baked in -- the framework joins it from ``sample()`` at API time.

As in the crop_production / plot_features siblings, this does NOT apply
``id_walk`` here; the framework runs it in ``_finalize_result`` on every
read.  The ``crop`` and ``u`` levels carry non-null sentinels
(``(not crop-specific)``, ``Unknown``), never NaN.

The declared index is unique: ECVMA's input_obs identifies the recorded
slot in its source release, while EHCVM retains unresolved_observation and
its existing uniqueness checks. No native seed line is selected or summed.
See ``_/CONTENTS.org`` for the injective source mapping (GH #1123).

``niger.concat_wave_parquets`` discovers registered logical waves and checks
their actual native-script/YAML declarations before output access (GH #1287).
Undeclared waves skip; declared missing/unreadable outputs fail before a
country write (GH #1123). Make-producer routing is a separate boundary; see
the helper's docstring and the country ledger.
"""
from lsms_library.local_tools import to_parquet
from niger import concat_wave_parquets



p = concat_wave_parquets('plot_inputs')

to_parquet(p, '../var/plot_inputs.parquet')
