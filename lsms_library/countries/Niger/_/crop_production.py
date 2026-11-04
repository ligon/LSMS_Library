"""Concatenate wave-level crop_production for Niger (GAP 1, item-level).

Each wave's ``Niger/<wave>/_/crop_production.py`` writes a parquet indexed
by ``(t, i, plot_id, crop, u, condition, production_obs)`` with the reported
harvest columns.  This script concatenates the four waves (2011-12, 2014-15,
2018-19, 2021-22), each of which has a crop/harvest module.  ``v`` is NOT
baked in — the framework joins it from ``sample()`` at API time.

As in the plot_features sibling, this does NOT apply ``id_walk`` here;
the framework runs it in ``_finalize_result`` on every read.  The index is
UNIQUE, wave by wave and across waves: a single (plot, crop, u) may carry
several reported harvest lines, and ``production_obs`` (the canonical native
observation coordinate) is the level that tells them apart, so nothing is
collapsed or summed.  Until GH #1360 the index stopped at ``u`` and was
documented here as "intentionally NON-unique"; the framework then collapsed
it, destroying 916 rows, and because only a non-unique index is collapsed and
key-sorted, the default and Make routes served the same rows in different
orders.

``niger.concat_wave_parquets`` discovers registered logical waves and checks
their actual native-script/YAML declarations before output access (GH #1287).
Undeclared waves skip; declared missing/unreadable outputs fail before a
country write (GH #1123). Make-producer routing is a separate boundary; see
the helper's docstring and the country ledger.
"""
from lsms_library.local_tools import to_parquet
from niger import concat_wave_parquets



p = concat_wave_parquets('crop_production')

to_parquet(p, '../var/crop_production.parquet')
