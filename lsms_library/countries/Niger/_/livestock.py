"""Concatenate wave-level livestock for Niger (GAP 4, item-level).

Each wave's ``Niger/<wave>/_/livestock.py`` writes a parquet indexed by
``(t, i, animal)`` with the reported per-species columns (HeadCount,
HeadAcquired, HeadSold).  This script concatenates the four waves
(2011-12, 2014-15, 2018-19, 2021-22), each of which has a livestock
roster.  ``v`` is NOT baked in: 'livestock' is in the framework
``_no_v_join`` set, so the household-level rows carry no cluster level and
the framework does not join one from ``sample()``.

The country cache stores household IDs after the existing single-pass
``id_walk`` (GH #1288 / CLAUDE "Panel ID Transitive Chains"). The unchanged
writer carries ``id_converted``, so finalization does not walk it again.
The other country compilers retain their own raw-ID contracts.
Each wave's ``_finish_livestock`` already sums the ECVMA animal
sub-type lines (bœuf/vache/... -> Cattle) onto one row per (household,
canonical species), so the ``(t, i, animal)`` index is UNIQUE within each
wave and the concatenation stays unique across waves (the four ``t`` values
are disjoint).  No rows are lost to the framework's canonical-index de-dup.

``niger.concat_wave_parquets`` discovers registered logical waves and checks
their actual native-script/YAML declarations before output access (GH #1287).
Undeclared waves skip; declared missing/unreadable outputs fail before a
country write (GH #1123). Make-producer routing is a separate boundary; see
the helper's docstring and the country ledger.
"""
from lsms_library import Country
from lsms_library.local_tools import id_walk, to_parquet
from niger import concat_wave_parquets



p = concat_wave_parquets('livestock')

# Walk the complete known-native input before serialization, like the default
# build. The writer reconstructs its ordinary stored index; readers/selectors
# preserve that country domain without another walk (ledger1288 §§3–5).
if 'i' in p.index.names and not p.attrs.get('id_converted'):
    updated_ids = Country('Niger', preload_panel_ids=False).updated_ids
    if updated_ids is not None:
        p = id_walk(p, updated_ids, state='raw')

to_parquet(p, '../var/livestock.parquet')
