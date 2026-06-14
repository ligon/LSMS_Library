"""Concatenate wave-level people_last7days for Niger (GAP 3, item-level).

Each wave's ``Niger/<wave>/_/people_last7days.py`` writes a parquet indexed
by ``(t, i, pid)`` with the reported per-individual 7-day activity columns
(farm_work, SOB_work, wage_work, farm_hrs, SB_hrs, wage_hrs, Industry,
working_age).  This script concatenates the four waves (2011-12, 2014-15,
2018-19, 2021-22), each of which has a 7-day labor module.  ``v`` is NOT
baked in — the framework joins it from ``sample()`` at API time.

As in the crop_production / plot_inputs / livestock siblings, this does NOT
apply ``id_walk`` here; the framework runs it in ``_finalize_result`` on
every read.  The ``(t, i, pid)`` index is unique within each wave
(``_finish_people_last7days`` collapses to one row per individual), so no
rows are lost to the framework's canonical-index de-dup collapse.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/people_last7days.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for people_last7days (no .py script / no parquet).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, 'people_last7days: no wave-level parquets found'

p = pd.concat(pieces)

to_parquet(p, '../var/people_last7days.parquet')
