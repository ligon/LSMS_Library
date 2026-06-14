"""Concatenate wave-level people_last7days for Mali (GAP 3b; parity loop).

Each EACI wave's ``Mali/<wave>/_/people_last7days.py`` writes a parquet
indexed by ``(t, i, pid)`` with the reported per-individual 7-day activity
columns (farm_work, SOB_work, wage_work, farm_hrs, SB_hrs, wage_hrs,
Industry, working_age).  The individual labor/time-use module lives in the
EACI waves only (2014-15 EACIIND_p1, 2017-18 eaci17_s04p1).  The EHCVM
waves (2018-19, 2021-22) carry no individual labor module, so they
contribute nothing — the same wave split as plot_labor / crop_production.

``v`` is NOT baked in — the framework joins it from ``sample()`` at API
time (people_last7days is household-linked and is NOT in the framework
``_no_v_join`` set).  ``id_walk`` is left to ``_finalize_result``.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['2014-15', '2017-18', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/people_last7days.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        continue
    pieces.append(df)

assert pieces, "people_last7days: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, pid) after concat"

to_parquet(p, '../var/people_last7days.parquet')
