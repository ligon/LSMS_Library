"""Concatenate wave-level anthropometry parquets for Malawi (GAP 5).

Each buildable wave's ``Malawi/<wave>/_/anthropometry.py`` produces a
parquet indexed (t, i, pid) with the reported item-level columns (Weight,
Height, Age_months, Sex).  This script concatenates them.  Cross-wave
id_walk (panel-id chaining) and the v-join from sample() are applied by the
framework at API time in _finalize_result.

Only the four IHS3+/IHPS waves carrying Module V anthropometry are
buildable (2010-11, 2013-14, 2016-17, 2019-20) -- the same waves as
crop_production / plot_inputs / livestock.  2004-05 (IHS2) is DEFERRED: its
household questionnaire carries no Module V body-measurement roster.

The WHO-2006 z-scores (haz06/waz06/whz06/bmiz06) and wasting/stunting that
the World Bank cleaning code derives from these measures are TRANSFORMS
(they require the WHO-2006 reference population); they are computed at query
time, NEVER stored here.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/anthropometry.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built (DVC raises
        # PathMissingError here, not FileNotFoundError).
        continue
    pieces.append(df)

assert pieces, "anthropometry: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/anthropometry.parquet')
