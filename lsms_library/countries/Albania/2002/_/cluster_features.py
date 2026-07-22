"""Build cluster_features for Albania 2002.

Source: metadata_cl.dta -- the HOUSEHOLD cover page (3,599 households in 450
PSUs).  cluster_features is CLUSTER grain (index ``(t, v)``), so the household
level must be reduced away.  Previously this was a YAML extraction declaring an
extra ``i: hh`` idxvar, and the framework silently collapsed the resulting 3,599
rows to 450 with ``groupby().first()`` (GH #323).

Region (m0_q1a, the district code) and Rural (m0_ur) are both verified CONSTANT
within psu (0 / 450 PSUs carry more than one value), so the reduction is
value-preserving -- but it must be CHECKED rather than assumed, which is what
``albania.cluster_reduce`` does.
"""
import sys
from pathlib import Path

import pandas as pd

from lsms_library.local_tools import get_dataframe, format_id, to_parquet

sys.path.append(str(Path(__file__).parent.parent.parent / '_'))
from albania import cluster_reduce  # noqa: E402


def _label(series):
    """Categorical/labelled column -> its string label; missing -> <NA>."""
    return series.astype(str).replace({'nan': pd.NA, 'None': pd.NA, '<NA>': pd.NA})


df = get_dataframe('../Data/metadata_cl.dta')

src = pd.DataFrame({
    'v': df['psu'].apply(format_id),
    'Region': _label(df['m0_q1a']),
    'Rural': _label(df['m0_ur']),
})

out = cluster_reduce(src, columns=['Region', 'Rural'], wave='2002')

to_parquet(out, 'cluster_features.parquet')
