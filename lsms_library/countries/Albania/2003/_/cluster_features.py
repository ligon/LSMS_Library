"""Build cluster_features for Albania 2003.

Source: w2_roster_all.dta.  NOTE this file is the PERSON roster (8,679 people) --
the wave has no cluster-level file, and STRATUM (the only cluster attribute we
carry) rides along on every person row.  cluster_features is CLUSTER grain
(index ``(t, v)``), so the person level must be reduced away.  Previously this
was a YAML extraction declaring an extra ``i: BHID`` idxvar, and the framework
silently collapsed 8,679 person rows to 450 with ``groupby().first()`` -- the
largest silent collapse of Albania's four waves (GH #323).

Two properties, both verified against the raw .dta:

- STRATUM is CONSTANT within PSU (0 / 449 PSUs carry more than one value), so
  the reduction is value-preserving -- but it is now CHECKED, not assumed.
- 711 person rows carry a NULL PSU.  The old silent collapse turned those into a
  451st... in practice a *v = NaN* cluster row -- a cluster keyed on no cluster.
  ``cluster_reduce`` drops them loudly, so 2003 yields 449 real clusters, not
  450.  (Their STRATUM is still unusable: with no PSU there is no cluster to
  attach it to.)

The source is deliberately left as the person roster rather than re-pointed at a
household file: STRATUM is constant within PSU, so *any* file keyed on PSU gives
identical values, and keeping the source fixed guarantees the refactor cannot
move a single Region value.
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


df = get_dataframe('../Data/w2_roster_all.dta')

src = pd.DataFrame({
    'v': df['PSU'].apply(format_id),
    'Region': _label(df['STRATUM']),
})

out = cluster_reduce(src, columns=['Region'], wave='2003')

to_parquet(out, 'cluster_features.parquet')
