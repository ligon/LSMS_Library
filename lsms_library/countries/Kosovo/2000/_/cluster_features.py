"""Kosovo 2000 cluster_features.  GH #323.

``ID.dta`` is HOUSEHOLD-level (2,880 rows, one per household, unique on hhid);
``cluster_features`` is CLUSTER-level, ``(t, v)``.  Kosovo's design is 360 PSUs
x exactly 8 households, so producing the cluster table necessarily reduces
2,880 rows to 360 -- an INTENDED aggregation.

Previously the wave YAML declared BOTH ``v: psu`` and ``i: hhid`` under
``cluster_features.idxvars`` even though ``i`` is not part of the declared
index, so a household-grain frame was handed to a cluster-grain index and
2,520 rows were collapsed away by ``Country.cluster_features()``'s
``groupby(level=[...]).first()`` (country.py, GH #161) -- silently, with no
warning at all.

The collapse was *correct*: Region (s0i_q07, municipality) and Rural (s0i_q09)
are constant within a PSU by construction of the sampling design, so `first`
picks a value equal to every other value in the group.  But NOTHING ENFORCED
that invariant -- the code comment asserting it was prose, and prose is not
enforcement.  If a future wave (or a re-coded municipality) ever broke the
invariant, `first` would silently publish whichever value happened to sort
first.

So this script performs the reduction EXPLICITLY and VERIFIES the invariant
that licenses it, raising rather than guessing if it is ever violated.
"""
import sys

import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id

t = '2000'

df = get_dataframe('../Data/ID.dta')
df['v'] = df['psu'].map(format_id)


def _s(col):
    return col.astype(str).replace({'nan': pd.NA, 'None': pd.NA, '<NA>': pd.NA, '': pd.NA})


df['Region'] = _s(df['s0i_q07'])   # "municipality"
df['Rural'] = _s(df['s0i_q09'])    # "urban - rural"

# ---- ENFORCE the invariant that makes the household -> cluster reduction safe.
counts = df.groupby('v')[['Region', 'Rural']].nunique(dropna=False)
violations = counts[(counts > 1).any(axis=1)]
if len(violations):
    raise ValueError(
        f"Kosovo/2000 cluster_features: Region/Rural are NOT constant within "
        f"{len(violations)} cluster(s), so the household->cluster reduction is "
        f"not well defined.  Offending v: {list(violations.index)[:20]}.  "
        f"Refusing to pick a value by row order.  GH #323."
    )

out = (df[['v', 'Region', 'Rural']]
       .drop_duplicates(subset='v')
       .assign(t=t)
       .set_index(['t', 'v'])
       .sort_index())

assert out.index.is_unique, (
    f"cluster_features index (t, v) is not unique: "
    f"{int(out.index.duplicated().sum())} duplicate tuple(s)"
)

to_parquet(out, 'cluster_features.parquet')
