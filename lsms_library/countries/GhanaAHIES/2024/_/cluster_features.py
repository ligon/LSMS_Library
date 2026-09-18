#!/usr/bin/env python
"""Cluster features (Region, District, Rural) for GhanaAHIES 2024, all four
quarters (t = 2024Q1 .. 2024Q4) from the one person x quarter file.

Source: ../Data/2024 AHIES Q1-Q4_20250827.dta (188,500 x 838), design
variables `cluster`, `region`, `regdist`, `urbrur`.  Read once through
get_dataframe.  Built DIRECTLY at the (t, v) cluster grain: the three
attributes are asserted constant within every (cluster, quarter) group (0
disagreements in 2024 -- and constant across the four quarters too), then the
person rows are de-duplicated to one per cluster-quarter.  That is a checked
de-dup, not an aggregation, and it keeps `i` out of the frame entirely, so the
framework's household-to-cluster projection (`Wave.cluster_features`, GH #323
Site 2) has nothing to collapse and nothing to audit.

Keys: v = cluster (EA number 1-600, as a string, exactly as `sample` spells
it); t = `quarter` ('2024Q1' ...).  600 clusters in every quarter.

Columns:
  Region    region label (16 regions, title case: 'Ashanti', 'Bono East',
            'Western North', ...).
  District  regdist value label, stripped -- the 2021 PHC district /
            municipal / metropolitan area name ('Jomoro Municipal', 'Accra
            Metropolitan Area-AMA', ...).  247 distinct labels in 2024, each
            in exactly one region; every raw label carries leading blanks.
  Rural     urbrur label ('Urban' / 'Rural').

No GPS is shipped in any AHIES file (no Latitude / Longitude).
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2024 AHIES Q1-Q4_20250827.dta'
KEYS = ['cluster', 'quarter']
COLS = {'region': 'Region', 'regdist': 'District', 'urbrur': 'Rural'}


def _label(s: pd.Series) -> pd.Series:
    """Decoded value label as a stripped string, NA where the source is NA."""
    out = s.astype(object).where(s.notna(), pd.NA)
    return out.map(lambda x: x.strip() if isinstance(x, str) else x)


def build(df: pd.DataFrame) -> pd.DataFrame:
    assert df[KEYS].notna().all().all(), 'null cluster / quarter in the person file'
    raw = df[KEYS + list(COLS)].copy()
    for c in COLS:
        raw[c] = _label(raw[c])

    # Cluster attributes must not vary within a cluster-quarter.  If they ever
    # do, that is a finding to report and select on (GH #323) -- never average.
    g = raw.groupby(KEYS, observed=True)[list(COLS)].nunique(dropna=False)
    bad = g[(g > 1).any(axis=1)]
    assert bad.empty, f'cluster attributes vary within a cluster-quarter:\n{bad.head()}'

    cf = raw.drop_duplicates(subset=KEYS).rename(columns=COLS)
    cf['t'] = cf['quarter'].astype(str)
    cf['v'] = cf['cluster'].astype(int).astype(str)
    cf = cf[['t', 'v'] + list(COLS.values())].set_index(['t', 'v']).sort_index()

    assert cf.index.is_unique
    assert cf.groupby(level='t').size().eq(600).all(), 'expected 600 clusters per quarter'
    assert cf[list(COLS.values())].notna().all().all(), 'null cluster attribute'
    return cf


if __name__ == '__main__':
    df = get_dataframe(PERSON_FILE)
    cf = build(df)
    print('cluster_features 2024:', cf.groupby(level='t').size().to_dict(),
          '| districts', cf['District'].nunique(), '| regions', cf['Region'].nunique())
    to_parquet(cf, 'cluster_features.parquet')
