#!/usr/bin/env python
"""Sample (cluster identity, weights, strata) for GhanaAHIES 2024, all four
quarters (t = 2024Q1 .. 2024Q4) from the one person x quarter file.

Source: ../Data/2024 AHIES Q1-Q4_20250827.dta (188,500 x 838; person x
quarter, Sections 1-4 plus design variables).  Read once through
get_dataframe (~20 s, ~3 GB); every household-level attribute used here is
constant within a household-quarter (asserted below), so the person rows are
collapsed to one row per (cluster, HholdID, quarter) by a checked de-dup --
not an aggregation.

Keys: i = "{cluster}-{HholdID}" (stable panel household id; the shipped
`hhid` is quarter-scoped and is NOT `i`); t = `quarter` ('2024Q1' ...).

Columns: v = cluster (EA number 1-600, as a string); weight = panel_weight =
hh_weight RAW (the survey ships one household weight per quarter and no
separate longitudinal weight; the library normalises to within-wave mean 1 at
read -- the parquet keeps expansion weights, household-grain mean 901-928 in
2024, person-grain 833-854); strata = "{region}-{Urban|Rural}" (the design
stratifies the 600 EAs by region x locality: 304 urban / 296 rural EAs; no
stratum variable is shipped); Rural = urbrur ('Urban'/'Rural').

Idiosyncrasies: 600 clusters in every quarter; region and urbrur are constant
within a cluster (0 disagreements).  The 2023Q3 hh_weight half-scale anomaly
is a 2023-folder matter (see CONTENTS.org) and does not touch this file.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import household_id  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2024 AHIES Q1-Q4_20250827.dta'
KEYS = ['cluster', 'HholdID', 'quarter']
HH_COLS = ['hh_weight', 'region', 'urbrur']

df = get_dataframe(PERSON_FILE)

assert df[KEYS].notna().all().all(), 'null household key in the person file'

# Household-level attributes must not vary within a household-quarter; if
# they ever do, this is a finding, not something to reduce away (GH #323).
g = df.groupby(KEYS, observed=True)[HH_COLS].nunique(dropna=False)
bad = g[(g > 1).any(axis=1)]
assert bad.empty, f'household attributes vary within a household-quarter:\n{bad.head()}'

hh = df.drop_duplicates(subset=KEYS)[KEYS + HH_COLS].copy()

sample = pd.DataFrame({
    'i': [household_id(c, h) for c, h in zip(hh['cluster'], hh['HholdID'])],
    't': hh['quarter'].astype(str).values,
    'v': hh['cluster'].astype(int).astype(str).values,
    'weight': hh['hh_weight'].astype(float).values,
    'strata': (hh['region'].astype(str).str.strip() + '-'
               + hh['urbrur'].astype(str).str.strip()).values,
    'Rural': hh['urbrur'].astype(str).str.strip().values,
})
sample['panel_weight'] = sample['weight']
sample = sample[['i', 't', 'v', 'weight', 'panel_weight', 'strata', 'Rural']]
sample = sample.set_index(['i', 't']).sort_index()

assert sample.index.is_unique
assert sample.groupby(level='t')['v'].nunique().eq(600).all(), 'expected 600 clusters per quarter'

to_parquet(sample, 'sample.parquet')
