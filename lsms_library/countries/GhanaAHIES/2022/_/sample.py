#!/usr/bin/env python
"""Sample (cluster identity, weights, strata) for GhanaAHIES 2022, all four
quarters (t = 2022Q1 .. 2022Q4) from the one person x quarter file.

Source: ../Data/2022 AHIES Q1-Q4_Rev_20250827.dta (209,943 x 838; person x
quarter, Sections 1-4 plus design variables; the same 838-column layout as
2023/2024, of which 319 columns are 100% null in 2022 -- see CONTENTS.org
s"2022 folder").  Read once through get_dataframe (~22 s, ~3 GB); every
household-level attribute used here is constant within a household-quarter
(asserted below), so the person rows are collapsed to one row per (cluster,
HholdID, quarter) by a checked de-dup -- not an aggregation.

Keys: i = "{cluster}-{HholdID}" (stable panel household id; the shipped
`hhid` is quarter-scoped and is NOT `i`); t = `quarter` ('2022Q1' ...), as
shipped.

Columns: v = cluster (EA number 1-600, as a string); weight = panel_weight =
hh_weight RAW (one household weight per quarter, no separate longitudinal
weight; the library normalises to within-wave mean 1 at read -- the parquet
keeps expansion weights: household-grain mean 776.7 / 786.4 / 803.2 / 852.1
for 2022Q1-Q4, person-grain 720.7 / 728.9 / 743.6 / 789.2, sums 8.36M /
8.36M / 8.37M / 8.78M households); strata = "{region}-{Urban|Rural}" (the
design stratifies the 600 EAs by region x locality; no stratum variable is
shipped; 32 values); Rural = urbrur ('Urban'/'Rural').

What differs from 2024: only the file name and the numbers.  Households per
quarter 10,761 / 10,628 / 10,422 / 10,309; 600 clusters in every quarter;
region and urbrur constant within a cluster (0 disagreements); hh_weight,
region and urbrur constant within a household-quarter (0 disagreements).
The 2022 SEC567 file's WTPP1..4 / WTHH_1 / WTHH_2 columns are NOT used:
they are populated for 2022Q4 only, and WTHH_2 equals this file's hh_weight
exactly (10,112 of 10,112 rows) -- see CONTENTS.org.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import household_id  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2022 AHIES Q1-Q4_Rev_20250827.dta'
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
