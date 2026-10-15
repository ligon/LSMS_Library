#!/usr/bin/env python
"""Sample (cluster identity, weights, strata) for GhanaAHIES 2023, all four
quarters (t = 2023Q1 .. 2023Q4) from the one person x quarter file.

Source: ../Data/2023 AHIES Q1-Q4_Rev_20250827.dta (196,953 x 838; person x
quarter, Sections 1-4 plus design variables; the same 838-column layout as
2024).  Read once through get_dataframe (~35 s, ~3 GB); every household-
level attribute used here is constant within a household-quarter (asserted
below), so the person rows are collapsed to one row per (cluster, HholdID,
quarter) by a checked de-dup -- not an aggregation.

Keys: i = "{cluster}-{HholdID}" (stable panel household id; the shipped
`hhid` is quarter-scoped and is NOT `i`); t = `quarter`, shipped as exactly
'2023Q1' .. '2023Q4' (asserted; `qtr` = 5..8 over the panel).

Columns: v = cluster (EA number 1-600, as a string; 600 in every quarter);
weight = panel_weight = hh_weight RAW (one household weight per quarter, no
separate longitudinal weight; the library normalises to within-wave mean 1
at read, the parquet keeps the expansion scale); strata =
"{region}-{Urban|Rural}" (32 values; region and urbrur are constant within a
cluster-quarter, 0 disagreements); Rural = urbrur.

What differs from 2024 -- THE 2023Q3 WEIGHT (recorded, not corrected).
Household-grain hh_weight means are 868.6 / 874.2 / 433.9 / 882.7 for
Q1-Q4 (person grain 803.6 / 809.9 / 402.3 / 813.9); sums 8.80 M / 8.84 M /
4.38 M / 8.76 M households (2021 PHC: 8.35 M; 2024 sums ~8.2 M).  2023Q3
is at exactly HALF scale: the per-household ratio Q3/Q2 is 0.4986 +- 0.0022
(min 0.495, max 0.503, n = 10,026) and Q4/Q3 is 2.03; the ratio of quarter
means is 0.495-0.504 in every one of the 16 regions and 0.495 / 0.498 for
urban / rural.  pop_weight is NOT halved (sums 30.9 M / 30.9 M / 30.8 M /
31.2 M persons; per-person Q3/Q2 0.98), and Q3 has an ordinary number of
household-quarters (10,085 vs 10,131 / 10,113 / 9,924).  So it is a uniform
factor-of-two error on hh_weight alone, not missing households or a
subset at zero (0 nulls, 0 zeros).  Because the library rescales each
quarter to mean 1, weighted means and ratios are unaffected at the API and
the anomaly is INVISIBLE there; anyone summing raw weights across quarters
must know.  Nothing is rescaled, dropped or imputed here -- that is a
country decision for the human (GSS has been asked; see CONTENTS.org).
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import household_id  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2023 AHIES Q1-Q4_Rev_20250827.dta'
QUARTERS = {'2023Q1', '2023Q2', '2023Q3', '2023Q4'}
KEYS = ['cluster', 'HholdID', 'quarter']
HH_COLS = ['hh_weight', 'region', 'urbrur']

df = get_dataframe(PERSON_FILE)

assert df[KEYS].notna().all().all(), 'null household key in the person file'
assert set(df['quarter'].astype(str).unique()) == QUARTERS, df['quarter'].unique()

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

# The 2023Q3 half-scale weight is reported every build so it cannot be
# forgotten; it is deliberately NOT repaired here (see the docstring).
print('sample 2023: raw hh_weight mean / sum per quarter (household grain):',
      sample.groupby(level='t')['weight'].agg(['mean', 'sum']).round(1).to_dict('index'))

to_parquet(sample, 'sample.parquet')
