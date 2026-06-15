#!/usr/bin/env python
"""GhanaLSS 1987-88 food_acquired (MONEY-ONLY wave).

This is the special money-only wave (design doc D3): the expenditure module
records only monetary VALUES -- no physical quantities and no units anywhere.
So every row uses the LCU convention (food-acquired SKILL.md "LCU-only goods:
u='Value'"):  u='Value', Quantity == Expenditure == the recorded value.

Sources:
  Y12A.DAT -- purchases   (food codes in Code_12A; per-recall value CFOODBLV)
  Y12B.DAT -- own production (food codes in Code_12B; per-recall value VFOODCPD)

Output (canonical, with a country-local degenerate `visit` level -- D1):
  index  : (t, i, j, u, s, visit)        NO v (joined from sample() at API time)
  columns: [Quantity, Expenditure]
  i = household id (canonical format via this wave's i() helper)
  j = harmonized food item (Preferred Label)
  u = 'Value' (synthetic LCU unit -- no physical units this wave)
  s in {purchased, produced}  as an INDEX level
  t = '1987-88'
  visit = 1  (single "since my last visit" recall -- degenerate but present, D1)
"""
import sys
sys.path.append('../../_')
from mapping import i as i_helper
import numpy as np
import pandas as pd
sys.path.append('../../../_/')
from lsms_library.local_tools import df_from_orgfile, to_parquet, get_dataframe

t = '1987-88'
VISIT = 1  # single "since my last visit" recall (D1: degenerate but kept)

# Harmonized food labels: Code_12A (purchases) / Code_12B (production) -> Preferred Label
labels = df_from_orgfile('./categorical_mapping.org', name='harmonize_food',
                         encoding='ISO-8859-1')
labelsd = {}
for column in ['Code_12A', 'Code_12B']:
    labels[column] = labels[column].astype('Int64').astype('string')
    labelsd[column] = labels[['Preferred Label', column]].set_index(column).to_dict('dict')


def _value_only_side(fn, code_col, value_col, s):
    """Build canonical long rows for one acquisition source (money-only).

    Returns a DataFrame indexed by (t, i, j, u, s, visit) with columns
    [Quantity, Expenditure], where Quantity == Expenditure == recorded value
    and u == 'Value'.
    """
    df = get_dataframe(fn)

    # household id via the wave's canonical i() helper (D5)
    df['i'] = df['HID'].apply(i_helper)

    # food code -> harmonized Preferred Label (j)
    df['j'] = df['FOODCD'].astype('string').replace(labelsd[code_col]['Preferred Label'])

    # per-recall-period value (NOT the pre-annualized *_yearly column)
    df['value'] = pd.to_numeric(df[value_col].replace({'.': np.nan}), errors='coerce')

    out = df[['i', 'j', 'value']].copy()
    # drop rows with no recorded value (missing '.' or zero) and unmapped/blank j
    out = out[out['value'].notna() & (out['value'] != 0)]
    out = out[out['j'].notna() & (out['j'].astype(str).str.strip() != '')]

    out['t'] = t
    out['u'] = 'Value'
    out['s'] = s
    out['visit'] = VISIT
    # LCU convention: Quantity == Expenditure == value
    out['Quantity'] = out['value']
    out['Expenditure'] = out['value']

    out = out.set_index(['t', 'i', 'j', 'u', 's', 'visit'])
    return out[['Quantity', 'Expenditure']]


purchased = _value_only_side('../Data/Y12A.DAT', 'Code_12A', 'CFOODBLV', 'purchased')
produced = _value_only_side('../Data/Y12B.DAT', 'Code_12B', 'VFOODCPD', 'produced')

f = pd.concat([purchased, produced])

to_parquet(f, 'food_acquired.parquet')
