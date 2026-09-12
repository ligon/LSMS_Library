#!/usr/bin/env python
"""GhanaLSS 1987-88 food_acquired (MONEY-ONLY wave).

This is the special money-only wave (design doc D3): the expenditure module
records only monetary VALUES -- no physical quantities and no units anywhere.
So every row uses the LCU convention (food-acquired SKILL.md "LCU-only goods:
u='Value'"):  u='Value', Quantity == Expenditure == the recorded value.

Sources:
  Y12A.DAT -- purchases   (food codes in Code_12A; CFOODBLV = amount spent
              "since my last visit", the single ~fortnight recall)
  Y12B.DAT -- own production (food codes in Code_12B).  Section 12B asks NO
              recall question: VFOODCPD is "How much would it cost to buy the
              amount they ate each time?" -- the value of ONE EATING OCCASION
              -- beside MFOODCLY (months eaten of the last 12), TFOODC (times
              per unit) and UTFOODC (unit code).  Serving VFOODCPD as the
              produced Expenditure (as this script did until 2026-09-11) put
              one meal beside a fortnight: the own-production share of food
              read 17% where a like-for-like window gives ~39%.

              The served produced value is therefore DERIVED --
              ghanalss.derive_12b_fortnight_value(): the year-average
              fortnight, VFOODCPD x TFOODC x (14 / days_per_unit) x
              MFOODCLY / 12 = GSS's own annual EXPEND.HPFOOD / 26 -- and every
              produced row carries the registry key in the `Derivation`
              column (see _/derivations.yml, SkunkWorks/derived_values.org).
              The raw answers come back by name from
              Country('GhanaLSS').derivation_inputs(DERIVATION_12B).

Output (canonical, with a country-local degenerate `visit` level -- D1):
  index  : (t, i, j, u, s, visit)        NO v (joined from sample() at API time)
  columns: [Quantity, Expenditure, Derivation]
  i = household id (canonical format via this wave's i() helper)
  j = harmonized food item (Preferred Label)
  u = 'Value' (synthetic LCU unit -- no physical units this wave)
  s in {purchased, produced}  as an INDEX level
  t = '1987-88'
  visit = 1  (a single ask -- degenerate but present, D1)
  Derivation = 'GhanaLSS::food_acquired::12b-fortnight' on produced rows, NA
               on purchased rows
"""
import sys
sys.path.append('../../_')
from mapping import i as i_helper
import numpy as np
import pandas as pd
sys.path.append('../../../_/')
from ghanalss import derive_12b_fortnight_value
from lsms_library.local_tools import (df_from_orgfile, format_id, get_dataframe,
                                      to_parquet)

t = '1987-88'
VISIT = 1  # a single ask per module (D1: degenerate but kept)
# The registry key stamped on every produced row (_/derivations.yml).
DERIVATION_12B = 'GhanaLSS::food_acquired::12b-fortnight'

# Harmonized food labels: Code_12A (purchases) / Code_12B (production) -> Preferred Label
labels = df_from_orgfile('./categorical_mapping.org', name='harmonize_food',
                         encoding='ISO-8859-1')
labelsd = {}
for column in ['Code_12A', 'Code_12B']:
    labels[column] = labels[column].astype('Int64').astype('string')
    labelsd[column] = labels[['Preferred Label', column]].set_index(column).to_dict('dict')


def _value_only_side(fn, code_col, value_col, s, derive=None):
    """Build canonical long rows for one acquisition source (money-only).

    Returns a DataFrame indexed by (t, i, j, u, s, visit) with columns
    [Quantity, Expenditure], where Quantity == Expenditure and u == 'Value'.
    The value is the recorded `value_col`, or -- when `derive` is given -- a
    function of the source frame (12B: the derived fortnight value).  Rows
    are kept or dropped on the RECORDED value either way, so a derivation
    that evaluates to 0 (MFOODCLY == 0, three rows here) is served as 0
    under its key rather than silently removed.
    """
    df = get_dataframe(fn)

    # household id via the wave's canonical i() helper (D5)
    df['i'] = df['HID'].apply(i_helper)

    # food code -> harmonized Preferred Label (j)
    #
    # Normalize via format_id, NOT .astype('string').  The label table's keys
    # are bare integer strings ('301'), and since GH #704 the .DAT reader
    # honours Stata's '.' missing marker, so FOODCD arrives as float64 --
    # .astype('string') would yield '301.0' and miss every code, shipping the
    # raw number as the food label.  format_id strips the '.0' (its documented
    # job) and returns None for NaN.  Same fix, same reason, as the GH #348
    # unit decode in 1991-92.
    df['j'] = (df['FOODCD'].apply(format_id).astype('string')
                           .replace(labelsd[code_col]['Preferred Label']))

    # the recorded value (NOT the pre-annualized *_yearly column)
    recorded = pd.to_numeric(df[value_col].replace({'.': np.nan}), errors='coerce')
    df['value'] = derive(df) if derive is not None else recorded

    out = df[['i', 'j', 'value']].copy()
    # drop rows with no RECORDED value (missing '.' or zero) and unmapped/blank j
    out = out[recorded.notna() & (recorded != 0)]
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


def _fortnight_12b(df):
    """Year-average fortnight value from the four 12B answers (see module doc)."""
    return derive_12b_fortnight_value(df['MFOODCLY'], df['TFOODC'],
                                      df['UTFOODC'], df['VFOODCPD'])


produced = _value_only_side('../Data/Y12B.DAT', 'Code_12B', 'VFOODCPD', 'produced',
                            derive=_fortnight_12b)

f = pd.concat([purchased, produced])

# Collapse within-grain duplicates: several raw food codes harmonize to one
# Preferred Label j (e.g. multiple "soup" codes -> "Soup") at the same
# (i, u, s, visit), so sum their value to keep the canonical (t,i,j,u,s,visit)
# index unique -- otherwise the API-layer canonical-shape guard would collapse
# them via groupby().first() and silently drop rows.
f = f.groupby(level=f.index.names).sum(min_count=1)
assert f.index.is_unique, 'non-unique (t, i, j, u, s, visit) after the collapse'

# Row label AFTER the collapse (a string must never reach the .sum()): every
# produced row is the derived value, every purchased row is a recorded answer.
produced_mask = f.index.get_level_values('s') == 'produced'
f['Derivation'] = pd.Series(np.where(produced_mask, DERIVATION_12B, None),
                            index=f.index, dtype='string')

to_parquet(f, 'food_acquired.parquet')
