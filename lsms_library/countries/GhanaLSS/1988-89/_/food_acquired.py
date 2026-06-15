#!/usr/bin/env python
"""GhanaLSS 1988-89 food_acquired (canonical).

MONEY-ONLY wave (design doc D3), structurally identical to 1987-88: the
expenditure module records monetary values only -- NO quantities, NO physical
units.  Every row therefore carries the synthetic unit ``u='Value'`` with
``Quantity == Expenditure == value`` (see the food-acquired skill, "LCU-only
goods: u='Value'").

Sources (read via the existing .DAT/.DCT get_dataframe path):
  - Y12A.DAT : purchases   (FOODCD == Code_12A) -> s='purchased'
  - Y12B.DAT : home produced (FOODCD == Code_12B) -> s='produced'

Output:
  - index  : (t, i, j, u, s, visit)   -- NO v (framework would join from
             sample(), but this wave declares no sample, so v is unavailable;
             nothing to join).  visit is KEPT as its own index level (design
             doc D1): this 1980s wave has a single, degenerate "since my last
             visit" recall, but it is present.
  - columns: [Quantity, Expenditure]
  - i = household id (built via the wave's canonical mapping.i() helper so the
        keys match sample/roster), j = harmonized food item, u = 'Value',
        s in {purchased, produced}, t = '1988-89'.

Value-column choice: the per-RECALL-PERIOD value -- CFOODBLV (purchased_value,
"amount spent since last visit") for purchases and VFOODCPD (produced_value
per day) for production -- NOT the annualized *_yearly columns.
"""
import sys
import numpy as np
import pandas as pd

sys.path.append('../../_')          # ghanalss.py (country-level helpers)
sys.path.append('.')                # mapping.py  (this wave's i() helper)
import mapping
from lsms_library.local_tools import df_from_orgfile, to_parquet, get_dataframe

t = '1988-89'
# Single, degenerate recall window for this 1980s wave (design doc D3).
VISIT = 'since last visit'

# ----------------------------------------------------------------------------
# Food-item harmonization: Code_12A / Code_12B -> canonical Preferred Label.
# ----------------------------------------------------------------------------
labels = df_from_orgfile('./categorical_mapping.org', name='harmonize_food',
                         encoding='ISO-8859-1')
labelsd = {}
for column in ['Code_12A', 'Code_12B']:
    labels[column] = labels[column].astype('Int64').astype('string')
    labelsd[column] = labels[['Preferred Label', column]].set_index(column).to_dict('dict')


def _load_side(fn, code_col, value_col, source):
    """Read one money-only side and reshape to canonical long rows.

    Returns a DataFrame with columns [i, j, s, Quantity, Expenditure] (u/t/visit
    added by the caller after concatenation).
    """
    df = get_dataframe(fn)

    # Household id via the wave's canonical helper (matches sample/roster).
    df['i'] = df['HID'].apply(mapping.i)

    # Harmonized food item.
    df['j'] = (df['FOODCD'].astype('string')
                           .replace(labelsd[code_col]['Preferred Label']))

    # Per-recall-period monetary value -> Expenditure (== Quantity, u='Value').
    val = df[value_col].replace({'.': np.nan}).astype('float64')

    out = pd.DataFrame({'i': df['i'], 'j': df['j'], 'value': val})
    out['s'] = source

    # Drop rows with no usable value (missing / zero) and unmapped food codes.
    out = out[out['value'].notna() & (out['value'] != 0)]
    out = out[out['j'].notna() & (out['j'].astype('string') != '')]

    out['Quantity'] = out['value']
    out['Expenditure'] = out['value']
    return out[['i', 'j', 's', 'Quantity', 'Expenditure']]


# Purchases (Y12A): CFOODBLV == purchased_value ("amount spent since last visit").
x = _load_side('../Data/Y12A.DAT', 'Code_12A', 'CFOODBLV', 'purchased')

# Home produced (Y12B): VFOODCPD == produced_value_daily.
y = _load_side('../Data/Y12B.DAT', 'Code_12B', 'VFOODCPD', 'produced')

f = pd.concat([x, y], ignore_index=True)

# Canonical unit / wave / visit levels.
f['u'] = 'Value'
f['t'] = t
f['visit'] = VISIT

f = f.set_index(['t', 'i', 'j', 'u', 's', 'visit'])[['Quantity', 'Expenditure']]

# Collapse within-grain duplicates: several raw food codes harmonize to one
# Preferred Label j (e.g. multiple "soup" codes -> "Soup") at the same
# (i, u, s, visit), so sum their value to keep the canonical (t,i,j,u,s,visit)
# index unique -- otherwise the API-layer canonical-shape guard would collapse
# them via groupby().first() and silently drop rows.
f = f.groupby(level=f.index.names).sum(min_count=1)

to_parquet(f, 'food_acquired.parquet')
