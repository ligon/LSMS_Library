#!/usr/bin/env python
"""Canonical food_acquired for Guatemala ENCOVI 2000 (Capitulo 12).

Emits the canonical long shape ``(t, i, j, u, s)`` with columns
``[Quantity, Expenditure, Price]`` so the framework's ``_FOOD_DERIVED``
transforms can derive food_expenditures / food_prices / food_quantities.

Authoritative spec: slurm_logs/2026-06-20_food_acquired_analysis/
BRIEF_guatemala_round3.md (supersedes round-1 and round-2).

Key decisions
-------------
* i = ``hogar`` (household), j = ``item`` (food, harmonized to a Preferred
  Label via food_items.org '2000' column).  This FIXES the legacy i/j swap,
  which mapped hogar->j and item->i.

* ACTUAL 15-DAY RECALL on BOTH sides (maintainer decision on PR #578).  The
  canonical food_acquired records the survey's actual last-15-days acquisition
  window, with NO assumption that it is representative of the rest of the year.
  We deliberately do NOT use the usual-month variables (p12a05 "gasto al mes",
  p12a09a "cantidad obtuvo al mes") nor the months-acquired frequency
  (p12a04 "meses compro" / p12a08 "meses obtuvo").

  - Purchased rows (s='purchased'): keep only rows with an actual 15-day
    purchase, p12a06a > 0 (equivalently p12a06d not-null -- lockstep).
        Quantity (lbs) = p12a06a ("cantidad compro", last 15d)
                         * p12a06c ("equivalencia" of the reported purchase
                           unit p12a06b into the reference unit `umr`)
                         * cnlib  ("factor de conversion a libras" of `umr`).
        Expenditure    = p12a06d ("gasto ult 15 dias").
        u = 'lbs'.

  - Obtained rows (s in {produced, inkind, other}): keep only rows with an
    actual 15-day obtained event, p12a10a > 0.
        Quantity (lbs) = p12a10a ("cantidad obtuvo ult 15 dias") * cnlib.
        Expenditure    = NaN (the instrument records NO value for obtained
                         acquisition).
        u = 'lbs'.

    OBTAINED EQUIVALENCE -- documented finding (the round-3 spec asks us to
    DETERMINE it).  Capitulo 12 has a purchased-side equivalence multiplier
    p12a06c that maps the reported purchase unit p12a06b into the per-item
    reference unit `umr`; `cnlib` then maps `umr` -> pounds.  There is NO
    analogous obtained-side multiplier: the obtained block carries only a
    reported-unit CODE (p12a09b monthly / p12a10b 15-day), not a multiplier,
    and p12a06c is NaN on every obtained-only row (verified 0/21823).  In the
    raw data the 15-day obtained unit p12a10b equals the reference unit `umr`
    on 80.8% of obtained rows -- i.e. the obtained quantity is recorded
    directly in `umr` for the dominant case.  We therefore apply the only
    available conversion, `cnlib` (umr->lbs), with an implicit obtained
    equivalence of 1 (no per-row multiplier exists to recover the remaining
    ~19%).  This mirrors the obtained treatment of round-1/round-2, switched
    here to the 15-day quantity p12a10a per the round-3 spec.

* s split via the p12a11* si/no flags on each obtained row (1=si, 2=no):
    own-production          p12a11a            -> produced
    gift (b) + in-kind pay (c)                 -> inkind
    business (d) + barter (e) + other (f),
        or no flag set                         -> other
  A handful of rows set more than one flag; each obtained row is assigned to
  exactly one s by the priority produced > inkind > other.

* u = 'lbs' (pounds): cnlib varies by item x native unit, so the native unit
  cannot be recovered by the framework's u-keyed kg map; the 15-day purchased
  and obtained quantities are already expressed in pounds.  The framework's
  KNOWN_METRIC handles 'lbs'/'pound' -> kg.
"""

import sys
sys.path.append('../../../_/')
import pandas as pd
import numpy as np
from lsms_library.local_tools import to_parquet, get_dataframe, df_from_orgfile

t = '2000'

df = get_dataframe('../Data/ECV13G12.DTA', convert_categoricals=True)

# --- harmonize the food item label (j) ----------------------------------
# The '2000' column of food_items.org holds the Spanish item text, so key on
# the categorical text (convert_categoricals=True) rather than the raw code.
# Coverage is 1:1 (99 items, 0 missing).
food_items = df_from_orgfile('../../_/food_items.org')
food_labels = food_items[['Preferred Label', '2000']].copy()
food_labels['2000'] = food_labels['2000'].str.strip()
food_labels = food_labels.replace(['', '---'], pd.NA).dropna()
fmap = food_labels.set_index('2000')['Preferred Label'].str.strip().to_dict()
df['item'] = df['item'].astype(str).str.strip().replace(fmap)

# household id (i) as a clean string
df['hogar'] = df['hogar'].astype(int).astype(str)

# Numeric coercion for the value columns we use.
for c in ['p12a06a', 'p12a06c', 'p12a06d', 'p12a10a', 'cnlib']:
    df[c] = pd.to_numeric(df[c], errors='coerce')

frames = []

# --- purchased rows: actual 15-day purchase (p12a06a > 0) ---------------
pur = df[df['p12a06a'] > 0].copy()
pur_lbs = pur['p12a06a'] * pur['p12a06c'] * pur['cnlib']
pur_price = (pur['p12a06d'] / pur_lbs).replace([np.inf, -np.inf], np.nan)

pur_out = pd.DataFrame({
    'i': pur['hogar'].to_numpy(),
    'j': pur['item'].to_numpy(),
    'u': 'lbs',
    's': 'purchased',
    'Quantity': pur_lbs.to_numpy(),
    'Expenditure': pur['p12a06d'].to_numpy(),
    'Price': pur_price.to_numpy(),
})
frames.append(pur_out)

# --- obtained rows: actual 15-day obtained (p12a10a > 0) -----------------
obt = df[df['p12a10a'] > 0].copy()
obt_lbs = obt['p12a10a'] * obt['cnlib']

# s-split from the p12a11* si/no flags, priority produced > inkind > other;
# no-flag rows -> other.
produced = (obt['p12a11a'] == 'si')
inkind = (~produced) & ((obt['p12a11b'] == 'si') | (obt['p12a11c'] == 'si'))
s_obt = np.where(produced, 'produced',
                 np.where(inkind, 'inkind', 'other'))

obt_out = pd.DataFrame({
    'i': obt['hogar'].to_numpy(),
    'j': obt['item'].to_numpy(),
    'u': 'lbs',
    's': s_obt,
    'Quantity': obt_lbs.to_numpy(),
    'Expenditure': np.nan,  # no obtained-value variable in the instrument
    'Price': np.nan,
})
frames.append(obt_out)

# --- assemble canonical long frame --------------------------------------
final = pd.concat(frames, ignore_index=True)
final = final.dropna(subset=['j'])
final['t'] = t

# Collapse duplicate (t, i, j, u, s) keys.  Quantity/Expenditure sum with
# min_count=1 so an all-NaN group stays NaN (does not become 0); Price is a
# per-unit value, so take the median.
final = final.groupby(['t', 'i', 'j', 'u', 's']).agg(
    Quantity=('Quantity', lambda x: x.sum(min_count=1)),
    Expenditure=('Expenditure', lambda x: x.sum(min_count=1)),
    Price=('Price', 'median'),
)

to_parquet(final, 'food_acquired.parquet')
