#!/usr/bin/env python
"""Burkina Faso 2014 (EMC) food_coping -- reduced Coping Strategies Index (rCSI).

Source: ``emc2014_p3_securitealimentaire_27022015.dta`` (Part 3 "Sécurité
alimentaire").  HOUSEHOLD-level (one row per HH, index i = each wave's roster i
= format_id(zd) + format_id(menage, zeropadding=3), e.g. zd=1, menage=1 -> '1001').

The coping battery ``SA2A..SA2E`` asks, for each strategy, "Au cours des 7
derniers jours, combien de jours vous, ou une autre personne du ménage, avez-
vous dû [...]" -- i.e. how many days in the last 7 the household had to resort
to the behaviour (native day-count 0-7).  The five items are the WFP-standard
reduced Coping Strategies Index battery, in canonical order:

    SA2A  rely on less preferred / less expensive food   -> LessPreferred
    SA2B  borrow food / rely on help from relatives       -> BorrowFood
    SA2C  limit portion size at meal-times                -> LimitPortion
    SA2D  restrict consumption by adults (so kids eat)     -> RestrictAdults
    SA2E  reduce the number of meals eaten per day         -> ReduceMeals

(The Stata variable labels are truncated at 80 chars to the shared prefix, so
the discriminating clause isn't recoverable from the .dta; the SA2A..SA2E order
is the universal WFP rCSI ordering, which the brief's five canonical names
follow exactly.)

SA1 (the 7-day gate) is NOT a coping day-count, so it is out of scope here;
SA3 (meals/day) and SA5/SA6 (12-month provisioning) belong to other features.

``Days`` is the native count of days in the last 7 (int 0-7).  WIDE->LONG: one
``(t, i, Strategy)`` row per strategy.  Rows where the household was not asked
(NaN) are dropped.  ``t`` = '2014'.  ``v`` is NOT baked in -- the framework
joins it from ``sample()`` at API time.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, format_id

# SA2{letter} -> canonical rCSI Strategy (WFP-standard ordering).
STRATEGIES = {
    'A': 'LessPreferred',
    'B': 'BorrowFood',
    'C': 'LimitPortion',
    'D': 'RestrictAdults',
    'E': 'ReduceMeals',
}

# convert_categoricals=False keeps the SA2 day-counts numeric.
df = get_dataframe('../Data/emc2014_p3_securitealimentaire_27022015.dta',
                   convert_categoricals=False)

item_cols = ['SA2' + x for x in STRATEGIES]

wide = df[['zd', 'menage'] + item_cols].copy()
wide['i'] = (wide['zd'].apply(format_id)
             + wide['menage'].apply(lambda m: format_id(m, zeropadding=3)))

long = wide.melt(id_vars='i', value_vars=item_cols,
                 var_name='item', value_name='Days')
long['Strategy'] = long['item'].str.removeprefix('SA2').map(STRATEGIES)
long['Days'] = pd.to_numeric(long['Days'], errors='coerce').round().astype('Int64')

long = long.dropna(subset=['Days'])

long['t'] = '2014'
out = long[['t', 'i', 'Strategy', 'Days']].set_index(['t', 'i', 'Strategy'])

if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

from lsms_library.local_tools import to_parquet
to_parquet(out, 'food_coping.parquet')
