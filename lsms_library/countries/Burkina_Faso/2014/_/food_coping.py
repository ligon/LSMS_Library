#!/usr/bin/env python
"""Burkina Faso 2014 (EMC) food_coping -- reduced Coping Strategies Index (rCSI).

Source: ``emc2014_p3_securitealimentaire_27022015.dta`` (Part 3 "Sécurité
alimentaire").  HOUSEHOLD-level (one row per HH, index i = each wave's roster i
= format_id(zd) + format_id(menage, zeropadding=3), e.g. zd=1, menage=1 -> '1001').

The coping battery ``SA2A..SA2E`` asks, for each strategy, "Au cours des 7
derniers jours, combien de jours vous, ou une autre personne du ménage, avez-
vous dû [...]" -- i.e. how many days in the last 7 the household had to resort
to the behaviour (native day-count 0-7).  The five items are the standard
reduced Coping Strategies Index battery, mapped in questionnaire order:

    SA2A  rely on less preferred / less expensive food   -> LessPreferred
    SA2B  limit portion size at meal-times                -> LimitPortion
    SA2C  reduce the number of meals eaten per day         -> ReduceMeals
    SA2D  restrict consumption by adults (so kids eat)     -> RestrictAdults
    SA2E  borrow food / rely on help from relatives        -> BorrowFood

The Stata variable labels are all truncated at Stata's 80-char cap to the
identical shared prefix above, so the discriminating clause is NOT recoverable
from the .dta and no questionnaire ships with the data.  The SA2A..SA2E ->
Strategy attribution is therefore INFERRED from peer surveys whose labels are
verifiable and which use this exact a-e ordering: Mali EACI 2014-15 (a
near-twin francophone-Sahelian WB survey, §17 s17q02a-e, label-verified) and
Malawi IHS Module H (verified vs the IHS3 questionnaire).  Only the per-strategy
NAME attribution rests on this inference; the ``Days`` counts are exact.

SA1 (the 7-day gate) is NOT a coping day-count, so it is out of scope here;
SA3 (meals/day) and SA5/SA6 (12-month provisioning) belong to other features.

``Days`` is the native count of days in the last 7 (int 0-7).  WIDE->LONG: one
``(t, i, Strategy)`` row per strategy.  Rows where the household was not asked
(NaN) are dropped.  ``t`` = '2014'.  ``v`` is NOT baked in -- the framework
joins it from ``sample()`` at API time.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, format_id

# SA2{letter} -> rCSI Strategy, in questionnaire order (a-e) inferred from the
# Mali EACI 2014-15 / Malawi IHS peer surveys (see module docstring); the
# Burkina .dta labels are 80-char-truncated and cannot disambiguate directly.
STRATEGIES = {
    'A': 'LessPreferred',
    'B': 'LimitPortion',
    'C': 'ReduceMeals',
    'D': 'RestrictAdults',
    'E': 'BorrowFood',
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
