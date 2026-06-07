#!/usr/bin/env python
"""Tanzania 2019-20 food_coping -- coping-strategies battery (SECTION H).

Source: ``HH_SEC_H.dta`` ("Food Security").  HOUSEHOLD-level (one row per
household, index ``i = sdd_hhid`` -- matches the wave's household_roster ``i``).

The §H coping battery asks, for each strategy, "In the past 7 days, how many
days had to: [...]".  The 8 items ``hh_h02a .. hh_h02h`` (full variable labels,
not truncated):

    hh_h02a  RELY ON LESS PREFERRED FOOD                  -> LessPreferred
    hh_h02b  LIMIT THE VARIETY OF FOODS EATEN             -> LimitVariety
    hh_h02c  LIMIT PORTION SIZE AT MEAL-TIMES             -> LimitPortion
    hh_h02d  REDUCE THE NUMBER OF MEALS EATEN IN A DAY    -> ReduceMeals
    hh_h02e  RESTRICT CONSUMPTION BY ADULTS (small kids)  -> RestrictAdults
    hh_h02f  BORROW FOOD / RELY ON HELP FROM A FRIEND     -> BorrowFood
    hh_h02g  HAD NO FOOD OF ANY KIND IN THE HOUSEHOLD     -> NoFood
    hh_h02h  GO A WHOLE DAY AND NIGHT WITHOUT EATING      -> WholeDayWithout

LessPreferred / LimitPortion / ReduceMeals / RestrictAdults / BorrowFood are
the five canonical rCSI strategies; LimitVariety / NoFood / WholeDayWithout are
the survey-specific extras (kept descriptively).

``Days`` is the native count of days in the last 7 (int 0-7).  ``t`` is added
here ('2019-20'); the country-level aggregator concatenates waves and applies
id_walk.  (hh_h08, the separate 12-month-insufficiency item, is out of scope.)
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

# hh_h02{letter} -> canonical (or descriptive) Strategy.
STRATEGIES = {
    'a': 'LessPreferred',
    'b': 'LimitVariety',
    'c': 'LimitPortion',
    'd': 'ReduceMeals',
    'e': 'RestrictAdults',
    'f': 'BorrowFood',
    'g': 'NoFood',
    'h': 'WholeDayWithout',
}

df = get_dataframe('../Data/HH_SEC_H.dta', convert_categoricals=False)

item_cols = [f'hh_h02{x}' for x in STRATEGIES]
wide = df[['sdd_hhid'] + item_cols].rename(columns={'sdd_hhid': 'i'}).copy()
wide['i'] = wide['i'].astype(str)

# --- WIDE -> LONG on (i, Strategy) ---------------------------------------
long = wide.melt(id_vars='i', value_vars=item_cols,
                 var_name='item', value_name='Days')
long['Strategy'] = long['item'].str.removeprefix('hh_h02').map(STRATEGIES)
long['Days'] = pd.to_numeric(long['Days'], errors='coerce').round().astype('Int64')

long = long.dropna(subset=['Days'])

long['t'] = '2019-20'
out = long[['t', 'i', 'Strategy', 'Days']].set_index(['t', 'i', 'Strategy'])

if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

to_parquet(out, 'food_coping.parquet')
