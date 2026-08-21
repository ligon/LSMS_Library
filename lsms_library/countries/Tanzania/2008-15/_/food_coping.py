#!/usr/bin/env python
"""Tanzania 2008-15 food_coping -- coping-strategies battery (SECTION H).

Source file: ``upd4_hh_h.dta`` -- the multi-round "Food Security" module.  As
with the other multi-round Tanzania scripts this writes ONE parquet carrying
all available rounds with ``t`` in the index; ``Wave.grab_data`` filters to the
requested sub-wave.

The §H coping-strategies battery (CSI / rCSI) asks, for each strategy, "In the
past 7 days, how many days have you or someone in your household had to [...]".
The module is HOUSEHOLD-level (one row per (round, r_hhid)); ``i = r_hhid``
matches the wave's household_roster ``i``.

NB the round-1 (2008-09) survey did NOT field §H -- the multi-round file only
carries rounds 2/3/4 (waves 2010-11, 2012-13, 2014-15) for this module.

The 8 day-count items ``hh_02_1 .. hh_02_8`` carry the same battery, in the
same order, as the later NPS waves' ``hh_h02a .. hh_h02h`` (whose full variable
labels, not truncated at Stata's 80-char limit, give the strategy text -- the
2008-15 labels are truncated after "had to [").  Positional mapping:

    hh_02_1  RELY ON LESS PREFERRED FOOD                  -> LessPreferred
    hh_02_2  LIMIT THE VARIETY OF FOODS EATEN             -> LimitVariety
    hh_02_3  LIMIT PORTION SIZE AT MEAL-TIMES             -> LimitPortion
    hh_02_4  REDUCE THE NUMBER OF MEALS EATEN IN A DAY    -> ReduceMeals
    hh_02_5  RESTRICT CONSUMPTION BY ADULTS (small kids)  -> RestrictAdults
    hh_02_6  BORROW FOOD / RELY ON HELP FROM A FRIEND     -> BorrowFood
    hh_02_7  HAD NO FOOD OF ANY KIND IN THE HOUSEHOLD     -> NoFood
    hh_02_8  GO A WHOLE DAY AND NIGHT WITHOUT EATING      -> WholeDayWithout

LessPreferred / LimitPortion / ReduceMeals / RestrictAdults / BorrowFood are
the five canonical rCSI strategies; LimitVariety / NoFood / WholeDayWithout are
the survey-specific extras (kept descriptively rather than discarded).

``Days`` is the native count of days in the last 7 (int 0-7).  ``t`` is the
round mapped to its wave label.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

# hh_02_N suffix -> canonical (or descriptive) Strategy.  Order matches the
# later waves' hh_h02a..h (see module docstring).
STRATEGIES = {
    1: 'LessPreferred',
    2: 'LimitVariety',
    3: 'LimitPortion',
    4: 'ReduceMeals',
    5: 'RestrictAdults',
    6: 'BorrowFood',
    7: 'NoFood',
    8: 'WholeDayWithout',
}

df = get_dataframe('../Data/upd4_hh_h.dta', convert_categoricals=False)

item_cols = [f'hh_02_{n}' for n in STRATEGIES]
wide = df[['round', 'r_hhid'] + item_cols].rename(columns={'r_hhid': 'i'}).copy()
wide['i'] = wide['i'].astype(str)
wide['t'] = wide['round'].astype(float).map(round_match)
wide = wide.drop(columns=['round'])

# --- WIDE -> LONG on (t, i, Strategy) ------------------------------------
long = wide.melt(id_vars=['t', 'i'], value_vars=item_cols,
                 var_name='item', value_name='Days')
long['Strategy'] = long['item'].str.removeprefix('hh_02_').astype(int).map(STRATEGIES)
long['Days'] = pd.to_numeric(long['Days'], errors='coerce').round().astype('Int64')

# Drop rows with no genuine count (question not asked / unanswered).
long = long.dropna(subset=['Days', 't'])

out = long[['t', 'i', 'Strategy', 'Days']].set_index(['t', 'i', 'Strategy'])

# GH #637 key-soundness review -- the key is SOUND; the collapse de-replicates.
#
# Same UPHI line replication as housing.py / sample.py: upd4_hh_h.dta is keyed
# on the panel-tracking LINE, so 23,122 source rows carry only 13,275
# household-rounds and 6,814 of those arrive 2..10 times.  After the melt that
# is 184,938 rows over 106,179 (t, i, Strategy) cells, 54,495 of them duplicated.
#
# The lines are copies, not different households: over the whole file, ZERO of
# the 6,814 duplicate (round, r_hhid) groups differ on ANY hh_* column -- not
# just the eight hh_02_* items this feature reads.  So no Days value is chosen
# over another; identical rows are discarded.  Per-round duplicate-group counts
# (R2 2,698 / R3 3,628 / R4 488) match housing.py's exactly, which is the same
# replication seen through a second module.
if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

to_parquet(out, 'food_coping.parquet')
