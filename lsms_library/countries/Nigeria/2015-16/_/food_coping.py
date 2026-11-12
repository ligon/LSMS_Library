"""Build food_coping for Nigeria GHS-Panel wave 3 (2015-16; #332).

Family B (coping-strategies / rCSI), at (t, i, Strategy) with Days.
PP section 9 supplies s9q1a..i under 2015Q3. Own PH section 12 Q1 and
the released s12q1a..i labels establish the same nine seven-day day counts
under 2016Q1 (GH #587). They are separate answers, not a PP carry-forward.
"""
import sys
import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import food_coping_for_wave, PP_QUARTER, PH_QUARTER

t = PP_QUARTER['2015-16']

df = get_dataframe('../Data/sect9_plantingw3.dta', convert_categoricals=False)

coping = food_coping_for_wave(t, df)

assert coping.index.is_unique, "Non-unique (t, i, Strategy) in food_coping 2015-16"
assert len(coping) > 0, "food_coping 2015-16 produced no rows"

ph = get_dataframe('../Data/sect12_harvestw3.dta', convert_categoricals=False)
ph_items = {
    's12q1a': 'LessPreferred',
    's12q1b': 'LimitVariety',
    's12q1c': 'LimitPortion',
    's12q1d': 'ReduceMeals',
    's12q1e': 'RestrictAdults',
    's12q1f': 'BorrowFood',
    's12q1g': 'NoFood',
    's12q1h': 'SleepHungry',
    's12q1i': 'WholeDayNoFood',
}
missing = [field for field in ph_items if field not in ph.columns]
if missing:
    raise ValueError(f"food_coping 2016Q1: missing PH day-count fields {missing}")
ph_coping = food_coping_for_wave(PH_QUARTER['2015-16'], ph, items=ph_items)
coping = pd.concat([coping, ph_coping])
assert coping.index.is_unique, "Non-unique (t, i, Strategy) in food_coping 2015-16"

to_parquet(coping, 'food_coping.parquet')
