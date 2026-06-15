"""Build food_coping for Nigeria GHS-Panel wave 2 (2012-13; #332).

Family B (coping-strategies / rCSI).  GHS section 9 ("Food Security")
is a coping day-count battery: items s9q1a..s9q1i ask "how many days
in the last 7 had to [strategy]", coded 0-7.  Collected in the
post-planting round only -> single t = 2012Q3.  Long-form
(t, i, Strategy) with Days.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import food_coping_for_wave, PP_QUARTER

t = PP_QUARTER['2012-13']

df = get_dataframe('../Data/Post Planting Wave 2/Household/'
                   'sect9_plantingw2.dta', convert_categoricals=False)

coping = food_coping_for_wave(t, df)

assert coping.index.is_unique, "Non-unique (t, i, Strategy) in food_coping 2012-13"
assert len(coping) > 0, "food_coping 2012-13 produced no rows"

to_parquet(coping, 'food_coping.parquet')
