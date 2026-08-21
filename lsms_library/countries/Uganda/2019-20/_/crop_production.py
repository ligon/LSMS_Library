#!/usr/bin/env python
"""crop_production for Uganda UNPS 2019-20 (GAP 1 item-level build).

2019-20 keeps the WB column names (s5aq06b_1 = harvest unit, s5aq06c_1 =
condition) and records two parallel harvest conditions (_1 / _2) per
(plot, crop); both are emitted as separate reported rows.  The _1/_2 slot
number is NOT an ordinal "first/second harvest" -- the Stata variable
labels read "(2018, full harvest, condition1/condition2)" and each slot
carries its own 6c condition column drawing on the same 20-code scheme as
the 2009-16 long-form waves.  That condition is now an index level
(GH #323/#637).

KNOWN GAP: season B's agsec5b.dta also carries a slot _2
(s5bq06a_2 / s5bq06b_2 / s5bq06c_2, labelled "(2019, condition2)", 1 306
non-null conditions) which CROP_COLMAPS does not read, so those harvest
records are absent from the table entirely.  Adding them ADDS mass and so
is deliberately left to a separate change; see Uganda/_/CONTENTS.org.

Crop module lives under Data/Agric/.  See uganda.CROP_COLMAPS['2019-20'].
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import crop_production_for_wave, CROP_COLMAPS

t = '2019-20'

def _try(path):
    try:
        return get_dataframe(path, convert_categoricals=False)
    except Exception:
        return None

df5a = _try('../Data/Agric/agsec5a.dta')
df5b = _try('../Data/Agric/agsec5b.dta')
df4a = _try('../Data/Agric/agsec4a.dta')

df = crop_production_for_wave(t, df5a, df5b, df4a, CROP_COLMAPS[t])
assert len(df) > 0, "crop_production produced no rows for 2019-20"
assert df.index.is_unique, "Non-unique crop_production index for 2019-20"
to_parquet(df, 'crop_production.parquet')
