#!/usr/bin/env python
"""crop_production for Uganda UNPS 2015-16 (GAP 1 item-level build).

Reads AGSEC5A (season 1), AGSEC5B (season 2) and the two plot-crop
rosters AGSEC4A / AGSEC4B (the crop-stand flag, ONE PER SEASON -- GH #872;
season B's flag comes from AGSEC4B and is never borrowed from AGSEC4A)
via get_dataframe and emits a canonical (t,i,plot,j,u,condition,season)
parquet of REPORTED harvest values.  Harvest unit is a5aq6c (the column
whose labels decode to Kg/Sack/Bunch); the harvest CONDITION is a5aq6b,
now an index level in its own right (GH #323/#637) so fresh and dry
records for one plot-crop no longer collide and get summed.
See uganda.CROP_COLMAPS for the per-wave column map.

GH #829: this wave ships `a5aq8` / `a5bq8` divided by 100; they are
multiplied back to shillings below.  See the block above the build call.

The SOLD unit (a5?q7c) and SOLD condition (a5?q7b) are carried as
Unit_sold / Condition_sold (GH #824): Value_sold / Quantity_sold is a price
per Unit_sold, never per u.  (Value_sold itself carries this wave's x100
fix, #829.)
"""
import sys
sys.path.append('../../_/')
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import crop_production_for_wave, CROP_COLMAPS

t = '2015-16'

def _try(path):
    try:
        return get_dataframe(path, convert_categoricals=False)
    except Exception:
        return None

df5a = _try('../Data/AGSEC5A.dta')
df5b = _try('../Data/AGSEC5B.dta')
df4a = _try('../Data/AGSEC4A.dta')
df4b = _try('../Data/AGSEC4B.dta')

# --- GH #829: restore the sale value to shillings -------------------------
#
# `a5aq8` / `a5bq8` ("What was the value?") are shipped DIVIDED BY 100 in
# this wave, and in this wave only.  This is a defect in the distributed
# file, not a survey convention: the 2015-16 Agriculture & Livestock
# questionnaire asks "What was the value?  UShs" on p.13 (SECTION 5A CONT'D)
# and p.21 (SECTION 5B CONT'D) in words identical to the 2013-14
# questionnaire's pp.13/21 -- no "in '000", no "in hundreds", no changed
# answer box; and the DDI ships no variable codebook and an empty
# data-processing section.  The divisor is MEASURED, not assumed:
#
#   1. Storage type.  Of the 432 variables carried by the same name in both
#      2013-14 and 2015-16 across 16 AGSEC files, 38 change Stata storage
#      type and `a5aq8` / `a5bq8` are the ONLY two that go int32 -> double.
#      A currency answer key is not a double.
#   2. `a5aq8 x 100` is an exact integer on 100.00% of the 6 906 sale rows
#      (3 898 season A + 3 008 season B), and the roundness ladder of the
#      reconstruction matches 2013-14's raw column: divisible by
#      100 / 1 000 / 10 000 in 0.9949 / 0.9510 / 0.6690 of rows against
#      0.9988 / 0.9561 / 0.6701.  The non-integer share left behind
#      (0.51% / 0.30%) is exactly the neighbouring waves' share of values
#      that are not multiples of 100.
#   3. On rows whose SOLD unit is literally Kg -- no conversion factor in
#      the arithmetic at all -- the median implied price is 10 UGX/kg
#      against 1 000 (2011-12), 1 000 (2013-14) and 1 300 (2019-20).  It is
#      not a mixture: 99.25% of those rows imply under 100 UGX/kg.
#
# There is no fixed re-release to acquire: this repo's AGSEC5A.dta is
# byte-identical (md5 3117b28c356a480a896701efd7779b47) to
# agriculture/AGSEC5A.dta inside the World Bank's current live
# UGA_2015_UNPS_v02_M_STATA14.zip.  REVERT THIS BLOCK if the World Bank
# re-releases the wave with the column at its proper scale.
#
# See `Uganda/_/CONTENTS.org` (#829) for the full evidence.
#
# The revert instruction above is prose, and prose is not enforcement: if the
# World Bank re-releases this wave at the proper scale and someone re-acquires
# the data, an unremoved x100 makes 2015-16 a hundred times too LARGE -- same
# shape, so the coverage grader cannot see it.  So the guard below asserts the
# defect is STILL THERE before correcting it.  The median of positive
# `a5aq8` / `a5bq8` is ~855 / ~1000 in the defective file against 70 000 -
# 150 000 in every neighbouring wave; 10 000 sits in that gap.
_VALUE_SOLD_SCALE = 100
_DEFECT_MEDIAN_CEILING = 10_000
for _df, _col in ((df5a, 'a5aq8'), (df5b, 'a5bq8')):
    if _df is not None:
        assert _col in _df.columns, f"{_col} absent from 2015-16 AGSEC5; #829 rescale would be silent"
        _v = pd.to_numeric(_df[_col], errors='coerce')
        _med = _v[_v > 0].median()
        assert _med < _DEFECT_MEDIAN_CEILING, (
            f"GH #829: {_col} median of positive values is {_med:,.0f}, at or above "
            f"{_DEFECT_MEDIAN_CEILING:,} -- the divide-by-100 defect this block corrects "
            f"is NOT present.  The World Bank has most likely re-released the wave; "
            f"REMOVE the x100 rather than relaxing this assertion."
        )
        _df[_col] = _v * _VALUE_SOLD_SCALE

df = crop_production_for_wave(t, df5a, df5b, df4a, CROP_COLMAPS[t], df4b=df4b)
assert len(df) > 0, f"crop_production produced no rows for {t}"
assert df.index.is_unique, f"Non-unique crop_production index for {t}"
to_parquet(df, 'crop_production.parquet')
