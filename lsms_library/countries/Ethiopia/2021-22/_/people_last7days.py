#!/usr/bin/env python
"""Build people_last7days for Ethiopia ESS 2021-22 (Wave 5; GAP 3.1).

Individual 7-day activity at (t, i, pid) from §4 (sect4_hh_w5).  Same s4*
scheme as W4 (separate did-activity dummies + hours):
  farm dummy s4q05 / hours s4q06 ; SOB dummy s4q08 / hours s4q09 ;
  wage dummy s4q12 / hours s4q13 ; industry s4q34d ;
  working_age = (s4q00 == 1).
i = household_id, pid = individual_id (match household_roster for W5).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import people_last7days_for_wave


lab = get_dataframe('../Data/sect4_hh_w5.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', pid='individual_id',
    farm_work='s4q05', sob_work='s4q08', wage_work='s4q12',
    farm_hrs='s4q06', sb_hrs='s4q09', wage_hrs='s4q13',
    industry='s4q34d',
    working_age='s4q00', working_age_marker=1,
)

df = people_last7days_for_wave('2021-22', lab, colmap)

assert len(df) > 0, "people_last7days 2021-22 produced no rows"
to_parquet(df, 'people_last7days.parquet')
