#!/usr/bin/env python
"""Build people_last7days for Ethiopia ESS 2015-16 (Wave 3; GAP 3.1).

Individual 7-day activity at (t, i, pid) from §4 (sect4_hh_w3).  Same
hh_s4* variable scheme as W1/W2 (farm/SOB/wage HOURS hh_s4q04/05/07;
industry hh_s4q11_b; gate hh_s4q09; working_age hh_s4q01=="X").
i = household_id2, pid = individual_id2 (match household_roster for W3).
See ../../2011-12/_/people_last7days.py for the column layout.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import people_last7days_for_wave


lab = get_dataframe('../Data/sect4_hh_w3.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id2', pid='individual_id2',
    dummy_from_hours=True,
    farm_hrs='hh_s4q04', sb_hrs='hh_s4q05', wage_hrs='hh_s4q07',
    industry='hh_s4q11_b',
    employed_gate='hh_s4q09', employed_no=2,
    working_age='hh_s4q01', working_age_marker='X',
)

df = people_last7days_for_wave('2015-16', lab, colmap)

assert len(df) > 0, "people_last7days 2015-16 produced no rows"
to_parquet(df, 'people_last7days.parquet')
