#!/usr/bin/env python
"""Build people_last7days for Ethiopia ESS 2011-12 (Wave 1; GAP 3.1).

Individual 7-day activity at (t, i, pid) from §4 labor roster
(sect4_hh_w1), mirroring Uganda's per-individual schema.  Reproduces the
REPORTED per-individual data the WB labor block reads
(ETH_ESS1.do:1242-1264) -- NOT the WB nb_members_working_age household-sum
or the six ind_* dummies (the single reported industry label is kept;
the dummies derive from it).

W1 source vars (hh_s4* prefix): farm hours hh_s4q04, SOB hours hh_s4q05,
wage hours hh_s4q07 (the WB recodes each else=1 -> work dummy, so the
dummies are recoded from these hours).  employed-gate hh_s4q09 (==2 or
missing blanks the wage-work industry).  industry hh_s4q11_b.
working_age = (hh_s4q01 == "X").
i = household_id, pid = individual_id (match household_roster for W1).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import people_last7days_for_wave


lab = get_dataframe('../Data/sect4_hh_w1.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', pid='individual_id',
    # W1-W3 record only HOURS; recode the did-activity dummies from them.
    dummy_from_hours=True,
    farm_hrs='hh_s4q04', sb_hrs='hh_s4q05', wage_hrs='hh_s4q07',
    industry='hh_s4q11_b',
    employed_gate='hh_s4q09', employed_no=2,
    working_age='hh_s4q01', working_age_marker='X',
)

df = people_last7days_for_wave('2011-12', lab, colmap)

assert len(df) > 0, "people_last7days 2011-12 produced no rows"
to_parquet(df, 'people_last7days.parquet')
