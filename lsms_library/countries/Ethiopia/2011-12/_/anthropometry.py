#!/usr/bin/env python
"""Build anthropometry for Ethiopia ESS 2011-12 (Wave 1; GAP 5).

Item-level body measures at (t, i, pid).  One row per measured individual
from the §3 health module (sect3_hh_w1), carrying only the REPORTED
weight/height the WB code feeds into zscore06 (ETH_ESS1.do:1217-1235) --
NOT the z-score outputs.  Age_months / Sex are joined from the §1 roster
(sect1_hh_w1) so the row is self-describing for the downstream z-score
transform.

W1 source vars:
  weight  hh_s3q22  (kg)
  height  hh_s3q23  (cm; >200 -> NaN)
  age     hh_s1q04_a (years) / hh_s1q04_b (months remainder)
  sex     hh_s1q03  (1=Male, 2=Female)
No MUAC in W1 (no ESS wave asks it).
i = household_id, pid = individual_id (match household_roster for W1).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import anthropometry_for_wave


health = get_dataframe('../Data/sect3_hh_w1.dta', convert_categoricals=False)
roster = get_dataframe('../Data/sect1_hh_w1.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', pid='individual_id',
    weight='hh_s3q22', height='hh_s3q23',
    r_hhid='household_id', r_pid='individual_id',
    age_yr='hh_s1q04_a', age_mo='hh_s1q04_b', sex='hh_s1q03',
)

df = anthropometry_for_wave('2011-12', health, roster, colmap)

assert len(df) > 0, "anthropometry 2011-12 produced no rows"
to_parquet(df, 'anthropometry.parquet')
