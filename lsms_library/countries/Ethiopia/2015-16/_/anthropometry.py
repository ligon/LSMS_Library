#!/usr/bin/env python
"""Build anthropometry for Ethiopia ESS 2015-16 (Wave 3; GAP 5).

Item-level body measures at (t, i, pid).  One row per measured individual
from the §3 health module (sect3_hh_w3), carrying only the REPORTED
weight/height the WB code feeds into zscore06 (ETH_ESS3.do:1318-1331) --
NOT the z-score outputs.  Age_months / Sex joined from §1 (sect1_hh_w3).

W3, like W2, uses the SECOND id scheme (household_id2 / individual_id2) to
match household_roster's (i, pid) for this wave.

W3 source vars (NB: §1 age vars are the NON-underscore variants this wave --
hh_s1q04a / hh_s1q04b -- which is what the W3 household_roster uses too; the
underscore hh_s1q04_b in this file is a different 1/2-coded question):
  weight  hh_s3q22  (kg)
  height  hh_s3q23  (cm; >200 -> NaN)
  age     hh_s1q04a (years) / hh_s1q04b (months remainder)
  sex     hh_s1q03  (1=Male, 2=Female)
No MUAC in W3.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import anthropometry_for_wave


health = get_dataframe('../Data/sect3_hh_w3.dta', convert_categoricals=False)
roster = get_dataframe('../Data/sect1_hh_w3.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id2', pid='individual_id2',
    weight='hh_s3q22', height='hh_s3q23',
    r_hhid='household_id2', r_pid='individual_id2',
    age_yr='hh_s1q04a', age_mo='hh_s1q04b', sex='hh_s1q03',
)

df = anthropometry_for_wave('2015-16', health, roster, colmap)

assert len(df) > 0, "anthropometry 2015-16 produced no rows"
to_parquet(df, 'anthropometry.parquet')
