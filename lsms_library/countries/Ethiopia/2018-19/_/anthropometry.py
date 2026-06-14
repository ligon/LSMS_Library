#!/usr/bin/env python
"""Build anthropometry for Ethiopia ESS 2018-19 (Wave 4; GAP 5).

Item-level body measures at (t, i, pid).  One row per measured individual
from the §3 health module (sect3_hh_w4), carrying only the REPORTED
weight/height the WB code feeds into zscore06 (ETH_ESS4.do:1358-1363) --
NOT the z-score outputs.  Age_months / Sex joined from §1 (sect1_hh_w4).

W4/W5 renumber the §3 anthro vars (s3q37/s3q38, NOT hh_s3q22/23 which here
are fertilizer-use flags) and the §1 age/sex vars (s1q03a/s1q03b, s1q02).
i = household_id, pid = individual_id (match household_roster for W4).

W4 source vars:
  weight  s3q37  (kg)
  height  s3q38  (cm; >200 -> NaN)
  age     s1q03a (years) / s1q03b (months remainder)
  sex     s1q02  (1=Male, 2=Female)
No MUAC in W4.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import anthropometry_for_wave


health = get_dataframe('../Data/sect3_hh_w4.dta', convert_categoricals=False)
roster = get_dataframe('../Data/sect1_hh_w4.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', pid='individual_id',
    weight='s3q37', height='s3q38',
    r_hhid='household_id', r_pid='individual_id',
    age_yr='s1q03a', age_mo='s1q03b', sex='s1q02',
)

df = anthropometry_for_wave('2018-19', health, roster, colmap)

assert len(df) > 0, "anthropometry 2018-19 produced no rows"
to_parquet(df, 'anthropometry.parquet')
