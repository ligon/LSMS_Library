"""people_last7days for Tanzania NPS 2019-20 (NPS-SDD, Extended Panel;
parity-loop GAP 3).

Per-individual 7-day activity at grain (t, i, pid) from HH_SEC_E1, mirroring
Uganda's people_last7days construct: work dummies (farm_work / SOB_work /
wage_work), hours (farm_hrs / SB_hrs / wage_hrs), wage-work industry and
working_age.  Source vars hh_e07/hh_e05/hh_e03 (7-day activity), hh_e08/hh_e06/
hh_e04 (hours), hh_e31b_2 (ISIC sector), hh_e01_1 (working age); see
``tanzania.people_last7days_for_wave``.  i/pid (sdd_hhid/sdd_indid) match the
household_roster keys; the country-level concatenator applies id_walk and the
framework joins ``v`` from sample().
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import people_last7days_for_wave


sec = get_dataframe('../Data/HH_SEC_E1.dta', convert_categoricals=False)

colmap = dict(hhid='sdd_hhid', pid='sdd_indid')

df = people_last7days_for_wave('2019-20', sec, colmap)
assert df.index.is_unique, "people_last7days 2019-20: (t,i,pid) not unique"
assert len(df) > 0
to_parquet(df, 'people_last7days.parquet')
