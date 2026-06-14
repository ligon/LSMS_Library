"""plot_labor for Tanzania NPS 2020-21 (NPS Y5 Refresh Panel;
parity-loop GAP 3).

Same AG_SEC_3A module and variable names as 2019-20 (identical NPS
questionnaire) for HIRED labor (ag3a_73 / ag3a_74_*).  The Y5 instrument
ALSO records per-member FAMILY DAYS (ag3a_72c_* prep / ag3a_72g_* weeding /
ag3a_72k_* harvest), so family rows ARE emitted this wave (unlike 2019-20,
whose family block holds worker IDs only).  See ``tanzania.plot_labor_for_wave``
for the schema.  File names are lowercase and the id columns differ
(y5_hhid / plot_id).  Emits raw y5_hhid as ``i`` and the within-HH plot_id as
``plot_id``; the country-level concatenator applies id_walk and the framework
joins ``v`` from sample().
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import plot_labor_for_wave


sec3a = get_dataframe('../Data/ag_sec_3a.dta', convert_categoricals=False)

colmap = dict(hhid='y5_hhid', plot='plot_id')

df = plot_labor_for_wave('2020-21', sec3a, colmap)
assert df.index.is_unique, "plot_labor 2020-21: (t,i,plot_id,source) not unique"
assert len(df) > 0
to_parquet(df, 'plot_labor.parquet')
