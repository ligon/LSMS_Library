"""plot_labor for Tanzania NPS 2019-20 (NPS-SDD, Extended Panel;
parity-loop GAP 3).

Item-level plot labor at grain (t, i, plot_id, source) from AG_SEC_3A:
  HIRED  -- ag3a_73 (hired y/n) + ag3a_74_{1,2,3}{a,b,c} (per-gender days per
            task) and ag3a_74_{1,2,3}d (cash wage paid, TSH).
  FAMILY -- the 2019-20 family-labor block records only the WORKER ROSTER IDs
            per task (ag3a_72b/f/j_*), NOT days, so family person-days are not
            reported this wave -> no family rows are emitted (we do not
            fabricate; the WB NPS5.do rowtotals the ID columns, which we do not
            reproduce).  Family days ARE reported in 2020-21.
Emits raw sdd_hhid as ``i`` and the within-HH plotnum as ``plot_id`` (the same
plot key as crop_production / plot_inputs, so plot_labor joins them on
(t, i, plot_id)); the country-level concatenator applies id_walk and the
framework joins ``v`` from sample().
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import plot_labor_for_wave


sec3a = get_dataframe('../Data/AG_SEC_3A.dta', convert_categoricals=False)

colmap = dict(hhid='sdd_hhid', plot='plotnum')

df = plot_labor_for_wave('2019-20', sec3a, colmap)
assert df.index.is_unique, "plot_labor 2019-20: (t,i,plot_id,source) not unique"
assert len(df) > 0
to_parquet(df, 'plot_labor.parquet')
