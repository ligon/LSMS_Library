"""plot_inputs for Tanzania NPS 2020-21 (NPS Y5 Refresh Panel;
parity-loop GAP 2).

Same two modules and variable names as 2019-20 (identical NPS questionnaire);
only the file names (lowercase) and the household / plot id columns differ
(y5_hhid / plot_id).  See ``tanzania.plot_inputs_for_wave`` for the schema.
Emits raw y5_hhid as ``i``; the country-level concatenator applies id_walk
and the framework joins ``v`` from sample().
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import plot_inputs_for_wave


sec3a = get_dataframe('../Data/ag_sec_3a.dta', convert_categoricals=False)
sec4a = get_dataframe('../Data/ag_sec_4a.dta', convert_categoricals=False)

colmap = dict(hhid='y5_hhid', plot='plot_id', crop='cropid')

df = plot_inputs_for_wave('2020-21', sec3a, sec4a, colmap)
assert df.index.is_unique, "plot_inputs 2020-21: (t,i,plot_id,input,crop) not unique"
assert len(df) > 0
to_parquet(df, 'plot_inputs.parquet')
