"""plot_inputs for Tanzania NPS 2019-20 (NPS-SDD, Extended Panel;
parity-loop GAP 2).

Item-level inputs at grain (t, i, plot_id, input, crop) from two modules:
  AG_SEC_3A  plot detail: organic fertilizer (ag3a_41..45), inorganic
             fertilizer type 1 (ag3a_47..51) and type 2 (ag3a_54..58),
             herbicide (ag3a_60..63), pesticide (ag3a_65a..65c).
  AG_SEC_4A  seasonal-crop seed: ag4a_08 improved flag, ag4a_10_1/2 total
             seed qty+unit, ag4a_10c_1/2 purchased seed qty+unit, ag4a_12
             amount paid; cropid -> the seed's crop (crop index level).
Emits raw sdd_hhid as ``i``; the country-level concatenator applies id_walk
and the framework joins ``v`` from sample().
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import plot_inputs_for_wave


sec3a = get_dataframe('../Data/AG_SEC_3A.dta', convert_categoricals=False)
sec4a = get_dataframe('../Data/AG_SEC_4A.dta', convert_categoricals=False)

colmap = dict(hhid='sdd_hhid', plot='plotnum', crop='cropid')

df = plot_inputs_for_wave('2019-20', sec3a, sec4a, colmap)
assert df.index.is_unique, "plot_inputs 2019-20: (t,i,plot_id,input,crop) not unique"
assert len(df) > 0
to_parquet(df, 'plot_inputs.parquet')
