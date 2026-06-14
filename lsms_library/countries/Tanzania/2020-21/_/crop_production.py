"""crop_production for Tanzania NPS 2020-21 (NPS Y5, Refresh Panel;
parity-loop GAP 1).

Same three-module harvest stack as 2019-20, but this wave keys on
``y5_hhid`` and ``plot_id`` (there is no ``plotnum`` column), matching the
plot_features grain.  Variable names (ag4a_27 / ag4a_19 / ag4a_24_2 /
ag4a_04 / ag6a_09 / ag6a_07_4 / ag6a_05 / ag5a_02 / ag5a_03 / ag7a_03 ...)
are identical to 2019-20.  See ``crop_production_for_wave`` in tanzania.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import crop_production_for_wave


seas = get_dataframe('../Data/ag_sec_4a.dta', convert_categoricals=False)
fruit = get_dataframe('../Data/ag_sec_6a.dta', convert_categoricals=False)
peren = get_dataframe('../Data/ag_sec_6b.dta', convert_categoricals=False)
s5 = get_dataframe('../Data/ag_sec_5a.dta', convert_categoricals=False)
s7a = get_dataframe('../Data/ag_sec_7a.dta', convert_categoricals=False)
s7b = get_dataframe('../Data/ag_sec_7b.dta', convert_categoricals=False)

colmaps = dict(
    seasonal=dict(hhid='y5_hhid', plot='plot_id', crop='cropid',
                  qty='ag4a_27', harvested='ag4a_19',
                  harvest_month='ag4a_24_2', intercrop='ag4a_04'),
    fruit=dict(hhid='y5_hhid', plot='plot_id', crop='cropid',
               qty='ag6a_09', harvested=None,
               harvest_month='ag6a_07_4', intercrop='ag6a_05'),
    perennial=dict(hhid='y5_hhid', plot='plot_id', crop='cropid',
                   qty='ag6b_09', harvested=None,
                   harvest_month='ag6b_07_4', intercrop='ag6b_05'),
    sales_seasonal=dict(hhid='y5_hhid', crop='cropid',
                        qty='ag5a_02', value='ag5a_03'),
    sales_fruit=dict(hhid='y5_hhid', crop='cropid',
                     qty='ag7a_03', value='ag7a_04'),
    sales_perennial=dict(hhid='y5_hhid', crop='cropid',
                         qty='ag7b_03', value='ag7b_04'),
)

df = crop_production_for_wave(
    '2020-21', seas, fruit, peren,
    sales=dict(seasonal=s5, fruit=s7a, perennial=s7b),
    colmaps=colmaps)
assert df.index.is_unique, "crop_production 2020-21: (t,i,plot_id,j) not unique"
assert len(df) > 0
to_parquet(df, 'crop_production.parquet')
