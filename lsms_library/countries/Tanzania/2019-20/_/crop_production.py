"""crop_production for Tanzania NPS 2019-20 (NPS-SDD, Extended Panel;
parity-loop GAP 1).

Item-level harvest at grain (t, i, plot_id, j) from three modules:
  AG_SEC_4A  seasonal/annual crops  (ag4a_27 kg; ag4a_19 harvested y/n;
             ag4a_04 intercrop; ag4a_24_2 harvest-end month)
  AG_SEC_6A  perennial FRUIT trees  (ag6a_09 kg; ag6a_05; ag6a_07_4)
  AG_SEC_6B  perennial non-fruit    (ag6b_09 kg; ag6b_05; ag6b_07_4)
Reported sales (qty kg + value TSH) at (hhid, crop) grain are attached only
to single-plot (hhid, crop) rows: AG_SEC_5A seasonal, 7A fruit, 7B non-fruit.
Emits raw sdd_hhid as ``i``; the country-level concatenator applies id_walk
and the framework joins ``v`` from sample().
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import crop_production_for_wave


seas = get_dataframe('../Data/AG_SEC_4A.dta', convert_categoricals=False)
fruit = get_dataframe('../Data/AG_SEC_6A.dta', convert_categoricals=False)
peren = get_dataframe('../Data/AG_SEC_6B.dta', convert_categoricals=False)
s5 = get_dataframe('../Data/AG_SEC_5A.dta', convert_categoricals=False)
s7a = get_dataframe('../Data/AG_SEC_7A.dta', convert_categoricals=False)
s7b = get_dataframe('../Data/AG_SEC_7B.dta', convert_categoricals=False)

colmaps = dict(
    seasonal=dict(hhid='sdd_hhid', plot='plotnum', crop='cropid',
                  qty='ag4a_27', harvested='ag4a_19',
                  harvest_month='ag4a_24_2', intercrop='ag4a_04'),
    fruit=dict(hhid='sdd_hhid', plot='plotnum', crop='cropid',
               qty='ag6a_09', harvested=None,
               harvest_month='ag6a_07_4', intercrop='ag6a_05'),
    perennial=dict(hhid='sdd_hhid', plot='plotnum', crop='cropid',
                   qty='ag6b_09', harvested=None,
                   harvest_month='ag6b_07_4', intercrop='ag6b_05'),
    sales_seasonal=dict(hhid='sdd_hhid', crop='cropid',
                        qty='ag5a_02', value='ag5a_03'),
    sales_fruit=dict(hhid='sdd_hhid', crop='cropid',
                     qty='ag7a_03', value='ag7a_04'),
    sales_perennial=dict(hhid='sdd_hhid', crop='cropid',
                         qty='ag7b_03', value='ag7b_04'),
)

df = crop_production_for_wave(
    '2019-20', seas, fruit, peren,
    sales=dict(seasonal=s5, fruit=s7a, perennial=s7b),
    colmaps=colmaps)
assert df.index.is_unique, "crop_production 2019-20: (t,i,plot_id,j) not unique"
assert len(df) > 0
to_parquet(df, 'crop_production.parquet')
