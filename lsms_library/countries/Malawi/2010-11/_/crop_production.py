"""Build crop_production for Malawi IHS3/IHPS 2010-11 (GAP 1).

Item-level (t, i, plot, crop) harvest feature.  Sources (Full_Sample/
Agriculture):
  * ag_mod_g — seasonal harvest, plot=ag_g0b, crop=ag_g0d (codes 1-48).
  * ag_mod_p — perennial harvest, plot=ag_p0b, crop=ag_p0d (codes->+1000).
  * ag_mod_i — seasonal SALE (hh, crop): crop=ag_i0b, sold=ag_i01,
    qty=ag_i02a, value=ag_i03.  No plot id.
  * ag_mod_q — perennial SALE (hh, crop): crop=ag_q0b, qty=ag_q02a,
    value=ag_q03.

i = format_id(case_id), aligning with plot_features 2010-11.  Sale is
attached only to single-plot (i, crop) rows (see malawi.assemble_
crop_production).  See lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import (_harvest_block, _sale_block, assemble_crop_production)


WAVE = '2010-11'
BASE = '../Data/Full_Sample/Agriculture/'

g = get_dataframe(BASE + 'ag_mod_g.dta', convert_categoricals=False)
p = get_dataframe(BASE + 'ag_mod_p.dta', convert_categoricals=False)
i_mod = get_dataframe(BASE + 'ag_mod_i.dta', convert_categoricals=False)
q = get_dataframe(BASE + 'ag_mod_q.dta', convert_categoricals=False)

for df in (g, p, i_mod, q):
    df['hhid'] = df['case_id'].apply(format_id)

harvest = [
    _harvest_block(g, hhid='hhid', plotkey='ag_g0b', cropcode='ag_g0d',
                   qty='ag_g13a', unit='ag_g13b', condition='ag_g13c',
                   plant_m='ag_g05a', plant_y='ag_g05b', harv_m='ag_g12b',
                   intercrop='ag_g01', perennial=False, t=WAVE),
    _harvest_block(p, hhid='hhid', plotkey='ag_p0b', cropcode='ag_p0d',
                   qty='ag_p09a', unit='ag_p09b', plant_y='ag_p04',
                   harv_m='ag_p06c', perennial=True, t=WAVE),
]

sale = [
    _sale_block(i_mod, hhid='hhid', cropcode='ag_i0b', sold_flag='ag_i01',
                qty_sold='ag_i02a', value_sold='ag_i03', perennial=False),
    _sale_block(q, hhid='hhid', cropcode='ag_q0b', sold_flag='ag_q01',
                qty_sold='ag_q02a', value_sold='ag_q03', perennial=True),
]

df = assemble_crop_production(WAVE, harvest, sale)

assert df.index.is_unique, f"Non-unique (t,i,plot,crop) in crop_production {WAVE}"
assert len(df) > 0, f"crop_production {WAVE} produced no rows"

to_parquet(df, 'crop_production.parquet')
