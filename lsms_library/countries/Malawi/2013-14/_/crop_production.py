"""Build crop_production for Malawi IHPS 2013-14 (GAP 1).

Item-level (t, i, plot, crop) harvest feature.  Variable names shifted
from 2010-11: in 2013-14 the seasonal Module G plot key is ag_g00 and
the crop code is ag_g0b (NOT ag_g0d); perennial Module P plot key is
ag_p00, crop ag_p0c.  Sources (flat Data/):
  * AG_MOD_G_13 — seasonal harvest, plot=ag_g00, crop=ag_g0b (1-48).
  * AG_MOD_P_13 — perennial harvest, plot=ag_p00, crop=ag_p0c (->+1000).
  * AG_MOD_I_13 — seasonal SALE (hh, crop): crop=ag_i0b, sold=ag_i01,
    qty=ag_i02a, value=ag_i03.
  * AG_MOD_Q_13 — perennial SALE (hh, crop): crop=ag_q0b, qty=ag_q02a,
    value=ag_q03.

i = format_id(y2_hhid), aligning with plot_features 2013-14.  See
lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import (_harvest_block, _sale_block, assemble_crop_production)


WAVE = '2013-14'

g = get_dataframe('../Data/AG_MOD_G_13.dta', convert_categoricals=False)
p = get_dataframe('../Data/AG_MOD_P_13.dta', convert_categoricals=False)
i_mod = get_dataframe('../Data/AG_MOD_I_13.dta', convert_categoricals=False)
q = get_dataframe('../Data/AG_MOD_Q_13.dta', convert_categoricals=False)

for df in (g, p, i_mod, q):
    df['hhid'] = df['y2_hhid'].apply(format_id)

harvest = [
    _harvest_block(g, hhid='hhid', plotkey='ag_g00', cropcode='ag_g0b',
                   qty='ag_g13a', unit='ag_g13b', condition='ag_g13c',
                   plant_m='ag_g05a', plant_y='ag_g05b', harv_m='ag_g12b',
                   intercrop='ag_g01', perennial=False, t=WAVE),
    _harvest_block(p, hhid='hhid', plotkey='ag_p00', cropcode='ag_p0c',
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
