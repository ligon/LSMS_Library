"""Build plot_inputs for Malawi IHPS 2013-14 (GAP 2).

Item-level (t, i, plot, input, crop, u) feature.  Variable names track
2010-11 except the household id (y2_hhid) and Module G/H crop-code columns
(seasonal crop is ag_g0b; Module H crop is ag_h0c).  Module D layout is
the IHS3/IHPS2 generation: slot-2 fertilizer type ag_d39f, applied-qty
ag_d39i.  No improved-seed flag in this wave.  Sources (flat Data/):
  * AG_MOD_D_13 -- plot inputs; plot key ag_d00.
  * AG_MOD_G_13 -- seasonal plot-crop roster; plot ag_g00, crop ag_g0b,
    seed ag_g04a/ag_g04b.
  * AG_MOD_H_13 -- purchased seed (hh, crop): ag_h16a/b + ag_h19,
    ag_h26a/b + ag_h29; crop ag_h0c.

i = format_id(y2_hhid), aligning with plot_features / crop_production.
See lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import (_fertilizer_block, _organic_block, _pesticide_block,
                    _seed_block, _seed_purchase_block, assemble_plot_inputs)


WAVE = '2013-14'

d = get_dataframe('../Data/AG_MOD_D_13.dta', convert_categoricals=False)
g = get_dataframe('../Data/AG_MOD_G_13.dta', convert_categoricals=False)
h = get_dataframe('../Data/AG_MOD_H_13.dta', convert_categoricals=False)

d['hhid'] = d['y2_hhid'].apply(format_id)
d['plotkey'] = d['ag_d00'].apply(format_id)
g['hhid'] = g['y2_hhid'].apply(format_id)
g['plotkey'] = g['ag_g00'].apply(format_id)
h['hhid'] = h['y2_hhid'].apply(format_id)

pieces = [
    _fertilizer_block(d, hhid='hhid', plotkey='plotkey',
                      type1='ag_d39a', qty1='ag_d39d', unit1='ag_d39e',
                      type2='ag_d39f', qty2='ag_d39i', unit2='ag_d39j',
                      t=WAVE),
    _organic_block(d, hhid='hhid', plotkey='plotkey', flag='ag_d36', t=WAVE),
    _pesticide_block(d, hhid='hhid', plotkey='plotkey', gate='ag_d40',
                     slots=[('ag_d41a', 'ag_d41b', 'ag_d41c'),
                            ('ag_d41d', 'ag_d41e', 'ag_d41f')], t=WAVE),
    _seed_block(g, hhid='hhid', plotkey='plotkey', cropcode='ag_g0b',
                qty='ag_g04a', unit='ag_g04b', improved=None, t=WAVE),
]

seed_purchase = [
    _seed_purchase_block(h, hhid='hhid', cropcode='ag_h0c',
                         qty_cols=['ag_h16a', 'ag_h26a'],
                         unit_cols=['ag_h16b', 'ag_h26b'],
                         value_cols=['ag_h19', 'ag_h29']),
]

df = assemble_plot_inputs(WAVE, pieces, seed_purchase)

assert df.index.is_unique, f"Non-unique (t,i,plot,input,crop,u) in plot_inputs {WAVE}"
assert len(df) > 0, f"plot_inputs {WAVE} produced no rows"

to_parquet(df, 'plot_inputs.parquet')
