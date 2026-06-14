"""Build plot_inputs for Malawi IHS3 2010-11 (GAP 2).

Item-level (t, i, plot, input, crop, u) feature -- one row per input
applied to a plot.  Sources (Full_Sample/Agriculture):
  * ag_mod_d -- plot-level input module.  Inorganic fertilizer in two
    slots (ag_d39a type / ag_d39d applied-qty / ag_d39e unit; ag_d39f
    type / ag_d39i applied-qty / ag_d39j unit), organic-fertilizer flag
    ag_d36, agrochemical types ag_d41a/d gated by ag_d40.  Plot key
    ag_d00 (the same R{n} key as plot_features' ag_c00 and
    crop_production's ag_g0b).
  * ag_mod_g -- seasonal plot-crop roster: seed planted ag_g04a/ag_g04b
    per (plot=ag_g0b, crop=ag_g0d).  No improved-seed flag in IHS3.
  * ag_mod_h -- purchased seed (hh, crop): ag_h16a/b + ag_h19 (source 1),
    ag_h26a/b + ag_h29 (source 2); crop=ag_h0b.  Attached to single-plot
    seed rows only (no plot in the module).

i = format_id(case_id), aligning with plot_features / crop_production.
See lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import (_fertilizer_block, _organic_block, _pesticide_block,
                    _seed_block, _seed_purchase_block, assemble_plot_inputs)


WAVE = '2010-11'
BASE = '../Data/Full_Sample/Agriculture/'

d = get_dataframe(BASE + 'ag_mod_d.dta', convert_categoricals=False)
g = get_dataframe(BASE + 'ag_mod_g.dta', convert_categoricals=False)
h = get_dataframe(BASE + 'ag_mod_h.dta', convert_categoricals=False)

d['hhid'] = d['case_id'].apply(format_id)
d['plotkey'] = d['ag_d00'].apply(format_id)
g['hhid'] = g['case_id'].apply(format_id)
g['plotkey'] = g['ag_g0b'].apply(format_id)
h['hhid'] = h['case_id'].apply(format_id)

pieces = [
    _fertilizer_block(d, hhid='hhid', plotkey='plotkey',
                      type1='ag_d39a', qty1='ag_d39d', unit1='ag_d39e',
                      type2='ag_d39f', qty2='ag_d39i', unit2='ag_d39j',
                      t=WAVE),
    _organic_block(d, hhid='hhid', plotkey='plotkey', flag='ag_d36', t=WAVE),
    _pesticide_block(d, hhid='hhid', plotkey='plotkey', gate='ag_d40',
                     slots=[('ag_d41a', 'ag_d41b', 'ag_d41c'),
                            ('ag_d41d', 'ag_d41e', 'ag_d41f')], t=WAVE),
    _seed_block(g, hhid='hhid', plotkey='plotkey', cropcode='ag_g0d',
                qty='ag_g04a', unit='ag_g04b', improved=None, t=WAVE),
]

seed_purchase = [
    _seed_purchase_block(h, hhid='hhid', cropcode='ag_h0b',
                         qty_cols=['ag_h16a', 'ag_h26a'],
                         unit_cols=['ag_h16b', 'ag_h26b'],
                         value_cols=['ag_h19', 'ag_h29']),
]

df = assemble_plot_inputs(WAVE, pieces, seed_purchase)

assert df.index.is_unique, f"Non-unique (t,i,plot,input,crop,u) in plot_inputs {WAVE}"
assert len(df) > 0, f"plot_inputs {WAVE} produced no rows"

to_parquet(df, 'plot_inputs.parquet')
