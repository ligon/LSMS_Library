"""Build plot_labor for Malawi IHS3 2010-11 (GAP 3, plot grain).

Item-level (t, i, plot, source) plot-labor feature -- one reported row per
labor source (family / hired / other) applied to a plot.  Source
(Full_Sample/Agriculture):
  * ag_mod_d -- the plot-input/plot-labor module.  Family labor is the
    day*#people products in the ag_d42/43/44 {b/c,f/g,j/k,n/o} person-slot
    blocks; hired labor is ag_d47a/ag_d48a (man), ag_d47c/ag_d48c (woman),
    ag_d47e/ag_d48e (child) days with wages ag_d47b/d/f, ag_d48b/d/f; other
    (free) labor is ag_d52a/b/c + ag_d54a/b/c.  Plot key ag_d00 (the same
    R{n} key as plot_inputs / crop_production), i = format_id(case_id).

This is the SAME Module D block the World Bank cleaning code
(MWI_IHPS1.do:703-757) reads then collapses to per-plot total_*_labor_days
/ hired_labor_value.  We keep the PRE-collapse reported person-days per
source; the across-source sum + median-wage valuation are transformations.
See lsms_library/countries/Malawi/_/malawi.py:_plot_labor_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import _plot_labor_block, assemble_plot_labor


WAVE = '2010-11'
BASE = '../Data/Full_Sample/Agriculture/'

d = get_dataframe(BASE + 'ag_mod_d.dta', convert_categoricals=False)
d['hhid'] = d['case_id'].apply(format_id)
d['plotkey'] = d['ag_d00'].apply(format_id)

piece = _plot_labor_block(d, hhid='hhid', plotkey='plotkey', t=WAVE,
                          hired_suffix='', include_other=True)

df = assemble_plot_labor(WAVE, [piece])

assert df.index.is_unique, f"Non-unique (t,i,plot,source) in plot_labor {WAVE}"
assert len(df) > 0, f"plot_labor {WAVE} produced no rows"

to_parquet(df, 'plot_labor.parquet')
