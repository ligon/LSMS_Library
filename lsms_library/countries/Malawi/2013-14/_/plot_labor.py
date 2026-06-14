"""Build plot_labor for Malawi IHPS 2013-14 (GAP 3, plot grain).

Item-level (t, i, plot, source) plot-labor feature.  Variable layout tracks
2010-11 (the IHS3/IHPS-2013 generation) except the household id (y2_hhid).
Source (flat Data/):
  * AG_MOD_D_13 -- plot key ag_d00.  Family labor = day*#people in the
    ag_d42/43/44 {b/c,f/g,j/k,n/o} person-slot blocks; hired = ag_d47a/c/e +
    ag_d48a/c/e days with wages ag_d47b/d/f + ag_d48b/d/f; other =
    ag_d52a/b/c + ag_d54a/b/c.

i = format_id(y2_hhid), aligning with plot_inputs / crop_production.  This
is the SAME Module D block the WB code (MWI_IHPS2.do:757-) reads then
collapses to per-plot totals.  See
lsms_library/countries/Malawi/_/malawi.py:_plot_labor_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import _plot_labor_block, assemble_plot_labor


WAVE = '2013-14'

d = get_dataframe('../Data/AG_MOD_D_13.dta', convert_categoricals=False)
d['hhid'] = d['y2_hhid'].apply(format_id)
d['plotkey'] = d['ag_d00'].apply(format_id)

piece = _plot_labor_block(d, hhid='hhid', plotkey='plotkey', t=WAVE,
                          hired_suffix='', include_other=True)

df = assemble_plot_labor(WAVE, [piece])

assert df.index.is_unique, f"Non-unique (t,i,plot,source) in plot_labor {WAVE}"
assert len(df) > 0, f"plot_labor {WAVE} produced no rows"

to_parquet(df, 'plot_labor.parquet')
