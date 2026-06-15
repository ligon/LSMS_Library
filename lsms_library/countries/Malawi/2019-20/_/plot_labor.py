"""Build plot_labor for Malawi IHS5 / IHPS 2019-20 (GAP 3, plot grain).

Item-level (t, i, plot, source) plot-labor feature.  IHS5 ships a
Cross_Sectional half (bare case_id -- sample().i for 2019-20 is the bare
case_id, NOT cs-prefixed) and a Panel half (y4_hhid), concatenated into the
single 2019-20 wave -- exactly like plot_inputs / crop_production.  Plot key
is gardenid_plotid in both halves.

Module D plot-labor layout is the IHS4/IHS5 generation:
  * family = ag_d4{2,3,4}b{n} * ag_d4{2,3,4}c{n} day*#people products;
  * hired  = ag_d47a1/a2/a3 + ag_d48a1/a2/a3 days, wages ag_d47b1/b2/b3 +
    ag_d48b1/b2/b3 ('1/2/3' hired suffix);
  * other  = ag_d52a/b/c + ag_d54a/b/c -- PRESENT in the Panel half, but the
    Cross_Sectional Module D drops those columns, so the CS half emits only
    family + hired rows (reported, not faked).

This is the SAME Module D block the WB code (MWI_IHPS4.do:739-) reads then
collapses to per-plot totals (the IHPS4 .do also folds a separate
post-harvest family-labor file, ag_i00b*ag_i00c, into total_family_labor_
days; that is an across-source SUM transform we do not bake in -- we carry
only the Module D plot-labor reported days).  See
lsms_library/countries/Malawi/_/malawi.py:_plot_labor_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import _plot_labor_block, assemble_plot_labor


WAVE = '2019-20'


def _plotkey(df):
    return (df['gardenid'].apply(format_id) + '_'
            + df['plotid'].apply(format_id))


pieces = []

# --- Cross_Sectional half (bare case_id; no "other" labor columns) ---
d_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_d.dta',
                     convert_categoricals=False)
d_xs['hhid'] = d_xs['case_id'].apply(format_id)
d_xs['plotkey'] = _plotkey(d_xs)
pieces.append(_plot_labor_block(d_xs, hhid='hhid', plotkey='plotkey', t=WAVE,
                                hired_suffix='1/2/3', include_other=False))

# --- Panel half (y4_hhid; has "other" labor) ---
d_pn = get_dataframe('../Data/Panel/ag_mod_d_19.dta', convert_categoricals=False)
d_pn['hhid'] = d_pn['y4_hhid'].apply(format_id)
d_pn['plotkey'] = _plotkey(d_pn)
pieces.append(_plot_labor_block(d_pn, hhid='hhid', plotkey='plotkey', t=WAVE,
                                hired_suffix='1/2/3', include_other=True))

df = assemble_plot_labor(WAVE, pieces)

assert df.index.is_unique, f"Non-unique (t,i,plot,source) in plot_labor {WAVE}"
assert len(df) > 0, f"plot_labor {WAVE} produced no rows"

to_parquet(df, 'plot_labor.parquet')
