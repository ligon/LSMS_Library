"""Build plot_labor for Niger EHCVM 2018-19 (GAP 3, item-level).

Single source file: s16a_me_ner2018.dta (agriculture-parcel module — the
same file plot_features reads, so plot ids align by construction).  EHCVM
records plot labor at the parcel level, family vs non-family (hired) only
(NO free/exchange "other" labor):

  Family labor (per family member, member-grid suffix _1.._N, three phases):
    s16aq33b_*  days each member worked, prep/sowing phase
    s16aq35b_*  days each member worked, maintenance phase
    s16aq37b_*  days each member worked, harvest phase
  -> family PersonDays = Σ over members & phases of those member-day cells.

  Non-family / hired labor (per worker gender category, grid suffix _1.._4,
  three phases; a = #workers, b = days each, c = wage paid):
    s16aq39{a,b,c}_*  prep/sowing phase
    s16aq41{a,b,c}_*  maintenance phase
    s16aq43{a,b,c}_*  harvest phase
  -> hired PersonDays = Σ over categories & phases of (a workers × b days);
     hired Wage       = Σ over categories & phases of c (cash paid).

One row per (plot, source); plot = "{s16aq02}_{s16aq03}" aligns with
crop_production / plot_features plot_id.  i = EHCVM composite via niger.i.
The build is shared with 2021-22 (niger.plot_labor_ehcvm — identical s16a
labor-grid scheme).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from niger import plot_labor_ehcvm, _finish_plot_labor


src = get_dataframe('../Data/s16a_me_ner2018.dta', convert_categoricals=False)
df = plot_labor_ehcvm(src, '2018-19')
df = _finish_plot_labor(df, '2018-19')

assert len(df) > 0, 'plot_labor 2018-19 produced no rows'
to_parquet(df, 'plot_labor.parquet')
