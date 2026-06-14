"""Build plot_labor for Niger EHCVM 2021-22 (GAP 3, item-level).

Single source file: s16a_me_ner2021.dta (agriculture-parcel module — the
same file plot_features reads, so plot ids align by construction).  The
s16a labor-grid scheme is identical to 2018-19, so the build is shared via
niger.plot_labor_ehcvm:

  Family labor (member-grids s16aq33b_*/35b_*/37b_*, three phases)
    -> family PersonDays = Σ member-day cells across members & phases.
  Non-family / hired (grids s16aq39/41/43 {a=#workers, b=days, c=wage})
    -> hired PersonDays = Σ (a × b); hired Wage = Σ c.

EHCVM records family vs non-family (hired) only (no free/exchange "other").
plot = "{s16aq02}_{s16aq03}" aligns with crop_production / plot_features;
i = EHCVM composite via niger.i.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from niger import plot_labor_ehcvm, _finish_plot_labor


src = get_dataframe('../Data/s16a_me_ner2021.dta', convert_categoricals=False)
df = plot_labor_ehcvm(src, '2021-22')
df = _finish_plot_labor(df, '2021-22')

assert len(df) > 0, 'plot_labor 2021-22 produced no rows'
to_parquet(df, 'plot_labor.parquet')
