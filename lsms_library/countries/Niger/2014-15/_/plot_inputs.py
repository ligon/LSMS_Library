"""Build plot_inputs for Niger ECVMA 2014-15 (GAP 2, item-level).

Single source file: ECVMA2_AS02CP1.dta — the household agricultural-input
roster, one row per (crop, input-type) the household was asked about.  Same
layout as 2011-12 but UPPERCASE columns:
  AS02CQ02  input type (organic / inorganic fert / phyto / 7 seed slots)
  AS02CQ03  USED this input? (1=Oui / 2=Non) — the application gate
  AS02CQ04  crop (same crop codes as the harvest module)
  AS02CQ05A / AS02CQ05B   quantity used + native unit
  AS02CQ07  purchased? (1=Oui / 2=Non)
  AS02CQ08A quantity purchased (native purchased-unit, AS02CQ08B)

Only the rows the household actually applied are reported inputs, so we
keep AS02CQ03==1.  i is built from (GRAPPE, MENAGE) via niger.i (matching
this wave's sample / roster / crop_production idxvars, which omit
EXTENSION).  No plot column; grain is (t, i, input, crop, u).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import (i as niger_i, _input_maps, _input_labels,
                   _unit_labels, _finish_plot_inputs, _crop_maps, _crop_labels)


base = '../Data/NER_2014_ECVMA-II_v02_M_STATA8/'
src = get_dataframe(base + 'ECVMA2_AS02CP1.dta', convert_categoricals=True)
srcn = get_dataframe(base + 'ECVMA2_AS02CP1.dta', convert_categoricals=False)

input_map, unit_map, _ = _input_maps()
crop_map, _ = _crop_maps()

# Keep only inputs the household actually applied (AS02CQ03 == 1 = Oui).
applied = srcn['AS02CQ03'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = src.apply(lambda r: niger_i(pd.Series([r['GRAPPE'], r['MENAGE']],
                                           index=['GRAPPE', 'MENAGE'])), axis=1)

# purchased: AS02CQ07 1 = Oui -> True, 2 = Non -> False (9 Manquant -> NA)
purchased = srcn['AS02CQ07'].map({1: True, 2: False})

df = pd.DataFrame({
    'i':                  hh.values,
    'input':              _input_labels(src['AS02CQ02'], input_map).values,
    'crop':               _crop_labels(srcn['AS02CQ04'], src['AS02CQ04'], crop_map).values,
    'u':                  _unit_labels(src['AS02CQ05B'], unit_map).values,
    'Quantity':           pd.to_numeric(srcn['AS02CQ05A'], errors='coerce').values,
    'Purchased':          purchased.values,
    'Quantity_purchased': pd.to_numeric(srcn['AS02CQ08A'], errors='coerce').values,
})

df = _finish_plot_inputs(df, '2014-15')

assert len(df) > 0, 'plot_inputs 2014-15 produced no rows'
to_parquet(df, 'plot_inputs.parquet')
