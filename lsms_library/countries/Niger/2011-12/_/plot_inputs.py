"""Build plot_inputs for Niger ECVMA 2011-12 (GAP 2, item-level).

Single source file: ecvmaas2c_p1.dta — the household agricultural-input
roster, one row per (crop, input-type) the household was ASKED about.
Columns:
  as02cq02  input type (organic / inorganic fert / phyto / 7 seed slots)
  as02cq03  USED this input? (1=Oui / 2=Non) — the application gate
  as02cq04  crop (same crop codes as the harvest module)
  as02cq05a / as02cq05b   quantity used + native unit
  as02cq07  purchased? (1=Oui / 2=Non)
  as02cq08a quantity purchased (native purchased-unit)

Only the rows the household actually applied are reported inputs, so we
keep as02cq03==1 (the .do code does the same: `replace seed_kg=0 if
as02cq03==2`, `keep if ... & as02cq03==1`).  i = str(int(hid)); hid
already equals grappe*100+menage (the canonical 2011-12 household id),
matching crop_production / sample.  No plot column in this roster (Niger
inputs are reported at the household level), so the grain is
(t, i, input, crop, u); see niger.py:_finish_plot_inputs.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import (i as niger_i, _input_maps, _input_labels,
                   _unit_labels, _finish_plot_inputs)


src = get_dataframe(
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta',
    convert_categoricals=True)
srcn = get_dataframe(
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta',
    convert_categoricals=False)

input_map, unit_map, _ = _input_maps()
# ECVMA reuses the harvest crop labels for its input-roster crop column.
from niger import _crop_maps, _crop_labels
crop_map, _ = _crop_maps()

# Keep only inputs the household actually applied (as02cq03 == 1 = Oui).
applied = srcn['as02cq03'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = srcn['hid'].apply(lambda x: niger_i(x) if pd.notna(x) else pd.NA)

# purchased: as02cq07 1 = Oui -> True, 2 = Non -> False (9 Manquant -> NA)
purchased = srcn['as02cq07'].map({1: True, 2: False})

df = pd.DataFrame({
    'i':                  hh.values,
    'input':              _input_labels(src['as02cq02'], input_map).values,
    'crop':               _crop_labels(srcn['as02cq04'], src['as02cq04'], crop_map).values,
    'u':                  _unit_labels(src['as02cq05b'], unit_map).values,
    'Quantity':           pd.to_numeric(srcn['as02cq05a'], errors='coerce').values,
    'Purchased':          purchased.values,
    'Quantity_purchased': pd.to_numeric(srcn['as02cq08a'], errors='coerce').values,
})

df = _finish_plot_inputs(df, '2011-12')

assert len(df) > 0, 'plot_inputs 2011-12 produced no rows'
to_parquet(df, 'plot_inputs.parquet')
