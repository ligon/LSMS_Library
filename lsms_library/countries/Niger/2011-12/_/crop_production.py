"""Build crop_production for Niger ECVMA 2011-12 (GAP 1, item-level).

Single source file: ecvmaas2e_p2.dta (rainy-season harvest module).  One
row per reported (field, parcel, crop) harvest line; harvest qty + unit
(as02eq07a / as02eq07b), sold qty + unit (as02eq12a / as02eq12b) and sale
value (as02eq13) are all at this plot-crop grain; the module's unlabelled
"don't know" codes (999999 on the quantities, 9999999 on the value) are
mapped to NA, exact value only (GH #865).  ``hid`` already equals
grappe*100+menage (the canonical 2011-12 household id), so i() just
str()s it.  No intercrop / perennial / date variables in this module
(left NaN).

plot = "{as02eq01}_{as02eq03}".  The pre-EHCVM waves have no plot_features
recipe yet, so this plot id is internal to crop_production (it still
aligns by construction with any future ECVMA plot_features build).
Index = (t, i, plot, crop, u).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _crop_maps, _crop_labels, _unit_labels,
                   _finish_crop_production, _na_exact)

# "Don't know" codes of this module, unlabelled in the .dta (GH #865).  Exact
# values only.  Raw counts, ecvmaas2e_p2.dta (12,091 rows): as02eq07a ==
# 999999 on 11 rows (all with unit code 99 'manquant', four crops: mil 3,
# sorgho 3, niebe 3, riz paddy 2), as02eq12a == 999999 on 3, as02eq13 ==
# 9999999 on 2 (both hid 20607, which carries all three codes).
QTY_SENTINELS = (999999,)
VALUE_SENTINELS = (9999999,)


src = get_dataframe(
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta',
    convert_categoricals=True)
srcn = get_dataframe(
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta',
    convert_categoricals=False)

crop_map, unit_map = _crop_maps()

hh = srcn['hid'].apply(lambda x: niger_i(x) if pd.notna(x) else pd.NA)
field = srcn['as02eq01'].apply(format_id)
parcel = srcn['as02eq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

df = pd.DataFrame({
    'i':             hh.values,
    'plot_id':          plot.values,
    'crop':          _crop_labels(srcn['as02eq06'], src['as02eq06'], crop_map).values,
    'u':             _unit_labels(src['as02eq07b'], unit_map).values,
    'Quantity':      _na_exact(srcn['as02eq07a'], QTY_SENTINELS).values,
    'Quantity_sold': _na_exact(srcn['as02eq12a'], QTY_SENTINELS).values,
    'Value_sold':    _na_exact(srcn['as02eq13'], VALUE_SENTINELS).values,
})

df = _finish_crop_production(df, '2011-12')

assert len(df) > 0, 'crop_production 2011-12 produced no rows'
to_parquet(df, 'crop_production.parquet')
