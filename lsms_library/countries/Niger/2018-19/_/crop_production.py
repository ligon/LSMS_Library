"""Build crop_production for Niger EHCVM 2018-19 (GAP 1, item-level).

Single source file: s16c_me_ner2018.dta (agriculture crop/harvest module).
One row per reported (field, parcel, crop) harvest record.  Harvest qty +
unit (s16cq12a / s16cq12b), sold qty + unit (s16cq16a / s16cq16b), sale
value (s16cq17), and the intercrop flag (s16cq07) are ALL recorded at this
plot-crop grain in 2018-19, so no cross-file join is needed.

Index = (t, i, plot, crop, u); plot = "{s16cq02}_{s16cq03}" aligns with
plot_features' plot_id.  See niger.py:_finish_crop_production for the shared
schema tail and the GAP-1 design note.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _crop_maps, _crop_labels, _unit_labels,
                   _finish_crop_production)


# convert_categoricals=True so crop / unit value labels arrive as the
# strings that harmonize_food / the u table key on.
src = get_dataframe('../Data/s16c_me_ner2018.dta', convert_categoricals=True)
src_codes = get_dataframe('../Data/s16c_me_ner2018.dta', convert_categoricals=False)

crop_map, unit_map = _crop_maps()

hh = src.apply(lambda r: niger_i(pd.Series([r['grappe'], r['menage']],
                                           index=['grappe', 'menage'])), axis=1)
field = src_codes['s16cq02'].apply(format_id)
parcel = src_codes['s16cq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

# intercrop: s16cq07 == 'Association de cultures' (code 2) -> True, 'Pure' -> False
intercropped = src['s16cq07'].map({'Association de cultures': True, 'Pure': False})

df = pd.DataFrame({
    'i':             hh.values,
    'plot':          plot.values,
    'crop':          _crop_labels(src_codes['s16cq04'], src['s16cq04'], crop_map).values,
    'u':             _unit_labels(src['s16cq12b'], unit_map).values,
    'Quantity':      src['s16cq12a'].values,
    'Quantity_sold': src['s16cq16a'].values,
    'Value_sold':    src['s16cq17'].values,
    'intercropped':  intercropped.values,
})

df = _finish_crop_production(df, '2018-19')

assert len(df) > 0, 'crop_production 2018-19 produced no rows'
to_parquet(df, 'crop_production.parquet')
