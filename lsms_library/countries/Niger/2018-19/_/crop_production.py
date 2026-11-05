"""Build crop_production for Niger EHCVM 2018-19 (GAP 1, item-level).

Single source file: s16c_me_ner2018.dta (agriculture crop/harvest module).
One row per reported (field, parcel, crop) harvest record.  Harvest qty +
unit (s16cq12a / s16cq12b), sold qty + unit (s16cq16a / s16cq16b -> the
``Unit_sold`` column, GH #824), sale
value (s16cq17), and the intercrop flag (s16cq07) are ALL recorded at this
plot-crop grain in 2018-19, so no cross-file join is needed.

Index = (t, i, plot_id, crop, u, condition, production_obs); plot =
"{s16cq02}_{s16cq03}" aligns with plot_features' plot_id.  ``condition`` is the
harvest's own "Etat du produit" (s16cq12c, GH #1436), folded from five answers
to three tokens; the column ``condition_native`` keeps the answer as recorded.
``Condition_sold`` is the state of what was sold (s16cq16c, the same code list,
through the same fold, GH #1462), NA on a line with no sale, and
``condition_sold_native`` keeps that answer as recorded.
The index is unique without a line coordinate, so ``production_obs`` is the
``unresolved_observation`` sentinel (GH #1360).  See
niger.py:_finish_crop_production for the shared schema tail and the GAP-1
design note.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _crop_maps, _crop_labels, _unit_labels,
                   _crop_conditions, _crop_condition_labels, _crop_sold_conditions,
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
    'plot_id':          plot.values,
    'crop':          _crop_labels(src_codes['s16cq04'], src['s16cq04'], crop_map).values,
    'u':             _unit_labels(src['s16cq12b'], unit_map).values,
    # By code: the 2021-22 code list is different, see categorical_mapping.org.
    'condition':     _crop_conditions(src_codes['s16cq12c'],
                                      'harmonize_crop_condition_ehcvm1').values,
    # What the fold above cannot show: the answer as recorded (ruling
    # @ligon 2026-11-04).  By label, from the converted read.
    'condition_native': _crop_condition_labels(src['s16cq12c']).values,
    'Quantity':      src['s16cq12a'].values,
    'Quantity_sold': src['s16cq16a'].values,
    'Value_sold':    src['s16cq17'].values,
    # The SOLD unit (s16cq16b), a second unit on the same line (GH #824);
    # identical 9-label set to the harvest unit s16cq12b, same `u` table.
    'Unit_sold':     _unit_labels(src['s16cq16b'], unit_map).values,
    # The SOLD state (s16cq16c): the harvest's code list and fold (GH #1462).
    'Condition_sold': _crop_sold_conditions(src_codes['s16cq16c'],
                                            'harmonize_crop_condition_ehcvm1').values,
    # What that fold cannot show, the sold-side twin of condition_native (ruling
    # @ligon 2026-11-05).
    'condition_sold_native': _crop_condition_labels(src['s16cq16c']).values,
    'intercropped':  intercropped.values,
})

df = _finish_crop_production(df, '2018-19')

assert df.index.is_unique, 'crop_production 2018-19 repeats a native harvest line'
assert len(df) > 0, 'crop_production 2018-19 produced no rows'
to_parquet(df, 'crop_production.parquet')
