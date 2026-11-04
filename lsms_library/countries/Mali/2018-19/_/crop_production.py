"""Build crop_production for Mali EHCVM 2018-19 (GH #824; Niger's pattern).

Single source file: s16c_me_mli2018.dta, one row per (field, parcel, crop) with
the harvest and the sale on the same line, so no cross-file join is needed.

    plot_id        "{s16cq02}_{s16cq03}", the spelling of plot_features' plot_id
                   (all 7,992 rows find their plot in s16a)
    crop           s16cq04 -> harmonize_food Preferred Label
    Quantity, u    s16cq12a, s16cq12b; NA on 607 rows with no harvest recorded,
                   236 of them 'Recolte nulle'
    Quantity_sold  s16cq16a
    Unit_sold      s16cq16b, in its own value-label set: code 7 is 'Sacs de
                   100 kg' on s16cq12b and 'Sac-100 Kg' here, so raw labels agree
                   on 705 of 2,101 priced rows and codes on 1,856.  Both go
                   through the u table, and the 245 real disagreements stay.
    Value_sold     s16cq17
    intercropped   s16cq07

Where Mali's file differs from Niger's: the index is (t, i, plot_id, crop), as in
Mali's EAC-I waves, with `u` a column that is NA where no unit is recorded; Niger
indexes `u` and fills `Unknown`.  Everything else is the same rule: served as
recorded, nothing masked, and NA rather than 0 on a line whose s16cq15 is 'Non'.
See mali.py, "crop_production: the EHCVM waves".
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import (ehcvm_crop_labels, ehcvm_household_ids, ehcvm_intercropped,
                  ehcvm_plot_ids, ehcvm_unit_labels,
                  finish_ehcvm_crop_production)

src = get_dataframe('../Data/s16c_me_mli2018.dta', convert_categoricals=True)

df = pd.DataFrame({
    'i':             ehcvm_household_ids(src).values,
    'plot_id':       ehcvm_plot_ids(src).values,
    'crop':          ehcvm_crop_labels(src['s16cq04']).values,
    'u':             ehcvm_unit_labels(src['s16cq12b']).values,
    'Quantity':      src['s16cq12a'].values,
    'Quantity_sold': src['s16cq16a'].values,
    'Unit_sold':     ehcvm_unit_labels(src['s16cq16b']).values,
    'Value_sold':    src['s16cq17'].values,
    'intercropped':  ehcvm_intercropped(src['s16cq07']).values,
})

df = finish_ehcvm_crop_production(df, '2018-19')

assert len(df) == len(src), 'crop_production 2018-19 lost a source row'
to_parquet(df, 'crop_production.parquet')
