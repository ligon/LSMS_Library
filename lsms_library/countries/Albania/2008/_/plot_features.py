"""Build plot_features for Albania 2008 (GH #167).

Source: Data/Modul_17_id_of_agric_household.sav -- the per-plot agriculture
roster (3977 rows, ~2.39 plots/HH).  Confirmed a plot roster via variable
labels: ``m17_q00`` = plot code, ``m17_q04`` = area of plot (sq meters),
``m17_q05`` = type of land.  The 2008 module records no acquisition / legal
/ irrigation question, so Tenure / TenureSystem / Irrigated are absent for
this wave (only Area / AreaUnit / SoilType).  ``i`` = format_id(psu)-
format_id(hh), matching Albania/2008/_/sample.py & mapping.py:i().
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from albania import plot_features_for_wave


src = get_dataframe('../Data/Modul_17_id_of_agric_household.sav',
                    convert_categoricals=True)

colmap = dict(
    psu       = 'psu',
    hh        = 'hh',
    i_style   = 'psu-hh',
    plot_code = 'm17_q00',
    area_sqm  = 'm17_q04',
    soil_type = 'm17_q05',   # type of land: annual crop land / pasture / tree crop / ...
)

df = plot_features_for_wave('2008', src, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2008"
assert len(df) > 0, "plot_features 2008 produced no rows"

to_parquet(df, 'plot_features.parquet')
