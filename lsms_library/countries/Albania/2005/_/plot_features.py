"""Build plot_features for Albania 2005 (GH #167).

Source: Data/agric/part1_roster_a.dta -- the per-plot agriculture roster
(5456 rows, ~2.96 plots/HH).  Confirmed a plot roster via variable labels:
``p1a_q00`` = plot code, ``p1a_q7a`` = sq_meters, ``p1a_q9a`` = acquire,
``p1a_q11a`` = legal (title document), ``p1a_q12a`` = cropping_use,
``p1a_q13`` = irrigated.  ``i`` = format_id(p0_q00)-format_id(p0_q01),
matching Albania/2005/_/sample.py & mapping.py:i().
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from albania import plot_features_for_wave


src = get_dataframe('../Data/agric/part1_roster_a.dta', convert_categoricals=True)

colmap = dict(
    psu           = 'p0_q00',
    hh            = 'p0_q01',
    i_style       = 'psu-hh',
    plot_code     = 'p1a_q00',
    area_sqm      = 'p1a_q7a',
    tenure        = 'p1a_q9a',    # acquire: privatised / inherited / purchased / ...
    tenure_system = 'p1a_q11a',   # legal: deed / usufruct / sales receipt / none / ...
    soil_type     = 'p1a_q12a',   # cropping_use: annual crop / tree crop / pasture / ...
    irrigated     = 'p1a_q13',
)

df = plot_features_for_wave('2005', src, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2005"
assert len(df) > 0, "plot_features 2005 produced no rows"

to_parquet(df, 'plot_features.parquet')
