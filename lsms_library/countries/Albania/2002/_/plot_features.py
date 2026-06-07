"""Build plot_features for Albania 2002 (GH #167).

Source: Data/agr_a1_cl.dta -- the per-plot agriculture roster (6082 rows,
~3.86 plots/HH).  Confirmed a plot roster via variable labels:
``mca1_q0a`` = Plot Code, ``mca1_q03`` = Plot Area (sq meters),
``mca1_q04`` = Kind of land, ``mca1_q06`` = Quality of land,
``mca1_q09`` = Irrigated plot, ``mca1_q11`` = Method of acquisition,
``mca1_q12`` = Legal documentation.  ``i`` = format_id(psu)-format_id(hh),
matching Albania/2002/_/sample.py & mapping.py:i() (NOT the roster's latent
``i:hh`` bug).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from albania import plot_features_for_wave


src = get_dataframe('../Data/agr_a1_cl.dta', convert_categoricals=True)

colmap = dict(
    psu           = 'psu',
    hh            = 'hh',
    i_style       = 'psu-hh',
    plot_code     = 'mca1_q0a',
    area_sqm      = 'mca1_q03',
    tenure        = 'mca1_q11',   # method of acquisition: Privatised / Inherited / ...
    tenure_system = 'mca1_q12',   # legal documentation: Deed / Usufruct / None / ...
    soil_type     = 'mca1_q04',   # kind of land: Annual crop land / Tree crop / Pasture
    irrigated     = 'mca1_q09',
)

df = plot_features_for_wave('2002', src, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2002"
assert len(df) > 0, "plot_features 2002 produced no rows"

to_parquet(df, 'plot_features.parquet')
