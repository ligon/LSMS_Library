"""Build plot_features for Albania 2012 (GH #167).

Source: Data/Modul_17_Identification_of_Agriculture_Hh_Q3-9.sav -- the
per-plot agriculture roster (4661 rows, ~1.54 plots/HH; one row per plot).
Confirmed a plot roster via variable labels: ``M17_Q07`` = Area of plot
(square meters), ``M17_Q08`` = Kind of land, ``M17_Q04`` = Origin of land,
``M17_Q06`` = Legal title and rights.  This file carries NO plot-code
column, so plot_id is synthesised as a stable 1..n sequence per household.
``i`` = format_id(psu*100+hh) (= the globally-unique hhid), matching
Albania/2012/_/sample.py & mapping.py:i().  No irrigation question in this
module, so Irrigated is absent for 2012.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from albania import plot_features_for_wave


src = get_dataframe('../Data/Modul_17_Identification_of_Agriculture_Hh_Q3-9.sav',
                    convert_categoricals=True)

colmap = dict(
    psu           = 'psu',
    hh            = 'hh',
    i_style       = 'hhid',     # i = psu*100 + hh
    plot_code     = None,       # no plot-code column -> synth 1..n per HH
    area_sqm      = 'M17_Q07',
    tenure        = 'M17_Q04',  # origin of land: Privatized (L.7501) / Inhereted / ...
    tenure_system = 'M17_Q06',  # legal title and rights: Deed / Sales receipt / None / ...
    soil_type     = 'M17_Q08',  # kind of land: Annual crop land / Pasture / Tree crop / ...
)

df = plot_features_for_wave('2012', src, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2012"
assert len(df) > 0, "plot_features 2012 produced no rows"

to_parquet(df, 'plot_features.parquet')
