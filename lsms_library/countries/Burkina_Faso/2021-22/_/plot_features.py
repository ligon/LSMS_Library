"""Build plot_features for Burkina Faso EHCVM 2021-22 (GH #167; EHCVM cluster).

Single source file: s16a_me_bfa2021.dta (agriculture-parcel module).
plot_id = "{field_no}_{parcel_no}" (s16aq02 _ s16aq03); unique within
each (grappe, menage).  See
lsms_library/countries/Burkina_Faso/_/burkina_faso.py:plot_features_for_wave
for the harmonization shared across both EHCVM waves (copied from the
Mali EHCVM reference, PR #284).

Household id (GH #460): UNLIKE 2018-19, the 2021-22 wave's mapping.py
does NOT override i(), so its sample() / household_roster fall back to
the country-level i() (grappe + 3-digit menage, no '0' separator).
Verified empirically: sample 2021-22 carries old-form ids (e.g. '9120'
for grappe=9, menage=120), NOT the canonical '90120'.  This script must
build plot_features' i with the SAME formatter, so it passes the
country i() (wrapped to ehcvm_i's (grappe, menage) signature).  Using
ehcvm_i here would instead strand every 3-digit-menage household off
sample().  (plot_features 2021-22 already reconciled 100% before #460;
this keeps it so while #460 fixes the 2018-19 mismatch.)
"""
import sys
import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from burkina_faso import plot_features_for_wave, i as country_i


def _old_i(grappe, menage):
    """Country-level i() exposed with ehcvm_i's (grappe, menage) signature."""
    return country_i(pd.Series([grappe, menage]))


# convert_categoricals=False keeps the integer s16a codes that the
# categorical_mapping.org harmonize_* tables key on.
src = get_dataframe('../Data/s16a_me_bfa2021.dta', convert_categoricals=False)

colmap = dict(
    grappe        = 'grappe',
    menage        = 'menage',
    field_no      = 's16aq02',
    parcel_no     = 's16aq03',
    area_gps      = 's16aq47',
    gps_measured  = 's16aq45',
    area_est      = 's16aq09a',
    area_est_unit = 's16aq09b',
    tenure        = 's16aq10',
    tenure_system = 's16aq13',
    soil_type     = 's16aq18',
    water_source  = 's16aq17',
)

df = plot_features_for_wave('2021-22', src, colmap, id_fn=_old_i)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2021-22"
assert len(df) > 0, "plot_features 2021-22 produced no rows"

to_parquet(df, 'plot_features.parquet')
