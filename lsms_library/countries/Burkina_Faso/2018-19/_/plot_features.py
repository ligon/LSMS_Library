"""Build plot_features for Burkina Faso EHCVM 2018-19 (GH #167; EHCVM cluster).

Single source file: s16a_me_bfa2018.dta (agriculture-parcel module).
plot_id = "{field_no}_{parcel_no}" (s16aq02 _ s16aq03); unique within
each (grappe, menage).  See
lsms_library/countries/Burkina_Faso/_/burkina_faso.py:plot_features_for_wave
for the harmonization shared across both EHCVM waves (copied from the
Mali EHCVM reference, PR #284).

Household id (GH #460): the 2018-19 wave's mapping.py overrides i() to
the canonical EHCVM form (grappe + '0' + 2-digit menage), so sample() /
household_roster carry 7-char ids for 3-digit menage.  This script must
build plot_features' i with the SAME formatter, so it passes ehcvm_i.
The previous default (the country-level 3-digit i()) stranded ~30% of
2018-19 plot_features households off sample() (the #460 i-key bug).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from burkina_faso import plot_features_for_wave, ehcvm_i


# convert_categoricals=False keeps the integer s16a codes that the
# categorical_mapping.org harmonize_* tables key on.
src = get_dataframe('../Data/s16a_me_bfa2018.dta', convert_categoricals=False)

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

df = plot_features_for_wave('2018-19', src, colmap, id_fn=ehcvm_i)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2018-19"
assert len(df) > 0, "plot_features 2018-19 produced no rows"

to_parquet(df, 'plot_features.parquet')
