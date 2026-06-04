"""Build plot_features for Malawi IHPS 2013-14 (GH #167).

Module C (AG_MOD_C_13) carries plot area; Module D (AG_MOD_D_13) carries
soil type, water source, and the acquire/tenure question ag_d03 (LABELED
in this wave).  NB: ag_d02 here is the respondent ID, NOT tenure -- we
use ag_d03.  Merge on (y2_hhid, ag_c00/ag_d00).

See lsms_library/countries/Malawi/_/malawi.py:plot_features_for_wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import plot_features_for_wave


WAVE = '2013-14'

df_c = get_dataframe('../Data/AG_MOD_C_13.dta', convert_categoricals=False)
df_d = get_dataframe('../Data/AG_MOD_D_13.dta', convert_categoricals=False)

df_c['hhid'] = df_c['y2_hhid'].apply(format_id)
df_c['plotkey'] = df_c['ag_c00'].apply(format_id)
df_d['hhid'] = df_d['y2_hhid'].apply(format_id)
df_d['plotkey'] = df_d['ag_d00'].apply(format_id)

colmap = dict(
    area_est     = 'ag_c04a',
    area_unit    = 'ag_c04b',
    area_gps     = 'ag_c04c',
    soil_type    = 'ag_d21',
    water_source = 'ag_d28a',
    acquire      = 'ag_d03',
)

df = plot_features_for_wave(WAVE, df_c, df_d, colmap)

assert df.index.is_unique, f"Non-unique (t,i,plot_id) in plot_features {WAVE}"
assert len(df) > 0, f"plot_features {WAVE} produced no rows"

to_parquet(df, 'plot_features.parquet')
