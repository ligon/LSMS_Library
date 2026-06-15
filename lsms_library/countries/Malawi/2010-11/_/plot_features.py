"""Build plot_features for Malawi IHS3 2010-11 (GH #167).

Module C (ag_mod_c) carries plot area; Module D (ag_mod_d) carries soil
type, water source, and the acquire/tenure question ag_d03 (LABELED in
this wave).  We merge the two on (case_id, ag_c00/ag_d00) -> the plotkey.

See lsms_library/countries/Malawi/_/malawi.py:plot_features_for_wave for
the harmonization logic shared across the four buildable IHS/IHPS waves.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import plot_features_for_wave


WAVE = '2010-11'

# convert_categoricals=False keeps the integer codes that the
# categorical_mapping.org harmonize_* tables key on.
df_c = get_dataframe('../Data/Full_Sample/Agriculture/ag_mod_c.dta',
                     convert_categoricals=False)
df_d = get_dataframe('../Data/Full_Sample/Agriculture/ag_mod_d.dta',
                     convert_categoricals=False)

df_c['hhid'] = df_c['case_id'].apply(format_id)
df_c['plotkey'] = df_c['ag_c00'].apply(format_id)
df_d['hhid'] = df_d['case_id'].apply(format_id)
df_d['plotkey'] = df_d['ag_d00'].apply(format_id)

colmap = dict(
    area_est     = 'ag_c04a',
    area_unit    = 'ag_c04b',
    area_gps     = 'ag_c04c',
    soil_type    = 'ag_d21',
    water_source = 'ag_d28a',
    acquire      = 'ag_d03',
    fallow       = 'ag_d14',
    erosion      = 'ag_d25a',
)

df = plot_features_for_wave(WAVE, df_c, df_d, colmap)

assert df.index.is_unique, f"Non-unique (t,i,plot_id) in plot_features {WAVE}"
assert len(df) > 0, f"plot_features {WAVE} produced no rows"

to_parquet(df, 'plot_features.parquet')
