"""Build plot_features for Malawi IHS5 2019-20 (GH #167).

IHS5 ships a Cross_Sectional half (keyed on bare case_id) and a Panel
half (keyed on y4_hhid).  Unlike IHS4, sample().i for the 2019-20 wave
is NOT cs-prefixed (the cross-sectional case_id is used verbatim), so we
emit bare ids on both halves and let id_walk chain the panel.

Tenure: ag_d03 (acquire) is ABSENT from IHS5 ag_mod_d; ag_d02 there is
"ID of Respondent", NOT tenure.  So Tenure / TenureSystem are NaN.

Plot key is (gardenid, plotid) -> 'gardenid_plotid'.

See lsms_library/countries/Malawi/_/malawi.py:plot_features_for_wave.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import plot_features_for_wave


WAVE = '2019-20'


def _plotkey(df):
    return (df['gardenid'].apply(format_id) + '_'
            + df['plotid'].apply(format_id))


colmap = dict(
    area_est     = 'ag_c04a',
    area_unit    = 'ag_c04b',
    area_gps     = 'ag_c04c',
    soil_type    = 'ag_d21',
    water_source = 'ag_d28a',
    fallow       = 'ag_d14',
    erosion      = 'ag_d25a',
    # acquire omitted: ag_d03 absent in IHS5 -> Tenure / PlotOwned NaN
    # (the WB derives plot_owned from the Module B2 parcel roster here,
    # a different grain; left NaN rather than cross-grain joined).
)

pieces = []

# --- Cross-sectional half (bare case_id) ---
c_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_c.dta',
                     convert_categoricals=False)
d_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_d.dta',
                     convert_categoricals=False)

c_xs['hhid'] = c_xs['case_id'].apply(format_id)
c_xs['plotkey'] = _plotkey(c_xs)
d_xs['hhid'] = d_xs['case_id'].apply(format_id)
d_xs['plotkey'] = _plotkey(d_xs)
pieces.append(plot_features_for_wave(WAVE, c_xs, d_xs, colmap))

# --- Panel half (y4_hhid) ---
c_pn = get_dataframe('../Data/Panel/ag_mod_c_19.dta',
                     convert_categoricals=False)
d_pn = get_dataframe('../Data/Panel/ag_mod_d_19.dta',
                     convert_categoricals=False)

c_pn['hhid'] = c_pn['y4_hhid'].apply(format_id)
c_pn['plotkey'] = _plotkey(c_pn)
d_pn['hhid'] = d_pn['y4_hhid'].apply(format_id)
d_pn['plotkey'] = _plotkey(d_pn)
pieces.append(plot_features_for_wave(WAVE, c_pn, d_pn, colmap))

df = pd.concat(pieces)

assert df.index.is_unique, f"Non-unique (t,i,plot_id) in plot_features {WAVE}"
assert len(df) > 0, f"plot_features {WAVE} produced no rows"

to_parquet(df, 'plot_features.parquet')
