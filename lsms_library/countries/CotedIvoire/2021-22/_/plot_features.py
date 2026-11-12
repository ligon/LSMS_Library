"""Build plot_features for CotedIvoire EHCVM 2021-22 (GH #1697).

One source file, s16a_me_civ2021.dta, with the column names and code lists of 2018-19,
checked label by label.  The rules are 2018-19's (2018-19/_/plot_features.py).  5,396 rows
are households with no field (s16aq02 = 0) and 2 have no parcel number and no answers; the
other 15,220 are parcels.  s16aq47 is filled only where s16aq45 = Oui, unlike Togo's.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import format_id, get_categorical_mapping, get_dataframe, to_parquet
from mapping import household_ids, plot_ids


def _codes(tablename):
    """{int code -> Preferred Label}; blank or '---' map to NA."""
    raw = get_categorical_mapping(tablename=tablename, idxvars='Code',
                                  **{'Preferred Label': 'Preferred Label'})
    return {int(k): (pd.NA if pd.isna(v) or str(v).strip() in ('---', '') else str(v).strip())
            for k, v in raw.items()}


def _map(series, table):
    out = series.astype('Int64').map(table)
    return out.astype('string').where(out.notna(), pd.NA)


src = get_dataframe('../Data/s16a_me_civ2021.dta', convert_categoricals=False)
src = src[(src['s16aq02'] > 0) & src['s16aq03'].notna()].copy()

# Hectares: the GPS measure where taken, else the farmer's estimate from its declared
# unit (1 hectare, 2 square metres).  Implausible areas go NA, the row stays (GH #327).
area_gps = pd.to_numeric(src['s16aq47'], errors='coerce').astype('Float64')
gps = src['s16aq45'].astype('Int64')
estimate = pd.to_numeric(src['s16aq09a'], errors='coerce').astype('Float64')
estimate = estimate.where(src['s16aq09b'].astype('Int64') != 2, estimate / 10000)
area = estimate.where(~((gps == 1) & area_gps.notna()), area_gps)
plausible = lambda value: value.where(((value > 0) & (value <= 1000)) | value.isna(), pd.NA)
area = plausible(area)

water = _map(src['s16aq17'], _codes('harmonize_water'))
irrigated = (water == 'Irrigated').astype('boolean').where(water.notna(), pd.NA)
document = src['s16aq13'].astype('Int64')
certificate = document.isin([1, 2, 3, 4, 5]).astype('boolean').where(
    document.isin([1, 2, 3, 4, 5, 7]), pd.NA)

df = pd.DataFrame({
    't': '2021-22',
    'i': household_ids(src).astype('string').values,
    'plot_id': plot_ids(src, 's16aq02', 's16aq03').values,
    'Area': area.values,
    'AreaUnit': pd.Series('hectares', index=src.index, dtype='string').where(area.notna(), pd.NA).values,
    'AreaSelfReported': plausible(estimate).values,
    'Tenure': _map(src['s16aq10'], _codes('harmonize_tenure')).values,
    'TenureSystem': _map(src['s16aq13'], _codes('harmonize_tenure_system')).values,
    'PlotCertificate': certificate.values,
    'SoilType': _map(src['s16aq18'], _codes('harmonize_soil')).values,
    'SoilFertility': _map(src['s16aq20'], {1: 'good', 2: 'medium', 3: 'poor'}).values,
    'Irrigated': irrigated.values,
}).set_index(['t', 'i', 'plot_id'])

assert df.index.is_unique, 'Non-unique (t, i, plot_id) index in plot_features 2021-22'
assert len(df) == len(src), 'plot_features 2021-22 lost a parcel'
to_parquet(df, 'plot_features.parquet')
