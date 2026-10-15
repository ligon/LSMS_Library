import json
import os
from pathlib import Path
import pandas as pd
import lsms_library as ll
from lsms_library.local_tools import get_dataframe

root = Path(os.environ['PYTHONPATH'])
assert Path(ll.__file__).is_relative_to(root), ll.__file__
out = Path(os.environ['LSMS_REVIEW_OUT'])
specs = [
    ('2011-12', 'sect_cover_hh_w1.dta', 'Pub_ETH_HouseholdGeovariables_Y1.dta', 'household_id', 'ea_id', 'LAT_DD_MOD', 'LON_DD_MOD', 'rural'),
    ('2013-14', 'sect_cover_hh_w2.dta', 'Pub_ETH_HouseholdGeovars_Y2.dta', 'household_id2', 'ea_id2', 'lat_dd_mod', 'lon_dd_mod', 'rural'),
    ('2015-16', 'sect_cover_hh_w3.dta', 'ETH_HouseholdGeovars_y3.dta', 'household_id2', 'ea_id2', 'lat_dd_mod', 'lon_dd_mod', 'rural'),
    ('2018-19', 'sect_cover_hh_w4.dta', 'ETH_HouseholdGeovariables_Y4.dta', 'household_id', 'ea_id', 'lat_mod', 'lon_mod', 'saq14'),
    ('2021-22', 'sect_cover_hh_w5.dta', 'eth_householdgeovariables_y5.dta', 'household_id', 'ea_id', 'lat_dd_mod', 'lon_dd_mod', 'saq14'),
]
result = {}
for wave, cf, gf, hh, ea, lat, lon, rural in specs:
    directory = root / 'lsms_library/countries/Ethiopia' / wave / 'Data'
    cover = get_dataframe(directory / cf, convert_categoricals=False)
    geo = get_dataframe(directory / gf, convert_categoricals=False)
    assert cover[hh].is_unique and geo[hh].is_unique, wave
    merged = cover[[hh, ea, 'saq01', 'saq02', rural]].merge(geo[[hh, lat, lon]], on=hh, how='left', validate='1:1', indicator=True)
    assert len(merged) == len(cover)
    groups = merged.groupby(ea, dropna=False, observed=True)
    record = {'cover_rows': len(cover), 'geo_rows': len(geo), 'cover_hh_unique': True, 'geo_hh_unique': True, 'left_merge_rows': len(merged), 'unmatched_cover_households': int(merged['_merge'].eq('left_only').sum()), 'clusters': groups.ngroups, 'composite_region_district_ea_clusters': len(cover[['saq01', 'saq02', ea]].drop_duplicates()), 'distinct_value_conflicts': {col: int((groups[col].nunique(dropna=True) > 1).sum()) for col in ['saq01', 'saq02', rural, lat, lon]}, 'mixed_missing_value_groups': {col: int(((groups[col].count() > 0) & (groups[col].count() < groups.size())).sum()) for col in [lat, lon]}}
    result[wave] = record
    print(wave, record, flush=True)
(out / 'ethiopia-raw-source.json').write_text(json.dumps(result, indent=2))
