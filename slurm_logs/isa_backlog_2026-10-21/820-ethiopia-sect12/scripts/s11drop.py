"""On DEVELOPMENT's ethiopia.py: rows the empty-row filter drops that carry a §11 sale value, per wave."""
import os, sys, json, inspect, textwrap, warnings
base = '/local/job40390255/worktrees/base-820'
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(base + os.sep)
warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe
import pandas as pd
sys.path.insert(0, f'{base}/lsms_library/countries/Ethiopia/_')
import ethiopia as E
src = textwrap.dedent(inspect.getsource(E.crop_production_for_wave))
old = """    empty_u = out['u'].isna()
    out = out[~(empty_u & out['Quantity'].isna()
                & out['Quantity_sold'].isna())]"""
new = """    empty_u = out['u'].isna()
    _drop = (empty_u & out['Quantity'].isna() & out['Quantity_sold'].isna()).to_numpy()
    _pos = (out['Value_sold'] > 0).fillna(False).to_numpy()
    _DROPS[t] = {'rows_dropped': int(_drop.sum()), 'dropped_with_Value_sold': int((_drop & out['Value_sold'].notna().to_numpy()).sum()),
                 'dropped_with_positive_Value_sold': int((_drop & _pos).sum()), 'birr': float(out.loc[_drop & _pos, 'Value_sold'].sum()),
                 'rows_with_positive_Value_sold_before_filter': int(_pos.sum())}
    out = out[~_drop]"""
assert src.count(old) == 1
ns = E.__dict__
ns['_DROPS'] = {}
exec(compile(src.replace(old, new), 'patched', 'exec'), ns)
D = f'{base}/lsms_library/countries/Ethiopia'
def rd(w, f, cat=False): return get_dataframe(f'{D}/{w}/Data/{f}', convert_categoricals=cat)
common = dict(holder_id='holder_id', parcel_id='parcel_id', field_id='field_id')
cfg = {
 '2011-12': (1, dict(hhid='household_id', crop_code='crop_code', quantity='ph_s9q12_a', harvest_month='ph_s9q13_b', ic_holder_id='holder_id', ic_parcel_id='parcel_id', ic_field_id='field_id', ic_flag='pp_s3q03', pl_crop_code='crop_code', pl_month='pp_s4q12_a', s_crop_code='crop_code', s_sold_flag='ph_s11q01', s_qty='ph_s11q03_a', s_value='ph_s11q04_a'), None, None),
 '2013-14': (2, dict(hhid='household_id2', crop_code='crop_code', quantity='ph_s9q04_a', unit='ph_s9q04_b', harvest_month='ph_s9q07_b', intercrop='ph_s9q01', pl_crop_code='crop_code', pl_month='pp_s4q12_a', s_crop_code='crop_code', s_sold_flag='ph_s11q01', s_qty='ph_s11q03_a', s_value='ph_s11q04'), 'ph_s9q04_b', None),
 '2015-16': (3, dict(hhid='household_id2', crop_code='crop_code', quantity='ph_s9q04_a', unit='ph_s9q04_b', harvest_month='ph_s9q07_b', intercrop='ph_s9q01', pl_crop_code='crop_code', pl_month='pp_s4q12_a', s_crop_code='crop_code', s_sold_flag='ph_s11q01', s_qty='ph_s11q03_a', s_value='ph_s11q04'), 'ph_s9q04_b', 'ph_s11q03_b'),
 '2018-19': (4, dict(hhid='household_id', crop_code='s9q00b', quantity='s9q05a', unit='s9q05b', harvest_month='s9q08b', intercrop='s9q02', pl_crop_code='s4q01b', pl_month='s4q13a', s_crop_code='s11q01', s_sold_flag='s11q07', s_qty='s11q11a', s_value='s11q12'), 's9q05b', 's11q11b'),
 '2021-22': (5, dict(hhid='household_id', crop_code='s9q00b', quantity='s9q05a', unit='s9q05b', harvest_month='s9q08b', intercrop='s9q02', pl_crop_code='s4q01b', pl_month='s4q13a', s_crop_code='s11q01b', s_sold_flag='s11q07', s_qty='s11q11a', s_value='s11q12'), 's9q05b', 's11q11b'),
}
for w, (n, cm, hu, su) in cfg.items():
    cm.update(common)
    h = rd(w, f'sect9_ph_w{n}.dta'); pl = rd(w, f'sect4_pp_w{n}.dta'); s = rd(w, f'sect11_ph_w{n}.dta')
    kw = {}
    if hu: kw['unit_labels'] = rd(w, f'sect9_ph_w{n}.dta', True)[hu]
    if su: kw['sale_unit_labels'] = rd(w, f'sect11_ph_w{n}.dta', True)[su]
    if n == 1: kw['intercrop'] = rd(w, 'sect3_pp_w1.dta')
    ns['crop_production_for_wave'](w, h, pl, s, cm, **kw)
print(json.dumps(ns['_DROPS'], indent=1))
json.dump(ns['_DROPS'], open(sys.argv[1], 'w'), indent=1)
