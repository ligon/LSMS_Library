"""Aggregate §12 attach report per wave (W1-W3) + W4/W5 §11 coverage verification.

usage: sect12_report.py <checkout> <out.json>   (reads raw .dta via get_dataframe; aggregates only)
"""
import os, sys, json, warnings
checkout, outp = sys.argv[1], sys.argv[2]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep)
from lsms_library.local_tools import get_dataframe, format_id
import pandas as pd, numpy as np
sys.path.insert(0, f'{checkout}/lsms_library/countries/Ethiopia/_')
import ethiopia as E
D = f'{checkout}/lsms_library/countries/Ethiopia'
def rd(w, f, cat=False):
    return get_dataframe(f'{D}/{w}/Data/{f}', convert_categoricals=cat)
reports = {}
orig = E._attach_sect12_sales
def spy(*a, **k):
    out, rep = orig(*a, **k)
    reports[rep['wave']] = rep
    return out, rep
E._attach_sect12_sales = spy
cfg = {
 '2011-12': dict(n=1, hh='household_id', q='ph_s9q12_a', unit=None, hm='ph_s9q13_b', ic=None, s_qty='ph_s11q03_a', s_val='ph_s11q04_a', su=None, su12=None),
 '2013-14': dict(n=2, hh='household_id2', q='ph_s9q04_a', unit='ph_s9q04_b', hm='ph_s9q07_b', ic='ph_s9q01', s_qty='ph_s11q03_a', s_val='ph_s11q04', su=None, su12=None),
 '2015-16': dict(n=3, hh='household_id2', q='ph_s9q04_a', unit='ph_s9q04_b', hm='ph_s9q07_b', ic='ph_s9q01', s_qty='ph_s11q03_a', s_val='ph_s11q04', su='ph_s11q03_b', su12='ph_s12q0b'),
}
warnings.simplefilter('ignore')
for w, k in cfg.items():
    n = k['n']
    h = rd(w, f'sect9_ph_w{n}.dta'); pl = rd(w, f'sect4_pp_w{n}.dta'); s = rd(w, f'sect11_ph_w{n}.dta'); s12 = rd(w, f'sect12_ph_w{n}.dta')
    hunit = rd(w, f'sect9_ph_w{n}.dta', True)[k['unit']] if k['unit'] else None
    sunit = rd(w, f'sect11_ph_w{n}.dta', True)[k['su']] if k['su'] else None
    sunit12 = rd(w, f'sect12_ph_w{n}.dta', True)[k['su12']] if k['su12'] else None
    ic = rd(w, 'sect3_pp_w1.dta') if n == 1 else None
    colmap = dict(hhid=k['hh'], holder_id='holder_id', parcel_id='parcel_id', field_id='field_id', crop_code='crop_code',
                  quantity=k['q'], harvest_month=k['hm'], pl_crop_code='crop_code', pl_month='pp_s4q12_a',
                  s_crop_code='crop_code', s_sold_flag='ph_s11q01', s_qty=k['s_qty'], s_value=k['s_val'],
                  s12_crop_code='crop_code', s12_sold_flag='ph_s12q06', s12_qty='ph_s12q07', s12_value='ph_s12q08')
    if k['unit']: colmap['unit'] = k['unit']
    if k['ic']: colmap['intercrop'] = k['ic']
    if n == 1:
        colmap.update(ic_holder_id='holder_id', ic_parcel_id='parcel_id', ic_field_id='field_id', ic_flag='pp_s3q03')
    E.crop_production_for_wave(w, h, pl, s, colmap, intercrop=ic, unit_labels=hunit, sale_unit_labels=sunit, sale12=s12, sale12_unit_labels=sunit12)
# W4/W5 verification
W = {'2011-12': 1, '2013-14': 2, '2015-16': 3, '2018-19': 4, '2021-22': 5}
cc9 = {1: 'crop_code', 2: 'crop_code', 3: 'crop_code', 4: 's9q00b', 5: 's9q00b'}
cc11 = {1: 'crop_code', 2: 'crop_code', 3: 'crop_code', 4: 's11q01', 5: 's11q01b'}
T, F = set(), set()
for w, n in list(W.items())[:3]:
    T |= set(pd.to_numeric(rd(w, f'sect12_ph_w{n}.dta').crop_code, errors='coerce').dropna().astype(int))
    F |= set(pd.to_numeric(rd(w, f'sect11_ph_w{n}.dta').crop_code, errors='coerce').dropna().astype(int))
only12 = T - F
ver = {'codes_in_W1W3_s12_never_W1W3_s11': sorted(int(x) for x in only12), 'share_of_s9_pairs_with_s11_record': {}}
for w, n in W.items():
    s9 = rd(w, f'sect9_ph_w{n}.dta'); s11 = rd(w, f'sect11_ph_w{n}.dta')
    k9 = pd.DataFrame({'h': s9.holder_id.apply(format_id).astype(str), 'c': pd.to_numeric(s9[cc9[n]], errors='coerce')}).dropna().drop_duplicates()
    k11 = pd.DataFrame({'h': s11.holder_id.apply(format_id).astype(str), 'c': pd.to_numeric(s11[cc11[n]], errors='coerce')}).dropna()
    hit = pd.MultiIndex.from_frame(k9).isin(pd.MultiIndex.from_frame(k11))
    grp = np.where(k9.c.isin(only12), 'only_ever_s12_codes', np.where(k9.c.isin(T), 'codes_in_both', 'field_only_codes'))
    ver['share_of_s9_pairs_with_s11_record'][w] = {g: {'pairs': int((grp == g).sum()), 'share': round(float(hit[grp == g].mean()), 3)} for g in ['field_only_codes', 'codes_in_both', 'only_ever_s12_codes']}
json.dump({'sect12_attach_report': reports, 'w4_w5_verification': ver}, open(outp, 'w'), indent=1)
print(json.dumps(ver['share_of_s9_pairs_with_s11_record'], indent=0))
