"""Warm-root checks for one side: Feature assembly and the Site Q strict lever.

usage: post.py <clone> <side> <frames_dir>
"""
import os
import re
import sys
import time
import traceback
import warnings

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env, dump, warn_summary  # noqa: E402

clone, side, frdir = sys.argv[1:4]
head = assert_env(clone)
import lsms_library as ll  # noqa: E402

out = {'head': head, 'side': side}

# 1. Feature assembly
t0 = time.time()
try:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        f = ll.Feature('crop_production')(['Niger', 'Mali', 'Uganda'])
    f.to_pickle(os.path.join(frdir, side, 'FEATURE__crop_production.pkl'))
    countries = f.index.get_level_values('country')
    out['feature'] = dict(
        rows=int(len(f)), index=[str(x) for x in f.index.names],
        cols=[str(x) for x in f.columns], secs=round(time.time() - t0, 1),
        rows_by_country={str(k): int(v) for k, v in countries.value_counts().items()},
        warnings=warn_summary(caught),
        attrs_keys=sorted(str(k) for k in f.attrs),
        derivations_attr=f.attrs.get('derivations'))
    if 'Quantity_sold_kg' in f.columns:
        kg = f['Quantity_sold_kg']
        out['feature']['kg_notna_by_country'] = {
            str(k): int(v) for k, v in kg.notna().groupby(countries).sum().items()}
        out['feature']['kg_dtype'] = str(kg.dtype)
except Exception as e:  # noqa: BLE001
    out['feature'] = dict(error=f'{type(e).__name__}: {str(e)[:400]}',
                          tb=traceback.format_exc()[-1500:])
dump(out, os.path.join(frdir, f'post_{side}.json'))
print('feature', {k: v for k, v in out['feature'].items() if k not in ('cols', 'tb', 'derivations_attr')}, flush=True)

# 2. Strict lever, warm, fresh Country each time
out['strict'] = {}
os.environ['LSMS_QUANTITY_STRICT'] = '1'
for country in ('Niger', 'Mali'):
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            df = ll.Country(country).crop_production()
        out['strict'][country] = dict(raised=False, rows=int(len(df)), warnings=warn_summary(caught))
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        cols = re.findall(r"report a '([A-Za-z_]+)'", msg)
        out['strict'][country] = dict(raised=True, type=type(e).__name__, columns_named=cols)
    print('strict', country, out['strict'][country], flush=True)
del os.environ['LSMS_QUANTITY_STRICT']

# 3. Default warm read: which Site Q warnings fire, per column (text redacted of ids)
out['siteq_warm'] = {}
for country in ('Niger', 'Mali'):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        ll.Country(country).crop_production()
    msgs = [str(w.message) for w in caught if w.category.__name__ == 'QuantityImplausibleWarning']
    per = {}
    redacted = []
    for m in msgs:
        col = (re.findall(r"report a '([A-Za-z_]+)'", m) or ['?'])[0]
        per[col] = per.get(col, 0) + 1
        if col == 'Quantity_sold_kg':
            r = re.sub(r'i=\S+', 'i=<redacted>', m)
            r = re.sub(r'plot=\S+', 'plot=<redacted>', r)
            r = re.sub(r'production_obs=\S+', 'production_obs=<redacted>', r)
            redacted.append(r[:1500])
    out['siteq_warm'][country] = dict(per_column=per, sold_kg_messages=redacted)
dump(out, os.path.join(frdir, f'post_{side}.json'))
print('POST DONE', side, flush=True)
