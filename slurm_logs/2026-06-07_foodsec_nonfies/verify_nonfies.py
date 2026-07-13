#!/usr/bin/env python
"""Parallel clean-cache verification of the non-FIES food-security features.

For each (country, feature) pair: build via diagnostics.load_feature with
LSMS_NO_CACHE=1 (caches already physically cleared by the wrapper), run
is_this_feature_sane, and print a one-line JSON result with rows, index
levels, the failed/warn checks, and report.ok.

Run AFTER `lsms-library cache clear --country <C>` for each country so the
script-path L2-wave parquets can't shadow a source fix.
"""
import os, sys, json
os.environ['LSMS_NO_CACHE'] = '1'
from concurrent.futures import ProcessPoolExecutor, as_completed

TARGETS = [
    ('India',       'months_food_inadequate'),
    ('Liberia',     'months_food_inadequate'),
    ('Timor-Leste', 'months_food_inadequate'),
    ('Uganda',      'months_food_inadequate'),
    ('Tajikistan',  'food_security_hfias'),
    ('Tanzania',    'food_coping'),
    ('Ethiopia',    'food_coping'),
    ('Nigeria',     'food_coping'),
]


def verify(pair):
    country, feature = pair
    os.environ['LSMS_NO_CACHE'] = '1'
    import warnings
    warnings.filterwarnings('ignore')
    out = {'country': country, 'feature': feature}
    try:
        import lsms_library as ll
        from lsms_library.diagnostics import load_feature, is_this_feature_sane
        df = load_feature(ll.Country(country), feature)
        out['rows'] = int(len(df))
        out['index'] = list(df.index.names)
        out['columns'] = list(df.columns)
        rep = is_this_feature_sane(df, country, feature)
        out['ok'] = bool(rep.ok)
        out['failed'] = [c.name for c in rep.checks if c.status == 'fail']
        out['warned'] = [c.name for c in rep.checks if c.status == 'warn']
        # a couple of head index tuples for eyeballing
        out['head_idx'] = [tuple(map(str, t)) for t in df.index[:2].tolist()]
    except Exception as e:
        import traceback
        out['ok'] = False
        out['error'] = f'{type(e).__name__}: {e}'
        out['trace'] = traceback.format_exc().splitlines()[-4:]
    return out


if __name__ == '__main__':
    results = []
    with ProcessPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(verify, t): t for t in TARGETS}
        for f in as_completed(futs):
            r = f.result()
            results.append(r)
            print(json.dumps(r), flush=True)
    print('=== SUMMARY ===', flush=True)
    for r in sorted(results, key=lambda x: (x['country'])):
        tag = 'OK ' if r.get('ok') else 'FAIL'
        extra = r.get('error', f"rows={r.get('rows')} idx={r.get('index')} "
                                f"fail={r.get('failed')} warn={r.get('warned')}")
        print(f"  [{tag}] {r['country']:<12} {r['feature']:<24} {extra}", flush=True)
