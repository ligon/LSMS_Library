#!/usr/bin/env python
"""Cold-cache verification of ALL food-security features on the merged dev tip.

Discovers (country, feature) pairs by scanning each country's
``_/data_scheme.yml`` for the four food-security feature names, so the
target list always matches the actual merged tree (no hand-transcription).

For each pair: build via diagnostics.load_feature with LSMS_NO_CACHE=1
(caches physically cleared by the driver first), run is_this_feature_sane,
and print a one-line JSON result.  Exits non-zero if any pair FAILs.

The only check we treat as a non-fatal WARN is the framework-joined-v
``index_levels_match_scheme`` warning (expected; v is joined at API time
and is not declared in feature data_scheme indexes).
"""
import os, sys, json, glob, re
os.environ['LSMS_NO_CACHE'] = '1'
from concurrent.futures import ProcessPoolExecutor, as_completed

FEATURES = ['food_security', 'food_coping',
            'months_food_inadequate', 'food_security_hfias']

COUNTRIES_ROOT = 'lsms_library/countries'


def discover_targets():
    """Return sorted [(country, feature)] for every declaration in the tree."""
    import yaml
    pairs = []
    for ds in glob.glob(f'{COUNTRIES_ROOT}/*/_/data_scheme.yml'):
        country = ds.split('/')[-3]
        try:
            with open(ds) as fh:
                txt = fh.read()
        except OSError:
            continue
        for feat in FEATURES:
            # data_scheme.yml declares each feature as an (indented) mapping
            # key: e.g. "  food_coping:".  Match a YAML key at any indent.
            if re.search(rf'^\s*{re.escape(feat)}:', txt, re.M):
                pairs.append((country, feat))
    return sorted(set(pairs))


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
        failed = [c.name for c in rep.checks if c.status == 'fail']
        warned = [c.name for c in rep.checks if c.status == 'warn']
        out['failed'] = failed
        out['warned'] = warned
        # report.ok already excludes the expected framework-v warn; treat
        # any genuine fail OR empty build as the failure signal.
        out['ok'] = bool(rep.ok) and out['rows'] > 0
    except Exception as e:
        import traceback
        out['ok'] = False
        out['error'] = f'{type(e).__name__}: {e}'
        out['trace'] = traceback.format_exc().splitlines()[-5:]
    return out


if __name__ == '__main__':
    workers = int(os.environ.get('VERIFY_WORKERS', '12'))
    targets = discover_targets()
    print(f'=== {len(targets)} (country, feature) pairs discovered; '
          f'workers={workers} ===', flush=True)
    for c, f in targets:
        print(f'  target: {c:<14} {f}', flush=True)

    results = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(verify, t): t for t in targets}
        for f in as_completed(futs):
            r = f.result()
            results.append(r)
            tag = 'OK' if r.get('ok') else 'FAIL'
            print(f'RESULT [{tag}] {json.dumps(r)}', flush=True)

    fails = [r for r in results if not r.get('ok')]
    print('=== SUMMARY ===', flush=True)
    for r in sorted(results, key=lambda x: (x['feature'], x['country'])):
        tag = 'OK ' if r.get('ok') else 'FAIL'
        extra = r.get('error', f"rows={r.get('rows')} "
                               f"fail={r.get('failed')} warn={r.get('warned')}")
        print(f'  [{tag}] {r["country"]:<14} {r["feature"]:<24} {extra}',
              flush=True)
    print(f'=== {len(results) - len(fails)}/{len(results)} OK, '
          f'{len(fails)} FAIL ===', flush=True)
    sys.exit(1 if fails else 0)
