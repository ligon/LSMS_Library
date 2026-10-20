"""Reproduce #1093 on one checkout in a private root; emit aggregates only.

Run from that checkout with matching PYTHONPATH/LSMS_COUNTRIES_ROOT, explicit
LSMS_DATA_DIR and --out.  L2 must start empty; only dvc-cache may be linked.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import warnings

import numpy as np
import pandas as pd
import lsms_library as ll
from lsms_library import transformations as T

args = argparse.ArgumentParser()
args.add_argument('--out', required=True)
ns = args.parse_args()
top = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
assert Path(ll.__file__).resolve().is_relative_to(top), ll.__file__
assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == top / 'lsms_library/countries'
root = Path(os.environ['LSMS_DATA_DIR']).resolve()
assert root != (Path.home() / '.local/share/lsms_library').resolve()
assert not list(root.glob('*/var/*.parquet')), 'Use a fresh private L2'
report = {'checkout': str(top), 'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
          'package': ll.__file__, 'data_root': str(root), 'country_hashes': {}, 'wave_hashes': {}}
# Actual complete hashes, not only registry fingerprints.
for name in ll.countries():
    c = ll.Country(name)
    for table in c.data_scheme:
        report['country_hashes'][f'{name}/{table}'] = c._table_cache_hash(table, c.waves)
        for wave in c.waves:
            report['wave_hashes'][f'{name}/{wave}/{table}'] = c[wave]._input_hash(table)
Path(ns.out).write_text(json.dumps(report, indent=2, sort_keys=True))


def fingerprint(df):
    flat = df.reset_index()
    flat = flat[sorted(flat.columns)].sort_values(sorted(flat.columns), kind='mergesort').reset_index(drop=True)
    return hashlib.sha256(pd.util.hash_pandas_object(flat.astype(str), index=False).to_numpy().tobytes()).hexdigest()


def by_wave(df, cols):
    return {str(w): {'rows': len(part), **{k: float(pd.to_numeric(part[k], errors='coerce').sum(min_count=1))
                                             for k in cols}}
            for w, part in df.groupby(level='t', dropna=False)}

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    c = ll.Country('Ethiopia')
    cp = c.crop_production()
    pi = c.plot_inputs()
    factor = T.harvest_kg_factors(cp)['kg_per_unit']
    valued = cp.copy()
    valued['_harvest_kg'] = pd.to_numeric(cp['Quantity'], errors='coerce') * factor
    out = T.median_price_valuation(valued, [], quantity_col='_harvest_kg')
    resolved = getattr(T, '_kg_factor_series_with_declared', T._kg_factor_series)(cp)
    sold = pd.to_numeric(cp['Quantity_sold'], errors='coerce')
    value = pd.to_numeric(cp['Value_sold'], errors='coerce')
    price = value / (sold * resolved)
    usable = price.notna() & np.isfinite(price) & (price > 0)
    u = pd.Series(T._level_or_column(cp, 'u')).astype(str)
    quintal = np.asarray(u.str.lower() == 'quintal')
    seed, nitrogen = T.seed_kg(pi), T.nitrogen_kg(pi)
report['ethiopia'] = {
    'crop_production': {'rows': len(cp), 'fingerprint': fingerprint(cp), 'declaration': cp.attrs.get('unit_kg')},
    'plot_inputs': {'rows': len(pi), 'fingerprint': fingerprint(pi), 'declaration': pi.attrs.get('unit_kg')},
    'positive_sold_quintal_rows': int((quintal & (sold > 0)).sum()),
    'price_pool_rows': int(usable.sum()), 'price_pool_quintal_rows': int((usable & quintal).sum()),
    'priced_rows': int(out['_unit_price'].notna().sum()),
    'valuation': by_wave(out, ['_unit_price', 'Value']),
    'seed': by_wave(seed, ['Seed_kg']), 'nitrogen': by_wave(nitrogen, ['Nitrogen_kg']),
    'warnings': dict(pd.Series([type(w.message).__name__ for w in caught]).value_counts().astype(int).items()),
}
Path(ns.out).write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
print('Completed aggregate probe', ns.out, flush=True)
