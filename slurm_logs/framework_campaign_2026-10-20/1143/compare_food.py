"""Compare private before/after snapshots and emit aggregate evidence only."""
from __future__ import annotations

import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from lsms_library.local_tools import get_dataframe

private, out = map(Path, sys.argv[1:])
report = {}
for country in ['Senegal', 'Benin', 'CotedIvoire']:
    before = private / f'before-{country}' / 'comparison-snapshots'
    after = private / f'final-{country}' / 'comparison-snapshots'
    frames = {}
    for name in ['food_acquired', 'quantities', 'prices']:
        a = get_dataframe(str(before / f'{country}-{name}.parquet'))
        b = get_dataframe(str(after / f'{country}-{name}.parquet'))
        pd.testing.assert_index_equal(a.index, b.index, exact=True)
        pd.testing.assert_index_equal(a.columns, b.columns, exact=True)
        assert list(a.dtypes) == list(b.dtypes)
        if name == 'food_acquired':
            pd.testing.assert_frame_equal(a, b, check_exact=True)
            units = a.index.get_level_values('u').astype(str)
            labels = {}
            for label in units[units.str.contains(r'\d,\d', regex=True)].unique():
                labels[label] = a[units == label].groupby(level='t').size().to_dict()
            frames[name] = {'rows': len(a), 'exactly_unchanged': True,
                            'comma_label_rows_by_wave': labels}
            continue
        col = 'Quantity' if name == 'quantities' else 'Price'
        av = a[col].to_numpy(dtype=float, na_value=np.nan)
        bv = b[col].to_numpy(dtype=float, na_value=np.nan)
        changed = ~np.isclose(av, bv, rtol=1e-12, atol=0, equal_nan=True)
        waves = {}
        for wave in a.index.get_level_values('t').unique():
            mask = a.index.get_level_values('t') == wave
            if name == 'quantities':
                mask &= a.index.get_level_values('u') == 'kg'
            def total(v):
                return math.fsum(float(x) for x in v if np.isfinite(x))
            waves[str(wave)] = {
                'before_sum': total(av[mask]), 'after_sum': total(bv[mask]),
                'delta': total(bv[mask] - av[mask]),
                'changed_rows': int(changed[mask].sum()),
            }
        if name == 'prices':
            valid = changed & np.isfinite(av) & np.isfinite(bv) & (av != 0)
            ratios, counts = np.unique(np.round(bv[valid] / av[valid], 10), return_counts=True)
            ratios = dict(zip(map(str, ratios), map(int, counts)))
        else:
            native = a.index.get_level_values('u') != 'kg'
            assert not changed[native].any(), 'Native quantities unexpectedly changed'
            ratios = None
        frames[name] = {
            'rows': len(a), 'changed_rows': int(changed.sum()), 'by_wave': waves,
            'price_ratio_counts': ratios, 'index_columns_dtypes_unchanged': True,
        }
    report[country] = frames

ha = json.loads((private / 'hash-before.json').read_text(encoding='utf-8'))
hb = json.loads((private / 'hash-final.json').read_text(encoding='utf-8'))
assert ha['errors'] == hb['errors'] == {}
hash_report = {}
for tier in ['country', 'wave', 'tagged']:
    assert ha[tier].keys() == hb[tier].keys()
    changed = [key for key in ha[tier] if ha[tier][key] != hb[tier][key]]
    hash_report[tier] = {'total': len(ha[tier]), 'changed': len(changed), 'changed_cells': changed}
    if tier != 'tagged':
        assert all(key.startswith('GhanaLSS/') for key in changed), changed
    else:
        assert not changed
report['hash_reach'] = hash_report
out.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n', encoding='utf-8')
print(json.dumps(report, indent=2, sort_keys=True))
